use std::{
    collections::HashMap,
    ops::Sub,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use log::*;
use rdkafka::{
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance},
    producer::{BaseProducer, BaseRecord},
    ClientContext, Message, TopicPartitionList,
};
use rdkafka::{error::KafkaError, producer::Producer};
use rdkafka::{producer::ProducerContext, ClientConfig};
use thiserror::Error;
use tokio::time;

static DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
static DEFAULT_MIN_POLL_DURATION: Duration = Duration::from_millis(100);
static DEFAULT_POLL_DURATION: Duration = Duration::from_millis(1);

#[derive(Debug)]
pub enum LeaderEvent {
    LeaderAcquired(i32),
    LeaderRevoked(i32),
    LeaderFenced(i32),
}

pub struct NeliConfigParams {
    pub name: String,
    pub leader_topic: String,
    pub leader_group_id: String,
    pub poll_duration: Option<Duration>,
    pub min_poll_duration: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub kafka_config: HashMap<String, String>,
}

pub struct NeliConfig {
    pub name: String,
    pub leader_topic: String,
    pub leader_group_id: String,
    pub poll_duration: Duration,
    pub min_poll_duration: Duration,
    pub heartbeat_timeout: Duration,
    pub kafka_config: HashMap<String, String>,
}

impl From<NeliConfigParams> for NeliConfig {
    fn from(params: NeliConfigParams) -> Self {
        NeliConfig {
            name: params.name,
            leader_topic: params.leader_topic,
            leader_group_id: params.leader_group_id,
            poll_duration: params.poll_duration.unwrap_or(DEFAULT_POLL_DURATION),
            min_poll_duration: params
                .min_poll_duration
                .unwrap_or(DEFAULT_MIN_POLL_DURATION),
            heartbeat_timeout: params
                .heartbeat_timeout
                .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT),
            kafka_config: params.kafka_config,
        }
    }
}

pub struct Neli<F>
where
    F: FnMut(Vec<LeaderEvent>) + Send + Sync,
{
    config: NeliConfig,
    consumer: BaseConsumer<NeliConsumerContext<F>>,
    producer: BaseProducer<NeliProducerContext>,
    run_state: Arc<Mutex<NeliRunState>>,
    partitions_state: Arc<Mutex<NeliState<F>>>,
}

pub struct NeliRunState {
    deadline: Deadline,
    status: NeliStatus,
    partition_count: usize,
}
pub struct NeliState<F>
where
    F: FnMut(Vec<LeaderEvent>) + Send + Sync,
{
    partitions: Vec<NeliPartitionState>,
    barrier: F,
}

pub struct NeliPartitionState {
    is_assigned: bool,
    is_live: bool,
    last_received: Option<Instant>,
}

pub enum NeliStatus {
    Live,
    Closing,
    Closed,
}

pub struct NeliConsumerContext<F>
where
    F: FnMut(Vec<LeaderEvent>) + Send + Sync,
{
    partitions_state: Arc<Mutex<NeliState<F>>>,
}

impl<F> NeliConsumerContext<F>
where
    F: FnMut(Vec<LeaderEvent>) + Send + Sync,
{
    fn new(leader_state: Arc<Mutex<NeliState<F>>>) -> NeliConsumerContext<F> {
        NeliConsumerContext {
            partitions_state: leader_state,
        }
    }
    fn on_assign(&self, tpl: &TopicPartitionList) {
        let mut state = self.partitions_state.lock().unwrap();
        let events: Vec<LeaderEvent> = tpl
            .elements()
            .iter()
            .map(|x| LeaderEvent::LeaderAcquired(x.partition()))
            .collect();
        for item in tpl.elements() {
            let partition = &mut state.partitions[item.partition() as usize];
            partition.is_assigned = true;
            partition.is_live = true;
            partition.last_received = Some(Instant::now());
        }
        (state.barrier)(events);
    }
    fn on_revoke(&self) {
        let mut state = self.partitions_state.lock().unwrap();
        let events: Vec<LeaderEvent> = state
            .partitions
            .iter_mut()
            .enumerate()
            .filter_map(|(index, partition)| {
                if partition.is_assigned {
                    partition.is_assigned = false;
                    partition.is_live = false;
                    Some(LeaderEvent::LeaderRevoked(index as i32))
                } else {
                    None
                }
            })
            .collect();
        (state.barrier)(events);
    }
}

impl<F> ClientContext for NeliConsumerContext<F> where F: FnMut(Vec<LeaderEvent>) + Send + Sync {}
impl<F> ConsumerContext for NeliConsumerContext<F>
where
    F: FnMut(Vec<LeaderEvent>) + Send + Sync,
{
    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(tpl) => self.on_assign(tpl),
            Rebalance::Revoke => {}
            Rebalance::Error(_) => {}
        }
    }
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(_) => {}
            Rebalance::Revoke => self.on_revoke(),
            Rebalance::Error(_) => {}
        }
    }
}

#[derive(Clone)]
pub struct NeliProducerContext {
    name: String,
}
impl ProducerContext for NeliProducerContext {
    type DeliveryOpaque = usize;
    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(message) => {
                trace!(
                    "[{}] Delivered message successfully: (topic: {}, partition: {}, offset: {})",
                    self.name,
                    message.topic(),
                    message.partition(),
                    message.offset()
                );
            }
            Err((err, _)) => {
                warn!("[{}] Failed to deliver message: {}", self.name, err);
            }
        }
    }
}
impl ClientContext for NeliProducerContext {}

#[derive(Debug, Error)]
pub enum NeliInitError {
    #[error("Failed to create kafka consumer")]
    ConsumerCreateError(#[source] KafkaError),
    #[error("Failed to subscribe to kafka topic")]
    ConsumerSubscribeError(#[source] KafkaError),
    #[error("Failed to create kafka producer")]
    ProducerCreateError(#[source] KafkaError),
    #[error("Failed to fetch metadata for leader topic")]
    LeaderTopicMetadataFetchError(#[source] KafkaError),
    #[error("Failed to get partition count for leader topic")]
    LeaderTopicPartitionCountError,
    #[error("Leader topic '{0}' does not exist")]
    LeaderTopicDoesNotExist(String),
}

#[derive(Debug, Error)]
pub enum PulseError {
    #[error("Deadline not passed")]
    DeadlineNotPassed(Duration),
    #[error("Pulse requested on non-live neli instance")]
    NonLivePulse,
}

impl<F> Neli<F>
where
    F: FnMut(Vec<LeaderEvent>) + Send + Sync,
{
    pub fn new(config: NeliConfigParams, barrier: F) -> Result<Neli<F>, NeliInitError>
    where
        F: FnMut(Vec<LeaderEvent>) + Send + Sync,
    {
        let config = NeliConfig::from(config);
        let mut consumer_config = ClientConfig::new();
        for (key, value) in config.kafka_config.iter() {
            consumer_config.set(key.clone(), value.clone());
        }
        consumer_config.set("group.id", config.leader_group_id.clone());
        consumer_config.set("enable.auto.commit", "false");
        consumer_config.set(
            "session.timeout.ms",
            (config.heartbeat_timeout * 3).as_millis().to_string(),
        );

        let partitions_state = Arc::new(Mutex::new(NeliState {
            partitions: Vec::new(),
            barrier,
        }));

        let consumer_context = NeliConsumerContext::new(partitions_state.clone());
        let consumer = consumer_config
            .create_with_context::<NeliConsumerContext<F>, BaseConsumer<NeliConsumerContext<F>>>(
                consumer_context,
            )
            .map_err(|err| NeliInitError::ConsumerCreateError(err))?;
        consumer
            .subscribe(&[config.leader_topic.as_str()])
            .map_err(|err| NeliInitError::ConsumerSubscribeError(err))?;

        let mut producer_config = ClientConfig::new();
        for (key, value) in config.kafka_config.iter() {
            producer_config.set(key.clone(), value.clone());
        }
        producer_config.set(
            "delivery.timeout.ms",
            config.heartbeat_timeout.as_millis().to_string(),
        );
        producer_config.set("linger.ms", "0");

        let producer = producer_config
            .create_with_context(NeliProducerContext {
                name: config.name.clone(),
            })
            .map_err(|err| NeliInitError::ProducerCreateError(err))?;

        let meta = consumer
            .fetch_metadata(Some(config.leader_topic.as_str()), Duration::from_secs(10))
            .map_err(|err| NeliInitError::LeaderTopicMetadataFetchError(err))?;
        let partition_count = *meta
            .topics()
            .into_iter()
            .filter_map(|x| {
                if x.name() == config.leader_topic {
                    Some(x.partitions().len())
                } else {
                    None
                }
            })
            .collect::<Vec<usize>>()
            .first()
            .ok_or(NeliInitError::LeaderTopicPartitionCountError)?;

        if partition_count == 0 {
            return Err(NeliInitError::LeaderTopicDoesNotExist(
                config.leader_topic.clone(),
            ));
        } else {
            info!(
                "[{}] {} partitions found on leader topic",
                config.name, partition_count
            );
        }
        let mut partitions_state_value = partitions_state.lock().unwrap();
        partitions_state_value.partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions_state_value.partitions.push(NeliPartitionState {
                is_assigned: false,
                is_live: false,
                last_received: None,
            });
        }
        drop(partitions_state_value);

        let run_state = Arc::new(Mutex::new(NeliRunState {
            deadline: Deadline::new(config.min_poll_duration),
            status: NeliStatus::Live,
            partition_count,
        }));

        Ok(Neli {
            config,
            run_state,
            partitions_state,
            consumer,
            producer,
        })
    }

    pub async fn pulse(&self, timeout: Duration) -> Result<Option<Vec<bool>>, PulseError> {
        let start = Instant::now();
        loop {
            match self.membership_check() {
                Ok(state) => {
                    return Ok(Some(state));
                }
                Err(PulseError::DeadlineNotPassed(remaining_duration)) => {
                    debug!(
                        "[{}] Deadline time remaining: {:.2}",
                        self.config.name,
                        remaining_duration.as_secs_f32()
                    );
                    // Wait for deadline to pass
                    let elapsed_duration = Instant::now().sub(start);
                    let remaining_timeout_duration = timeout.checked_sub(elapsed_duration);
                    if let Some(remaining_timeout_duration) = remaining_timeout_duration {
                        if remaining_duration < remaining_timeout_duration {
                            debug!(
                                "[{}] Awaiting next poll deadline in {:.2} secs",
                                self.config.name,
                                remaining_duration.as_secs_f32()
                            );
                            time::sleep(remaining_duration).await;
                            continue;
                        } else {
                            time::sleep(remaining_timeout_duration).await;
                            debug!("[{}] Leader status check timed out", self.config.name);
                            return Ok(None);
                        }
                    }
                    return Ok(None);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }

    fn membership_check(&self) -> Result<Vec<bool>, PulseError> {
        let mut run_state = self.run_state.lock().unwrap();
        let partition_count = run_state.partition_count;
        if let NeliStatus::Live = run_state.status {
            if let Some(remaining_duration) = run_state.deadline.try_run(|| {
                let mut hearbeats_received: Vec<bool> = Vec::with_capacity(partition_count);
                for _ in 0..partition_count {
                    hearbeats_received.push(false);
                }
                debug!("[{}] Polling for heartbeats", self.config.name);
                let mut no_heartbeat_received = true;
                loop {
                    let message = self.consumer.poll(self.config.poll_duration);

                    if let Some(message) = message {
                        no_heartbeat_received = false;
                        match message {
                            Ok(message) => {
                                debug!(
                                    "[{}] Heartbeats received for partition {}",
                                    self.config.name,
                                    message.partition()
                                );
                                hearbeats_received[message.partition() as usize] = true;
                            }
                            Err(err) => {
                                warn!(
                                    "[{}] Error while polling for heartbeats: {:?}",
                                    self.config.name, err
                                );
                                break;
                            }
                        }
                    } else {
                        if no_heartbeat_received {
                            debug!("[{}] No heartbeats received", self.config.name,);
                        }
                        break;
                    }
                }

                let mut partitions_state = self.partitions_state.lock().unwrap();
                let mut events = Vec::new();
                let now = Instant::now();

                for (index, partition_state) in partitions_state.partitions.iter_mut().enumerate() {
                    if partition_state.is_assigned {
                        if hearbeats_received[index] {
                            partition_state.last_received = Some(Instant::now());
                            if !partition_state.is_live {
                                partition_state.is_live = true;
                                events.push(LeaderEvent::LeaderAcquired(index as i32));
                            }
                        } else if let (Some(last_received), true) =
                            (partition_state.last_received, partition_state.is_live)
                        {
                            if let Some(elasped_duration) =
                                now.checked_duration_since(last_received)
                            {
                                if elasped_duration > self.config.heartbeat_timeout {
                                    partition_state.is_live = false;
                                    events.push(LeaderEvent::LeaderFenced(index as i32));
                                }
                            }
                        }
                        match self.producer.send(
                            BaseRecord::with_opaque_to(self.config.leader_topic.as_str(), 0)
                                .partition(index as i32)
                                .key("")
                                .payload(""),
                        ) {
                            Ok(_) => {
                                debug!(
                                    "[{}] Heartbeat sent on partition {}",
                                    self.config.name, index
                                );
                            }
                            Err((err, _)) => {
                                debug!(
                                    "[{}] Failed to send heartbeat on partition {}: {:?}",
                                    self.config.name, index, err
                                );
                            }
                        }
                    }
                }
                (partitions_state.barrier)(events);
                self.producer.poll(Duration::from_secs(0));
            }) {
                Err(PulseError::DeadlineNotPassed(remaining_duration))
            } else {
                let partitions_state = self.partitions_state.lock().unwrap();
                Ok(partitions_state
                    .partitions
                    .iter()
                    .map(|x| x.is_live)
                    .collect())
            }
        } else {
            error!("[{}] Pulse requested on dead instance", self.config.name);
            Err(PulseError::NonLivePulse)
        }
    }

    pub fn close(&self) {
        self.producer.flush(Duration::from_secs(10));
        let mut run_state = self.run_state.lock().unwrap();
        run_state.status = NeliStatus::Closed;
    }
}

pub struct Deadline {
    last_run: Option<Instant>,
    interval: Duration,
}

impl Deadline {
    pub fn new(interval: Duration) -> Deadline {
        Deadline {
            last_run: None,
            interval,
        }
    }
    pub fn elasped(&self) -> Option<Duration> {
        self.last_run.map(|x| Instant::now() - x)
    }
    pub fn remaining(&self) -> Option<Duration> {
        self.elasped()
            .map_or(None, |x| self.interval.checked_sub(x))
    }
    pub fn try_run<F>(&mut self, mut task: F) -> Option<Duration>
    where
        F: FnMut(),
    {
        if let Some(remaining_duration) = self.remaining() {
            Some(remaining_duration)
        } else {
            task();
            self.last_run = Some(Instant::now());
            None
        }
    }
}
