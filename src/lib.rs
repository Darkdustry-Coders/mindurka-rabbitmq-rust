use std::{collections::HashMap, io::Cursor, sync::Arc};

use futures_lite::StreamExt;
use lapin::{
    Channel, Connection, ConnectionProperties, Error,
    options::{
        BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    protocol::basic::AMQPProperties,
    types::FieldTable,
};
pub use mindurka_rabbitmq_rust_macro::*;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    sync::{broadcast, mpsc},
    task::{JoinHandle, JoinSet},
};
use tracing::{error, info, warn};

pub trait NetworkEvent: Clone + Send {
    const QUEUE: &str;
    const TTL: u64;
    const CONSUMER_SERVICE: &str;
}

pub struct Rabbitmq {
    pub connection: Arc<Connection>,
}

pub struct QueuePair<T, R> {
    pub broadcast: broadcast::Receiver<T>,
    pub mpsc: mpsc::Sender<R>,
}

impl<T: Clone, R: Clone> Clone for QueuePair<T, R> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.resubscribe(),
            mpsc: self.mpsc.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RabbitReceiveMetadata<T: NetworkEvent + DeserializeOwned> {
    pub data: T,
    pub metadata: AMQPProperties,
}

#[derive(Clone)]
pub struct RabbitSendMetadata<T: NetworkEvent + Serialize> {
    pub data: T,
    pub service: String,
}

pub struct QueueWithHandles<T, R>(pub QueuePair<T, R>, pub JoinHandle<()>, pub JoinHandle<()>);

impl Rabbitmq {
    pub async fn new(url: &str) -> Result<Self, Error> {
        let connection =
            Connection::connect(url, ConnectionProperties::default().enable_auto_recover()).await?;

        let connection = Arc::new(connection);
        Ok(Self { connection })
    }
}

impl Rabbitmq {
    // С этого момента мы входим в АД.
    pub async fn connect_different<
        T: NetworkEvent + DeserializeOwned + 'static,
        R: NetworkEvent + Serialize + 'static,
    >(
        &self,
        services: Vec<String>,
    ) -> Result<QueueWithHandles<RabbitReceiveMetadata<T>, RabbitSendMetadata<R>>, Error> {
        let (br_send, br_recv) = broadcast::channel(64);
        let (mpsc_send, mpsc_recv) = mpsc::channel(64);

        let poll_services = services.clone();
        let poll_connection = Arc::clone(&self.connection);

        let poll_handle = tokio::spawn(poll_from_services(br_send, poll_services, poll_connection));

        let push_services = services.clone();
        let push_connection = Arc::clone(&self.connection);

        let push_handle = tokio::spawn(push_to_services(push_connection, mpsc_recv, push_services));

        Ok(QueueWithHandles(
            QueuePair {
                broadcast: br_recv,
                mpsc: mpsc_send,
            },
            poll_handle,
            push_handle,
        ))
    }

    pub async fn connect_symmetrical<T: NetworkEvent + Serialize + DeserializeOwned + 'static>(
        &self,
        services: Vec<String>,
    ) -> Result<QueueWithHandles<RabbitReceiveMetadata<T>, RabbitSendMetadata<T>>, Error> {
        self.connect_different(services).await
    }
}

async fn poll_from_services<T: NetworkEvent + DeserializeOwned + 'static>(
    br_send: broadcast::Sender<RabbitReceiveMetadata<T>>,
    poll_services: Vec<String>,
    poll_connection: Arc<Connection>,
) {
    let mut join_set = JoinSet::new();
    for service in poll_services.iter() {
        let service_sender = br_send.clone();
        let service_connection = Arc::clone(&poll_connection);
        join_set.spawn(poll_from_service(
            service_connection,
            service_sender,
            service.clone(),
        ));
    }
    join_set.join_all().await;
}

async fn push_to_services<R: NetworkEvent + Serialize + 'static>(
    connection: Arc<Connection>,
    mut receiver: mpsc::Receiver<RabbitSendMetadata<R>>,
    services: Vec<String>,
) {
    let mut join_set = JoinSet::new();

    let mut senders: HashMap<String, mpsc::Sender<R>> = HashMap::new();

    let mut receivers: Vec<(String, mpsc::Receiver<R>)> = Vec::with_capacity(services.len());

    for service in services {
        let (sender, receiver) = mpsc::channel(64);

        senders.insert(service.clone(), sender);
        receivers.push((service, receiver));
    }

    join_set.spawn(async move {
        while let Some(metadata) = receiver.recv().await {
            let RabbitSendMetadata { data, service } = metadata;
            if let Some(service) = senders.get(&service) {
                let _ = service.send(data).await;
            }
        }
    });

    while let Some((service, service_receiver)) = receivers.pop() {
        let connection = Arc::clone(&connection);
        join_set.spawn(push_to_service(connection, service_receiver, service));
    }

    join_set.join_all().await;
}

async fn poll_from_service<T: NetworkEvent + DeserializeOwned + 'static>(
    connection: Arc<Connection>,
    sender: broadcast::Sender<RabbitReceiveMetadata<T>>,
    service: String,
) {
    let channel = match connection.create_channel().await {
        Ok(channel) => channel,
        Err(err) => {
            error!(
                "Failed to create channel for service {} with error: {}",
                service, err
            );
            return;
        }
    };

    match ensure_exchange(&channel, T::QUEUE, &service).await {
        Ok(_) => (),
        Err(err) => {
            warn!(
                "Error while creating exchange and queues for service {} (note: probably its properly configured via other service): {}",
                service, err
            );
        }
    };

    let mut consumer = match channel
        .basic_consume(
            format!("main.{}.{}", T::QUEUE, service).into(),
            T::CONSUMER_SERVICE.into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(consumer) => consumer,
        Err(err) => {
            error!(
                "Failed to create consumer for service {} with error: {}",
                service, err
            );
            return;
        }
    };

    while let Some(Ok(delivery)) = consumer.next().await {
        if delivery.properties.app_id() == &Some(T::CONSUMER_SERVICE.into()) {
            let _ = delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: true,
                })
                .await;
            continue;
        };

        let reader = Cursor::new(&delivery.data);

        match ciborium::from_reader(reader) {
            Ok(data) => {
                let meta = RabbitReceiveMetadata {
                    data,
                    metadata: delivery.properties,
                };
                match sender.send(meta) {
                    Ok(_) => {}
                    Err(_) => {
                        // All receivers closed, so we can stop consuming
                        let _ = channel.close(0, "".into()).await;
                        return;
                    }
                };
            }
            Err(err) => {
                error!(
                    "Failed to decerialize payload from service {} with following error: {}",
                    service, err
                );
                let _ = delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: false,
                    })
                    .await;
                continue;
            }
        }
    }
}

async fn push_to_service<R: NetworkEvent + Serialize + 'static>(
    connection: Arc<Connection>,
    mut receiver: mpsc::Receiver<R>,
    service: String,
) {
    let channel = match connection.create_channel().await {
        Ok(channel) => channel,
        Err(err) => {
            error!(
                "Failed to create channel for service {} with error: {}",
                service, err
            );
            return;
        }
    };

    match ensure_exchange(&channel, R::QUEUE, &service).await {
        Ok(_) => (),
        Err(err) => {
            warn!(
                "Error while creating exchange and queues for service {} (note: probably its properly configured via other service): {}",
                service, err
            );
        }
    };

    while let Some(data) = receiver.recv().await {
        let mut buf = Cursor::new(Vec::with_capacity(1024));
        match ciborium::into_writer(&data, &mut buf) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to encode data into cbor: {}", err);
                continue;
            }
        }
        match channel
            .basic_publish(
                format!("main.{}", R::QUEUE).into(),
                service.clone().into(),
                BasicPublishOptions::default(),
                buf.get_ref(),
                AMQPProperties::default()
                    .with_app_id(R::CONSUMER_SERVICE.into())
                    .with_content_encoding("text/cbor".into()),
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send with error: {}", err);
                continue;
            }
        }
    }
}

async fn ensure_exchange(
    channel: &Channel,
    exchange: &str,
    service: &str,
) -> Result<(), lapin::Error> {
    channel
        .exchange_declare(
            format!("main.{}", exchange).into(),
            lapin::ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                auto_delete: false,
                internal: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            format!("main.{}.{}", exchange, service).into(),
            QueueDeclareOptions {
                durable: true,
                exclusive: false,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            format!("main.{}.{}", exchange, service).into(),
            format!("main.{}", exchange).into(),
            service.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok(())
}

pub trait InjectQueues<T, R> {
    fn inject_with_handles(&mut self, queue_with_handles: QueueWithHandles<T, R>) {
        self.inject_pair(queue_with_handles.0);
    }

    fn inject_pair(&mut self, queue_pair: QueuePair<T, R>);
}
