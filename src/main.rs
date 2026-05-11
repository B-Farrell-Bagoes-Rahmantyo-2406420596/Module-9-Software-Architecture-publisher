use borsh::BorshSerialize;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};

#[derive(Debug, Clone, BorshSerialize)]
pub struct UserCreatedEventMessage {
    pub user_id: String,
    pub user_name: String,
}

pub struct UserCreatedHandler;

impl UserCreatedHandler {
    pub fn get_handler_action(&self) -> String {
        todo!()
    }
}

#[tokio::main]
async fn main() {
    let amqp_uri = "amqp://guest:guest@localhost:5672";

    match publish_messages(amqp_uri).await {
        Ok(_) => println!("All messages published successfully"),
        Err(e) => eprintln!("Error: {}", e),
    }
}

async fn publish_messages(amqp_uri: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create AMQP connection
    let connection = Connection::connect(amqp_uri, ConnectionProperties::default())
        .await?;

    // Create channel
    let channel = connection.create_channel().await?;

    // Declare queue
    let queue_name = "user_created";
    let queue = channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("Queue declared: {:?}", queue);

    // Messages to publish
    let messages = vec![
        UserCreatedEventMessage {
            user_id: "1".to_owned(),
            user_name: "2406420596-Amir".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "2".to_owned(),
            user_name: "2406420596-Budi".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "3".to_owned(),
            user_name: "2406420596-Cica".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "4".to_owned(),
            user_name: "2406420596-Dira".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "5".to_owned(),
            user_name: "2406420596-Emir".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "6".to_owned(),
            user_name: "2406420596-Farrell".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "7".to_owned(),
            user_name: "2406420596-Geral".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "8".to_owned(),
            user_name: "2406420596-Hana".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "9".to_owned(),
            user_name: "2406420596-Ica".to_owned(),
        },
        UserCreatedEventMessage {
            user_id: "10".to_owned(),
            user_name: "2406420596-Jokan".to_owned(),
        },
    ];

    // Publish each message
    for (index, message) in messages.iter().enumerate() {
        match publish_event(&channel, queue_name, message).await {
            Ok(_) => println!("Message {} published: {:?}", index + 1, message),
            Err(e) => eprintln!("Error publishing message {}: {}", index + 1, e),
        }
    }

    Ok(())
}

async fn publish_event(
    channel: &Channel,
    queue_name: &str,
    message: &UserCreatedEventMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    // Serialize message using Borsh
    let payload = message.try_to_vec()?;

    // Publish message
    channel
        .basic_publish(
            "",                           // exchange name (empty = default)
            queue_name,                   // routing key
            BasicPublishOptions::default(),
            &payload,                     // Use reference to payload
            BasicProperties::default(),
        )
        .await?
        .await?; // Wait for confirmation

    Ok(())
}