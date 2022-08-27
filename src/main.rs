use color_eyre::eyre;
use moleculer::{
    config::{ConfigBuilder, Transporter},
    service::{Action, ActionBuilder, Context, Event, EventBuilder, Service},
    ServiceBroker,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    pretty_env_logger::init();
    color_eyre::install()?;

    let config = ConfigBuilder::default()
        .transporter(Transporter::nats("nats://localhost:4222"))
        .build();

    let emit_hi = EventBuilder::new("emitHi").add_callback(emit_hi).build();

    let broadcast_name = EventBuilder::new("broadcastName")
        .add_callback(broadcast_name)
        .build();
    let add_action = ActionBuilder::new("add").add_callback(add).build();
    let squared_sum = ActionBuilder::new("squaredSum")
        .add_callback(squared_sum)
        .build();
    let greeter_service = Service::new("rustGreeter")
        .add_event(emit_hi)
        .add_event(broadcast_name)
        .add_action(add_action)
        .add_action(squared_sum);

    let service_broker = ServiceBroker::new(config).add_service(greeter_service);
    service_broker.start().await;

    Ok(())
}

fn add(ctx: Context<Action>) -> Result<(), Box<dyn Error>> {
    println!("Recieved add action");
    let msg: AddMessage = serde_json::from_value(ctx.params.clone())?;
    let result = msg.a + msg.b;
    ctx.reply(result.into());
    Ok(())
}

fn emit_hi(ctx: Context<Event>) -> Result<(), Box<dyn Error>> {
    println!("Received emitHi in rust");
    ctx.broker.emit("test", serde_json::json!({}));

    Ok(())
}

fn broadcast_name(ctx: Context<Event>) -> Result<(), Box<dyn Error>> {
    let msg: PrintNameMessage = serde_json::from_value(ctx.params)?;
    println!("Received broadcastName in rust");
    ctx.broker
        .broadcast("testWithParam", serde_json::to_value(&msg)?);

    Ok(())
}

fn squared_sum(ctx: Context<Action>) -> Result<(), Box<dyn Error>> {
    let msg: SquaredSumMessage = serde_json::from_value(ctx.params.clone())?;
    let result = msg.arr.par_iter().map(|x| x * x).sum::<i64>();
    ctx.reply(result.into());
    Ok(())
}

#[derive(Deserialize, Serialize)]
struct PrintNameMessage {
    name: String,
}
#[derive(Debug, Deserialize, Serialize)]
struct AddMessage {
    a: i64,
    b: i64,
}
#[derive(Debug, Deserialize, Serialize)]
struct SquaredSumMessage {
    arr: Vec<i64>,
}
