use mapreduce::pb::{
    Heartbeat, Register, TaskResult, WorkerMsg, master_client::MasterClient, master_msg, worker_msg,
};
use std::time::Duration;
use tokio::fs::read;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, transport::Channel};
use tracing::{info, warn};

pub async fn read_file(path: &str) -> Result<Vec<u8>, std::io::Error> {
    read(path).await
}

pub async fn count_42(data: &[u8]) -> u32 {
    data.iter().filter(|&&c| c == b'*').count() as u32
}

pub async fn run(addr: String, id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr
    } else {
        format!("http://{addr}")
    };
    info!("Client connecting to {endpoint}");
    let channel: Channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut client = MasterClient::new(channel);
    let (tx, rx) = mpsc::channel::<WorkerMsg>(32);
    let outbound = ReceiverStream::new(rx);
    let response = client.work_stream(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();
    // 1) Register
    tx.send(WorkerMsg {
        msg: Some(worker_msg::Msg::Register(Register {
            worker_id: id.to_string(),
            slots: 1,
        })),
    })
    .await?;
    // 2) Heartbeat loop
    let tx_hb = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            let msg = WorkerMsg {
                msg: Some(worker_msg::Msg::Heartbeat(Heartbeat { free_slots: 1 })),
            };
            if tx_hb.send(msg).await.is_err() {
                break;
            }
        }
    });
    // 3) Read master messages
    while let Some(item) = inbound.next().await {
        match item {
            Ok(msg) => match msg.msg {
                Some(master_msg::Msg::Task(task)) => {
                    let payload = String::from_utf8_lossy(&task.payload);
                    info!("Task: task_id={} payload={}", task.task_id, payload);
                    let data = read_file(&payload).await?;
                    let count = count_42(&data).await;
                    // demo: сразу отвечаем "результатом"
                    let result = WorkerMsg {
                        msg: Some(worker_msg::Msg::Result(TaskResult {
                            task_id: task.task_id,
                            payload: count.to_string().into_bytes(),
                        })),
                    };
                    tx.send(result).await?;
                }
                Some(master_msg::Msg::Cancel(cancel)) => {
                    info!("Cancel: task_id={}", cancel.task_id);
                }
                None => {
                    warn!("MasterMsg without oneof");
                }
            },
            Err(status) => {
                warn!("Inbound stream error: {status}");
                break;
            }
        }
    }
    info!("Server closed stream");
    Ok(())
}
