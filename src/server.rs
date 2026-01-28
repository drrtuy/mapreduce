use std::{
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};

use tokio::sync::mpsc;
use tonic::{Request, Response, Streaming};
use tracing::{info, warn};

use mapreduce::pb::{
    MasterMsg, Task, WorkerMsg, master_msg,
    master_server::{Master, MasterServer},
    worker_msg,
};
use tonic::{Status, transport::Server};

use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};

#[derive(Default)]
struct MasterSvc;
type OutStream = Pin<Box<dyn Stream<Item = Result<MasterMsg, Status>> + Send + 'static>>;

static COUNT_42: AtomicUsize = AtomicUsize::new(0);

#[tonic::async_trait]
impl Master for MasterSvc {
    type WorkStreamStream = OutStream;

    async fn work_stream(
        &self,
        request: Request<Streaming<WorkerMsg>>,
    ) -> Result<Response<Self::WorkStreamStream>, Status> {
        info!("Worker connected: {:?}", request.remote_addr());
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<MasterMsg, Status>>(32);
        let files = vec![
            "/git/oscourse/README",
            "/git/misc/ya_algo/7_dynamic_prog/M_backpack.cpp",
            "/git/misc/ya_algo/7_dynamic_prog/G_coin_change_variants.cpp",
            "/git/misc/ya_algo/7_dynamic_prog/A_greedy_stock.cpp",
            "/git/misc/ya_algo/7_dynamic_prog/C_knapsack_simple.cpp",
        ];

        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(msg) => match msg.msg {
                        Some(worker_msg::Msg::Register(r)) => {
                            info!("Register: worker_id={} slots={}", r.worker_id, r.slots);
                            let worker_id = r.worker_id.parse::<usize>().unwrap();
                            let task_msg = MasterMsg {
                                msg: Some(master_msg::Msg::Task(Task {
                                    task_id: "task-1".to_string(),
                                    payload: files[worker_id].as_bytes().to_vec(),
                                })),
                            };
                            if let Err(_) = tx.send(Ok(task_msg)).await {
                                warn!("failed to send task: client disconnected");
                            }
                        }
                        Some(worker_msg::Msg::Heartbeat(h)) => {
                            info!("Heartbeat: free_slots={}", h.free_slots);
                        }
                        Some(worker_msg::Msg::Result(r)) => {
                            let payload = String::from_utf8_lossy(&r.payload);
                            COUNT_42.fetch_add(payload.parse().unwrap(), Ordering::SeqCst);
                            let count = COUNT_42.load(Ordering::SeqCst);
                            info!(
                                "Result: task_id={} payload={} count={}",
                                r.task_id, payload, count
                            );
                        }
                        None => {
                            warn!("WorkerMsg without oneof");
                        }
                    },
                    Err(status) => {
                        warn!("Inbound stream error: {status}");
                        break;
                    }
                }
            }
            info!("Worker disconnected (stream closed)");
        });
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WorkStreamStream
        ))
        // todo!()
    }
}

pub async fn run(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = addr.parse()?;
    info!("Server Listening on {}", addr);
    Server::builder()
        .add_service(MasterServer::new(MasterSvc::default()))
        .serve(addr)
        .await?;
    Ok(())
}
