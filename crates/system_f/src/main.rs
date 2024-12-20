use std::mem::{self, MaybeUninit};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use rand::Rng;
use sha2::{Digest, Sha512};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpListener;
use tokio::task::JoinError;

const DATA_FILE: &str = "data.bin";
// Total size of the file.
const FILE_SIZE: u64 = 128 * 1024 * 1024 * 1024;
// Size of one random piece we process as result of a request.
const PROCESS_SIZE: u64 = 32 * 1024 * 1024;
// Size of one piece whose checksum we calculate concurrently.
const CHUNK_SIZE: u64 = 1 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    {
        let file = File::create(DATA_FILE).await?;
        file.set_len(FILE_SIZE).await?;
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;

        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(_err) = http1::Builder::new()
                .serve_connection(io, service_fn(hello))
                .await
            {
                //eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}

async fn hello(
    _: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let offset_in_file = choose_offset_in_file().await;

    get_auth_token().await;

    let mut file = tokio::fs::File::open(DATA_FILE).await?;
    let chunk_count = PROCESS_SIZE / CHUNK_SIZE;
    let chunks = 0..chunk_count;
    let mut checksum_tasks = Vec::with_capacity(chunk_count as usize);

    log_something().await;

    for chunk_index in chunks {
        let mut chunk: Vec<MaybeUninit<u8>> = Vec::with_capacity(CHUNK_SIZE as usize);
        unsafe {
            chunk.set_len(CHUNK_SIZE as usize);
        }
        chunk.fill(MaybeUninit::new(0));

        let mut chunk: Vec<u8> = unsafe { mem::transmute(chunk) };

        let chunk_offset = offset_in_file + chunk_index * CHUNK_SIZE;
        file.seek(std::io::SeekFrom::Start(chunk_offset)).await?;
        file.read_exact(chunk.as_mut_slice()).await?;

        checksum_tasks.push(schedule_checksum_task(chunk).await);
    }

    let mut checksum_total: u64 = 0;

    for task in checksum_tasks {
        checksum_total = checksum_total.wrapping_add(task.await?);
    }

    log_something().await;

    Ok(Response::new(Full::new(Bytes::from(
        checksum_total.to_string(),
    ))))
}

static CONCURRENT_CHECKSUM_TASKS: AtomicUsize = AtomicUsize::new(0);
// If we start to accumulate more than this amount of tasks, processing starts to slow down rapidly.
const MAX_FAST_CONCURRENT_CHECKSUM_TASKS: usize = 50;
const MAX_CHECKSUM_DIFFICULTY: usize = 32;

async fn schedule_checksum_task(bytes: Vec<u8>) -> impl Future<Output = Result<u64, JoinError>> {
    tokio::task::spawn(async move {
        log_something().await;

        let tasks = CONCURRENT_CHECKSUM_TASKS.fetch_add(1, Ordering::SeqCst);

        let rounds = 2usize
            .pow(tasks.saturating_sub(MAX_FAST_CONCURRENT_CHECKSUM_TASKS) as u32 / 3)
            .min(MAX_CHECKSUM_DIFFICULTY);

        let mut hasher = Sha512::new();

        for _ in 0..rounds {
            hasher.update(&bytes);
        }

        let result = hasher.finalize();

        CONCURRENT_CHECKSUM_TASKS.fetch_sub(1, Ordering::SeqCst);

        u64::from_be_bytes(result[0..8].try_into().unwrap())
    })
}

async fn choose_offset_in_file() -> u64 {
    // We spawn a task to simulate some more realism (e.g. maybe the offset comes from some
    // config file or gets deserialized from a request or is received from a work queue).
    tokio::task::spawn(async move {
        log_something().await;
        rand::thread_rng().gen_range(0..FILE_SIZE - PROCESS_SIZE)
    })
    .await
    .unwrap()
}

async fn log_something() {
    // Just some fake logging for extra realism.
    tokio::task::yield_now().await;
}

async fn get_auth_token() {
    // Simulate talking to some imaginary web service.

    tokio::task::spawn(async move {
        log_something().await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    })
    .await
    .unwrap()
}
