use std::mem::{self, MaybeUninit};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use rand::Rng;
use scope_guard::scope_guard;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpListener;
use tokio::task::JoinError;

mod peaker;
use peaker::Peaker;

const DATA_FILE: &str = "data.bin";
// Total size of the file.
const FILE_SIZE: u64 = 128 * 1024 * 1024 * 1024;
// Size of one random piece we process as result of a request.
const PROCESS_SIZE: u64 = 32 * 1024 * 1024;
// Size of one piece whose checksum we calculate concurrently.
const CHUNK_SIZE: u64 = 1024 * 1024;

// If we are in a delaying mode of operation, delay a request by this factor multiplied by the ratio
// of our capacity used (so if all capacity is used we delay up to this many milliseconds, if half
// capacity is used we delay half this amount, if double capacity is used we delay twice this).
const NOMINAL_DELAY_MILLIS: u64 = 100;

// We start delaying once we have reached this ratio of our nominal capacity.
const SAFE_LOAD_RATIO: f64 = 0.5;

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
    let recent_peak_load = (REQUESTS_IN_STARTUP.load(Ordering::SeqCst)
        + CONCURRENT_IO_TASKS.lock().unwrap().peak()) as f64
        / MAX_FAST_CONCURRENT_IO_TASKS as f64;

    REQUESTS_IN_STARTUP.fetch_add(1, Ordering::SeqCst);

    {
        let _guard = scope_guard!(|| {
            REQUESTS_IN_STARTUP.fetch_sub(1, Ordering::SeqCst);
        });

        if recent_peak_load > SAFE_LOAD_RATIO {
            let delay = (NOMINAL_DELAY_MILLIS as f64 * (recent_peak_load - SAFE_LOAD_RATIO)
                / (1.0 - SAFE_LOAD_RATIO)) as u64;
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }

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

        {
            let concurrent_tasks = {
                let mut guard = CONCURRENT_IO_TASKS.lock().unwrap();
                let new_value = guard.get() + 1;
                guard.set(new_value)
            };
            let _tasks_dec_guard = scope_guard!(|| {
                let mut guard = CONCURRENT_IO_TASKS.lock().unwrap();
                let new_value = guard.get() - 1;
                guard.set(new_value);
            });

            let extra_rounds = 2usize
                .pow(concurrent_tasks.saturating_sub(MAX_FAST_CONCURRENT_IO_TASKS) as u32)
                .min(MAX_IO_DIFFICULTY)
                - 1;

            file.read_exact(chunk.as_mut_slice()).await?;

            // If there are already too many concurrent I/O tasks, the I/O device starts
            // to slow down (simulated here by sleeping). We need to simulate because our
            // real I/O on the test system can easily handle far larger workloads.
            if extra_rounds > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(extra_rounds as u64)).await;
            }
        }

        checksum_tasks.push(schedule_checksum_task(chunk).await);
    }

    let mut checksum_total: u32 = 0;

    for task in checksum_tasks {
        checksum_total = checksum_total.wrapping_add(task.await?);
    }

    log_something().await;

    Ok(Response::new(Full::new(Bytes::from(
        checksum_total.to_string(),
    ))))
}

// Whenever we accept a request we suspect will perform I/O, we increment this counter
// and count it as "preloaded" for the purpose of determining I/O load levels. We use this
// to track requests that we predict will perform I/O, while they may still be in the delay-start
// phase of their lifecycle (i.e. they are "phantom load" we need to account for).
static REQUESTS_IN_STARTUP: AtomicUsize = AtomicUsize::new(0);

static CONCURRENT_IO_TASKS: Mutex<Peaker<100>> = Mutex::new(Peaker::new());

// If we start to accumulate more than this amount of tasks, processing starts to slow down rapidly.
const MAX_FAST_CONCURRENT_IO_TASKS: usize = 4;
const MAX_IO_DIFFICULTY: usize = 32;

async fn schedule_checksum_task(bytes: Vec<u8>) -> impl Future<Output = Result<u32, JoinError>> {
    tokio::task::spawn(async move {
        log_something().await;

        crc32fast::hash(&bytes)
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
