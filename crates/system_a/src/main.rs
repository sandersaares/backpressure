use std::mem::{self, MaybeUninit};
use std::net::SocketAddr;

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
const PROCESS_SIZE: u64 = 100 * 1024 * 1024;
// Size of one piece whose checksum we calculate concurrently.
const CHUNK_SIZE: u64 = 10 * 1024 * 1024;

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
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(hello))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}

async fn hello(
    _: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let offset_in_file = rand::thread_rng().gen_range(0..FILE_SIZE - PROCESS_SIZE);

    let mut file = tokio::fs::File::open(DATA_FILE).await?;
    let chunk_count = PROCESS_SIZE / CHUNK_SIZE;
    let chunks = 0..chunk_count;
    let mut checksum_tasks = Vec::with_capacity(chunk_count as usize);

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

    Ok(Response::new(Full::new(Bytes::from(
        checksum_total.to_string(),
    ))))
}

async fn schedule_checksum_task(bytes: Vec<u8>) -> impl Future<Output = Result<u64, JoinError>> {
    tokio::task::spawn(async move {
        let mut hasher = Sha512::new();
        hasher.update(&bytes);
        let result = hasher.finalize();

        u64::from_be_bytes(result[0..8].try_into().unwrap())
    })
}
