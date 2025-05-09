use std::{fs, path::PathBuf};
use std::cmp::{max, min};
use clap::{Parser, Subcommand};
use log::debug;
//use dotenvy::dotenv;
use dup_file_finder::dup_finder::{delete_not_found, export_dups, find_dups, CONCURRENCY_LIMIT};
use sqlx::SqlitePool;
use serde::Deserialize;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::runtime;
use tokio::runtime::Handle;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    // /// Optional name to operate on
    // name: Option<String>,

    /// Sets a config file
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,

    // /// Sets a custom config file
    // #[arg(short, long, value_name = "FILE")]
    // config: Option<PathBuf>,

    // /// Turn debugging information on
    // #[arg(short, long, action = clap::ArgAction::Count)]
    // debug: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {

    FindDups,
    
    DeleteFilesNotFound,

    ExportResult

    // /// does testing things
    // Test {
    //     /// lists test values
    //     #[arg(short, long)]
    //     list: bool,
    // },
}

#[derive(Deserialize)]
struct Config {
    database_url: String,
    search_path: PathBuf,
    result_output_path: PathBuf,
}


fn main() -> Result<(),Box<dyn std::error::Error>> {

    use std::thread::available_parallelism;
    let default_parallelism_approx = available_parallelism().unwrap().get();
    //eprintln!("Available parallelism: {:?}", &default_parallelism_approx);
    //panic!("test panic");
    
    // load environment variables from .env file
    //dotenv().expect(".env file not found");
    env_logger::init();
    
    // limit the number of threads to a maximum of core count
    let max_threads = min(default_parallelism_approx, CONCURRENCY_LIMIT);

    let runtime = runtime::Builder::new_multi_thread()
        .worker_threads(max_threads)
        .enable_all()
        .build()?;
    
    let cli = Cli::parse();
    let command = cli.command;

    let config: Config = toml::from_str(
        &fs::read_to_string(cli.config)?
    )?;

    // // Get the current runtime handle
    // let handle = Handle::current();
    // 
    // // Get metrics about the runtime
    // let metrics = handle.metrics();
    // 
    // // Print the number of worker threads
    // debug!("Number of worker threads: {}", metrics.num_workers());
    // 
    //panic!("test panic");
    
    //let pool = SqlitePool::connect(config.database_url.as_str()).await?;
    let pool: SqlitePool = runtime.block_on(async {
        let pool = SqlitePoolOptions::new()
            //.min_connections(CONCURRENCY_LIMIT as u32)
            .max_connections(CONCURRENCY_LIMIT as u32 * 2)
            .connect(config.database_url.as_str())
            .await?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await?;
        Ok::<SqlitePool, Box<dyn std::error::Error>>(pool)
    })?;

    runtime.block_on(async {
        match command {
            Commands::FindDups => {
                let path_buf = config.search_path;
                find_dups(&path_buf, &pool).await?;
                eprintln!("Exporting duplicates...");
                export_dups(&pool,&config.result_output_path).await?;
            }
            Commands::DeleteFilesNotFound => {
                delete_not_found(&pool).await?;
                eprintln!("Exporting duplicates...");
                export_dups(&pool,&config.result_output_path).await?;
            }
            Commands::ExportResult => {
                // Implement the export duplicates functionality here
                eprintln!("Exporting duplicates...");
                export_dups(&pool,&config.result_output_path).await?;
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
