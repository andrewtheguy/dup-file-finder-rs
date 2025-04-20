use std::{fs, path::PathBuf};

use clap::{Parser, Subcommand};
//use dotenvy::dotenv;
use dup_file_finder::dup_finder::{delete_not_found, export_dups, find_dups};
use sqlx::SqlitePool;
use serde::Deserialize;
use sqlx::sqlite::SqlitePoolOptions;

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


#[tokio::main]
async fn main() -> Result<(),Box<dyn std::error::Error>> {
    // load environment variables from .env file
    //dotenv().expect(".env file not found");
    env_logger::init();
    
    let cli = Cli::parse();
    let command = cli.command;

    let config: Config = toml::from_str(
        &fs::read_to_string(cli.config)?
    )?;

    //let pool = SqlitePool::connect(config.database_url.as_str()).await?;
    let pool: SqlitePool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(config.database_url.as_str())
        .await?;
    sqlx::migrate!("./migrations")
    .run(&pool)
    .await?;

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

    Ok(())
}
