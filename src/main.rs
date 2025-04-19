use std::{env, path::PathBuf, process::exit};

use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use hashfile::dup_finder::{delete_not_found, export_dups, find_dups};
use sqlx::SqlitePool;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    // /// Optional name to operate on
    // name: Option<String>,


    // /// Sets a custom config file
    // #[arg(short, long, value_name = "FILE")]
    // config: Option<PathBuf>,

    // /// Turn debugging information on
    // #[arg(short, long, action = clap::ArgAction::Count)]
    // debug: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {

    DeDup {
        path: String
    },

    DeleteNotFound,
    
    ExportDups

    // /// does testing things
    // Test {
    //     /// lists test values
    //     #[arg(short, long)]
    //     list: bool,
    // },
}

#[tokio::main]
async fn main() -> Result<(),Box<dyn std::error::Error>> {
    // load environment variables from .env file
    dotenv().expect(".env file not found");
    //let args: Vec<String> = env::args().collect();
    //let path = args[1].as_str();
    // match hash_file(path) {
    //     Ok(hash) => println!("Hash: {:x}", hash),
    //     Err(e) => eprintln!("Error: {}", e),
    // }
    
    let cli = Cli::parse();
    let command = match cli.command {
        Some(command) => command,
        None => {
            eprintln!("No command provided");
            exit(1);
        }
    };

    let pool = SqlitePool::connect(&env::var("DATABASE_URL")?).await?;

    sqlx::migrate!("./migrations")
    .run(&pool)
    .await?;

    match command {
        Commands::DeDup { path } => {
            let path_buf = PathBuf::from(path);
            find_dups(&path_buf, &pool).await?;        
        }
        Commands::DeleteNotFound => {
            delete_not_found(&pool).await?;
        }
        Commands::ExportDups => {
            // Implement the export duplicates functionality here
            eprintln!("Exporting duplicates...");
            export_dups(&pool).await?;
        }
    }

    Ok(())
}
