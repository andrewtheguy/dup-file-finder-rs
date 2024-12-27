use std::{env, path::PathBuf};
use dotenvy::dotenv;
use hashfile::dup_finder::find_dups;
use sqlx::SqlitePool;


#[tokio::main]
async fn main() -> Result<(),Box<dyn std::error::Error>> {
    // load environment variables from .env file
    dotenv().expect(".env file not found");
    let args: Vec<String> = env::args().collect();
    let path = args[1].as_str();
    // match hash_file(path) {
    //     Ok(hash) => println!("Hash: {:x}", hash),
    //     Err(e) => eprintln!("Error: {}", e),
    // }

    let pool = SqlitePool::connect(&env::var("DATABASE_URL")?).await?;

    sqlx::migrate!("./migrations")
    .run(&pool)
    .await?;

    let path_buf = PathBuf::from(path);
    find_dups(&path_buf, &pool).await?;


    Ok(())
}
