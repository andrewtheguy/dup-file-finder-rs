use dotenvy::dotenv;
use walkdir::WalkDir;
use core::panic;
use std::path::PathBuf;
use std::{env, fs};
use std::fs::File;
use std::io::{self, BufReader, Read};
use twox_hash::XxHash3_64;
use std::hash::Hasher;
use sea_query::{ColumnDef, Expr, Func, Iden, OnConflict, Order, Query, SqliteQueryBuilder, Table};
use sea_query_binder::SqlxBinder;
use sqlx::{Pool, Row, SqlitePool};
use log::{debug, info};

fn has_ignore_dir(path: &PathBuf) -> bool {
    path.components().any(|component| {
        let dir = component.as_os_str();
         dir == ".git" || dir == "node_modules"
    })
}

fn hash_file(path: &PathBuf) -> io::Result<u64> {
    // Open the file
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    
    // Create a new XXHash64 hasher
    let mut hasher = XxHash3_64::default();

    // Buffer for reading the file in chunks
    let mut buffer = [0; 8192];

    // Read the file in chunks and update the hasher
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break; // End of file
        }
        hasher.write(&buffer[..bytes_read]);
    }

    // Return the hash value
    Ok(hasher.finish())
}

async fn save_file_hash_assoc(path: &PathBuf, pool: &Pool<sqlx::Sqlite>,existing_hash_id: u64)-> Result<(), Box<dyn std::error::Error>> {
    assert!(existing_hash_id > 0);
    let (sql, values) = Query::insert()
    .into_table(FileObj::Table)
    .on_conflict( OnConflict::columns([FileObj::FilePath])
        .value(FileObj::HashId, existing_hash_id)
        .to_owned())
    .columns([
        FileObj::FilePath,
        FileObj::HashId,
    ])
    .values_panic([
        path.to_str().unwrap().into(),
        existing_hash_id.into(),
    ])
    .build_sqlx(SqliteQueryBuilder);

    //panic!("sql: {}",sql);

    let row = sqlx::query_with(&sql, values).execute(pool).await?;
    let id: i64 = row.last_insert_rowid();
    debug!("Insert into file: last_insert_id = {id}\n");
    Ok(())
}

async fn run(path: &PathBuf, pool: &Pool<sqlx::Sqlite>) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = fs::metadata(&path)?;

    let filesize = metadata.len();

    // skip files with size 0
    if filesize == 0 {
        return Ok(());
    }

    let hash  = hash_file(&path)?;

    // Create
    let (sql, values) = Query::insert()
    .into_table(FileHash::Table)
    .on_conflict( OnConflict::columns([FileHash::FileSize, FileHash::Hash])
        .do_nothing()
        .to_owned(),)
    .columns([
        FileHash::FileSize,
        FileHash::Hash,
    ])
    .values_panic([
        filesize.into(),
        format!("{:x}", hash).into(),
        
    ])
    .build_sqlx(SqliteQueryBuilder);

    let row = sqlx::query_with(&sql, values).execute(pool).await?;
    let mut id: i64 = row.last_insert_rowid();
    debug!("Insert into file hash: last_insert_id = {id}\n");
    if id == 0 {
        //debug!("File already exists in the database");
        let (sql2, values2) = Query::select()
        .columns([FileHash::Id])
        .from(FileHash::Table)
        .and_where(Expr::col(FileHash::FileSize).eq(filesize).and(Expr::col(FileHash::Hash).eq(format!("{:x}", hash)))).build_sqlx(SqliteQueryBuilder);
        let row2 = sqlx::query_with(&sql2, values2).fetch_one(pool).await?;
        id = row2.get("id");
        debug!("File already exists in the database: id = {id}\n");
    }
    save_file_hash_assoc(path,pool,id as u64).await?;
    Ok(())
}

#[derive(Iden)]
enum FileHash {
    Table,
    Id,
    FileSize,
    Hash,
}


#[derive(Iden)]
enum FileObj {
    #[iden = "file"]
    Table,
    Id,
    FilePath,
    HashId,
}


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

    // // Read the directory
    // for entry in fs::read_dir(path)? {
    //     let entry = entry?; // Handle the Result
    //     let path = entry.path();
        
    //     if path.is_file() {
    //         println!("File: {:?}", path);
    //         run(&path,&pool).await?;
    //     } else if path.is_dir() {
    //         println!("Directory: {:?}", path);
    //     }
    // }

    // Create a WalkDir iterator
    for entry in WalkDir::new(path)
        .follow_links(false) // Do not follow symbolic links
        .into_iter()
        .filter_map(Result::ok) // Filter out errors
    {
        let path = entry.path();
        let path_buf = &path.to_path_buf();

        // Skip the .git directory
        if has_ignore_dir(path_buf) {
            continue;
        }

        if path.is_file() {
            eprintln!("File: {:?}", path);
            run(path_buf,&pool).await?;
        } else if path.is_dir() {
            info!("Directory: {:?}", path);
        }
    }

    Ok(())
}
