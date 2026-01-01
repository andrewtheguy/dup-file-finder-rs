use csv::Writer;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read};
use twox_hash::XxHash3_64;
use std::hash::Hasher;
use sea_query::{Expr, Iden, OnConflict, Query, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use sqlx::{Column, Pool, Row};
use log::{debug, info};
use anyhow::{Result, Error, bail};
use jwalk::WalkDir;
use tokio::sync::watch;
use tokio::task::{JoinSet};

pub const CONCURRENCY_LIMIT: usize = 10;

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
    FileSize,
    FileModificationTime,
    HashId,
}

// just for convenience to pass data around
// no id just yet
struct FileObjRow {
    file_path: PathBuf,
    file_size: u64,
    file_modification_time: u64,
}

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

async fn save_file_hash_assoc(file_row: &FileObjRow, existing_hash_id: i64, pool: &Pool<sqlx::Sqlite>)-> Result<()> {
    assert!(existing_hash_id > 0);
    let (sql, values) = Query::insert()
    .into_table(FileObj::Table)
    .on_conflict( OnConflict::columns([FileObj::FilePath])
        .value(FileObj::HashId, existing_hash_id)
        .value(FileObj::FileSize, file_row.file_size)
        .value(FileObj::FileModificationTime, file_row.file_modification_time)
        .to_owned())
    .columns([
        FileObj::FilePath,
        FileObj::HashId,
        FileObj::FileSize,
        FileObj::FileModificationTime,
    ])
    .values_panic([
        file_row.file_path.to_str().unwrap().into(),
        existing_hash_id.into(),
        file_row.file_size.into(),
        file_row.file_modification_time.into(),
    ])
    .build_sqlx(SqliteQueryBuilder);

    //panic!("sql: {}",sql);

    sqlx::query_with(&sql, values).execute(pool).await?;

    Ok(())
}

fn get_file_obj_row(path: &PathBuf) -> FileObjRow {
    let metadata = fs::metadata(&path).unwrap();
    let filesize = metadata.len();
    let file_modification_time = metadata.modified().unwrap().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();


    FileObjRow {
        file_path: path.clone(),
        file_size: filesize,
        file_modification_time: file_modification_time,
    }
}

async fn file_exists(file_row: &FileObjRow, pool: &Pool<sqlx::Sqlite>) -> Result<bool> {
    let (sql, values) = Query::select()
    .columns([FileObj::Id])
    .from(FileObj::Table)
    .and_where(Expr::col(FileObj::FilePath).eq(file_row.file_path.to_str().unwrap()).
    and(Expr::col(FileObj::FileSize).eq(file_row.file_size).
    and(Expr::col(FileObj::FileModificationTime).eq(file_row.file_modification_time)))).build_sqlx(SqliteQueryBuilder);
    //eprintln!("sql: {}",sql);
    //eprintln!("values: {:?}",values);
    let row = sqlx::query_with(&sql, values).fetch_optional(pool).await?;
    //eprintln!("row: {:?}",row.is_some());
    Ok(row.is_some())
}

pub async fn export_dups(pool: &Pool<sqlx::Sqlite>,result_output_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let sql = r#"
with dup as (select file.hash_id, count(id) as hash_count from file group by 1 having count(id) > 1) select file.*,file_hash.file_size,hash_count from file INNER join dup on file.hash_id = dup.hash_id inner join file_hash on file.hash_id=file_hash.id order by file_path asc
"#;
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    if rows.is_empty() {
        eprintln!("No rows returned from query.");
        return Ok(());
    }

    // Dynamically extract column names from the first row
    let column_names: Vec<&str> = rows[0].columns().iter().map(|col| col.name()).collect();

    let file = File::create(result_output_path)?;
    let mut wtr = Writer::from_writer(file);

    // Write dynamic headers
    wtr.write_record(&column_names)?;
    // Write row values
    for row in &rows {
        let mut record = Vec::new();
        for col in &column_names {
            // Convert each value to string, handling NULLs
            let value: String = row.try_get_unchecked(*col)?;
            record.push(value);
        }
        wtr.write_record(record)?;
    }

    wtr.flush()?;
    eprintln!("Data exported to {} with dynamic headers.", result_output_path.as_path().display());

    Ok(())
}

async fn run(path: &PathBuf, pool: &Pool<sqlx::Sqlite>) -> Result<()> {
    // Gather metadata without blocking the async runtime.
    let path_for_metadata = path.clone();
    let file_obj_row = tokio::task::spawn_blocking(move || get_file_obj_row(&path_for_metadata))
        .await??;

    // skip files with size 0
    if file_obj_row.file_size == 0 {
        eprintln!("File size is 0, skipping");
        return Ok(());
    }
    if file_exists(&file_obj_row, pool).await? {
        eprintln!("File already exists in the database with the same size and modification time, skipping");
        return Ok(());
    }

    //panic!("File does not exist in the database");


    let path_for_hash = file_obj_row.file_path.clone();
    let hash  = tokio::task::spawn_blocking(move || hash_file(&path_for_hash))
        .await??;
    let hex_hash = format!("{:x}", hash);



        // Create
        let (sql, values) = Query::insert()
        .into_table(FileHash::Table)
        .columns([
            FileHash::FileSize,
            FileHash::Hash,
        ])
        .values_panic([
            filesize.into(),
            hex_hash.clone().into(),
            
        ])
            .on_conflict(OnConflict::columns([FileHash::FileSize,FileHash::Hash]).value(FileHash::FileSize,filesize).to_owned()).returning(crate::dup_finder::
            Query::returning().columns([FileHash::Id]))
        .build_sqlx(SqliteQueryBuilder);

        let row =sqlx::query_with(&sql, values).fetch_one(pool).await?;
        let existing_hash_id: i64 = row.try_get(0)?;
        debug!("Insert into file: last_insert_id = {existing_hash_id}\n");


    save_file_hash_assoc(&file_obj_row,existing_hash_id,pool).await?;
    Ok(())
}

pub async fn find_dups(path: &PathBuf, pool: &Pool<sqlx::Sqlite>) -> Result<()> {

    let max_concurrent_tasks = CONCURRENCY_LIMIT;

    // Used to signal cancellation
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut join_set = JoinSet::new();
    let mut in_flight = 0usize;

    // Create a WalkDir iterator
    for entry in WalkDir::new(path)
        .follow_links(false) // Do not follow symbolic links
        .into_iter()
        .filter_map(Result::ok) // Filter out errors
    {
        //eprintln!("cancel_rx: {:?}", cancel_rx.borrow());
        // Stop loop if cancellation signal received
        if *cancel_rx.borrow() {
            break;
        }
        //sleep(Duration::from_secs(1)).await;
        let path = entry.path();
        let path_buf = &path.to_path_buf();

        // Skip the .git directory
        if has_ignore_dir(path_buf) {
            continue;
        }

        if path.is_file() {
            debug!("File: {:?}", path);
            let path_buf = path_buf.clone();
            let pool = pool.clone();
            let cancel_tx = cancel_tx.clone();
            let cancel_rx = cancel_rx.clone();
            join_set.spawn(async move {
                if *cancel_rx.borrow() {
                    debug!("Cancellation signal received, skipping task.");
                    drop(pool);
                    return Ok(());
                }
                debug!("doing work for: {:?}", path_buf);

                let result = run(&path_buf,&pool).await;

                if result.is_err() {
                    eprintln!("Error processing file {:?}: {:?}",path_buf, result);
                    // Signal cancellation on failure
                    let _ = cancel_tx.send(true);
                }
                drop(pool);
                result.map(|_| ())
            });
            in_flight += 1;
            if in_flight >= max_concurrent_tasks {
                if let Some(result) = join_set.join_next().await {
                    in_flight -= 1;
                    if let Err(join_err) = result {
                        let _ = cancel_tx.send(true);
                        return Err(Error::new(join_err));
                    }
                }
            }
        } else if path.is_dir() {
            info!("Directory: {:?}", path);
        }
        //sleep(Duration::from_secs(5)).await;
    }

    if *cancel_rx.borrow() {
        return Err(Error::msg("Cancellation signal received because error occurred, quitting."));
    }

    while in_flight > 0 {
        if let Some(result) = join_set.join_next().await {
            in_flight -= 1;
            if let Err(join_err) = result {
                return Err(Error::new(join_err));
            }
        }
    }

    Ok(())
}

pub async fn delete_not_found(pool: &Pool<sqlx::Sqlite>) -> Result<(), Box<dyn std::error::Error>> {
    let (sql, values) = Query::select()
    .columns([FileObj::Id, FileObj::FilePath])
    .from(FileObj::Table)
    .build_sqlx(SqliteQueryBuilder);
    let rows = sqlx::query_with(&sql, values).fetch_all(pool).await?;
    for row in rows {
        let id: i64 = row.get(0);
        let file_path: String = row.get(1);
        let path = Path::new(&file_path);
        if !path.exists() {
            let (sql, values) = Query::delete()
            .from_table(FileObj::Table)
            .and_where(Expr::col(FileObj::Id).eq(id)).build_sqlx(SqliteQueryBuilder);
            sqlx::query_with(&sql, values).execute(pool).await?;
            eprintln!("Deleted not found entry from db: {:?}", path);
        }
    }
    Ok(())
}
