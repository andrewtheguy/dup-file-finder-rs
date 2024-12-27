use walkdir::WalkDir;
use core::panic;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read};
use twox_hash::XxHash3_64;
use std::hash::Hasher;
use sea_query::{ColumnDef, Expr, Func, Iden, OnConflict, Order, Query, SqliteQueryBuilder, Table};
use sea_query_binder::SqlxBinder;
use sqlx::{Pool, Row};
use log::{debug, info};


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

async fn save_file_hash_assoc(file_row: &FileObjRow, existing_hash_id: i64, pool: &Pool<sqlx::Sqlite>)-> Result<(), Box<dyn std::error::Error>> {
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

    let row = sqlx::query_with(&sql, values).execute(pool).await?;

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

async fn file_exists(file_row: &FileObjRow, pool: &Pool<sqlx::Sqlite>) -> Result<bool, Box<dyn std::error::Error>> {
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

async fn run(path: &PathBuf, pool: &Pool<sqlx::Sqlite>) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = fs::metadata(&path)?;

    let filesize = metadata.len();

    // skip files with size 0
    if filesize == 0 {
        eprintln!("File size is 0, skipping");
        return Ok(());
    }

    let file_obj_row = get_file_obj_row(path);
    if file_exists(&file_obj_row, pool).await? {
        eprintln!("File already exists in the database with the same size and modification time, skipping");
        return Ok(());
    }

    //panic!("File does not exist in the database");


    let hash  = hash_file(&path)?;
    let hex_hash = format!("{:x}", hash);



    // let mut id: i64 = row.last_insert_rowid();
    // debug!("Insert into file hash: last_insert_id = {id}\n");
    // if id == 0 {
        //debug!("File already exists in the database");
    let (sql2, values2) = Query::select()
    .columns([FileHash::Id])
    .from(FileHash::Table)
    .and_where(Expr::col(FileHash::FileSize).eq(filesize).and(Expr::col(FileHash::Hash).eq(hex_hash.clone()))).build_sqlx(SqliteQueryBuilder);
    let row2 = sqlx::query_with(&sql2, values2).fetch_optional(pool).await?;
    
    let existing_hash_id = if row2.is_some(){
        let row2 = row2.unwrap();
        let existing_hash_id: i64 = row2.get(0);
        debug!("File hash already exists in the database: id = {existing_hash_id}\n");
        existing_hash_id
    }else{
        debug!("File hash does not exist in the database");
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
        .build_sqlx(SqliteQueryBuilder);

        let row =sqlx::query_with(&sql, values).execute(pool).await?;
        let existing_hash_id: i64 = row.last_insert_rowid();
        debug!("Insert into file: last_insert_id = {existing_hash_id}\n");
        existing_hash_id
    };
    save_file_hash_assoc(&file_obj_row,existing_hash_id,pool).await?;
    Ok(())
}

pub async fn find_dups(path: &PathBuf, pool: &Pool<sqlx::Sqlite>) -> Result<(), Box<dyn std::error::Error>> {

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
            let row = sqlx::query_with(&sql, values).execute(pool).await?;
            eprintln!("Deleted not found file: {:?}", path);
        }
    }
    Ok(())
}