-- Add migration script here
create table file_hash (
    id integer primary key AUTOINCREMENT not null,
    file_size int not null,
    hash TEXT not null
);

CREATE UNIQUE INDEX file_hash_uniq ON file_hash(file_size, hash);