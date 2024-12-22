-- Add migration script here
create table file (
    id integer primary key AUTOINCREMENT not null,
    file_path TEXT not null,
    hash_id int not null
);

CREATE UNIQUE INDEX file_hash_assoc_uniq ON file(file_path, hash_id);