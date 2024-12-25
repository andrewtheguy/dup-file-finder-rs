-- Add migration script here
create table file (
    id integer primary key AUTOINCREMENT not null,
    file_path TEXT not null,
    file_size 'UNSIGNED BIG INT' not null,
    file_modification_time 'UNSIGNED BIG INT' not null,
    hash_id int not null
);

CREATE UNIQUE INDEX file_path_uniq ON file(file_path);