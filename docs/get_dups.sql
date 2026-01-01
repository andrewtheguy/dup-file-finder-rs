with dup as (
    select
        file.hash_id,
        count(file.id) as hash_count
    from file
    group by file.hash_id
    having count(file.id) > 1
)
select
    file.*,
    file_hash.file_size,
    dup.hash_count
from file
inner join dup on file.hash_id = dup.hash_id
inner join file_hash on file.hash_id = file_hash.id
order by file_path asc;
