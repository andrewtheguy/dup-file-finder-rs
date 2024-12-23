-- Add migration script here
CREATE VIEW file_view
AS 
   select file.id, file.file_path, file.hash_id, file_hash.file_size, file_hash.hash from file inner join file_hash on file.hash_id = file_hash.id;