insert into files (uuid, path, sha256, size, chunks, status)
values (:uuid, :path, :sha256, :size, :chunks, 'PENDING')