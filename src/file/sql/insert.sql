insert into files (uuid, path, sha256, size, chunks, status, mode)
values (:uuid, :path, :sha256, :size, :chunks, 'PENDING', :mode)