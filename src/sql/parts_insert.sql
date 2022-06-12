insert into parts (uuid, file_uuid, idx, sha256, size, payload_size, status)
values (:uuid, :file_uuid, :idx, :sha256, :size, :payload_size, 'PENDING')