insert into chunks (uuid, file_uuid, idx, sha256, offset, size, payload_size, status)
values (:uuid, :file_uuid, :idx, :sha256, :offset, :size, :payload_size, 'PENDING')