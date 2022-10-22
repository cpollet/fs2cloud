select uuid, file_uuid, idx, sha256, offset, size, payload_size, status
from chunks
where file_uuid = :file_uuid
order by idx