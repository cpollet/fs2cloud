select uuid, file_uuid, idx, sha256, offset, size, payload_size, status
from chunks
where file_uuid = (select file_uuid from chunks where uuid = :uuid)