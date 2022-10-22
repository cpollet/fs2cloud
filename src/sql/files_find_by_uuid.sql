select uuid, path, sha256, size, chunks
from files
where uuid = :uuid