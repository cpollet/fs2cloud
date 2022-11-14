select uuid, path, sha256, size, chunks, mode
from files
where uuid = :uuid