select uuid, path, sha256, size
from files
where uuid = :uuid