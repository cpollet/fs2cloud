update chunks
set
    sha256=:sha256,
    size=:size,
    status='DONE'
where uuid=:uuid