update chunks set file_uuid=:file_uuid, idx=:idx, sha256=:sha256, offset=:offset, size=:size, payload_size=:payload_size, status=:status
where uuid=:uuid