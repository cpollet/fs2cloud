select id, parent_id, file_uuid, name
from inodes
where parent_id = :parent_id
order by id