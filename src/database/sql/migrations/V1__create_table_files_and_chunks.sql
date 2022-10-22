create table files
(
    uuid   varchar primary key,
    path   varchar,
    sha256 varchar,
    size   number,
    chunks number,
    status varchar, -- upload status: PENDING, SUCCESS, ERROR
    unique (path)
);

create table chunks
(
    uuid         varchar primary key,
    file_uuid    varchar,
    idx          number,
    sha256       varchar, -- sum of the encrypted data, i.e. what is sent to the cloud
    offset       number,  -- offset of the chunk
    size         number,  -- size of the encrypted data, i.e. what is sent to the cloud
    payload_size number,  -- size of the encrypted payload
    status       varchar, -- upload status: PENDING, SUCCESS, ERROR
    unique (file_uuid, idx)
);

create table inodes
(
    id        integer primary key,
    parent_id number,
    file_uuid varchar,
    name      varchar
)