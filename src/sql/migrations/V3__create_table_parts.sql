create table parts
(
    uuid         varchar primary key,
    file_uuid    varchar,
    idx          number,
    sha256       varchar, -- sum of the encrypted data, i.e. what is sent to the cloud
    size         number,  -- size of the encrypted data, i.e. what is sent to the cloud
    payload_size number   -- size of the encrypted payload
);

alter table files
    add parts_count number default 0;