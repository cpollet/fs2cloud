create table parts
(
    uuid      varchar primary key,
    file_uuid varchar,
    idx       number,
    sha256    varchar,
    size      number
);

alter table files add parts_count number default 0;