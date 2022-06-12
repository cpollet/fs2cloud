create table files
(
    uuid   varchar primary key,
    path   varchar,
    sha256 varchar,
    size   number,
    status varchar -- upload status: PENDING, SUCCESS, ERROR
)