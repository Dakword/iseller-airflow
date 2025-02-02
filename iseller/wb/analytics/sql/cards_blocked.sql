CREATE TABLE IF NOT EXISTS {{ params.table }}
(
    nm_id          integer   not null   constraint {{ params.table }}_pk primary key,
    seller_id      integer   not null,
    vendor_code    varchar   not null,
    brand          varchar,
    title          varchar,
    reason         varchar
);

CREATE INDEX IF NOT EXISTS {{params.table}}_seller__index ON {{ params.table }} (seller_id);
