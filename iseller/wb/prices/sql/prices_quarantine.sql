CREATE TABLE IF NOT EXISTS {{params.table}}
(
    nm_id           integer   not null   constraint {{ params.table }}_pk primary key,
    seller_id       integer,
    old_price       integer,
    new_price       integer,
    old_discount    integer,
    new_discount    integer,
    price_diff      integer,
    sync_time       integer
);

CREATE INDEX IF NOT EXISTS {{params.table}}_seller__index ON {{ params.table }} (seller_id);