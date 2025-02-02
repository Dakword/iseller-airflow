CREATE TABLE IF NOT EXISTS {{params.table}}
(
    seller_id              integer,
    nm_id                  integer,
    vendor_code            varchar,
    chrt_id                integer,
    tech_size              varchar,
    price                  integer,
    discount               integer,
    sale_price             integer,
    club_discount          integer,
    club_price             integer,
    size_price_editable    boolean,
    sync_time              integer
);

CREATE UNIQUE INDEX IF NOT EXISTS {{params.table}}_prices_uindex
    ON {{params.table}} (nm_id, chrt_id);
