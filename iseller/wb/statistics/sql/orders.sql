CREATE TABLE IF NOT EXISTS {{params.table}}
(
    g_number            varchar     not null     constraint orders_pk primary key,
    seller_id           integer,
    srid                varchar     not null,
    nm_id               integer     not null,
    order_type          varchar,
    date                timestamp   not null,
    last_change_date    timestamp   not null,
    warehouse_type      varchar,
    warehouse_name      varchar,
    vendor_code         varchar,
    barcode             varchar,
    tech_size           varchar,
    category            varchar,
    subject             varchar,
    brand               varchar,
    income_id           integer,
    is_supply           boolean     default false   not null,
    is_realization      boolean     default false   not null,
    is_cancel           boolean     default false   not null,
    cancel_date         date,
    sticker             varchar,
    country             varchar,
    okrug               varchar,
    region              varchar,
    price               integer,
    discount            integer,
    sale_price          decimal,
    spp                 integer,
    retail_price        decimal
);

CREATE INDEX IF NOT EXISTS {{params.table}}_changedate__index
    ON {{params.table}} (last_change_date desc);

CREATE INDEX IF NOT EXISTS {{params.table}}_seller__index
    ON {{params.table}} (seller_id);
