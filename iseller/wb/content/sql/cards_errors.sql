CREATE TABLE IF NOT EXISTS {{ params.table }}
(
    seller_id      integer     not null,
    vendor_code    varchar     not null,
    object_id      integer     not null,
    object_name    varchar     not null,
    updated_at     timestamp   not null,
    errors         json,

    constraint {{ params.table }}_uk unique (seller_id, vendor_code)
);
