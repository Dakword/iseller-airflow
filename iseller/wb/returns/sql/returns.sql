CREATE TABLE IF NOT EXISTS {{params.table}}
(
    id                varchar     not null   constraint {{params.table}}_pk primary key,
    seller_id         integer     not null,
    srid              varchar     not null,
    nm_id             integer     not null,
    date              timestamp   not null,
    order_date        timestamp   not null,
    update_date       timestamp   not null,
    source            integer     not null,
    status            integer     not null,
    product_status    integer     not null,
    product_name      varchar     not null,
    user_comment      text        not null,
    photos            json,
    videos            json,
    user_price        integer     not null,
    actions           json        not null,
    wb_comment        text
);

CREATE INDEX IF NOT EXISTS {{params.table}}_seller__index ON {{ params.table }} (seller_id);
CREATE INDEX IF NOT EXISTS {{params.table}}_nm__index     ON {{ params.table }} (nm_id);

DO $$
BEGIN
    -- claim_source
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{{params.table}}_claim_source') THEN
        CREATE TABLE {{params.table}}_claim_source
        (
            source_id    integer   not null   constraint claim_source_pk primary key,
            name         varchar   not null
        );
        INSERT INTO {{params.table}}_claim_source (source_id, name)
        VALUES (1, 'портал покупателей'),
               (3, 'чат');
    END IF;

    -- claim_status
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{{params.table}}_claim_status') THEN
        CREATE TABLE {{params.table}}_claim_status
        (
            status_id    integer   not null   constraint claim_status_pk primary key,
            name         varchar   not null
        );
        INSERT INTO {{params.table}}_claim_status (status_id, name)
        VALUES (0, 'на рассмотрении'),
               (1, 'отказ'),
               (2, 'одобрено');
    END IF;

    -- product_status
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{{params.table}}_product_status') THEN
        CREATE TABLE {{params.table}}_product_status
        (
            status_id    integer   not null   constraint product_status_pk primary key,
            name         varchar   not null
        );
        INSERT INTO {{params.table}}_product_status (status_id, name)
        VALUES (0, 'на рассмотрении'),
               (1, 'остаётся у покупателя'),
               (2, 'без возврата'),
               (5, 'без возврата'),
               (8, 'товар будет возвращён в реализацию или оформлен на возврат после проверки WB'),
               (10, 'возврат продавцу');
    END IF;

    -- actions
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{{params.table}}_actions') THEN
        CREATE TABLE {{params.table}}_actions
        (
            action    varchar   not null   constraint claim_action_pk primary key,
            name      varchar   not null
        );
        INSERT INTO {{params.table}}_actions (action, name)
        VALUES ('approve1', 'одобрить с проверкой брака'),
               ('approve2', 'одобрить и забрать товар'),
               ('autorefund1', 'одобрить без возврата товара'),
               ('reject1', 'отклонить с шаблоном ответа "Брак не обнаружен"'),
               ('reject2', 'отклонить с шаблоном ответа "Добавить фото/видео"'),
               ('reject3', 'отклонить с шаблоном ответа "Направить в сервисный центр"'),
               ('rejectcustom', 'отклонить с комментарием'),
               ('approvecc1', 'одобрить заявку с возвратом товара в магазин продавца'),
               ('confirmreturngoodcc1', 'подтвердить приёмку товара от покупателя');
    END IF;
END $$;
