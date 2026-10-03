create table suppliers (
    id   uuid primary key,
    code varchar(32)  not null unique,
    name varchar(200) not null
);

create table products (
    id          uuid primary key,
    sku         varchar(16)  not null unique check (sku ~ '^SHW-[0-9]{4}$'),
    name        varchar(200) not null,
    description text         not null,
    category    varchar(32)  not null,
    price_paise bigint       not null check (price_paise > 0),
    supplier_id uuid         not null references suppliers (id),
    created_at  timestamptz  not null,
    version     bigint       not null
);

create table product_dietary_tags (
    product_id uuid        not null references products (id) on delete cascade,
    tag        varchar(64) not null,
    primary key (product_id, tag)
);

create index products_category_price on products (category, price_paise);
