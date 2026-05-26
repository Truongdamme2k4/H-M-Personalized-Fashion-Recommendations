-- OLTP schema cho hệ thống H&M (mô phỏng DB sản phẩm + đơn hàng)
-- Auto-run khi container postgres khởi tạo lần đầu.

CREATE TABLE articles (
    article_id                      VARCHAR(10) PRIMARY KEY,
    product_code                    INTEGER,
    prod_name                       TEXT,
    product_type_no                 INTEGER,
    product_type_name               TEXT,
    product_group_name              TEXT,
    graphical_appearance_no         INTEGER,
    graphical_appearance_name       TEXT,
    colour_group_code               INTEGER,
    colour_group_name               TEXT,
    perceived_colour_value_id       INTEGER,
    perceived_colour_value_name     TEXT,
    perceived_colour_master_id      INTEGER,
    perceived_colour_master_name    TEXT,
    department_no                   INTEGER,
    department_name                 TEXT,
    index_code                      VARCHAR(8),
    index_name                      TEXT,
    index_group_no                  INTEGER,
    index_group_name                TEXT,
    section_no                      INTEGER,
    section_name                    TEXT,
    garment_group_no                INTEGER,
    garment_group_name              TEXT,
    detail_desc                     TEXT
);

CREATE TABLE customers (
    customer_id             VARCHAR(64) PRIMARY KEY,
    fn                      NUMERIC,
    active                  NUMERIC,
    club_member_status      VARCHAR(16),
    fashion_news_frequency  VARCHAR(16),
    age                     NUMERIC,
    postal_code             VARCHAR(64)
);

CREATE TABLE transactions (
    id              BIGSERIAL PRIMARY KEY,
    t_dat           DATE NOT NULL,
    customer_id     VARCHAR(64) NOT NULL,
    article_id      VARCHAR(10) NOT NULL,
    price           NUMERIC(20, 16),
    sales_channel_id SMALLINT
);

CREATE INDEX idx_transactions_t_dat ON transactions(t_dat);
CREATE INDEX idx_transactions_customer ON transactions(customer_id);
