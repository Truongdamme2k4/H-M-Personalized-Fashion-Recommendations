#!/bin/bash
# Load CSV từ /seed (mount từ data/raw/) vào các bảng OLTP.
# Chạy 1 lần khi container init.
set -e

echo "[oltp-init] Loading articles..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\COPY articles FROM '/seed/articles.csv' WITH (FORMAT csv, HEADER true);"

echo "[oltp-init] Loading customers..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\COPY customers FROM '/seed/customers.csv' WITH (FORMAT csv, HEADER true, NULL '');"

echo "[oltp-init] Loading transactions..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'EOF'
\COPY transactions(t_dat, customer_id, article_id, price, sales_channel_id) FROM '/seed/transactions_train.csv' WITH (FORMAT csv, HEADER true);
EOF

psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT 'articles' AS tbl, count(*) FROM articles
UNION ALL SELECT 'customers', count(*) FROM customers
UNION ALL SELECT 'transactions', count(*) FROM transactions;
"

echo "[oltp-init] Done."
