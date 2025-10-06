# Final Views

This file contains final fact tables and analytical views built on top of cleaned Silver-layer data.

---

## Fact Table: `olist_gmv.fact_orders`

A one-row-per-order fact table that includes:
- Main product category (by highest spend)
- State of customer at purchase time
- Primary payment type (based on largest value)
- Review score (if available)
- Order-level financials: net GMV, gross GMV, freight, item count
- Delivery and SLA timestamps
- Partitioned by `order_year` and `order_month`

This table is constructed by joining multiple cleaned inputs:
- `orders_clean`
- `order_items_clean`
- `products_clean`
- `order_payments_clean`
- `order_reviews_clean`
- `customer_clean`

```sql
-- Compute per-order spend by product category (net of freight)
CREATE OR REPLACE VIEW olist_gmv.tmp_order_category_amount AS
SELECT
  oi.order_id,
  p.product_category_name,
  SUM(oi.price) AS category_amount
FROM olist_db.order_items_clean oi
LEFT JOIN olist_db.products_clean p
  ON p.product_id = oi.product_id
GROUP BY 1, 2;
````
```sql
-- Pick the "main category" per order as the category with the largest spend
-- Tie-break: alphabetical order of category name for deterministic selection
CREATE OR REPLACE VIEW olist_gmv.tmp_order_main_category AS
WITH ranked AS (
  SELECT
    order_id,
    product_category_name,
    category_amount,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY category_amount DESC, product_category_name ASC
    ) AS rn
  FROM olist_gmv.tmp_order_category_amount
)
SELECT order_id, product_category_name
FROM ranked
WHERE rn = 1;
````
```sql
CREATE TABLE olist_gmv.fact_orders
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/gold/gmv/fact_orders/',
    partitioned_by = ARRAY['order_year', 'order_month']
) AS
WITH
-- Base: delivered orders, aggregated to order-level financials & counts
base AS (
  SELECT
      o.order_id,

      -- Normalize time: keep the raw purchase ts plus derived month partition keys
      MAX(o.order_purchase_ts) AS order_purchase_ts,
      MAX(o.order_status)      AS order_status,

      -- Delivery signals for SLA analysis
      MAX(o.delivered_customer_ts) AS delivered_customer_ts,
      MAX(o.estimated_delivery_ts) AS estimated_delivery_ts,

      -- Customer geography at order time
      MAX(c.customer_state) AS customer_state,

      -- Order-level financials (net vs gross)
      SUM(oi.price)                          AS order_amount_net,   -- GMV (net)
      SUM(oi.price + oi.freight_value)       AS order_amount_gross, -- net + freight
      SUM(oi.freight_value)                  AS order_freight,

      -- Item metrics
      COUNT(1)                               AS items_per_order,     -- number of lines/items
      COUNT(DISTINCT oi.product_id)          AS distinct_skus,

      -- Partitions (reuse Silver's normalized month; ensures cheap scans)
      MAX(o.order_year)  AS order_year,
      MAX(o.order_month) AS order_month
  FROM olist_db.orders_clean        AS o
  JOIN olist_db.order_items_clean   AS oi ON oi.order_id = o.order_id
  JOIN olist_db.customer_clean     AS c  ON c.customer_id = o.customer_id
  WHERE o.order_status = 'delivered'
  GROUP BY o.order_id
),

-- Payment primary: choose the payment record with the largest payment_value
pay_primary AS (
  SELECT order_id, payment_type, payment_installments
  FROM (
    SELECT
      p.order_id,
      MAX_BY(p.payment_type,        p.payment_value)    AS payment_type,
      MAX_BY(p.payment_installments,p.payment_value)    AS payment_installments
    FROM olist_db.order_payments_clean p
    GROUP BY p.order_id
  )
),

-- Review latest: prefer the latest review per order (if duplicates exist)
rv_latest AS (
  SELECT order_id, review_score
  FROM (
    SELECT
      r.order_id,
      r.review_score,
      ROW_NUMBER() OVER (PARTITION BY r.order_id
                         ORDER BY r.review_creation_ts DESC NULLS LAST) AS rn
    FROM olist_db.order_reviews_clean r
  )
  WHERE rn = 1
),

-- Main category chosen in step (2)
cat AS (
  SELECT order_id, product_category_name
  FROM olist_gmv.tmp_order_main_category
)

SELECT
  b.order_id,
  DATE_TRUNC('month', b.order_purchase_ts) AS order_month_ts, -- convenience month ts
  b.order_purchase_ts,
  b.order_status,
  b.delivered_customer_ts,
  b.estimated_delivery_ts,

  COALESCE(cat.product_category_name, 'unknown') AS main_category,
  b.customer_state,

  -- Payment attributes
  pay.payment_type,
  pay.payment_installments,

  -- Review score (1..5) if present
  rv.review_score,

  -- Financials & counts
  b.order_amount_net,
  b.order_amount_gross,
  b.order_freight,
  b.items_per_order,
  b.distinct_skus,

  -- Partition columns (ints)
  b.order_year,
  b.order_month
FROM base b
LEFT JOIN cat        ON cat.order_id = b.order_id
LEFT JOIN pay_primary AS pay ON pay.order_id = b.order_id
LEFT JOIN rv_latest   AS rv  ON rv.order_id  = b.order_id
;
````

---

## 📈 L0 View: `agg_monthly_core`

This monthly aggregation view summarizes GMV performance over time:
- Total GMV (net and gross)
- Total number of orders
- AOV (Average Order Value)

Used as the baseline for downstream decomposition.

```sql
-- Creates a clean monthly core view on the fact table
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_core AS
SELECT
    order_month_ts                          AS order_month,   -- month timestamp WITH year
    COUNT(DISTINCT order_id)                AS orders,
    SUM(order_amount_net)                   AS gmv,
    SUM(order_amount_gross)                 AS gmv_gross,
    CAST(SUM(order_amount_net) / NULLIF(COUNT(DISTINCT order_id), 0) AS DOUBLE) AS aov
FROM olist_gmv.fact_orders
GROUP BY 1;
````
---

## 📉 L1 View: `agg_monthly_mom_waterfall_long`

This view implements a **month-over-month GMV decomposition**, breaking changes into:
- Volume effect (ΔOrders × previous AOV)
- AOV effect (ΔAOV × previous Orders)
- Interaction term (ΔOrders × ΔAOV)

It is formatted in a "long" shape suitable for Tableau waterfall charts, with labeled stages:
- Start (previous GMV)
- Orders (volume effect)
- AOV (value effect)
- Interaction (cross-term)
- End (current GMV)

Each row is tagged with `sort_key` and `is_total` to help with custom visual styling.

```sql
-- Decompose ΔGMV into Volume (Orders), AOV, and Interaction in a long, Tableau-friendly shape
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_mom_waterfall_long AS
WITH core AS (
  SELECT
    order_month,
    gmv,
    orders,
    aov,
    LAG(gmv)    OVER (ORDER BY order_month) AS gmv_prev,
    LAG(orders) OVER (ORDER BY order_month) AS orders_prev,
    LAG(aov)    OVER (ORDER BY order_month) AS aov_prev
  FROM olist_gmv.agg_monthly_core
),
mom AS (
  SELECT
    order_month,
    gmv,
    gmv_prev,
    orders, orders_prev,
    aov,    aov_prev,
    -- Effects (standard multiplicative decomposition)
    (aov_prev    * (orders - orders_prev))      AS effect_volume,      -- volume effect
    (orders_prev * (aov    - aov_prev))         AS effect_aov,         -- AOV effect
    ((orders - orders_prev) * (aov - aov_prev)) AS effect_interaction  -- cross-term
  FROM core
  WHERE gmv_prev IS NOT NULL  -- drop the first month which lacks a baseline
),
indexed AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (ORDER BY m.order_month) AS month_index
  FROM mom m
)
-- Long form: one row per step for Tableau
SELECT
  order_month,
  month_index,
  0                        AS sort_in_month,
  month_index * 10 + 0     AS sort_key,              -- handy global sort key
  'Start'                  AS stage,                 -- starting level = previous GMV
  'GMV'                    AS component,             -- for color/legend grouping
  gmv_prev                 AS amount,
  TRUE                     AS is_total               -- mark totals to style differently if desired
FROM indexed
UNION ALL
SELECT
  order_month, month_index,
  1, month_index * 10 + 1,
  'Orders', 'Orders',
  effect_volume,
  FALSE
FROM indexed
UNION ALL
SELECT
  order_month, month_index,
  2, month_index * 10 + 2,
  'AOV', 'AOV',
  effect_aov,
  FALSE
FROM indexed
UNION ALL
SELECT
  order_month, month_index,
  3, month_index * 10 + 3,
  'Interaction', 'Interaction',
  effect_interaction,
  FALSE
FROM indexed
UNION ALL
SELECT
  order_month, month_index,
  4, month_index * 10 + 4,
  'End', 'GMV',
  gmv,
  TRUE
FROM indexed
;
````

---
## 🧮 L2 Views: AOV Mix vs. Like-for-Like Decomposition

To isolate whether AOV changes are caused by structure (Mix) or within-category shifts (LFL), we create monthly aggregation views:

### `monthly_by_category`
- Aggregates GMV and order counts per product category per month.
- Computes category-level AOV = GMV / Orders.

```sql
-- Category-level monthly aggregation:
-- - orders_c: number of orders in this category (per month)
-- - gmv_c:    net GMV in this category
-- - aov_c:    category AOV = gmv_c / orders_c
CREATE OR REPLACE VIEW olist_gmv.monthly_by_category AS
SELECT
  DATE_TRUNC('month', order_purchase_ts)                    AS order_month,
  COALESCE(main_category, 'unknown')                        AS main_category,
  COUNT(DISTINCT order_id)                                  AS orders_c,
  SUM(order_amount_net)                                     AS gmv_c,
  CAST(SUM(order_amount_net) / NULLIF(COUNT(DISTINCT order_id), 0) AS DOUBLE) AS aov_c
FROM olist_gmv.fact_orders
WHERE order_status = 'delivered'
GROUP BY 1, 2;
````
```sql
-- Decompose AOV change (month-over-month) into Like-for-Like vs Mix.
-- Dimension: main_category. Uses average weights on the intersection set (common categories).
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_aov_lfl_by_category AS
WITH core AS (
  -- Overall monthly totals from your L0 core (already computed on fact table)
  SELECT
    order_month,
    SUM(gmv)    OVER (PARTITION BY 1 ORDER BY order_month ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS gmv_dummy, -- noop, keeps columns explicit
    gmv,
    orders,
    aov,
    LAG(gmv)    OVER (ORDER BY order_month) AS gmv_prev,
    LAG(orders) OVER (ORDER BY order_month) AS orders_prev,
    LAG(aov)    OVER (ORDER BY order_month) AS aov_prev
  FROM olist_gmv.agg_monthly_core
),
cat AS (
  -- Category metrics
  SELECT order_month, main_category, orders_c, gmv_c,
         CAST(gmv_c / NULLIF(orders_c,0) AS DOUBLE) AS aov_c
  FROM olist_gmv.monthly_by_category
),
joined AS (
  -- Join category metrics to previous month within the same category
  SELECT
    c.order_month,
    c.main_category,

    -- current month
    c.orders_c                AS orders_cur,
    c.gmv_c                   AS gmv_cur,
    c.aov_c                   AS aov_cur,

    -- previous month (per category)
    LAG(c.orders_c) OVER (PARTITION BY c.main_category ORDER BY c.order_month) AS orders_prev_cat,
    LAG(c.gmv_c)    OVER (PARTITION BY c.main_category ORDER BY c.order_month) AS gmv_prev_cat,
    LAG(c.aov_c)    OVER (PARTITION BY c.main_category ORDER BY c.order_month) AS aov_prev_cat
  FROM cat c
),
with_totals AS (
  -- Bring in total orders to compute weights per month
  SELECT
    j.*,
    co.orders      AS orders_total_cur,
    co_prev.orders AS orders_total_prev
  FROM joined j
  LEFT JOIN core co
    ON co.order_month = j.order_month
  LEFT JOIN core co_prev
    ON co_prev.order_month = DATE_TRUNC('month', j.order_month - INTERVAL '1' MONTH)
),
common_only AS (
  -- Keep only categories present in BOTH months (intersection S_common)
  SELECT
    order_month,
    main_category,
    orders_cur,
    orders_prev_cat,
    aov_cur,
    aov_prev_cat,
    CAST(orders_cur       AS DOUBLE) / NULLIF(orders_total_cur,  0) AS w_cur,
    CAST(orders_prev_cat  AS DOUBLE) / NULLIF(orders_total_prev, 0) AS w_prev
  FROM with_totals
  WHERE orders_cur > 0 AND orders_prev_cat > 0
),
lfl_effect AS (
  -- LFL across S_common using average weights
  SELECT
    order_month,
    SUM( ((COALESCE(w_cur,0)+COALESCE(w_prev,0))/2.0) * (aov_cur - aov_prev_cat) ) AS effect_lfl
  FROM common_only
  GROUP BY 1
),
delta_aov AS (
  -- Total ΔAOV from core
  SELECT
    order_month,
    aov - aov_prev AS delta_aov
  FROM core
  WHERE aov_prev IS NOT NULL
)
SELECT
  d.order_month,
  d.delta_aov                       AS delta_aov,
  l.effect_lfl                      AS effect_lfl,
  (d.delta_aov - l.effect_lfl)      AS effect_mix
FROM delta_aov d
LEFT JOIN lfl_effect l
  ON l.order_month = d.order_month;
````
```sql
-- Long format for Tableau: Start AOV, LFL effect, Mix effect, End AOV
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_aov_lfl_waterfall_long AS
WITH core AS (
  SELECT
    order_month,
    aov,
    LAG(aov) OVER (ORDER BY order_month) AS aov_prev
  FROM olist_gmv.agg_monthly_core
),
decomp AS (
  SELECT
    a.order_month,
    a.delta_aov,
    a.effect_lfl,
    a.effect_mix,
    c.aov,
    c.aov_prev
  FROM olist_gmv.agg_monthly_aov_lfl_by_category a
  JOIN core c on a.order_month = c.order_month
),
indexed AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (ORDER BY d.order_month) AS month_index
  FROM decomp d
)
SELECT order_month, month_index, 0 AS sort_in_month, month_index*10+0 AS sort_key,
       'Start' AS stage, 'AOV' AS component, aov_prev AS amount, TRUE AS is_total
FROM indexed
UNION ALL
SELECT order_month, month_index, 1, month_index*10+1,
       'LFL', 'LFL', effect_lfl, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index, 2, month_index*10+2,
       'Mix', 'Mix', effect_mix, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index, 3, month_index*10+3,
       'End', 'AOV', aov, TRUE
FROM indexed;
````

### `monthly_by_state`
- Aggregates GMV and order counts per customer state per month.
- Computes state-level AOV = GMV / Orders.

These views support comparison of structure stability vs. internal category-level pricing behavior.

```sql
-- Monthly metrics by state (customer_state):
--   orders_s: orders in the state for the month
--   gmv_s:    net GMV in the state
--   aov_s:    state-level AOV
CREATE OR REPLACE VIEW olist_gmv.monthly_by_state AS
SELECT
  DATE_TRUNC('month', order_purchase_ts)          AS order_month,
  -- normalize missing
  COALESCE(customer_state, 'NA')                  AS customer_state,     
  COUNT(DISTINCT order_id)                        AS orders_s,
  SUM(order_amount_net)                           AS gmv_s,
  CAST(SUM(order_amount_net) / NULLIF(COUNT(DISTINCT order_id), 0) AS DOUBLE) AS aov_s
FROM olist_gmv.fact_orders
WHERE order_status = 'delivered'
GROUP BY 1, 2;
````
```sql
-- Decompose AOV month-over-month into:
--   - effect_lfl: Like-for-Like (within-state AOV change, using average weights on common states)
--   - effect_mix: Mix (change in state weights), defined as total ΔAOV minus LFL
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_aov_lfl_by_state AS
WITH core AS (
  -- Overall monthly totals from L0
  SELECT
    order_month,
    gmv,
    orders,
    aov,
    LAG(gmv)    OVER (ORDER BY order_month) AS gmv_prev,
    LAG(orders) OVER (ORDER BY order_month) AS orders_prev,
    LAG(aov)    OVER (ORDER BY order_month) AS aov_prev
  FROM olist_gmv.agg_monthly_core
),
st AS (
  -- State metrics per month
  SELECT
    order_month,
    customer_state,
    orders_s,
    gmv_s,
    CAST(gmv_s / NULLIF(orders_s, 0) AS DOUBLE) AS aov_s
  FROM olist_gmv.monthly_by_state
),
joined AS (
  -- Join a state to its previous-month values (per state)
  SELECT
    s.order_month,
    s.customer_state,

    -- current
    s.orders_s AS orders_cur,
    s.gmv_s    AS gmv_cur,
    s.aov_s    AS aov_cur,

    -- previous per-state
    LAG(s.orders_s) OVER (PARTITION BY s.customer_state ORDER BY s.order_month) AS orders_prev_state,
    LAG(s.gmv_s)    OVER (PARTITION BY s.customer_state ORDER BY s.order_month) AS gmv_prev_state,
    LAG(s.aov_s)    OVER (PARTITION BY s.customer_state ORDER BY s.order_month) AS aov_prev_state
  FROM st s
),
with_totals AS (
  -- Bring in total orders for weight computation (w = orders_state / orders_total)
  SELECT
    j.*,
    co.orders      AS orders_total_cur,
    co_prev.orders AS orders_total_prev
  FROM joined j
  LEFT JOIN core co
    ON co.order_month = j.order_month
  LEFT JOIN core co_prev
    ON co_prev.order_month = DATE_TRUNC('month', j.order_month - INTERVAL '1' MONTH)
),
common_only AS (
  -- Intersection set: states present in BOTH months
  SELECT
    order_month,
    customer_state,
    orders_cur,
    orders_prev_state,
    aov_cur,
    aov_prev_state,
    CAST(orders_cur        AS DOUBLE) / NULLIF(orders_total_cur,  0) AS w_cur,
    CAST(orders_prev_state AS DOUBLE) / NULLIF(orders_total_prev, 0) AS w_prev
  FROM with_totals
  WHERE orders_cur > 0 AND orders_prev_state > 0
),
lfl_effect AS (
  -- Like-for-Like: sum over common states of avg(weight) * ΔAOV_state
  SELECT
    order_month,
    SUM( ((COALESCE(w_cur,0) + COALESCE(w_prev,0)) / 2.0) * (aov_cur - aov_prev_state) ) AS effect_lfl
  FROM common_only
  GROUP BY 1
),
delta_aov AS (
  -- Total ΔAOV from core
  SELECT
    order_month,
    aov - aov_prev AS delta_aov
  FROM core
  WHERE aov_prev IS NOT NULL
)
SELECT
  d.order_month,
  d.delta_aov                                   AS delta_aov,
  COALESCE(l.effect_lfl, 0.0)                   AS effect_lfl,
  (d.delta_aov - COALESCE(l.effect_lfl, 0.0))   AS effect_mix
FROM delta_aov d
LEFT JOIN lfl_effect l
  ON l.order_month = d.order_month;
````
```sql
-- Long format for Tableau waterfall on AOV by state decomposition:
--   For each month (except the first), output:
--     Start (prev AOV), LFL effect, Mix effect, End (current AOV)
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_aov_lfl_waterfall_state_long AS
WITH core AS (
  SELECT
    order_month,
    aov,
    LAG(aov) OVER (ORDER BY order_month) AS aov_prev
  FROM olist_gmv.agg_monthly_core
),
decomp AS (
  SELECT
    a.order_month,
    a.delta_aov,
    a.effect_lfl,
    a.effect_mix,
    c.aov,
    c.aov_prev
  FROM olist_gmv.agg_monthly_aov_lfl_by_state a
  JOIN core c on a.order_month = c.order_month
),
indexed AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (ORDER BY d.order_month) AS month_index
  FROM decomp d
)
SELECT order_month, month_index,
       0 AS sort_in_month, month_index*10+0 AS sort_key,
       'Start' AS stage, 'AOV' AS component, aov_prev AS amount, TRUE AS is_total
FROM indexed
UNION ALL
SELECT order_month, month_index,
       1, month_index*10+1,
       'LFL', 'LFL', effect_lfl, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index,
       2, month_index*10+2,
       'Mix', 'Mix', effect_mix, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index,
       3, month_index*10+3,
       'End', 'AOV', aov, TRUE
FROM indexed;
````

---

## ⚖️ L2-B View: AOV Price × Basket Decomposition

View: `monthly_aov_price_qty`
- Decomposes AOV into:
  - `Unit Price` = GMV / Total Items
  - `Basket Size` = Items / Orders
  - AOV = Unit Price × Basket Size

This allows further diagnosis of whether AOV changes are driven by pricing or quantity shifts.

```sql
-- Monthly AOV components built from the Gold fact table
-- AOV = UnitPrice * BasketSize, where:
--   UnitPrice   = (SUM of item prices) / (SUM of items)
--   BasketSize  = (SUM of items) / (COUNT of orders)
CREATE OR REPLACE VIEW olist_gmv.monthly_aov_price_qty AS
SELECT
  DATE_TRUNC('month', order_purchase_ts) AS order_month,
  COUNT(DISTINCT order_id) AS orders,
  
   -- total items in the month
  SUM(items_per_order) AS items, 
  SUM(order_amount_net) AS gmv_net,
  CAST(SUM(order_amount_net) / NULLIF(SUM(items_per_order), 0) AS DOUBLE) AS unit_price, 
  CAST(SUM(items_per_order) / NULLIF(COUNT(DISTINCT order_id), 0) AS DOUBLE) AS basket_size, 
  CAST(
    (SUM(order_amount_net) / NULLIF(SUM(items_per_order), 0)) *
    (SUM(items_per_order) / NULLIF(COUNT(DISTINCT order_id), 0))
  AS DOUBLE) AS aov_recalc 
FROM olist_gmv.fact_orders
WHERE order_status = 'delivered'
GROUP BY 1;
````
```sql
-- MoM decomposition of AOV into Unit Price vs Basket Size
-- Output a long, Tableau-friendly waterfall with 5 steps per month:
--   Start (prev AOV), Price effect, Basket effect, Interaction, End (current AOV)
CREATE OR REPLACE VIEW olist_gmv.agg_monthly_aov_priceqty_waterfall_long AS
WITH pq AS (
  SELECT
    order_month,
    unit_price,
    basket_size,
    aov_recalc AS aov,
    LAG(unit_price)  OVER (ORDER BY order_month) AS unit_price_prev,
    LAG(basket_size) OVER (ORDER BY order_month) AS basket_size_prev,
    LAG(aov_recalc)  OVER (ORDER BY order_month) AS aov_prev
  FROM olist_gmv.monthly_aov_price_qty
),
effects AS (
  SELECT
    order_month,
    aov,
    aov_prev,
    -- effects
    (basket_size_prev * (unit_price - unit_price_prev)) AS effect_price,
    (unit_price_prev  * (basket_size - basket_size_prev)) AS effect_basket,
    ((unit_price - unit_price_prev) * (basket_size - basket_size_prev)) AS effect_interaction
  FROM pq
  WHERE aov_prev IS NOT NULL   -- drop the first month
),
indexed AS (
  SELECT
    e.*,
    ROW_NUMBER() OVER (ORDER BY e.order_month) AS month_index
  FROM effects e
)
-- Long output: Start, Price, Basket, Interaction, End
SELECT
  order_month,
  month_index,
  0 AS sort_in_month, month_index*10+0 AS sort_key,
  'Start' AS stage, 'AOV' AS component,
  aov_prev AS amount, TRUE AS is_total
FROM indexed
UNION ALL
SELECT order_month, month_index, 1, month_index*10+1,
       'Price', 'Price', effect_price, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index, 2, month_index*10+2,
       'Basket', 'Basket', effect_basket, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index, 3, month_index*10+3,
       'Interaction', 'Interaction', effect_interaction, FALSE
FROM indexed
UNION ALL
SELECT order_month, month_index, 4, month_index*10+4,
       'End', 'AOV', aov, TRUE
FROM indexed;
````

---

## 🧮 L3 Views: Per-Category Unit Price and Basket Size

View: `monthly_pq_by_category`
- Shows monthly GMV, item count, order count by category
- Calculates per-category Unit Price and Basket Size

```sql
-- Monthly unit price & basket size by category
-- AOV_c = unit_price_c * basket_size_c
CREATE OR REPLACE VIEW olist_gmv.monthly_pq_by_category AS
WITH base AS (
  SELECT
    DATE_TRUNC('month', o.order_purchase_ts) AS order_month,
    COALESCE(p.product_category_name, 'unknown') AS main_category,
    oi.order_id,
    oi.price AS line_price
  FROM olist_db.order_items_clean oi
  JOIN olist_db.orders_clean o
    ON o.order_id = oi.order_id
  LEFT JOIN olist_db.products_clean    pr
    ON pr.product_id = oi.product_id
  LEFT JOIN olist_db.product_category_translation_clean     p        
    ON p.product_category_name = pr.product_category_name
  WHERE o.order_status = 'delivered'
),
agg AS (
  SELECT
    order_month,
    main_category,
    COUNT(*) AS items_c,  
    COUNT(DISTINCT order_id) AS orders_c,
    SUM(line_price) AS gmv_c
  FROM base
  GROUP BY order_month, main_category
)
SELECT
  order_month,
  main_category,
  orders_c,
  items_c,
  gmv_c,
  CAST(gmv_c / NULLIF(items_c, 0) AS DOUBLE) AS unit_price_c,
  CAST(items_c / NULLIF(orders_c, 0) AS DOUBLE) AS basket_size_c
FROM agg;
````
```sql
-- Top 10 categories by change in unit price between 2018-07 and 2018-08
WITH two_months AS (
  SELECT
    DATE(order_month) AS mth, 
    main_category,
    unit_price_c
  FROM olist_gmv.monthly_pq_by_category
  WHERE DATE(order_month) IN (DATE '2018-07-01', DATE '2018-08-01')
),
pivoted AS (
  SELECT
    main_category,
    MAX(CASE WHEN mth = DATE '2018-07-01' THEN unit_price_c END) AS unit_price_2018_07,
    MAX(CASE WHEN mth = DATE '2018-08-01' THEN unit_price_c END) AS unit_price_2018_08
  FROM two_months
  GROUP BY main_category
  HAVING COUNT(DISTINCT mth) = 2   
)
SELECT
  main_category,
  unit_price_2018_07,
  unit_price_2018_08,
  (unit_price_2018_08 - unit_price_2018_07)                   AS delta_unit_price,
  ABS(unit_price_2018_08 - unit_price_2018_07)                AS abs_delta_unit_price,
  CASE 
    WHEN unit_price_2018_07 = 0 THEN NULL
    ELSE (unit_price_2018_08 - unit_price_2018_07) / unit_price_2018_07
  END                                                         AS pct_change
FROM pivoted
ORDER BY abs_delta_unit_price DESC
```` 

---

These final views allow clear, visual tracking of GMV changes over time and support business diagnostics around performance drivers.
