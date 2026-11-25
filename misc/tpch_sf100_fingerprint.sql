-- TPC-H SF100 fingerprint. Change samples.tpch_sf100 to where your copy lives.
-- Compare each row with the expected values at the bottom.
SELECT 'region' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(r_regionkey AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.region
UNION ALL
SELECT 'nation' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(n_nationkey AS BIGINT), CAST(n_regionkey AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.nation
UNION ALL
SELECT 'supplier' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(s_suppkey AS BIGINT), CAST(s_nationkey AS BIGINT), CAST(round(s_acctbal * 100) AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.supplier
UNION ALL
SELECT 'customer' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(c_custkey AS BIGINT), CAST(c_nationkey AS BIGINT), CAST(round(c_acctbal * 100) AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.customer
UNION ALL
SELECT 'part' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(p_partkey AS BIGINT), CAST(p_size AS BIGINT), CAST(round(p_retailprice * 100) AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.part
UNION ALL
SELECT 'partsupp' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(ps_partkey AS BIGINT), CAST(ps_suppkey AS BIGINT), CAST(ps_availqty AS BIGINT), CAST(round(ps_supplycost * 100) AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.partsupp
UNION ALL
SELECT 'orders' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(o_orderkey AS BIGINT), CAST(o_custkey AS BIGINT), CAST(round(o_totalprice * 100) AS BIGINT), CAST(datediff(o_orderdate, DATE '1970-01-01') AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.orders
UNION ALL
SELECT 'lineitem' AS table_name, count(*) AS row_count, CAST(sum(CAST(xxhash64(CAST(l_orderkey AS BIGINT), CAST(l_linenumber AS BIGINT), CAST(l_partkey AS BIGINT), CAST(l_suppkey AS BIGINT), CAST(round(l_quantity * 100) AS BIGINT), CAST(round(l_extendedprice * 100) AS BIGINT), CAST(round(l_discount * 100) AS BIGINT), CAST(datediff(l_shipdate, DATE '1970-01-01') AS BIGINT), CAST(datediff(l_receiptdate, DATE '1970-01-01') AS BIGINT)) AS DECIMAL(38,0))) AS DECIMAL(38,0)) AS hash_sum FROM samples.tpch_sf100.lineitem
ORDER BY table_name;

-- Expected for SF100:
--   customer      15000000  3602533371103406336902
--   lineitem     600037902  -179549416792424644036657
--   nation              25  -78255226521009464302
--   orders       150000000  -52916726037809301477313
--   part          20000000  7624925755610617594283
--   partsupp      80000000  -18547872856527434475586
--   region               5  -12002864373251589671
--   supplier       1000000  1890552607869578991156
