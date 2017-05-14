from datetime import datetime, time
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = "s3://aqapop/DataAlgebraData/tpch/100G/1024/"

data_frames = {}
tables = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]

def return_dataframe():
    for table in tables:
        # create df
        data_frames[table] = spark.read.parquet(data_path + table + ".parquet")
        # register tempTables
        data_frames[table].createOrReplaceTempView(table)

return_dataframe()


runtimes = []

def run_query(query):
    global benchmark_version
    query_start_time = datetime.now()
    df = spark.sql(query)
    df.show(100)
    query_stop_time = datetime.now()
    run_time = (query_stop_time-query_start_time).seconds
    runtimes.append(run_time)
    print("Runtime: %s seconds" % run_time)

# what do my tables look like?
run_query("""
SELECT
    *
FROM
    customer
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    orders
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    lineitem
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    supplier
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    nation
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    region
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    part
LIMIT 5
""")

run_query("""
SELECT
    *
FROM
    partsupp
LIMIT 5
""")

# where are the numbers I can use to determine important financial info?
run_query("""
SELECT
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax
FROM
    lineitem
LIMIT 15
""")

# what is the date range for these tables?
run_query("""
SELECT
    min(o_orderdate) as earliest_order,
    max(o_orderdate) as latest_order
FROM
    orders
""")

# what other dates are available?
run_query("""
SELECT
    l_shipdate,
    l_commitdate,
    l_receiptdate
FROM
    lineitem
LIMIT 5
""")

# looks like the earliest dates are in commitdate and the latest are in receiptdate
# what is the total date range in lineitem?
run_query("""
SELECT
    min(l_commitdate) as earliest_date,
    max(l_commitdate) as latest_date
FROM
    lineitem
""")

# what are the names of the different regions?
run_query("""
SELECT DISTINCT
    r_name
FROM
    region
""")

# what are the names of the different nations?
run_query("""
SELECT DISTINCT
    n_name
FROM
    nation
""")

# does a nation map to a region?
run_query("""
SELECT
    n_name,
    r_name
FROM
    nation,
    region
""")
# it looks like I need an additional filter

# does the regionkey map to a nationkey? yes it does
run_query("""
SELECT
    r_name,
    n_name
FROM
    nation,
    region
WHERE
    n_regionkey = r_regionkey
ORDER BY
    r_name,
    n_name
""")

# does the supplier key map to a nationkey?
run_query("""
SELECT
    n_name,
    s_name
FROM
    nation,
    supplier
WHERE
    s_nationkey = n_nationkey
ORDER BY
    n_name
""")

# are order keys related?
run_query("""
SELECT
    *
FROM
    lineitem,
    orders
WHERE
    l_orderkey = o_orderkey
""")

# are supply keys related?
run_query("""
SELECT
    *
FROM
    lineitem,
    supplier
WHERE
    l_suppkey = s_suppkey
""")

# can I calculate the total revenue for Africa?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
""")

# yup so let's calculate the total revenue for Africa
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Africa in 1992?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Africa in 1993?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Africa in 1994?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")
3
# what is the revenue for Africa in 1995?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Africa in 1996?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Africa in 1997?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# can I calculate the total revenue for America?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
""")

# yup so let's calculate the total revenue for America
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for America in 1992?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for America in 1993?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for America in 1994?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for America in 1995?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for America in 1996?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for America in 1997?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# can I calculate the total revenue for Asia?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
""")

# yup so let's calculate the total revenue for Asia
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Asia in 1992?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Asia in 1993?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Asia in 1994?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Asia in 1995?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Asia in 1996?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Asia in 1997?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# can I calculate the total revenue for Europe?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
""")

# yup so let's calculate the total revenue for Europe
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Europe in 1992?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Europe in 1993?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Europe in 1994?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Europe in 1995?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Europe in 1996?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Europe in 1997?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# can I calculate the total revenue for Middle East?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
""")

# yup so let's calculate the total revenue for Middle East
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Middle East in 1992?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Middle East in 1993?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Middle East in 1994?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Middle East in 1995?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Middle East in 1996?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# what is the revenue for Middle East in 1997?
run_query("""
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    revenue desc
""")

# can I calculate the max discount aggregation for Africa from 1992-1997?
run_query("""
SELECT
    n_name,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
""")

# yup so let's calculate the total max discount aggregation for Africa
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Africa region in 1992
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Africa region in 1993
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Africa region in 1994
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Africa region in 1995
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Africa region in 1996
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Africa region in 1997
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# can I calculate the max discount aggregation for America from 1992-1997?
run_query("""
SELECT
    n_name,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
""")

# yup so let's calculate the total max discount aggregation for America
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for America in 1992
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for America in 1993
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for America in 1994
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for America in 1995
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for America in 1996
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for America in 1997
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# can I calculate the max discount aggregation for Asia from 1992-1997?
run_query("""
SELECT
    n_name,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
""")

# yup so let's calculate the total max discount aggregation for Asia
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Asia region in 1992
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Asia region in 1993
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Asia region in 1994
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Asia region in 1995
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Asia region in 1996
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Asia region in 1997
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# can I calculate the max discount aggregation for Europe from 1992-1997?
run_query("""
SELECT
    n_name,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
""")

# yup so let's calculate the total max discount aggregation for Europe
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Europe region in 1992
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Europe region in 1993
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Europe region in 1994
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Europe region in 1995
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Europe region in 1996
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Europe region in 1997
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# can I calculate the max discount aggregation for Middle East from 1992-1997?
run_query("""
SELECT
    n_name,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
""")

# yup so let's calculate the total max discount aggregation for Middle East
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Middle East region in 1992
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Middle East region in 1993
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Middle East region in 1994
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Middle East region in 1995
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Middle East region in 1996
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# max discount aggregation for Middle East region in 1997
run_query("""
SELECT
    n_name,
    max(l_discount) as max_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    max_discount desc
""")

# can I calculate the total discounts offered by Africa from 1992-1997?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
""")

# can I calculate the total discounts offered by Africa from 1992-1997?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
""")

# yup so let's calculate the total discounts offered by Africa
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Africa in 1992
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Africa in 1993
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Africa in 1994
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Africa in 1995
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Africa in 1996
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Africa in 1997
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# can I calculate the total discounts offered by America from 1992-1997?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
""")

# yup so let's calculate the total discounts offered by America
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by America in 1992
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by America in 1993
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by America in 1994
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by America in 1995
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by America in 1996
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by America in 1997
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# can I calculate the total discounts offered by Asia from 1992-1997?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
""")

# yup so let's calculate the total discounts offered by Asia
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Asia in 1992
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Asia in 1993
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Asia in 1994
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Asia in 1995
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Asia in 1996
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Asia in 1997
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# can I calculate the total discounts offered by Europe from 1992-1997?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
""")

# yup so let's calculate the total discounts offered by Europe
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Europe in 1992
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Europe in 1993
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Europe in 1994
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Europe in 1995
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Europe in 1996
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Europe in 1997
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# can I calculate the total discounts offered by Middle East from 1992-1997?
run_query("""
SELECT
    n_name,
    l_extendedprice,
    l_discount
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
""")

# yup so let's calculate the total discounts offered by Middle East
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Middle East in 1992
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Middle East in 1993
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Middle East in 1994
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Middle East in 1995
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Middle East in 1996
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# discounts offered by Middle East in 1997
run_query("""
SELECT
    n_name,
    sum(l_extendedprice *l_discount) as discounts
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    discounts desc
""")

# can I calculate the min and max price offered in Africa?
run_query("""
SELECT
    n_name,
    l_extendedprice
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
""")

# yup so let's calculate the absolute min and absolute max price offered in Africa
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Africa in 1992
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Africa in 1993
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Africa in 1994
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Africa in 1995
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Africa in 1996
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Africa in 1997
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# can I calculate the min and max price offered in America?
run_query("""
SELECT
    n_name,
    l_extendedprice
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
""")

# yup so let's calculate the absolute min and absolute max price offered in America
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by America in 1992
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by America in 1993
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by America in 1994
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by America in 1995
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by America in 1996
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by America in 1997
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# can I calculate the min and max price offered in Asia?
run_query("""
SELECT
    n_name,
    l_extendedprice
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
""")

# yup so let's calculate the absolute min and absolute max price offered in Asia
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Asia in 1992
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Asia in 1993
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Asia in 1994
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Asia in 1995
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Asia in 1996
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Asia in 1997
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# can I calculate the min and max price offered in Middle East?
run_query("""
SELECT
    n_name,
    l_extendedprice
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
""")

# yup so let's calculate the absolute min and absolute max price offered in Middle East
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Middle East in 1992
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1992-01-01' as date)
    AND o_orderdate < add_months(cast('1992-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Middle East in 1993
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1993-01-01' as date)
    AND o_orderdate < add_months(cast('1993-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Middle East in 1994
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1994-01-01' as date)
    AND o_orderdate < add_months(cast('1994-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Middle East in 1995
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1995-01-01' as date)
    AND o_orderdate < add_months(cast('1995-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Middle East in 1996
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1996-01-01' as date)
    AND o_orderdate < add_months(cast('1996-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

# min and max price offered by Middle East in 1997
run_query("""
SELECT
    n_name,
    min(l_extendedprice) as min_price,
    max(l_extendedprice) as max_price
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= cast('1997-01-01' as date)
    AND o_orderdate < add_months(cast('1997-01-01' as date), '12')
GROUP BY
    n_name
ORDER BY
    min_price desc,
    max_price desc
""")

print(runtimes)
