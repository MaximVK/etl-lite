-- @meta.engine: clickhouse
--   type: clickhouse

-- @target.table: City level price statistics
--   name: default.city_stats
--   engine: Log
--   columns:
--     city: String
--     year: UInt16
--     transactions: UInt32
--     avg_price: UInt32
--     median_price: UInt32

-- @main
SELECT
    city,
    toYear(toDate(substring(date, 1, 10))) as year,
    count() as transactions,
    round(avg(price)) as avg_price,
    round(median(price)) as median_price
FROM uk_house_prices
GROUP BY
    city,
    year
ORDER BY
    city,
    year