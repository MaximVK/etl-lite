
-- @target.table: reports.client_volume

-- @invariant.sum: Total volume should stay the same
--   name: total_volume
--   column: amount
--   tolerance: relative(0.001)

-- @test.no_duplicates: Ensure no duplicate records
--   name: unique_clients
--   columns: [client_id, trade_date]

-- @main
SELECT client_id, trade_date, sum(amount)
FROM {{source.table}}
GROUP BY client_id, trade_date
