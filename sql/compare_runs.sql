-- Executed by python -m etl.compare with bound parameters, not string substitution.
-- Compare every batch AND final output (scope 0); runtime/timestamps are not results.
WITH targets AS (
    SELECT %(left_run)s::uuid AS run_id UNION ALL SELECT %(right_run)s::uuid
), scopes AS (
    SELECT r.run_id, generate_series(0, r.num_batches) AS scope
    FROM etl.etl_run r JOIN targets t USING (run_id)
), queries(query_id) AS (VALUES ('q1'), ('q2'), ('q3')),
actual_counts AS (
    SELECT run_id, coalesce(batch_id,0) scope, 'q1' query_id, count(*) n
    FROM etl.q1_daily_traffic JOIN targets USING(run_id) GROUP BY run_id,batch_id
    UNION ALL
    SELECT run_id, coalesce(batch_id,0), 'q2', count(*)
    FROM etl.q2_top_resources JOIN targets USING(run_id) GROUP BY run_id,batch_id
    UNION ALL
    SELECT run_id, coalesce(batch_id,0), 'q3', count(*)
    FROM etl.q3_hourly_errors JOIN targets USING(run_id) GROUP BY run_id,batch_id
), execution_checks AS (
    SELECT q.query_id, count(*) FILTER (
        WHERE e.run_id IS NULL OR e.row_count <> coalesce(a.n,0)
    ) AS invalid_executions
    FROM scopes s CROSS JOIN queries q
    LEFT JOIN etl.query_execution e ON e.run_id=s.run_id
        AND coalesce(e.batch_id,0)=s.scope AND e.query_id=q.query_id
    LEFT JOIN actual_counts a ON a.run_id=s.run_id AND a.scope=s.scope AND a.query_id=q.query_id
    GROUP BY q.query_id
), q1 AS (
    SELECT 'q1' query_id, count(l.run_id) left_rows, count(r.run_id) right_rows,
        count(*) FILTER (WHERE l.run_id IS NULL OR r.run_id IS NULL
            OR (l.request_count,l.total_bytes) IS DISTINCT FROM (r.request_count,r.total_bytes)) differences
    FROM (SELECT * FROM etl.q1_daily_traffic WHERE run_id=%(left_run)s) l
    FULL JOIN (SELECT * FROM etl.q1_daily_traffic WHERE run_id=%(right_run)s) r
        ON coalesce(l.batch_id,0)=coalesce(r.batch_id,0)
        AND l.log_date=r.log_date AND l.status_code=r.status_code
), q2 AS (
    SELECT 'q2' query_id, count(l.run_id) left_rows, count(r.run_id) right_rows,
        count(*) FILTER (WHERE l.run_id IS NULL OR r.run_id IS NULL
            OR (l.request_count,l.total_bytes,l.distinct_host_count) IS DISTINCT FROM
               (r.request_count,r.total_bytes,r.distinct_host_count)) differences
    FROM (SELECT * FROM etl.q2_top_resources WHERE run_id=%(left_run)s) l
    FULL JOIN (SELECT * FROM etl.q2_top_resources WHERE run_id=%(right_run)s) r
        ON coalesce(l.batch_id,0)=coalesce(r.batch_id,0) AND l.resource_path=r.resource_path
), q3 AS (
    SELECT 'q3' query_id, count(l.run_id) left_rows, count(r.run_id) right_rows,
        count(*) FILTER (WHERE l.run_id IS NULL OR r.run_id IS NULL
            OR (l.error_request_count,l.total_request_count,l.distinct_error_hosts) IS DISTINCT FROM
               (r.error_request_count,r.total_request_count,r.distinct_error_hosts)
            OR abs(l.error_rate-r.error_rate)>%(rate_tolerance)s) differences
    FROM (SELECT * FROM etl.q3_hourly_errors WHERE run_id=%(left_run)s) l
    FULL JOIN (SELECT * FROM etl.q3_hourly_errors WHERE run_id=%(right_run)s) r
        ON coalesce(l.batch_id,0)=coalesce(r.batch_id,0)
        AND l.log_date=r.log_date AND l.log_hour=r.log_hour
), results AS (SELECT * FROM q1 UNION ALL SELECT * FROM q2 UNION ALL SELECT * FROM q3)
SELECT r.*, e.invalid_executions FROM results r JOIN execution_checks e USING(query_id)
ORDER BY query_id;
