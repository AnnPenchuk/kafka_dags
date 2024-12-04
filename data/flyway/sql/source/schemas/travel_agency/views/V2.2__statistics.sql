-- View for checking connections
CREATE OR REPLACE VIEW travel_agency.system_connections AS
SELECT
  pid,
  usename,
  client_addr,
  application_name,
  backend_start,
  state,
  query
FROM pg_stat_activity
WHERE state IN ('active', 'idle');

-- View for checking lock
CREATE OR REPLACE VIEW travel_agency.lock_monitor AS
SELECT 
  bl.pid AS blocked_pid, 
  a.usename AS blocked_user,
  a.query AS blocked_query,
  a.query_start AS blocked_query_start,
  now() - a.query_start AS blocked_duration,
  kl.pid AS blocking_pid,
  ka.usename AS blocking_user,
  ka.query AS blocking_query,
  kl.mode AS lock_type,
  kl.granted AS lock_granted
FROM pg_locks bl
JOIN pg_stat_activity a
  ON bl.pid = a.pid
JOIN pg_locks kl
  ON bl.locktype = kl.locktype
    AND bl.DATABASE IS NOT DISTINCT FROM kl.DATABASE
    AND bl.relation IS NOT DISTINCT FROM kl.relation
    AND bl.page IS NOT DISTINCT FROM kl.page
    AND bl.tuple IS NOT DISTINCT FROM kl.tuple
    AND bl.transactionid IS NOT DISTINCT FROM kl.transactionid
    AND bl.classid IS NOT DISTINCT FROM kl.classid
    AND bl.objid IS NOT DISTINCT FROM kl.objid
    AND bl.objsubid IS NOT DISTINCT FROM kl.objsubid
    AND kl.granted
JOIN pg_stat_activity ka 
  ON kl.pid = ka.pid
WHERE 1=1
  AND NOT bl.granted
ORDER BY a.query_start;
