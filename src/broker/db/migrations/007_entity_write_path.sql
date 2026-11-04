-- Write-path tuning for high update rates (load test, ~10k updates/s):
--  * the GIN index on attrs is not used by any query (no JSON containment filters)
--    and had to be rewritten on every update
--  * the updated_at index changed on every write and prevented HOT updates;
--    orderBy=updatedAt now sorts the tenant's rows instead
--  * free space in each page lets PostgreSQL update rows in place (HOT)
DROP INDEX IF EXISTS entities_attrs_idx;
DROP INDEX IF EXISTS entities_updated_idx;
ALTER TABLE entities SET (fillfactor = 70);
