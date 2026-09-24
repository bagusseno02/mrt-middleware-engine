-- Destructive rollback: run only after a backup and explicit operator approval.
DROP TABLE IF EXISTS measurements;
DROP TABLE IF EXISTS meters;
DELETE FROM schema_migrations WHERE version = 1;
