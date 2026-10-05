DELETE FROM model_rows;
INSERT INTO model_rows (tenant, site, entity, record_id, document)
SELECT tenant, site, entity, record_id, document FROM migration_snapshot_rows;
