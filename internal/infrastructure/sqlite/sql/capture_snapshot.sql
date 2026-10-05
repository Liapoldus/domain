DELETE FROM migration_snapshot_rows;
INSERT INTO migration_snapshot_rows (tenant, site, entity, record_id, document)
SELECT tenant, site, entity, record_id, document FROM model_rows;
