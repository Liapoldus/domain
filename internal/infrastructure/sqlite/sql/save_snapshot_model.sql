INSERT INTO migration_snapshot (singleton, model_document) VALUES (1, ?)
ON CONFLICT(singleton) DO UPDATE SET model_document = excluded.model_document;
