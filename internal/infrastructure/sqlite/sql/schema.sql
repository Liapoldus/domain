CREATE TABLE IF NOT EXISTS model_state (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  document BLOB NOT NULL,
  epoch INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS migration_snapshot (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  model_document BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_snapshot_rows (
  tenant TEXT NOT NULL,
  site TEXT NOT NULL,
  entity TEXT NOT NULL,
  record_id TEXT NOT NULL,
  document BLOB NOT NULL,
  PRIMARY KEY (tenant, site, entity, record_id)
);
CREATE TABLE IF NOT EXISTS model_rows (
  tenant TEXT NOT NULL,
  site TEXT NOT NULL,
  entity TEXT NOT NULL,
  record_id TEXT NOT NULL,
  document BLOB NOT NULL,
  PRIMARY KEY (tenant, site, entity, record_id)
);
