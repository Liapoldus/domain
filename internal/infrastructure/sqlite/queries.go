package sqlite

// SQL belongs to this storage adapter. These literals preserve the approved
// source bytes, including final newlines, statement order and placeholders.
const (
	queryCaptureSnapshot = `DELETE FROM migration_snapshot_rows;
INSERT INTO migration_snapshot_rows (tenant, site, entity, record_id, document)
SELECT tenant, site, entity, record_id, document FROM model_rows;
`

	queryClearSnapshot = `DELETE FROM migration_snapshot_rows;
DELETE FROM migration_snapshot;
`

	queryDeleteRow = `DELETE FROM model_rows WHERE tenant = ? AND site = ? AND entity = ? AND record_id = ?;
`

	queryExportAudit = `SELECT audit_seq, tenant, site, group_name, kind, write_id,
  epoch_before, epoch_after, fingerprint_before, fingerprint_after
FROM migration_audit
ORDER BY audit_seq;
`

	queryExportLedger = `SELECT tenant, site, write_id, outcome, seq FROM write_ledger ORDER BY seq, tenant, site, write_id;
`

	queryExportModel = `SELECT document, epoch FROM model_state WHERE singleton = 1;
`

	queryExportRows = `SELECT tenant, site, entity, record_id, document FROM model_rows ORDER BY tenant, site, entity, record_id;
`

	queryExportSnapshotRows = `SELECT tenant, site, entity, record_id, document FROM migration_snapshot_rows ORDER BY tenant, site, entity, record_id;
`

	queryImportModel = `INSERT INTO model_state (singleton, document, epoch) VALUES (1, ?, ?);
`

	queryImportReset = `DELETE FROM model_rows;
DELETE FROM migration_snapshot_rows;
DELETE FROM migration_snapshot;
DELETE FROM write_ledger;
DELETE FROM model_state;
DELETE FROM migration_audit;
`

	queryImportSnapshotRow = `INSERT INTO migration_snapshot_rows (tenant, site, entity, record_id, document) VALUES (?, ?, ?, ?, ?);
`

	queryInsertModel = `INSERT INTO model_state (singleton, document) VALUES (1, ?);
`

	queryInsertRow = `INSERT INTO model_rows (tenant, site, entity, record_id, document) VALUES (?, ?, ?, ?, ?);
`

	queryLedgerGet = `SELECT outcome, seq FROM write_ledger WHERE tenant = ? AND site = ? AND write_id = ?;
`

	queryLedgerInsert = `INSERT INTO write_ledger (tenant, site, write_id, outcome, seq) VALUES (?, ?, ?, ?, ?);
`

	queryLedgerPrune = `DELETE FROM write_ledger WHERE seq < (
  SELECT MIN(seq) FROM (SELECT seq FROM write_ledger ORDER BY seq DESC LIMIT 4096)
);
`

	queryMigrationAuditImport = `INSERT INTO migration_audit (
  audit_seq, tenant, site, group_name, kind, write_id,
  epoch_before, epoch_after, fingerprint_before, fingerprint_after
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`

	queryMigrationAuditInsert = `INSERT INTO migration_audit (
  audit_seq, tenant, site, group_name, kind, write_id,
  epoch_before, epoch_after, fingerprint_before, fingerprint_after
)
SELECT COALESCE(MAX(audit_seq), 0) + 1, ?, ?, ?, ?, ?, ?, ?, ?, ?
FROM migration_audit;
`

	queryMigrationAuditPrune = `DELETE FROM migration_audit WHERE audit_seq < (
  SELECT MIN(audit_seq) FROM (SELECT audit_seq FROM migration_audit ORDER BY audit_seq DESC LIMIT 512)
);
`

	queryMigrationAuditRecent = `SELECT kind, write_id, epoch_before, epoch_after, fingerprint_before, fingerprint_after
FROM migration_audit
WHERE tenant = ? AND site = ?
ORDER BY audit_seq DESC
LIMIT ?;
`

	queryRaftFsmAppliedGet = `SELECT last_applied_index FROM raft_fsm_state WHERE singleton = 1;
`

	queryRaftFsmAppliedSet = `UPDATE raft_fsm_state SET last_applied_index = ? WHERE singleton = 1;
`

	queryRaftLogBounds = `SELECT COALESCE(MIN(log_index), 0), COALESCE(MAX(log_index), 0)
FROM raft_logs;
`

	queryRaftLogDelete = `DELETE FROM raft_logs
WHERE log_index BETWEEN ? AND ?;
`

	queryRaftLogGet = `SELECT log_index, term, log_type, data, extensions, appended_at
FROM raft_logs
WHERE log_index = ?;
`

	queryRaftLogStore = `INSERT INTO raft_logs (log_index, term, log_type, data, extensions, appended_at)
VALUES (?, ?, ?, CASE WHEN ? IS NULL OR length(?) = 0 THEN zeroblob(0) ELSE ? END,
        CASE WHEN ? IS NULL OR length(?) = 0 THEN zeroblob(0) ELSE ? END, ?)
ON CONFLICT (log_index) DO UPDATE SET
  term = excluded.term,
  log_type = excluded.log_type,
  data = excluded.data,
  extensions = excluded.extensions,
  appended_at = excluded.appended_at;
`

	queryRaftStableGet = `SELECT value
FROM raft_stable_store
WHERE key = CASE WHEN ? IS NULL OR length(?) = 0 THEN zeroblob(0) ELSE ? END;
`

	queryRaftStableSet = `INSERT INTO raft_stable_store (key, value)
VALUES (CASE WHEN ? IS NULL OR length(?) = 0 THEN zeroblob(0) ELSE ? END,
        CASE WHEN ? IS NULL OR length(?) = 0 THEN zeroblob(0) ELSE ? END)
ON CONFLICT (key) DO UPDATE SET value = excluded.value;
`

	queryReadModel = `SELECT document FROM model_state WHERE singleton = 1;
`

	queryReadRow = `SELECT document FROM model_rows WHERE tenant = ? AND site = ? AND entity = ? AND record_id = ?;
`

	queryReadSnapshotModel = `SELECT model_document FROM migration_snapshot WHERE singleton = 1;
`

	queryRestoreModel = `UPDATE model_state SET document = ?, epoch = epoch + 1 WHERE singleton = 1;
`

	queryRestoreRows = `DELETE FROM model_rows;
INSERT INTO model_rows (tenant, site, entity, record_id, document)
SELECT tenant, site, entity, record_id, document FROM migration_snapshot_rows;
`

	querySaveSnapshotModel = `INSERT INTO migration_snapshot (singleton, model_document) VALUES (1, ?)
ON CONFLICT(singleton) DO UPDATE SET model_document = excluded.model_document;
`

	querySchema = `CREATE TABLE IF NOT EXISTS model_state (
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
CREATE TABLE IF NOT EXISTS raft_logs (
  log_index INTEGER PRIMARY KEY CHECK (log_index > 0),
  term INTEGER NOT NULL CHECK (term >= 0),
  log_type INTEGER NOT NULL CHECK (log_type >= 0 AND log_type <= 255),
  data BLOB NOT NULL,
  extensions BLOB NOT NULL,
  appended_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raft_stable_store (
  key BLOB NOT NULL PRIMARY KEY,
  value BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS write_ledger (
  tenant TEXT NOT NULL,
  site TEXT NOT NULL,
  write_id TEXT NOT NULL,
  outcome BLOB NOT NULL,
  seq INTEGER NOT NULL CHECK (seq > 0),
  PRIMARY KEY (tenant, site, write_id)
);
CREATE INDEX IF NOT EXISTS write_ledger_seq ON write_ledger (seq);
CREATE TABLE IF NOT EXISTS raft_fsm_state (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  last_applied_index INTEGER NOT NULL DEFAULT 0 CHECK (last_applied_index >= 0)
);
INSERT OR IGNORE INTO raft_fsm_state (singleton, last_applied_index) VALUES (1, 0);
CREATE TABLE IF NOT EXISTS migration_audit (
  audit_seq INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  site TEXT NOT NULL,
  group_name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('migrate', 'rollback')),
  write_id TEXT NOT NULL,
  epoch_before INTEGER NOT NULL CHECK (epoch_before >= 1),
  epoch_after INTEGER NOT NULL CHECK (epoch_after >= 1),
  fingerprint_before TEXT NOT NULL,
  fingerprint_after TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS migration_audit_scope_seq ON migration_audit (tenant, site, audit_seq);
`

	querySelectEntityRows = `SELECT tenant, site, record_id, document FROM model_rows WHERE entity = ?;
`

	querySelectScopedEntityRows = `SELECT record_id, document FROM model_rows
WHERE tenant = ? AND site = ? AND entity = ?;
`

	querySnapshotMaxEntryBytes = `SELECT MAX(entry_bytes)
FROM (
  SELECT length(document) AS entry_bytes FROM model_state
  UNION ALL SELECT length(model_document) FROM migration_snapshot
  UNION ALL SELECT length(document) + 6 * (
    length(CAST(tenant AS BLOB)) + length(CAST(site AS BLOB)) +
    length(CAST(entity AS BLOB)) + length(CAST(record_id AS BLOB))
  ) + 128 FROM model_rows
  UNION ALL SELECT length(document) + 6 * (
    length(CAST(tenant AS BLOB)) + length(CAST(site AS BLOB)) +
    length(CAST(entity AS BLOB)) + length(CAST(record_id AS BLOB))
  ) + 128 FROM migration_snapshot_rows
  UNION ALL SELECT length(outcome) + 6 * (
    length(CAST(tenant AS BLOB)) + length(CAST(site AS BLOB)) +
    length(CAST(write_id AS BLOB))
  ) + 128 FROM write_ledger
);
`

	queryUpdateModel = `UPDATE model_state SET document = ?, epoch = epoch + 1 WHERE singleton = 1;
`

	queryUpdateRow = `UPDATE model_rows SET document = ? WHERE tenant = ? AND site = ? AND entity = ? AND record_id = ?;
`
)
