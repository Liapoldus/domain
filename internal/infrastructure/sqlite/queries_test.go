package sqlite

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

// These digests and lengths were captured from all 40 SQL assets in the
// approved dirty worktree before relocation, including schema/import_reset WIP.
// Keep whitespace, final newlines, statement order and positional parameters.
func TestSQLPreservation(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		digest     string
		bytes      int
		parameters int
	}{
		{"capture_snapshot.sql", queryCaptureSnapshot, "8e265ee0051bb330e1db66d0c9065a9e8b9b34cb80c4cd1daa31caa4e8368252", 183, 0},
		{"clear_snapshot.sql", queryClearSnapshot, "dac0b58b688cdca5e11f1fc09f4df55d028edb1b90e6155ba88bb45c7d6ffd35", 69, 0},
		{"delete_row.sql", queryDeleteRow, "0d0b62e3cf90aa84509f2b2cefd72713075dc94527030287376934f6a09351a0", 87, 4},
		{"export_audit.sql", queryExportAudit, "09b2417ed0064a082213e71ccf9442bdb51cb9bdf1df736fc4d62606134bf74a", 168, 0},
		{"export_ledger.sql", queryExportLedger, "36bce10d6503b3e473f270acb7b1f9f296f3c7a26f8ec176332cf2adba323d33", 100, 0},
		{"export_model.sql", queryExportModel, "32016b4fac795e1f1844d8ce1556537008d5e432a37f955b5186fb6de753a586", 61, 0},
		{"export_rows.sql", queryExportRows, "8daa34e2516cbe5b1972002e1e42bbea47de335acbe6c7914e807d04db439626", 107, 0},
		{"export_snapshot_rows.sql", queryExportSnapshotRows, "96caa198038a2fecf6078d2362ca978cb9167868379ca503e7588cfc6394fbf6", 120, 0},
		{"import_model.sql", queryImportModel, "8bc9b23222e788057092957f0b7fd35b8846f824b31f36e2c1740f117903a612", 71, 2},
		{"import_reset.sql", queryImportReset, "a45360d4696de5e5a4508f6944cccfe1e45f665fdf2d273f7a60e4ca27046e36", 173, 0},
		{"import_snapshot_row.sql", queryImportSnapshotRow, "71ca20f0c64620c5c2284c69e60e949d8da9be77ed2a6da56b770944db8057c7", 104, 5},
		{"insert_model.sql", queryInsertModel, "47314e9430f399c1169d7be53764f2205315ac538596c86d4549a0d25343202a", 61, 1},
		{"insert_row.sql", queryInsertRow, "e301c5591ad2868d6eb529c1946f64f256d91722c153fdcff8db2051da0f34ca", 91, 5},
		{"ledger_get.sql", queryLedgerGet, "003920e43723d879ff920a5ac6dbe263a470c35318c892ee78c5666223d682ba", 86, 3},
		{"ledger_insert.sql", queryLedgerInsert, "be071d153ca439d8985e6f63b252eb62ca7c79fef9cce9ad15d76f1ccb6188bf", 88, 5},
		{"ledger_prune.sql", queryLedgerPrune, "5de2e721fdc46c4b1913728752c28ffdad87ae993d34eaf0aefe9749ec48c2f7", 125, 0},
		{"migration_audit_import.sql", queryMigrationAuditImport, "b7c4cac02068b74fc4a8cadaa5da5f65ed83b3307fcaf3da5abaa6be9cb37dd3", 193, 10},
		{"migration_audit_insert.sql", queryMigrationAuditInsert, "64f582af72a26dfeb5aae9d8ad05d7ee8ebb234b6c3bac01608bd6fc25bb591a", 242, 9},
		{"migration_audit_prune.sql", queryMigrationAuditPrune, "7982774c195ff119490ba63deff5a1ce264da07c4571aa745bac209960ed3d7a", 154, 0},
		{"migration_audit_recent.sql", queryMigrationAuditRecent, "652f5395b0fcd4bec4407f08ad590e7119eec4d2ff88d087bcb3eaae86057c1b", 172, 3},
		{"raft_fsm_applied_get.sql", queryRaftFsmAppliedGet, "0a6084212e28fe46ca6693d622cec00d5eec0a75bfd1159bf9f8f8bb65b3d1d4", 67, 0},
		{"raft_fsm_applied_set.sql", queryRaftFsmAppliedSet, "7fe3c2312363f1dbf0fa77c6a8b2b13171a8d6a3cc80c5ef5f58cdb9b0c0e9e3", 70, 1},
		{"raft_log_bounds.sql", queryRaftLogBounds, "11887e7091203fe2385bdb52834c8a6ccb4cf13132b3533941f30033cd719b73", 80, 0},
		{"raft_log_delete.sql", queryRaftLogDelete, "854a707672386dac36087d9a631249f1654c6ac1850b3cea3883760c87adc424", 55, 2},
		{"raft_log_get.sql", queryRaftLogGet, "40154ad97255fc23411e1581c6c44caaebedb8f1bd8c0cd31620ec0513281899", 100, 1},
		{"raft_log_store.sql", queryRaftLogStore, "c8c80a1a608ec3894f3a90b0b375a49f9f7cdb67d74bb5a47642a89aca1f880d", 433, 10},
		{"raft_stable_get.sql", queryRaftStableGet, "0de254ec38a892afab91f87f7a9f81be904828609e415ff646ada2eca8ea7850", 114, 3},
		{"raft_stable_set.sql", queryRaftStableSet, "36162ec25e591b2a537882780d1a3f08a4de07a144d16fd49802e8b080c7ed93", 247, 6},
		{"read_model.sql", queryReadModel, "a7c4cffc97bcf3706a9cd3926370d6561b45613efef0a3952bf6857161baaf79", 54, 0},
		{"read_row.sql", queryReadRow, "20de024bc8d12214e1d5b5409b80a3aab493863825078a9badd3aa7704974bfc", 96, 4},
		{"read_snapshot_model.sql", queryReadSnapshotModel, "a05118a6f9b71f0f6a0d1b6d5989a4e439c66cf46eac77d639030a60ea7a6ab4", 67, 0},
		{"restore_model.sql", queryRestoreModel, "e337e42425a284651989a9e398bcf6a4086c048ac54c39b4b79c3ec2f47a94bc", 76, 1},
		{"restore_rows.sql", queryRestoreRows, "c6a3df07bfbb42d6d5d1c115bc02db44c8741c1a33ea4b0930276b95a92ed70c", 170, 0},
		{"save_snapshot_model.sql", querySaveSnapshotModel, "8f069086116b797654c95de5f0576a1d1659a986a32c10f5bdb6aa19e3a483d8", 152, 1},
		{"schema.sql", querySchema, "3d2125a9e6ea7c9381eaa1ee249a4ca95b7d4660cc28fd5d6c4e5355a62dc49e", 2214, 0},
		{"select_entity_rows.sql", querySelectEntityRows, "ae7a6ef2aa1a867261e9fb72a29fe0053dbd8540c3ef0d813f8754fb43e95e73", 75, 1},
		{"select_scoped_entity_rows.sql", querySelectScopedEntityRows, "4cdf8aefe2bfe193297ce28de3c617548922fca697935bf088442feece47f685", 89, 3},
		{"snapshot_max_entry_bytes.sql", querySnapshotMaxEntryBytes, "7dd2424fe730ff312fa5b403251fada48264110b1f64904927608f7717cb1058", 743, 0},
		{"update_model.sql", queryUpdateModel, "e337e42425a284651989a9e398bcf6a4086c048ac54c39b4b79c3ec2f47a94bc", 76, 1},
		{"update_row.sql", queryUpdateRow, "fd711283609fa54291f5502a0e49050cf5cd97a3b5e6ca2fea5290e64fae888d", 99, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.query) != tc.bytes {
				t.Errorf("SQL byte length = %d, want %d", len(tc.query), tc.bytes)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256([]byte(tc.query))); got != tc.digest {
				t.Error("SQL bytes differ from pre-migration source")
			}
			if got := strings.Count(tc.query, "?"); got != tc.parameters {
				t.Errorf("positional parameters = %d, want %d", got, tc.parameters)
			}
		})
	}
}
