package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/Liapoldus/domain/internal/domain/models"
)

type migrationAuditMeta struct {
	tenant  string
	site    string
	group   string
	writeID string
	kind    string
}

func auditMetaFor(command models.RaftCommand) *migrationAuditMeta {
	if command.WriteID == "" || command.Tenant == "" || command.Site == "" {
		return nil
	}
	return &migrationAuditMeta{
		tenant:  command.Tenant,
		site:    command.Site,
		group:   command.Group,
		writeID: command.WriteID,
		kind:    command.Kind,
	}
}

type auditSnapshotEntry struct {
	AuditSeq          int64  `json:"auditSeq"`
	Tenant            string `json:"tenant"`
	Site              string `json:"site"`
	Group             string `json:"group"`
	Kind              string `json:"kind"`
	WriteID           string `json:"writeId"`
	EpochBefore       int64  `json:"epochBefore"`
	EpochAfter        int64  `json:"epochAfter"`
	FingerprintBefore string `json:"fingerprintBefore"`
	FingerprintAfter  string `json:"fingerprintAfter"`
}

func validateAudit(entries []auditSnapshotEntry) error {
	seen := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		if entry.AuditSeq <= 0 || entry.Tenant == "" || entry.Site == "" || entry.WriteID == "" ||
			(entry.Kind != "migrate" && entry.Kind != "rollback") ||
			entry.EpochBefore < 1 || entry.EpochAfter < 1 ||
			entry.FingerprintBefore == "" || entry.FingerprintAfter == "" {
			return ErrInvalidRow
		}
		if _, duplicate := seen[entry.AuditSeq]; duplicate {
			return ErrInvalidRow
		}
		seen[entry.AuditSeq] = struct{}{}
	}
	return nil
}

func sha256Hex(document []byte) string {
	sum := sha256.Sum256(document)
	return hex.EncodeToString(sum[:])
}

func insertMigrationAuditTx(ctx context.Context, tx *sql.Tx, meta *migrationAuditMeta, epochBefore, epochAfter int64, documentBefore, documentAfter []byte) error {
	if meta == nil || meta.tenant == "" || meta.site == "" || meta.writeID == "" {
		return nil
	}
	query := queryMigrationAuditInsert
	_, err := tx.ExecContext(ctx, query,
		meta.tenant, meta.site, meta.group, meta.kind, meta.writeID,
		epochBefore, epochAfter, sha256Hex(documentBefore), sha256Hex(documentAfter),
	)
	if err != nil {
		return err
	}
	return migrationAuditPruneTx(ctx, tx)
}

func migrationAuditPruneTx(ctx context.Context, tx *sql.Tx) error {
	query := queryMigrationAuditPrune
	_, err := tx.ExecContext(ctx, query)
	return err
}

func auditImportTx(ctx context.Context, tx *sql.Tx, entry auditSnapshotEntry) error {
	query := queryMigrationAuditImport
	_, err := tx.ExecContext(ctx, query,
		entry.AuditSeq, entry.Tenant, entry.Site, entry.Group, entry.Kind, entry.WriteID,
		entry.EpochBefore, entry.EpochAfter, entry.FingerprintBefore, entry.FingerprintAfter,
	)
	return err
}

func writeSnapshotAudit(ctx context.Context, tx *sql.Tx, writer io.Writer) (retErr error) {
	if _, err := io.WriteString(writer, `,"audit":[`); err != nil {
		return err
	}
	query := queryExportAudit
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	first := true
	for rows.Next() {
		var entry auditSnapshotEntry
		if err := rows.Scan(&entry.AuditSeq, &entry.Tenant, &entry.Site, &entry.Group, &entry.Kind,
			&entry.WriteID, &entry.EpochBefore, &entry.EpochAfter, &entry.FingerprintBefore, &entry.FingerprintAfter); err != nil {
			return err
		}
		if err := validateAudit([]auditSnapshotEntry{entry}); err != nil {
			return err
		}
		if !first {
			if _, err := io.WriteString(writer, ","); err != nil {
				return err
			}
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if _, err := writer.Write(encoded); err != nil {
			return err
		}
		first = false
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = io.WriteString(writer, `]`)
	return err
}

func decodeSnapshotAudit(ctx context.Context, decoder *json.Decoder, tx *sql.Tx) error {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return ErrInvalidRow
	}
	for decoder.More() {
		var entry auditSnapshotEntry
		if err := decoder.Decode(&entry); err != nil {
			return ErrInvalidRow
		}
		if err := validateAudit([]auditSnapshotEntry{entry}); err != nil {
			return err
		}
		if err := auditImportTx(ctx, tx, entry); err != nil {
			return err
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim(']') {
		return ErrInvalidRow
	}
	return nil
}
