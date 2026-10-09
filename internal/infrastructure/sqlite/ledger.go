package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type ledgerEntry struct {
	Tenant  string          `json:"tenant"`
	Site    string          `json:"site"`
	WriteID string          `json:"writeId"`
	Outcome json.RawMessage `json:"outcome"`
	Seq     int64           `json:"seq"`
}

type ledgerKey struct {
	tenant  string
	site    string
	writeID string
}

type ledgerOutcome struct {
	Kind    string `json:"kind"`
	Entity  string `json:"entity"`
	ID      string `json:"id"`
	Applied int    `json:"applied"`
}

func validateLedgerEntry(entry ledgerEntry) error {
	if entry.Tenant == "" || entry.Site == "" || entry.WriteID == "" ||
		entry.Seq <= 0 || len(entry.Outcome) == 0 || !json.Valid(entry.Outcome) {
		return ErrInvalidRow
	}
	return nil
}

func validateLedger(entries []ledgerEntry) error {
	seen := make(map[ledgerKey]struct{}, len(entries))
	for _, entry := range entries {
		if err := validateLedgerEntry(entry); err != nil {
			return err
		}
		key := ledgerKey{tenant: entry.Tenant, site: entry.Site, writeID: entry.WriteID}
		if _, duplicate := seen[key]; duplicate {
			return ErrInvalidRow
		}
		seen[key] = struct{}{}
	}
	return nil
}

// ledgerContainsTx reports whether the scoped writeId already has a durable
// ledger record. A corrupt stored outcome is surfaced as corrupt storage.
func ledgerContainsTx(ctx context.Context, tx *sql.Tx, tenant, site, writeID string) (bool, error) {
	query := queryLedgerGet
	var outcome []byte
	var seq int64
	err := tx.QueryRowContext(ctx, query, tenant, site, writeID).Scan(&outcome, &seq)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if seq <= 0 || len(outcome) == 0 || !json.Valid(outcome) {
		return false, ErrCorruptRaftStorage
	}
	return true, nil
}

// ledgerRecordTx durably records one applied writeId outcome and prunes the
// ledger back to its capacity inside the same transaction.
func ledgerRecordTx(ctx context.Context, tx *sql.Tx, tenant, site, writeID string, outcome ledgerOutcome, seq int64) error {
	document, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	return ledgerInsertTx(ctx, tx, tenant, site, writeID, document, seq)
}

func ledgerInsertTx(ctx context.Context, tx *sql.Tx, tenant, site, writeID string, outcome []byte, seq int64) error {
	insertQuery := queryLedgerInsert
	if _, err := tx.ExecContext(ctx, insertQuery, tenant, site, writeID, outcome, seq); err != nil {
		return err
	}
	return ledgerPruneTx(ctx, tx)
}

func ledgerPruneTx(ctx context.Context, tx *sql.Tx) error {
	query := queryLedgerPrune
	_, err := tx.ExecContext(ctx, query)
	return err
}
