package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

// snapshotFormatVersion is the durable snapshot schema version carried by
// every exported snapshot from this build.
const snapshotFormatVersion = 2

type storedRow struct {
	Tenant string          `json:"tenant"`
	Site   string          `json:"site"`
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Data   json.RawMessage `json:"data"`
}

type persistedState struct {
	FormatVersion    int                  `json:"formatVersion,omitempty"`
	Model            json.RawMessage      `json:"model"`
	Epoch            int64                `json:"epoch"`
	RaftAppliedIndex *uint64              `json:"raftAppliedIndex"`
	Rows             []storedRow          `json:"rows"`
	Ledger           []ledgerEntry        `json:"ledger,omitempty"`
	Audit            []auditSnapshotEntry `json:"audit,omitempty"`
	SnapshotModel    json.RawMessage      `json:"snapshotModel,omitempty"`
	SnapshotRows     []storedRow          `json:"snapshotRows,omitempty"`
}

// ExportTo writes a consistent materialized state without collecting the full
// dataset in memory. The caller owns writer. maximumBytes includes JSON framing.
func (store *Store) ExportTo(ctx context.Context, writer io.Writer, maximumBytes int64) (retErr error) {
	if maximumBytes <= 0 || writer == nil {
		return ErrInvalidRow
	}
	bounded := &snapshotLimitWriter{writer: writer, maximum: maximumBytes}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()

	query := querySnapshotMaxEntryBytes
	var largest sql.NullInt64
	if err := tx.QueryRowContext(ctx, query).Scan(&largest); err != nil {
		return err
	}
	if largest.Valid && largest.Int64 > maximumBytes {
		return ErrSnapshotTooLarge
	}

	modelQuery := queryExportModel
	var model []byte
	var epoch int64
	if err := tx.QueryRowContext(ctx, modelQuery).Scan(&model, &epoch); err != nil {
		return err
	}
	if !json.Valid(model) || epoch <= 0 {
		return ErrInvalidRow
	}
	appliedQuery := queryRaftFsmAppliedGet
	var appliedIndex int64
	if err := tx.QueryRowContext(ctx, appliedQuery).Scan(&appliedIndex); err != nil || appliedIndex < 0 {
		return ErrCorruptRaftStorage
	}
	if err := writeSnapshotPrefix(bounded, model, epoch, uint64(appliedIndex)); err != nil {
		return err
	}
	rowsQuery := queryExportRows
	if err := writeSnapshotRows(ctx, tx, bounded, rowsQuery); err != nil {
		return err
	}
	if err := writeSnapshotLedger(ctx, tx, bounded); err != nil {
		return err
	}
	if err := writeSnapshotAudit(ctx, tx, bounded); err != nil {
		return err
	}

	modelQuery = queryReadSnapshotModel
	var snapshotModel []byte
	err = tx.QueryRowContext(ctx, modelQuery).Scan(&snapshotModel)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := writeSnapshotMiddle(bounded, snapshotModel); err != nil {
		return err
	}
	if err := writeSnapshotSuffix(bounded); err != nil {
		return err
	}
	snapshotRowsQuery := queryExportSnapshotRows
	if err := writeSnapshotRows(ctx, tx, bounded, snapshotRowsQuery); err != nil {
		return err
	}
	if err := writeSnapshotEnd(bounded); err != nil {
		return err
	}
	return tx.Commit()
}

func writeSnapshotPrefix(writer io.Writer, model []byte, epoch int64, appliedIndex uint64) error {
	if _, err := io.WriteString(writer, `{"formatVersion":`+strconv.Itoa(snapshotFormatVersion)+`,"model":`); err != nil {
		return err
	}
	if _, err := writer.Write(model); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, `,"epoch":`+strconv.FormatInt(epoch, 10)+`,"raftAppliedIndex":`+strconv.FormatUint(appliedIndex, 10)+`,"rows":[`); err != nil {
		return err
	}
	return nil
}

func writeSnapshotRows(ctx context.Context, tx *sql.Tx, writer io.Writer, query string) (retErr error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	first := true
	for rows.Next() {
		var entry storedRow
		if err := rows.Scan(&entry.Tenant, &entry.Site, &entry.Entity, &entry.ID, &entry.Data); err != nil {
			return err
		}
		if !json.Valid(entry.Data) {
			return ErrInvalidRow
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
	return rows.Err()
}

// writeSnapshotLedger closes the rows array and writes the full writeId
// ledger. The ledger key is always present, even when empty.
func writeSnapshotLedger(ctx context.Context, tx *sql.Tx, writer io.Writer) (retErr error) {
	if _, err := io.WriteString(writer, `],"ledger":[`); err != nil {
		return err
	}
	query := queryExportLedger
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	first := true
	for rows.Next() {
		var entry ledgerEntry
		var outcome []byte
		if err := rows.Scan(&entry.Tenant, &entry.Site, &entry.WriteID, &outcome, &entry.Seq); err != nil {
			return err
		}
		if entry.Seq <= 0 || len(outcome) == 0 || !json.Valid(outcome) {
			return ErrInvalidRow
		}
		entry.Outcome = outcome
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
	if _, err := io.WriteString(writer, `]`); err != nil {
		return err
	}
	return nil
}

func writeSnapshotMiddle(writer io.Writer, snapshotModel []byte) error {
	if len(snapshotModel) == 0 {
		return nil
	}
	if !json.Valid(snapshotModel) {
		return ErrInvalidRow
	}
	if _, err := io.WriteString(writer, `,"snapshotModel":`); err != nil {
		return err
	}
	_, err := writer.Write(snapshotModel)
	return err
}

func writeSnapshotSuffix(writer io.Writer) error {
	if _, err := io.WriteString(writer, `,"snapshotRows":[`); err != nil {
		return err
	}
	return nil
}

func writeSnapshotEnd(writer io.Writer) error {
	_, err := io.WriteString(writer, `]}`)
	return err
}

type snapshotLimitWriter struct {
	writer  io.Writer
	maximum int64
	written int64
}

func (writer *snapshotLimitWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > writer.maximum-writer.written {
		return 0, ErrSnapshotTooLarge
	}
	n, err := writer.writer.Write(data)
	writer.written += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

// Import replaces a local materialization atomically. The input must be an
// authenticated Raft snapshot; callers cannot use this as a public data API.
func (store *Store) Import(ctx context.Context, data []byte) (retErr error) {
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil || !json.Valid(state.Model) || state.Epoch <= 0 || state.RaftAppliedIndex == nil {
		return ErrInvalidRow
	}
	if state.FormatVersion < 0 || state.FormatVersion > snapshotFormatVersion {
		return ErrInvalidRow
	}
	if err := validateLedger(state.Ledger); err != nil {
		return err
	}
	if err := validateAudit(state.Audit); err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()
	resetQuery := queryImportReset
	if _, err := tx.ExecContext(ctx, resetQuery); err != nil {
		return err
	}
	modelQuery := queryImportModel
	if _, err := tx.ExecContext(ctx, modelQuery, []byte(state.Model), state.Epoch); err != nil {
		return err
	}
	if *state.RaftAppliedIndex > uint64(^uint64(0)>>1) {
		return ErrCorruptRaftStorage
	}
	applyIndexQuery := queryRaftFsmAppliedSet
	if _, err := tx.ExecContext(ctx, applyIndexQuery, int64(*state.RaftAppliedIndex)); err != nil {
		return err
	}
	rowQuery := queryInsertRow
	for _, row := range state.Rows {
		if err := importRow(ctx, tx, rowQuery, row); err != nil {
			return err
		}
	}
	for _, entry := range state.Ledger {
		if err := ledgerInsertTx(ctx, tx, entry.Tenant, entry.Site, entry.WriteID, []byte(entry.Outcome), entry.Seq); err != nil {
			return err
		}
	}
	for _, entry := range state.Audit {
		if err := auditImportTx(ctx, tx, entry); err != nil {
			return err
		}
	}
	if len(state.SnapshotModel) != 0 {
		if !json.Valid(state.SnapshotModel) {
			return ErrInvalidRow
		}
		snapshotModelQuery := querySaveSnapshotModel
		if _, err := tx.ExecContext(ctx, snapshotModelQuery, []byte(state.SnapshotModel)); err != nil {
			return err
		}
	}
	snapshotRowQuery := queryImportSnapshotRow
	for _, row := range state.SnapshotRows {
		if err := importRow(ctx, tx, snapshotRowQuery, row); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ImportFrom atomically restores a snapshot while decoding rows incrementally.
// The caller must enforce the byte limit on reader before invoking it.
func (store *Store) ImportFrom(ctx context.Context, reader io.Reader) (retErr error) {
	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return ErrInvalidRow
	}
	key, err := nextSnapshotKey(decoder)
	if err != nil {
		return ErrInvalidRow
	}
	if key == "formatVersion" {
		var formatVersion int
		if err := decoder.Decode(&formatVersion); err != nil || formatVersion < 0 || formatVersion > snapshotFormatVersion {
			return ErrInvalidRow
		}
		key, err = nextSnapshotKey(decoder)
		if err != nil {
			return ErrInvalidRow
		}
	}
	if key != "model" {
		return ErrInvalidRow
	}
	var model json.RawMessage
	if err := decoder.Decode(&model); err != nil || !json.Valid(model) {
		return ErrInvalidRow
	}
	if err := expectSnapshotKey(decoder, "epoch"); err != nil {
		return err
	}
	var epoch int64
	if err := decoder.Decode(&epoch); err != nil || epoch <= 0 {
		return ErrInvalidRow
	}
	if err := expectSnapshotKey(decoder, "raftAppliedIndex"); err != nil {
		return err
	}
	var appliedIndex uint64
	if err := decoder.Decode(&appliedIndex); err != nil || appliedIndex > uint64(^uint64(0)>>1) {
		return ErrInvalidRow
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()
	resetQuery := queryImportReset
	if _, err := tx.ExecContext(ctx, resetQuery); err != nil {
		return err
	}
	modelQuery := queryImportModel
	if _, err := tx.ExecContext(ctx, modelQuery, []byte(model), epoch); err != nil {
		return err
	}
	applyIndexQuery := queryRaftFsmAppliedSet
	if _, err := tx.ExecContext(ctx, applyIndexQuery, int64(appliedIndex)); err != nil {
		return err
	}
	rowQuery := queryInsertRow
	if err := expectSnapshotKey(decoder, "rows"); err != nil {
		return err
	}
	if err := decodeSnapshotRows(ctx, decoder, tx, rowQuery); err != nil {
		return err
	}
	key, err = nextSnapshotKey(decoder)
	if err != nil {
		return ErrInvalidRow
	}
	if key == "ledger" {
		if err := decodeSnapshotLedger(ctx, decoder, tx); err != nil {
			return err
		}
		key, err = nextSnapshotKey(decoder)
		if err != nil {
			return ErrInvalidRow
		}
	}
	if key == "audit" {
		if err := decodeSnapshotAudit(ctx, decoder, tx); err != nil {
			return err
		}
		key, err = nextSnapshotKey(decoder)
		if err != nil {
			return ErrInvalidRow
		}
	}
	if key == "snapshotModel" {
		var snapshotModel json.RawMessage
		if err := decoder.Decode(&snapshotModel); err != nil || !json.Valid(snapshotModel) {
			return ErrInvalidRow
		}
		snapshotModelQuery := querySaveSnapshotModel
		if _, err := tx.ExecContext(ctx, snapshotModelQuery, []byte(snapshotModel)); err != nil {
			return err
		}
		key, err = nextSnapshotKey(decoder)
		if err != nil {
			return ErrInvalidRow
		}
	}
	if key != "snapshotRows" {
		return ErrInvalidRow
	}
	snapshotRowQuery := queryImportSnapshotRow
	if err := decodeSnapshotRows(ctx, decoder, tx, snapshotRowQuery); err != nil {
		return err
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return ErrInvalidRow
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidRow
	}
	return tx.Commit()
}

func expectSnapshotKey(decoder *json.Decoder, expected string) error {
	key, err := nextSnapshotKey(decoder)
	if err != nil || key != expected {
		return ErrInvalidRow
	}
	return nil
}

func nextSnapshotKey(decoder *json.Decoder) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	key, ok := token.(string)
	if !ok {
		return "", ErrInvalidRow
	}
	return key, nil
}

func decodeSnapshotRows(ctx context.Context, decoder *json.Decoder, tx *sql.Tx, query string) error {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return ErrInvalidRow
	}
	for decoder.More() {
		var row storedRow
		if err := decoder.Decode(&row); err != nil {
			return ErrInvalidRow
		}
		if err := importRow(ctx, tx, query, row); err != nil {
			return err
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim(']') {
		return ErrInvalidRow
	}
	return nil
}

// decodeSnapshotLedger validates every streamed ledger entry and inserts it
// inside the caller's transaction, pruning to capacity after each insert.
func decodeSnapshotLedger(ctx context.Context, decoder *json.Decoder, tx *sql.Tx) error {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return ErrInvalidRow
	}
	seen := make(map[ledgerKey]struct{})
	for decoder.More() {
		var entry ledgerEntry
		if err := decoder.Decode(&entry); err != nil {
			return ErrInvalidRow
		}
		if err := validateLedgerEntry(entry); err != nil {
			return err
		}
		key := ledgerKey{tenant: entry.Tenant, site: entry.Site, writeID: entry.WriteID}
		if _, duplicate := seen[key]; duplicate {
			return ErrInvalidRow
		}
		seen[key] = struct{}{}
		if err := ledgerInsertTx(ctx, tx, entry.Tenant, entry.Site, entry.WriteID, []byte(entry.Outcome), entry.Seq); err != nil {
			return err
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim(']') {
		return ErrInvalidRow
	}
	return nil
}

func importRow(ctx context.Context, tx *sql.Tx, query string, row storedRow) error {
	if row.Tenant == "" || row.Site == "" || row.Entity == "" || row.ID == "" || !json.Valid(row.Data) {
		return ErrInvalidRow
	}
	_, err := tx.ExecContext(ctx, query, row.Tenant, row.Site, row.Entity, row.ID, []byte(row.Data))
	return err
}
