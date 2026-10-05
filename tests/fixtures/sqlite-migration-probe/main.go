package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"

	"github.com/Liapoldus/domain/internal/domain/models"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
)

func main() {
	if len(os.Args) != 2 {
		os.Exit(2)
	}
	var request struct {
		Previous models.Model    `json:"previous"`
		Next     models.Model    `json:"next"`
		Row      json.RawMessage `json:"row"`
		Rollback bool            `json:"rollback"`
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		os.Exit(2)
	}
	ctx := context.Background()
	store, err := modelsqlite.Open(ctx, os.Args[1])
	if err != nil {
		os.Exit(2)
	}
	if err := store.InitModel(ctx, request.Previous); err != nil {
		os.Exit(2)
	}
	if err := store.PutRow(ctx, "tenant-a", "site-1", "entries", "a", request.Row); err != nil {
		os.Exit(2)
	}
	applyErr := store.ApplyMigration(ctx, request.Previous, request.Next)
	rolledBack := false
	laterWritePresent := false
	if request.Rollback && applyErr == nil {
		if err := store.PutRow(ctx, "tenant-a", "site-1", "entries", "b", []byte(`{"id":"b","heading":"later"}`)); err != nil {
			os.Exit(2)
		}
		rolledBack = store.RollbackMigration(ctx, request.Previous) == nil
		_, err := store.ReadRow(ctx, "tenant-a", "site-1", "entries", "b")
		laterWritePresent = !errors.Is(err, sql.ErrNoRows)
	}
	store.Close()
	store, err = modelsqlite.Open(ctx, os.Args[1])
	if err != nil {
		os.Exit(2)
	}
	defer store.Close()
	row, err := store.ReadRow(ctx, "tenant-a", "site-1", "entries", "a")
	if err != nil {
		os.Exit(2)
	}
	response := struct {
		Applied           bool            `json:"applied"`
		Row               json.RawMessage `json:"row"`
		RolledBack        bool            `json:"rolledBack,omitempty"`
		LaterWritePresent bool            `json:"laterWritePresent"`
	}{Applied: applyErr == nil, Row: row, RolledBack: rolledBack, LaterWritePresent: laterWritePresent}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		os.Exit(2)
	}
}
