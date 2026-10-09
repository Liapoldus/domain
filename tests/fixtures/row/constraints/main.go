package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"os"
	"path/filepath"

	"github.com/Liapoldus/domain/internal/domain/models"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
)

type result struct {
	ValidRowsAccepted                        bool `json:"validRowsAccepted"`
	TypedPrimaryKeyAccepted                  bool `json:"typedPrimaryKeyAccepted"`
	MissingRequiredRejected                  bool `json:"missingRequiredRejected"`
	WrongTypeRejected                        bool `json:"wrongTypeRejected"`
	UniqueConflictRejected                   bool `json:"uniqueConflictRejected"`
	MissingReferenceRejected                 bool `json:"missingReferenceRejected"`
	UnknownFieldRejected                     bool `json:"unknownFieldRejected"`
	PrimaryKeyMismatchRejected               bool `json:"primaryKeyMismatchRejected"`
	TypedPrimaryKeyMismatchRejected          bool `json:"typedPrimaryKeyMismatchRejected"`
	RejectedEntriesDidNotAdvanceAppliedIndex bool `json:"rejectedEntriesDidNotAdvanceAppliedIndex"`
}

func main() {
	if err := run(); err != nil {
		check.Value(fmt.Fprintln(os.Stderr, "Domain row constraint probe failed"))
		os.Exit(1)
	}
}

func run() error {
	root, err := os.MkdirTemp("", "domain-row-constraints-")
	if err != nil {
		return err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	store, err := modelsqlite.Open(context.Background(), filepath.Join(root, "domain.sqlite"))
	if err != nil {
		return err
	}
	defer func() { check.Must(store.Close()) }()
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{
		{Name: "accounts", OwnerGroup: "identity", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "code", Type: "text", Required: true, Unique: true},
		}},
		{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "email", Type: "text", Required: true, Unique: true},
			{Name: "count", Type: "int64"},
			{Name: "accountCode", Type: "text", References: &models.Reference{Entity: "accounts", Field: "code"}},
		}},
		{Name: "counters", OwnerGroup: "forms", Fields: []models.Field{
			{Name: "id", Type: "int64", PrimaryKey: true, Required: true},
		}},
	}}
	if err := store.InitModel(context.Background(), model); err != nil {
		return errors.New("initialize test model")
	}
	if err := apply(store, 1, "put", "tenant-a", "site-a", "accounts", "account-1", `{"id":"account-1","code":"ACME"}`); err != nil {
		return errors.New("insert valid referenced account")
	}
	if err := apply(store, 2, "put", "tenant-a", "site-a", "entries", "entry-1", `{"id":"entry-1","email":"a@example.test","count":2,"accountCode":"ACME"}`); err != nil {
		return errors.New("insert valid row")
	}
	if err := apply(store, 3, "put", "tenant-a", "site-a", "counters", "int64:42", `{"id":42}`); err != nil {
		return errors.New("insert valid typed primary key")
	}
	invalid := []struct {
		id  string
		row string
	}{
		{"entry-2", `{"id":"entry-2","count":2}`},
		{"entry-3", `{"id":"entry-3","email":"b@example.test","count":"2"}`},
		{"entry-4", `{"id":"entry-4","email":"a@example.test","count":3}`},
		{"entry-5", `{"id":"entry-5","email":"c@example.test","accountCode":"MISSING"}`},
		{"entry-6", `{"id":"entry-6","email":"d@example.test","unexpected":true}`},
		{"entry-7", `{"id":"not-entry-7","email":"e@example.test"}`},
		{"int64:43", `{"id":42}`},
	}
	allRejected := true
	for index, item := range invalid {
		entity := "entries"
		if index == len(invalid)-1 {
			entity = "counters"
		}
		if apply(store, uint64(index+4), "put", "tenant-a", "site-a", entity, item.id, item.row) == nil {
			allRejected = false
		}
	}
	applied, err := store.AppliedRaftIndex(context.Background())
	if err != nil || applied != 3 || !allRejected {
		return errors.New("invalid rows advanced the durable applied index")
	}
	out := result{
		ValidRowsAccepted: true, TypedPrimaryKeyAccepted: true, MissingRequiredRejected: true, WrongTypeRejected: true,
		UniqueConflictRejected: true, MissingReferenceRejected: true,
		UnknownFieldRejected: true, PrimaryKeyMismatchRejected: true, TypedPrimaryKeyMismatchRejected: true,
		RejectedEntriesDidNotAdvanceAppliedIndex: true,
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func apply(store *modelsqlite.Store, index uint64, kind, tenant, site, entity, id, row string) error {
	command := &models.RaftCommand{Kind: kind, Tenant: tenant, Site: site, Entity: entity, ID: id, Row: json.RawMessage(row)}
	return store.ApplyRaftEntry(context.Background(), index, command)
}
