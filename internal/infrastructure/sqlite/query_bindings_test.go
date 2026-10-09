package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/hashicorp/raft"
)

func openQueryTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestScopedQueryBindings(t *testing.T) {
	store := openQueryTestStore(t)
	ctx := context.Background()
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{
		Name: "items", OwnerGroup: "owner", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "value", Type: "text"},
		},
	}}}
	if err := store.InitModel(ctx, model); err != nil {
		t.Fatal(err)
	}
	// Quotes and SQL-looking text must stay bound data in every scope position.
	scopes := [][2]string{{"tenant' OR 1=1 --", "site; DELETE FROM model_rows;"}, {"other", "site; DELETE FROM model_rows;"}, {"tenant' OR 1=1 --", "other"}}
	id := "id' OR 1=1 --"
	for i, scope := range scopes {
		document, err := json.Marshal(map[string]string{"id": id, "value": scope[0] + scope[1]})
		if err != nil {
			t.Fatal(err)
		}
		command := models.RaftCommand{Kind: "create", Tenant: scope[0], Site: scope[1], Group: "owner", Entity: "items", ID: id, Row: document}
		if err := store.ApplyRaftEntry(ctx, uint64(i+1), &command); err != nil {
			t.Fatal(err)
		}
	}
	document, err := json.Marshal(map[string]string{"id": id, "value": "updated' ?;"})
	if err != nil {
		t.Fatal(err)
	}
	command := models.RaftCommand{Kind: "update", Tenant: scopes[0][0], Site: scopes[0][1], Group: "owner", Entity: "items", ID: id, Row: document}
	if err := store.ApplyRaftEntry(ctx, 4, &command); err != nil {
		t.Fatal(err)
	}
	for i, scope := range scopes {
		row, err := store.ReadRow(ctx, scope[0], scope[1], "items", id)
		if err != nil {
			t.Fatal(err)
		}
		var values map[string]string
		if err := json.Unmarshal(row, &values); err != nil {
			t.Fatal(err)
		}
		want := scope[0] + scope[1]
		if i == 0 {
			want = "updated' ?;"
		}
		if values["value"] != want {
			t.Fatalf("scope %d changed unexpectedly", i)
		}
	}
	command.Kind = "delete"
	if err := store.ApplyRaftEntry(ctx, 5, &command); err != nil {
		t.Fatal(err)
	}
	for i, scope := range scopes {
		_, err := store.ReadRow(ctx, scope[0], scope[1], "items", id)
		if i == 0 && !errors.Is(err, sql.ErrNoRows) || i != 0 && err != nil {
			t.Fatalf("delete changed wrong scope %d", i)
		}
	}
}

func TestRaftRepeatedBlobBindings(t *testing.T) {
	store := openQueryTestStore(t)
	values := [][]byte{nil, {}, {0, 39, 63, 255}, []byte("?'; DELETE FROM raft_logs;")}
	for i, data := range values {
		entry := &raft.Log{Index: uint64(i + 1), Term: uint64(i + 10), Type: raft.LogCommand, Data: data, Extensions: values[len(values)-1-i], AppendedAt: time.Date(2026, 10, 9, 1, 2, i, 123, time.UTC)}
		if err := store.StoreLog(entry); err != nil {
			t.Fatal(err)
		}
		var got raft.Log
		if err := store.GetLog(entry.Index, &got); err != nil {
			t.Fatal(err)
		}
		if got.Index != entry.Index || got.Term != entry.Term || got.Type != entry.Type || !bytes.Equal(got.Data, entry.Data) || !bytes.Equal(got.Extensions, entry.Extensions) || !got.AppendedAt.Equal(entry.AppendedAt) {
			t.Fatalf("log %d bindings changed", i)
		}
		if err := store.Set(data, entry.Extensions); err != nil {
			t.Fatal(err)
		}
		value, err := store.Get(data)
		if err != nil || !bytes.Equal(value, entry.Extensions) {
			t.Fatalf("stable-store %d bindings changed", i)
		}
	}
	if err := store.DeleteRange(2, 3); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		var entry raft.Log
		err := store.GetLog(i, &entry)
		if i >= 2 && i <= 3 {
			if !errors.Is(err, raft.ErrLogNotFound) {
				t.Fatal("inclusive delete-range bindings changed")
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
