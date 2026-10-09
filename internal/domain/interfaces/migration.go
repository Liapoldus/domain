package interfaces

import (
	"context"
	"github.com/Liapoldus/domain/internal/domain/models"
)

// MigrationStore is the durable port the migration service reads revisions
// and audit records from and runs row preflight against.
type MigrationStore interface {
	CurrentRevision(ctx context.Context) (models.StoredRevision, error)
	SnapshotRevision(ctx context.Context) (models.StoredRevision, bool, error)
	PreflightMigration(ctx context.Context, previous models.StoredRevision, next models.Model) error
	RecentMigrations(ctx context.Context, tenant, site string, limit int) ([]models.MigrationAudit, error)
	AppliedRaftIndex(ctx context.Context) (uint64, error)
}
