package raftfsm

import (
	"context"
	"errors"
	"time"

	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

var ErrFreshReadUnavailable = errors.New("fresh Domain read is unavailable")

// CommitReadBarrier asks the current leader to establish a quorum-backed Raft
// barrier and returns the leader's durable Domain FSM index. The internal Raft
// barrier record itself is not product state and is not assumed to be
// materialized by every FSM. Callers must not serve a read if this fails.
func CommitReadBarrier(ctx context.Context, leader *raft.Raft, leaderStore *modelsqlite.Store, deadline time.Duration) (uint64, error) {
	if ctx == nil || leader == nil || leaderStore == nil || deadline <= 0 {
		return 0, ErrFreshReadUnavailable
	}
	bounded, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	if err := awaitFuture(bounded, leader.VerifyLeader()); err != nil {
		return 0, ErrFreshReadUnavailable
	}
	if err := awaitFuture(bounded, leader.Barrier(deadline)); err != nil {
		return 0, ErrFreshReadUnavailable
	}
	index, err := leaderStore.AppliedRaftIndex(bounded)
	if err != nil {
		return 0, ErrFreshReadUnavailable
	}
	return index, nil
}

// WaitForAppliedIndex fences reads on a selected replica until its durable FSM
// materialization has applied the barrier committed by the leader. It returns
// unavailable on deadline or store failure rather than exposing stale SQLite
// state.
func WaitForAppliedIndex(ctx context.Context, store *modelsqlite.Store, required uint64, pollInterval time.Duration) error {
	if ctx == nil || store == nil || pollInterval <= 0 {
		return ErrFreshReadUnavailable
	}
	for {
		applied, err := store.AppliedRaftIndex(ctx)
		if err != nil {
			return ErrFreshReadUnavailable
		}
		if applied >= required {
			return nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ErrFreshReadUnavailable
		case <-timer.C:
		}
	}
}

// RequireFreshRead composes the leader barrier and selected-replica apply
// fence. The source must be the current leader's Raft handle; the target can be
// another replica. Product handlers perform the actual data read only after it
// returns successfully.
func RequireFreshRead(ctx context.Context, leader *raft.Raft, leaderStore, selected *modelsqlite.Store, deadline, pollInterval time.Duration) (uint64, error) {
	index, err := CommitReadBarrier(ctx, leader, leaderStore, deadline)
	if err != nil {
		return 0, ErrFreshReadUnavailable
	}
	bounded, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	if err := WaitForAppliedIndex(bounded, selected, index, pollInterval); err != nil {
		return 0, ErrFreshReadUnavailable
	}
	return index, nil
}

func awaitFuture(ctx context.Context, future raft.Future) error {
	if future == nil {
		return ErrFreshReadUnavailable
	}
	completed := make(chan error, 1)
	go func() { completed <- future.Error() }()
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
