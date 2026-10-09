import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';



import { root } from '../../support/paths.js';

describe('durable SQLite Raft storage', () => {
  it('persists logs and stable metadata across a full three-node cluster restart', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/raft/storage'], {
      cwd: root,
      env: { ...process.env, GOWORK: 'off' },
      encoding: 'utf8',
      timeout: 60_000,
    });

    expect(JSON.parse(output)).toEqual({
      firstCommit: true,
      restartRecoveredCommit: true,
      restartedNodeAppliedState: true,
      stableMetadataPreserved: true,
      logStoreOperations: true,
      emptyStableValueRoundTrip: true,
      corruptStableIntegerRejected: true,
      durableFailoverCommit: true,
      durableMinorityRefused: true,
      replayedRaftLogAcknowledged: true,
    });
  }, 65_000);
});
