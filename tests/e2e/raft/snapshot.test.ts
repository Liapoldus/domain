import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

describe('bounded Raft snapshots', () => {
  it('restores a compacted cluster from a persisted snapshot after restart', () => {
    const stateDir = mkdtempSync(join(tmpdir(), 'domain-raft-snapshot-test-'));
    try {
      execFileSync('go', ['run', './tests/fixtures/raft/snapshot', 'prepare', stateDir], {
        cwd: root,
        env: { ...process.env, GOWORK: 'off' },
        encoding: 'utf8',
        timeout: 60_000,
      });

      const output = execFileSync('go', ['run', './tests/fixtures/raft/snapshot', 'recover', stateDir], {
        cwd: root,
        env: { ...process.env, GOWORK: 'off' },
        encoding: 'utf8',
        timeout: 60_000,
      });

      expect(JSON.parse(output)).toEqual({
        snapshotCreated: true,
        logsCompacted: true,
        restartRestoredSnapshot: true,
        snapshotRestoredAppliedIndex: true,
        snapshotLimitRejected: true,
        failedRestorePreservedState: true,
      });
    } finally {
      rmSync(stateDir, { recursive: true, force: true });
    }
  }, 65_000);
});
