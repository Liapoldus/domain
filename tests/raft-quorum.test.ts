import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

describe('three-voter Raft quorum', () => {
  it('commits through leader failover and refuses writes without a majority', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/raft-quorum-probe'], {
      cwd: root, env: { ...process.env, GOWORK: 'off' }, encoding: 'utf8', timeout: 30_000,
    });
    expect(JSON.parse(output)).toEqual({ firstCommit: true, failoverCommit: true, minorityRefused: true });
  }, 35_000);
});
