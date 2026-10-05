import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

describe('Raft FSM materialization boundary', () => {
  it('applies a committed migration and restores the same SQLite state from snapshot', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/raft-fsm-probe'], {
      cwd: root, env: { ...process.env, GOWORK: 'off' }, encoding: 'utf8',
    });
    expect(JSON.parse(output)).toMatchObject({
      applied: true,
      restored: { id: 'a', heading: 'hello' },
    });
  });
});
