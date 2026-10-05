import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const base = {
  schemaVersion: '1', entities: [{ name: 'entries', ownerGroup: 'forms', fields: [
    { name: 'id', type: 'text', primaryKey: true, required: true },
    { name: 'title', type: 'text', required: true },
  ] }],
};

function scenario(next: unknown, row: Record<string, unknown>, rollback = false) {
  const directory = mkdtempSync(join(tmpdir(), 'model-sqlite-'));
  try {
    const output = execFileSync('go', ['run', './tests/fixtures/sqlite-migration-probe', join(directory, 'state.sqlite')], {
      cwd: root, env: { ...process.env, GOWORK: 'off' },
      input: JSON.stringify({ previous: base, next, row, rollback }), encoding: 'utf8',
    });
    return JSON.parse(output);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

describe('durable SQLite migration apply', () => {
  it('preserves rows across rename, new default, and process reopen', () => {
    const next = structuredClone(base) as typeof base & { migrations: unknown[] };
    next.entities[0].fields[1].name = 'heading';
    next.entities[0].fields.push({ name: 'published', type: 'bool', required: true, default: false } as never);
    next.migrations = [{ entity: 'entries', from: 'title', to: 'heading' }];
    expect(scenario(next, { id: 'a', title: 'hello' })).toMatchObject({
      applied: true, row: { id: 'a', heading: 'hello', published: false },
    });
  });

  it('keeps the old row when conversion preflight fails', () => {
    const next = structuredClone(base) as typeof base & { migrations: unknown[] };
    next.entities[0].fields[1].type = 'int64';
    next.migrations = [{ entity: 'entries', from: 'title', to: 'title', conversion: 'parseInt64' }];
    expect(scenario(next, { id: 'a', title: 'not-a-number' })).toMatchObject({
      applied: false, row: { id: 'a', title: 'not-a-number' },
    });
  });

  it('restores the pre-migration snapshot and drops later writes on rollback', () => {
    const next = structuredClone(base) as typeof base & { migrations: unknown[] };
    next.entities[0].fields[1].name = 'heading';
    next.migrations = [{ entity: 'entries', from: 'title', to: 'heading' }];
    expect(scenario(next, { id: 'a', title: 'before' }, true)).toMatchObject({
      applied: true, rolledBack: true, row: { id: 'a', title: 'before' },
      laterWritePresent: false,
    });
  });
});
