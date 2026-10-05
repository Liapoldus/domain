import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

function plan(previous: unknown, next: unknown) {
  const output = execFileSync('go', ['run', './tests/fixtures/migration-probe'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    input: JSON.stringify({ previous, next }),
    encoding: 'utf8',
  });
  return JSON.parse(output);
}

const base = {
  schemaVersion: '1',
  entities: [{ name: 'entries', ownerGroup: 'forms', fields: [
    { name: 'id', type: 'text', primaryKey: true, required: true },
    { name: 'title', type: 'text', required: true },
  ] }],
};

describe('Domain migration planning', () => {
  it('automatically accepts an additive nullable field', () => {
    const next = structuredClone(base);
    next.entities[0].fields.push({ name: 'note', type: 'text', required: false } as never);
    expect(plan(base, next)).toMatchObject({ safe: true, steps: [{ kind: 'addField', entity: 'entries', field: 'note' }] });
  });

  it('rejects an implicit rename or drop', () => {
    const next = structuredClone(base);
    next.entities[0].fields[1].name = 'heading';
    expect(plan(base, next)).toMatchObject({ safe: false, error: 'mapping_required' });
  });

  it('accepts an explicit rename mapping without dropping data', () => {
    const next = structuredClone(base) as typeof base & { migrations: unknown[] };
    next.entities[0].fields[1].name = 'heading';
    next.migrations = [{ entity: 'entries', from: 'title', to: 'heading' }];
    expect(plan(base, next)).toMatchObject({ safe: true, steps: [{ kind: 'renameField', entity: 'entries', field: 'title', target: 'heading' }] });
  });

  it('rejects adding a required field without a default', () => {
    const next = structuredClone(base);
    next.entities[0].fields.push({ name: 'owner', type: 'text', required: true } as never);
    expect(plan(base, next)).toMatchObject({ safe: false, error: 'default_required' });
  });

  it('requires an explicit conversion mapping for a type change', () => {
    const next = structuredClone(base) as typeof base & { migrations: unknown[] };
    next.entities[0].fields[1].type = 'int64';
    expect(plan(base, next)).toMatchObject({ safe: false, error: 'mapping_required' });
    next.migrations = [{ entity: 'entries', from: 'title', to: 'title', conversion: 'parseInt64' }];
    expect(plan(base, next)).toMatchObject({ safe: true, steps: [{ kind: 'convertField', entity: 'entries', field: 'title', target: 'title' }] });
  });

  it('rejects an unknown conversion rather than treating it as safe', () => {
    const next = structuredClone(base) as typeof base & { migrations: unknown[] };
    next.entities[0].fields[1].type = 'int64';
    next.migrations = [{ entity: 'entries', from: 'title', to: 'title', conversion: 'dropInvalidRows' }];
    expect(plan(base, next)).toMatchObject({ safe: false, error: 'invalid_conversion' });
  });
});
