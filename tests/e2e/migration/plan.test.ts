import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';



import { parseJson, type Model } from '../../support/json.js';

import { root } from '../../support/paths.js';

function plan(previous: unknown, next: unknown) {
  const output = execFileSync('go', ['run', './tests/fixtures/migration/plan'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    input: JSON.stringify({ previous, next }),
    encoding: 'utf8',
  });
  return parseJson(output);
}

const base: Model = {
  schemaVersion: '1',
  entities: [{ name: 'entries', ownerGroup: 'forms', fields: [
    { name: 'id', type: 'text', primaryKey: true, required: true },
    { name: 'title', type: 'text', required: true },
  ] }],
};

describe('Domain migration planning', () => {
  it('automatically accepts an additive nullable field', () => {
    const next = structuredClone(base);
    next.entities[0].fields.push({ name: 'note', type: 'text', required: false } as Model["entities"][number]["fields"][number]);
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
    next.entities[0].fields.push({ name: 'owner', type: 'text', required: true } as Model["entities"][number]["fields"][number]);
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

  it('rejects ambiguous mappings that reuse a source or target field', () => {
    const renamed = structuredClone(base) as typeof base & { migrations: unknown[] };
    renamed.entities[0].fields[1].name = 'heading';
    renamed.migrations = [
      { entity: 'entries', from: 'title', to: 'heading' },
      { entity: 'entries', from: 'title', to: 'id' },
    ];
    expect(plan(base, renamed)).toMatchObject({ safe: false, error: 'invalid_mapping' });

    const twoSources = structuredClone(base) as typeof base & { migrations: unknown[] };
    twoSources.entities[0].fields[1].name = 'heading';
    twoSources.entities[0].fields.push({ name: 'subtitle', type: 'text' } as Model["entities"][number]["fields"][number]);
    twoSources.migrations = [
      { entity: 'entries', from: 'title', to: 'heading' },
      { entity: 'entries', from: 'subtitle', to: 'heading' },
    ];
    expect(plan(base, twoSources)).toMatchObject({ safe: false, error: 'invalid_mapping' });
  });

  it('rejects identifiers and field types outside the published schema', () => {
    const invalidIdentifier = structuredClone(base);
    invalidIdentifier.entities[0].name = 'bad-name';
    expect(plan(base, invalidIdentifier)).toMatchObject({ safe: false, error: 'invalid_model' });

    const invalidType = structuredClone(base);
    invalidType.entities[0].fields[1].type = 'json';
    expect(plan(base, invalidType)).toMatchObject({ safe: false, error: 'invalid_model' });
  });

  it('validates foreign references against an existing unique field of the same type', () => {
    const related = structuredClone(base) ;
    related.entities.push({
      name: 'accounts', ownerGroup: 'forms', fields: [
        { name: 'id', type: 'text', primaryKey: true, required: true },
        { name: 'slug', type: 'text', required: true },
        { name: 'code', type: 'text', unique: true, required: true },
      ],
    });
    related.entities[0].fields.push({
      name: 'accountCode', type: 'text', references: { entity: 'accounts', field: 'code' },
    } as Model["entities"][number]["fields"][number]);
    expect(plan({}, related)).toMatchObject({ safe: true });

    const missingTarget = structuredClone(related);
    (missingTarget.entities[0].fields[2] as unknown as { references: unknown }).references = {
      entity: 'missing', field: 'id',
    };
    expect(plan({}, missingTarget)).toMatchObject({ safe: false, error: 'invalid_reference' });

    const nonUniqueTarget = structuredClone(related);
    (nonUniqueTarget.entities[0].fields[2] as unknown as { references: { entity: string; field: string } }).references.field = 'slug';
    expect(plan({}, nonUniqueTarget)).toMatchObject({ safe: false, error: 'invalid_reference' });

    const mismatchedType = structuredClone(related);
    (mismatchedType.entities[0].fields[2] as unknown as { type: string }).type = 'int64';
    expect(plan({}, mismatchedType)).toMatchObject({ safe: false, error: 'invalid_reference' });
  });
});
