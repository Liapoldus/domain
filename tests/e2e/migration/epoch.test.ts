import { parseJson, propertiesOf, type Schema } from '../../support/json.js';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let apiBinaryDirectory: string;
let apiBinary: string;
let commandsBinaryDirectory: string;
let commandsBinary: string;

beforeAll(() => {
  apiBinaryDirectory = mkdtempSync(join(tmpdir(), 'domain-epoch-api-'));
  apiBinary = join(apiBinaryDirectory, 'product-api-probe');
  execFileSync('go', ['build', '-o', apiBinary, './tests/fixtures/row/api'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    timeout: 120_000,
  });
  commandsBinaryDirectory = mkdtempSync(join(tmpdir(), 'domain-epoch-commands-'));
  commandsBinary = join(commandsBinaryDirectory, 'product-commands-probe');
  execFileSync('go', ['build', '-o', commandsBinary, './tests/fixtures/row/commands'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    timeout: 120_000,
  });
}, 180_000);

afterAll(() => {
  if (apiBinaryDirectory) rmSync(apiBinaryDirectory, { recursive: true, force: true });
  if (commandsBinaryDirectory) rmSync(commandsBinaryDirectory, { recursive: true, force: true });
}, 180_000);

const FORMS = 'spiffe://liapoldus/domain/product/owner-forms';

type Step = Record<string, unknown>;

type EnvelopeError = {
  code: string;
  retryable: boolean;
  unknownOutcome: boolean;
  message: string;
};

type Envelope = {
  ok: boolean;
  data?: Record<string, unknown>;
  error?: EnvelopeError;
};

type CallResult = {
  ok: boolean;
  envelope?: Envelope;
  protocolError?: string;
  epoch?: number;
};

type StepResult = {
  ok: boolean;
  code?: string;
  duplicate?: boolean;
  applied?: number;
  found?: boolean;
  row?: Record<string, unknown>;
  appliedIndex?: number;
  snapshot?: string;
  epoch?: number;
};

const call = (target: string, method: string, identity: string, payload: Step): Step => ({
  action: 'call',
  target,
  method,
  identity,
  payload,
});

const migrate = (): Step => ({ action: 'migrate' });

const runApiProbe = (steps: Step[], timeout = 120_000): CallResult[] => {
  const output = execFileSync(apiBinary, {
    input: JSON.stringify({ steps }),
    encoding: 'utf8',
    timeout,
    maxBuffer: 64 * 1024 * 1024,
  });
  const parsed = JSON.parse(output) as { results: CallResult[] };
  expect(parsed.results).toHaveLength(steps.length);
  return parsed.results;
};

const runFsmProbe = (steps: Step[], timeout = 120_000): StepResult[] => {
  const output = execFileSync(commandsBinary, {
    input: JSON.stringify({ steps }),
    encoding: 'utf8',
    timeout,
    maxBuffer: 64 * 1024 * 1024,
  });
  const parsed = JSON.parse(output) as { results: StepResult[] };
  expect(parsed.results).toHaveLength(steps.length);
  return parsed.results;
};

const expectData = (result: CallResult): Record<string, unknown> => {
  expect(result.protocolError).toBeUndefined();
  expect(result.envelope?.ok).toBe(true);
  return result.envelope?.data ?? {};
};

const expectFailure = (result: CallResult, code: string): EnvelopeError => {
  expect(result.protocolError).toBeUndefined();
  expect(result.envelope?.ok).toBe(false);
  const error = result.envelope?.error;
  expect(error?.code).toBe(code);
  return error as EnvelopeError;
};

const expectEpochMismatch = (result: CallResult): void => {
  const error = expectFailure(result, 'epoch_mismatch');
  expect(error.retryable).toBe(true);
  expect(error.unknownOutcome).toBe(false);
  expect(error.message).toBeTruthy();
};

const row = (id: string, overrides: Record<string, unknown> = {}): Step => ({
  id,
  title: `title-${id}`,
  slug: `slug-${id}`,
  count: 1,
  ...overrides,
});

type Scope = { tenant?: string; site?: string; group?: string };
type CommandOptions = Scope & { row?: Record<string, unknown>; writeId?: string; epoch?: number };

const command = (kind: string, entity: string, id: string, options: CommandOptions = {}): Step => {
  const value: Step = { kind, tenant: options.tenant ?? 't1', site: options.site ?? 's1', entity, id };
  if (options.row !== undefined) value.row = options.row;
  if (options.writeId !== undefined) value.writeId = options.writeId;
  if (options.group !== undefined) value.group = options.group;
  if (options.epoch !== undefined) value.epoch = options.epoch;
  return value;
};

const create = (entity: string, id: string, rowValue: Record<string, unknown>, options: CommandOptions = {}): Step =>
  command('create', entity, id, { ...options, row: rowValue });

const batch = (ops: Step[], options: CommandOptions = {}): Step => {
  const value: Step = { kind: 'batch', tenant: options.tenant ?? 't1', site: options.site ?? 's1', ops };
  if (options.writeId !== undefined) value.writeId = options.writeId;
  if (options.group !== undefined) value.group = options.group;
  if (options.epoch !== undefined) value.epoch = options.epoch;
  return value;
};

const operation = (kind: string, entity: string, id: string, options: CommandOptions = {}): Step => {
  const value: Step = { kind, entity, id };
  if (options.row !== undefined) value.row = options.row;
  return value;
};

const applyTo = (steps: Step[], target: string, index: number, value: Step): number => {
  steps.push({ action: 'apply', target, index, command: value });
  return steps.length - 1;
};

const changeModelOf = (steps: Step[], target: string, action: 'migrate' | 'rollback', index: number, epoch: number): number => {
  steps.push({ action, target, index, epoch });
  return steps.length - 1;
};

const readOf = (steps: Step[], target: string, entity: string, id: string, scope: Scope = {}): number => {
  steps.push({
    action: 'read',
    target,
    entity,
    id,
    tenant: scope.tenant ?? 't1',
    site: scope.site ?? 's1',
  });
  return steps.length - 1;
};

const appliedIndexOf = (steps: Step[], target: string): number => {
  steps.push({ action: 'appliedIndex', target });
  return steps.length - 1;
};

const exportOf = (steps: Step[], target: string): number => {
  steps.push({ action: 'export', target });
  return steps.length - 1;
};

const cursorOver = (results: StepResult[]) => {
  let cursor = 0;
  return (): StepResult => {
    const value = results[cursor];
    cursor += 1;
    return value;
  };
};

const expectApplied = (result: StepResult, duplicate: boolean, appliedIndex: number): void => {
  expect(result.ok).toBe(true);
  expect(result.duplicate).toBe(duplicate);
  expect(result.appliedIndex).toBe(appliedIndex);
};

const expectFsmEpochMismatch = (result: StepResult, appliedIndex: number, epoch: number): void => {
  expect(result.ok).toBe(false);
  expect(result.code).toBe('epoch_mismatch');
  expect(result.appliedIndex).toBe(appliedIndex);
  expect(result.epoch).toBe(epoch);
};

const expectEpoch = (result: StepResult, epoch: number): void => {
  expect(result.epoch).toBe(epoch);
};

const expectFound = (result: StepResult, found: boolean): void => {
  expect(result.ok).toBe(true);
  expect(result.found).toBe(found);
};

type SnapshotDocument = {
  formatVersion?: number;
  model: { schemaVersion?: string; entities: { name: string; ownerGroup: string; fields?: unknown[] }[] };
  epoch: number;
  raftAppliedIndex: number;
  rows: { tenant: string; site: string; entity: string; id: string; data: Record<string, unknown> }[];
};

const parseSnapshot = (value: string | undefined): SnapshotDocument => {
  expect(value, 'expected a snapshot in the probe result').toBeTruthy();
  return JSON.parse(value as string) as SnapshotDocument;
};

describe('Epoch fencing across the product service (raft probe)', () => {
  it('rejects stale writes and reads after a migration and applies the retried writes with the fresh epoch', () => {
    const steps: Step[] = [
      call('leader', 'domain.cluster.status', FORMS, {}),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1'), epoch: 1 }),
      migrate(),
      call('leader', 'domain.cluster.status', FORMS, {}),
      call('leader', 'domain.update', FORMS, { entity: 'records', id: 'r1', row: row('r1', { count: 7 }), epoch: 1 }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r2', row: row('r2'), epoch: 1, writeId: 'stale-w' }),
      call('leader', 'domain.delete', FORMS, { entity: 'records', id: 'r1', epoch: 1 }),
      call('leader', 'domain.batch', FORMS, {
        epoch: 1,
        writeId: 'stale-b',
        ops: [{ op: 'create', entity: 'records', id: 'r3', row: row('r3') }],
      }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r2' }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1', epoch: 1 }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1', epoch: 2 }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1', epoch: 0 }),
      call('leader', 'domain.query', FORMS, { sql: 'select id, title from records order by id', epoch: 1 }),
      call('leader', 'domain.query', FORMS, { sql: 'select id, title from records order by id', epoch: 2 }),
      call('leader', 'domain.query', FORMS, { sql: 'select id, title from records order by id' }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r2', row: row('r2'), epoch: 2, writeId: 'stale-w' }),
      call('leader', 'domain.update', FORMS, { entity: 'records', id: 'r1', row: row('r1', { count: 7 }), epoch: 2 }),
      call('leader', 'domain.batch', FORMS, {
        epoch: 2,
        writeId: 'stale-b',
        ops: [{ op: 'create', entity: 'records', id: 'r3', row: row('r3') }],
      }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1', epoch: 2 }),
      call('leader', 'domain.delete', FORMS, { entity: 'records', id: 'r1', epoch: 2 }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1', epoch: 2 }),
    ];
    const results = runApiProbe(steps);
    const [baselineStatus, created, migrated, statusAfter, staleUpdate, staleCreate, staleDelete, staleBatch,
      unfencedRead, unfencedMissing, staleRead, freshRead, zeroRead, staleQuery, freshQuery, unfencedQuery,
      retriedCreate, retriedUpdate, retriedBatch, readBack, deleted, gone] = results;

    expect(expectData(baselineStatus).epoch).toBe(1);
    expect(expectData(created)).toMatchObject({ duplicate: false, entity: 'records', id: 'r1', epoch: 1 });

    expect(migrated.epoch).toBe(2);
    expect(expectData(statusAfter).epoch).toBe(2);

    expectEpochMismatch(staleUpdate);
    expectEpochMismatch(staleCreate);
    expectEpochMismatch(staleDelete);
    expectEpochMismatch(staleBatch);

    const unchanged = expectData(unfencedRead);
    expect(unchanged).toMatchObject({ found: true, epoch: 2 });
    expect((unchanged.row as Record<string, unknown>).count).toBe(1);
    expect(expectData(unfencedMissing)).toMatchObject({ found: false, epoch: 2 });

    expectEpochMismatch(staleRead);
    expect(expectData(freshRead)).toMatchObject({ found: true, epoch: 2 });
    expect(expectData(zeroRead)).toMatchObject({ found: true, epoch: 2 });

    expectEpochMismatch(staleQuery);
    expect(expectData(freshQuery)).toMatchObject({
      columns: ['id', 'title'],
      rows: [['r1', 'title-r1']],
      rowCount: 1,
      epoch: 2,
    });
    expect(expectData(unfencedQuery).epoch).toBe(2);

    expect(expectData(retriedCreate)).toMatchObject({ duplicate: false, epoch: 2 });
    expect(typeof expectData(retriedCreate).appliedIndex).toBe('number');
    expect(expectData(retriedUpdate)).toMatchObject({ duplicate: false, entity: 'records', id: 'r1', epoch: 2 });
    expect(expectData(retriedBatch)).toMatchObject({ duplicate: false, applied: true, epoch: 2 });

    expect(expectData(readBack).row).toMatchObject({ id: 'r1', count: 7 });
    expect(expectData(deleted)).toMatchObject({ duplicate: false, epoch: 2 });
    expect(expectData(gone)).toMatchObject({ found: false, epoch: 2 });
  }, 130_000);

  it('advances the epoch on every migration and fences each generation of writes', () => {
    const steps: Step[] = [
      call('leader', 'domain.cluster.status', FORMS, {}),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'a1', row: row('a1'), epoch: 1 }),
      migrate(),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'a2', row: row('a2'), epoch: 1 }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'a2', row: row('a2'), epoch: 2 }),
      migrate(),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'a3', row: row('a3'), epoch: 2 }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'a3', row: row('a3'), epoch: 3 }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'a1', epoch: 3 }),
      call('leader', 'domain.cluster.status', FORMS, {}),
    ];
    const results = runApiProbe(steps);
    const [status, first, migrated1, staleSecond, freshSecond, migrated2, staleThird, freshThird, readFirst, statusAfter] = results;

    expect(expectData(status).epoch).toBe(1);
    expect(expectData(first)).toMatchObject({ duplicate: false, epoch: 1 });
    expect(migrated1.epoch).toBe(2);
    expectEpochMismatch(staleSecond);
    expect(expectData(freshSecond)).toMatchObject({ duplicate: false, epoch: 2 });
    expect(migrated2.epoch).toBe(3);
    expectEpochMismatch(staleThird);
    expect(expectData(freshThird)).toMatchObject({ duplicate: false, epoch: 3 });
    expect(expectData(readFirst)).toMatchObject({ found: true, epoch: 3 });
    expect(expectData(statusAfter).epoch).toBe(3);
  }, 130_000);

  it('rejects schema-invalid epochs before they reach the service', () => {
    const steps: Step[] = [
      call('leader', 'domain.batch', FORMS, {
        writeId: 'per-op-epoch',
        ops: [{ op: 'create', entity: 'records', id: 'r9', row: row('r9'), epoch: 1 }],
      }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r9', row: row('r9'), epoch: -1 }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r9', epoch: 1.5 }),
      call('leader', 'domain.delete', FORMS, { entity: 'records', id: 'r9', epoch: -1 }),
    ];
    const results = runApiProbe(steps);
    for (const result of results) {
      const error = expectFailure(result, 'invalid_request');
      expect(error.retryable).toBe(false);
      expect(error.unknownOutcome).toBe(false);
    }
  }, 130_000);
});

describe('Epoch fencing at the FSM boundary (direct apply probe)', () => {
  it('fences stale write, batch, migrate and rollback commands without advancing epoch or applied index', () => {
    const steps: Step[] = [];
    const apply = (index: number, value: Step): number => applyTo(steps, 'primary', index, value);
    const change = (action: 'migrate' | 'rollback', index: number, epoch: number): number =>
      changeModelOf(steps, 'primary', action, index, epoch);

    apply(1, create('accounts', 'a1', { id: 'a1', code: 'ACME' }, { group: 'identity', epoch: 1 }));
    apply(2, create('entries', 'e9', { id: 'e9', email: 'e9@example.test' }, { group: 'forms', epoch: 99 }));
    readOf(steps, 'primary', 'entries', 'e9');
    apply(3, batch([operation('create', 'accounts', 'a2', { row: { id: 'a2', code: 'ACME2' } })], { group: 'identity', writeId: 'B', epoch: 99 }));
    readOf(steps, 'primary', 'accounts', 'a2');
    apply(4, batch([operation('create', 'accounts', 'a2', { row: { id: 'a2', code: 'ACME2' } })], { group: 'identity', writeId: 'B', epoch: 1 }));
    change('migrate', 5, 99);
    const staleExport = exportOf(steps, 'primary');
    change('migrate', 6, 1);
    const migratedExport = exportOf(steps, 'primary');
    apply(7, create('entries', 'e5', { id: 'e5', email: 'e5@example.test' }, { group: 'forms', epoch: 1 }));
    readOf(steps, 'primary', 'entries', 'e5');
    apply(8, create('entries', 'e5', { id: 'e5', email: 'e5@example.test' }, { group: 'forms', epoch: 2 }));
    change('rollback', 9, 1);
    change('rollback', 10, 2);
    const rolledBackExport = exportOf(steps, 'primary');
    apply(11, create('entries', 'e6', { id: 'e6', email: 'e6@example.test' }, { group: 'forms', epoch: 2 }));
    apply(12, create('entries', 'e6', { id: 'e6', email: 'e6@example.test' }, { group: 'forms', epoch: 3 }));
    readOf(steps, 'primary', 'entries', 'e6');
    appliedIndexOf(steps, 'primary');

    const results = runFsmProbe(steps);
    const next = cursorOver(results);

    const seeded = next();
    expectApplied(seeded, false, 1);
    expectEpoch(seeded, 1);

    expectFsmEpochMismatch(next(), 1, 1);
    expectFound(next(), false);

    expectFsmEpochMismatch(next(), 1, 1);
    expectFound(next(), false);

    const batchApplied = next();
    expectApplied(batchApplied, false, 4);
    expect(batchApplied.applied).toBe(1);
    expectEpoch(batchApplied, 1);

    expectFsmEpochMismatch(next(), 4, 1);
    const staleExportResult = next();
    expect(staleExportResult.ok).toBe(true);
    const beforeMigrate = parseSnapshot(results[staleExport].snapshot);
    expect(beforeMigrate.epoch).toBe(1);
    expect(beforeMigrate.model.entities.map((entity) => entity.name)).toEqual(['accounts', 'entries']);

    const migrated = next();
    expectApplied(migrated, false, 6);
    expectEpoch(migrated, 2);
    const migratedExportResult = next();
    expect(migratedExportResult.ok).toBe(true);
    const afterMigrate = parseSnapshot(results[migratedExport].snapshot);
    expect(afterMigrate.epoch).toBe(2);
    expect(afterMigrate.model.entities.map((entity) => entity.name)).toContain('mig1');

    expectFsmEpochMismatch(next(), 6, 2);
    expectFound(next(), false);
    expectApplied(next(), false, 8);

    expectFsmEpochMismatch(next(), 8, 2);
    const rolledBack = next();
    expectApplied(rolledBack, false, 10);
    expectEpoch(rolledBack, 3);
    const rolledBackExportResult = next();
    expect(rolledBackExportResult.ok).toBe(true);
    const restored = parseSnapshot(results[rolledBackExport].snapshot);
    expect(restored.epoch).toBe(3);
    expect(restored.model.entities.map((entity) => entity.name)).toEqual(['accounts', 'entries']);

    expectFsmEpochMismatch(next(), 10, 3);
    expectApplied(next(), false, 12);
    const finalRead = next();
    expectFound(finalRead, true);
    const finalState = next();
    expect(finalState.ok).toBe(true);
    expect(finalState.appliedIndex).toBe(12);
  }, 130_000);
});

describe('Epoch fencing contract (schemas, read-only)', () => {
  const readSchema = (name: string): Schema =>
    parseJson(readFileSync(join(root, 'contracts', 'v1', 'schemas', name), 'utf8')) as Schema;

  it('declares epoch_mismatch as a first-class error code in the envelope contract', () => {
    const envelope = readSchema('envelope.json');
    const code = propertiesOf(propertiesOf(envelope).error).code;
    expect(code.enum).toContain('epoch_mismatch');
    expect(propertiesOf(envelope).error.required).toEqual(['code', 'retryable', 'unknownOutcome', 'message']);
  });

  it('declares an optional non-negative epoch on every fenced request schema', () => {
    const fencedRequests = [
      'create.request.json',
      'update.request.json',
      'delete.request.json',
      'get.request.json',
      'query.request.json',
      'batch.request.json',
    ];
    for (const name of fencedRequests) {
      const schema = readSchema(name);
      expect(propertiesOf(schema).epoch).toEqual({ type: 'integer', minimum: 0 });
      expect(schema.required ?? []).not.toContain('epoch');
      expect(schema.additionalProperties).toBe(false);
    }
  });

  it('keeps per-op epoch out of the batch contract and requires epoch on fenced responses', () => {
    const batch = readSchema('batch.request.json');
    expect(batch.$defs?.op.properties?.epoch).toBeUndefined();
    expect(batch.$defs?.op.additionalProperties).toBe(false);

    const fencedResponses = [
      'write.response.json',
      'get.response.json',
      'query.response.json',
      'batch.response.json',
      'status.response.json',
    ];
    for (const name of fencedResponses) {
      expect(readSchema(name).required).toContain('epoch');
    }
  });
});
