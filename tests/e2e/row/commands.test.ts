import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-product-commands-'));
  probeBinary = join(binaryDirectory, 'product-commands-probe');
  execFileSync('go', ['build', '-o', probeBinary, './tests/fixtures/row/commands'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    timeout: 120_000,
  });
}, 180_000);

afterAll(() => {
  if (binaryDirectory) rmSync(binaryDirectory, { recursive: true, force: true });
}, 180_000);

type Step = Record<string, unknown>;

type StepResult = {
  ok: boolean;
  code?: string;
  duplicate?: boolean;
  applied?: number;
  found?: boolean;
  row?: Record<string, unknown>;
  appliedIndex?: number;
  snapshot?: string;
};

type Scope = { tenant?: string; site?: string; group?: string };
type CommandOptions = Scope & { row?: Record<string, unknown>; writeId?: string };

const command = (kind: string, entity: string, id: string, options: CommandOptions = {}): Step => {
  const value: Step = { kind, tenant: options.tenant ?? 't1', site: options.site ?? 's1', entity, id };
  if (options.row !== undefined) value.row = options.row;
  if (options.writeId !== undefined) value.writeId = options.writeId;
  if (options.group !== undefined) value.group = options.group;
  return value;
};

const create = (entity: string, id: string, row: Record<string, unknown>, options: CommandOptions = {}): Step =>
  command('create', entity, id, { ...options, row });

const update = (entity: string, id: string, row: Record<string, unknown>, options: CommandOptions = {}): Step =>
  command('update', entity, id, { ...options, row });

const remove = (entity: string, id: string, options: CommandOptions = {}): Step =>
  command('delete', entity, id, options);

const operation = (kind: string, entity: string, id: string, options: CommandOptions = {}): Step => {
  const value: Step = { kind, entity, id };
  if (options.row !== undefined) value.row = options.row;
  if (options.writeId !== undefined) value.writeId = options.writeId;
  if (options.group !== undefined) value.group = options.group;
  return value;
};

const batch = (ops: Step[], options: CommandOptions = {}): Step => {
  const value: Step = { kind: 'batch', tenant: options.tenant ?? 't1', site: options.site ?? 's1', ops };
  if (options.writeId !== undefined) value.writeId = options.writeId;
  if (options.group !== undefined) value.group = options.group;
  return value;
};

const applyTo = (steps: Step[], target: string, index: number, value: Step): number => {
  steps.push({ action: 'apply', target, index, command: value });
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

const importOf = (
  steps: Step[],
  target: string,
  mode: string,
  source: { snapshot?: string; snapshotRef?: number },
): number => {
  steps.push({ action: 'import', target, mode, ...source });
  return steps.length - 1;
};

const runProbe = (steps: Step[], timeout: number): StepResult[] => {
  const output = execFileSync(probeBinary, {
    input: JSON.stringify({ steps }),
    encoding: 'utf8',
    timeout,
    maxBuffer: 64 * 1024 * 1024,
  });
  const parsed = JSON.parse(output) as { results: StepResult[] };
  expect(parsed.results).toHaveLength(steps.length);
  return parsed.results;
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

const expectRejected = (result: StepResult, code: string, appliedIndex: number): void => {
  expect(result.ok).toBe(false);
  expect(result.code).toBe(code);
  expect(result.appliedIndex).toBe(appliedIndex);
};

const expectRefused = (result: StepResult, code: string): void => {
  expect(result.ok).toBe(false);
  expect(result.code).toBe(code);
};

const expectAccepted = (result: StepResult): void => {
  expect(result.ok).toBe(true);
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
  ledger?: { tenant: string; site: string; writeId: string; outcome: Record<string, unknown>; seq: number }[];
  snapshotRows?: unknown[];
};

const parseSnapshot = (value: string | undefined): SnapshotDocument => {
  expect(value, 'expected a snapshot in the probe result').toBeTruthy();
  return JSON.parse(value as string) as SnapshotDocument;
};

describe('Domain product command FSM', () => {
  it('applies writes, maps FSM sentinels to spec codes and never advances the index on rejection', () => {
    const steps: Step[] = [];
    const apply = (index: number, value: Step): number => applyTo(steps, 'primary', index, value);
    const account = (id: string, code: string): Step =>
      create('accounts', id, { id, code }, { group: 'identity' });
    const entry = (id: string, email: string, count: number, accountCode: string): Step =>
      create('entries', id, { id, email, count, accountCode }, { group: 'forms' });

    apply(1, account('a1', 'ACME'));
    apply(2, account('a1', 'ACME'));
    apply(3, entry('e1', 'e1@example.test', 2, 'ACME'));
    apply(4, entry('e1', 'e1@example.test', 2, 'ACME'));
    apply(5, update('entries', 'e1', { id: 'e1', email: 'e1@example.test', count: 5, accountCode: 'ACME' }, { group: 'forms' }));
    apply(6, update('entries', 'e9', { id: 'e9', email: 'e9@example.test', count: 1 }, { group: 'forms' }));
    apply(7, update('entries', 'e1', { id: 'e1', email: 'e1@example.test', count: 5, accountCode: 'ACME' }, { group: 'identity' }));
    apply(8, remove('entries', 'e9', { group: 'identity' }));
    apply(9, create('entries', 'e2', { id: 'e2', email: 'e2@example.test' }, { group: 'identity' }));
    apply(10, create('entries', 'e2', { id: 'e2', email: 'e1@example.test', accountCode: 'ACME' }, { group: 'forms' }));
    apply(11, create('entries', 'e3', { id: 'e3', email: 'e3@example.test', accountCode: 'MISSING' }, { group: 'forms' }));
    apply(12, create('entries', 'e4', { id: 'e4', email: 'e4@example.test', accountCode: 'MISSING' }, { group: 'identity' }));
    apply(13, remove('accounts', 'a1', { group: 'identity' }));
    apply(14, remove('entries', 'e1', { group: 'forms' }));
    apply(15, remove('entries', 'e1', { group: 'forms' }));
    apply(16, remove('accounts', 'a1', { group: 'identity' }));
    apply(17, remove('accounts', 'a1', { group: 'identity' }));
    apply(18, create('accounts', 'a2', { id: 'a2', code: 'ACME2' }, { group: 'identity', writeId: 'w1' }));
    apply(19, create('accounts', 'a3', { id: 'a3', code: 'ACME3' }, { group: 'identity', writeId: 'w1' }));
    readOf(steps, 'primary', 'accounts', 'a3');
    apply(18, create('accounts', 'a2', { id: 'a2', code: 'ACME2' }, { group: 'identity', writeId: 'w1' }));
    appliedIndexOf(steps, 'primary');

    const results = runProbe(steps, 120_000);
    const next = cursorOver(results);

    expectApplied(next(), false, 1);
    expectRejected(next(), 'conflict', 1);
    expectApplied(next(), false, 3);
    expectRejected(next(), 'conflict', 3);
    expectApplied(next(), false, 5);
    expectRejected(next(), 'not_found', 5);
    expectRejected(next(), 'forbidden', 5);
    expectRejected(next(), 'forbidden', 5);
    expectRejected(next(), 'forbidden', 5);
    expectRejected(next(), 'conflict', 5);
    expectRejected(next(), 'conflict', 5);
    expectRejected(next(), 'forbidden', 5);
    expectRejected(next(), 'conflict', 5);
    expectApplied(next(), false, 14);
    expectRejected(next(), 'not_found', 14);
    expectApplied(next(), false, 16);
    expectRejected(next(), 'not_found', 16);
    expectApplied(next(), false, 18);
    expectApplied(next(), true, 19);
    expectFound(next(), false);
    expectApplied(next(), true, 19);
    const finalState = next();
    expect(finalState.ok).toBe(true);
    expect(finalState.appliedIndex).toBe(19);
  }, 130_000);

  it('applies batch commands atomically with outer-scope ACL and writeId deduplication', () => {
    const steps: Step[] = [];
    const apply = (index: number, value: Step): number => applyTo(steps, 'primary', index, value);

    apply(1, batch([operation('create', 'accounts', 'a1', { row: { id: 'a1', code: 'ACME' } })], { group: 'identity', writeId: 'BA' }));
    apply(2, batch([operation('create', 'entries', 'e1', { row: { id: 'e1', email: 'e1@example.test', count: 2, accountCode: 'ACME' } })], { group: 'identity' }));
    readOf(steps, 'primary', 'entries', 'e1');
    apply(3, batch([operation('create', 'entries', 'e1', { row: { id: 'e1', email: 'e1@example.test', count: 2, accountCode: 'ACME' } })], { group: 'forms' }));
    apply(4, batch([
      operation('create', 'accounts', 'a2', { row: { id: 'a2', code: 'ACME2' } }),
      operation('delete', 'accounts', 'a1'),
    ], { group: 'identity' }));
    readOf(steps, 'primary', 'accounts', 'a2');
    apply(5, batch([
      operation('create', 'entries', 'e2', { row: { id: 'e2', email: 'e2@example.test', accountCode: 'ACME' } }),
      operation('create', 'entries', 'e3', { row: { id: 'e3', email: 'e2@example.test', accountCode: 'ACME' } }),
    ], { group: 'forms' }));
    readOf(steps, 'primary', 'entries', 'e2');
    apply(6, batch([operation('create', 'widgets', 'w1', { row: { id: 'w1' } })], { group: 'forms' }));
    apply(7, batch([operation('upsert', 'entries', 'e1', { row: { id: 'e1', email: 'e1@example.test', count: 1 } })], { group: 'forms' }));
    apply(8, batch([], { group: 'forms' }));
    apply(9, batch(
      Array.from({ length: 101 }, (_, n) =>
        operation('create', 'entries', `bulk${n}`, { row: { id: `bulk${n}`, email: `bulk${n}@example.test`, accountCode: 'ACME' } }),
      ),
      { group: 'forms' },
    ));
    apply(10, batch(
      [operation('update', 'entries', 'e1', { row: { id: 'e1', email: 'e1@example.test', count: 7, accountCode: 'ACME' }, writeId: 'inner', group: 'identity' })],
      { group: 'forms', writeId: 'B6' },
    ));
    readOf(steps, 'primary', 'entries', 'e1');
    apply(11, batch(
      [operation('update', 'entries', 'e1', { row: { id: 'e1', email: 'e1@example.test', count: 99, accountCode: 'ACME' } })],
      { group: 'forms', writeId: 'B6' },
    ));
    readOf(steps, 'primary', 'entries', 'e1');
    apply(12, create('accounts', 'a5', { id: 'a5', code: 'ACME5' }, { group: 'identity', writeId: 'inner' }));
    appliedIndexOf(steps, 'primary');

    const results = runProbe(steps, 120_000);
    const next = cursorOver(results);

    const fresh = next();
    expectApplied(fresh, false, 1);
    expect(fresh.applied).toBe(1);
    expectRejected(next(), 'forbidden', 1);
    expectFound(next(), false);
    const created = next();
    expectApplied(created, false, 3);
    expect(created.applied).toBe(1);
    expectRejected(next(), 'conflict', 3);
    expectFound(next(), false);
    expectRejected(next(), 'conflict', 3);
    expectFound(next(), false);
    expectRejected(next(), 'invalid_request', 3);
    expectRejected(next(), 'invalid_request', 3);
    expectRejected(next(), 'invalid_request', 3);
    expectRejected(next(), 'invalid_request', 3);
    const nested = next();
    expectApplied(nested, false, 10);
    expect(nested.applied).toBe(1);
    const afterNestedWrite = next();
    expectFound(afterNestedWrite, true);
    expect(afterNestedWrite.row?.count).toBe(7);
    const deduplicated = next();
    expectApplied(deduplicated, true, 11);
    expect(deduplicated.applied).toBe(0);
    const stillOld = next();
    expectFound(stillOld, true);
    expect(stillOld.row?.count).toBe(7);
    expectApplied(next(), false, 12);
    const finalState = next();
    expect(finalState.ok).toBe(true);
    expect(finalState.appliedIndex).toBe(12);
  }, 130_000);

  it('scopes writeId deduplication to tenant, site and entity identity', () => {
    const steps: Step[] = [];
    const apply = (index: number, value: Step): number => applyTo(steps, 'primary', index, value);
    const account = (id: string, code: string, scope: Scope): Step =>
      create('accounts', id, { id, code }, { ...scope, group: 'identity', writeId: 'K' });

    apply(1, account('a1', 'C1', { tenant: 't1', site: 's1' }));
    apply(2, account('a2', 'C2', { tenant: 't1', site: 's2' }));
    apply(3, account('a3', 'C3', { tenant: 't2', site: 's1' }));
    apply(4, account('a4', 'C4', { tenant: 't2', site: 's2' }));
    apply(5, account('a5', 'C5', { tenant: 't1', site: 's1' }));
    readOf(steps, 'primary', 'accounts', 'a5', { tenant: 't1', site: 's1' });
    readOf(steps, 'primary', 'accounts', 'a4', { tenant: 't2', site: 's2' });
    appliedIndexOf(steps, 'primary');

    const results = runProbe(steps, 120_000);
    const next = cursorOver(results);

    expectApplied(next(), false, 1);
    expectApplied(next(), false, 2);
    expectApplied(next(), false, 3);
    expectApplied(next(), false, 4);
    expectApplied(next(), true, 5);
    expectFound(next(), false);
    expectFound(next(), true);
    const finalState = next();
    expect(finalState.ok).toBe(true);
    expect(finalState.appliedIndex).toBe(5);
  }, 130_000);

  it('prunes the write ledger at the 4096-entry cap and keeps retained writeIds deduplicating', () => {
    const steps: Step[] = [];
    const apply = (index: number, value: Step): number => applyTo(steps, 'primary', index, value);
    const row = (count: number): Record<string, unknown> => ({
      id: 'p1',
      email: 'p1@example.test',
      count,
    });

    apply(1, create('entries', 'p1', row(0), { group: 'forms' }));
    for (let seed = 1; seed <= 4097; seed += 1) {
      apply(seed + 1, update('entries', 'p1', row(seed), { group: 'forms', writeId: `f-${seed}` }));
    }
    apply(4099, update('entries', 'p1', row(777), { group: 'forms', writeId: 'retained' }));
    const exported = exportOf(steps, 'primary');
    apply(4100, update('entries', 'p1', row(999), { group: 'forms', writeId: 'f-1' }));
    readOf(steps, 'primary', 'entries', 'p1');
    apply(4101, update('entries', 'p1', row(555), { group: 'forms', writeId: 'retained' }));
    readOf(steps, 'primary', 'entries', 'p1');
    apply(4102, update('entries', 'p1', row(333), { group: 'forms', writeId: 'f-4097' }));
    readOf(steps, 'primary', 'entries', 'p1');
    apply(4101, update('entries', 'p1', row(555), { group: 'forms', writeId: 'retained' }));
    appliedIndexOf(steps, 'primary');

    const results = runProbe(steps, 300_000);
    const next = cursorOver(results);

    expectApplied(next(), false, 1);
    for (let seed = 1; seed <= 4097; seed += 1) {
      expectApplied(next(), false, seed + 1);
    }
    expectApplied(next(), false, 4099);
    expectAccepted(next());
    const snapshot = parseSnapshot(results[exported].snapshot);
    expect(snapshot.formatVersion).toBe(2);
    expect(snapshot.raftAppliedIndex).toBe(4099);
    expect(snapshot.ledger).toBeDefined();
    expect(snapshot.ledger).toHaveLength(4096);
    const writeIds = new Set((snapshot.ledger ?? []).map((entry) => entry.writeId));
    expect(writeIds.has('f-1')).toBe(false);
    expect(writeIds.has('f-2')).toBe(false);
    expect(writeIds.has('f-3')).toBe(true);
    expect(writeIds.has('f-4097')).toBe(true);
    expect(writeIds.has('retained')).toBe(true);
    expectApplied(next(), false, 4100);
    const prunedReplay = next();
    expectFound(prunedReplay, true);
    expect(prunedReplay.row?.count).toBe(999);
    expectApplied(next(), true, 4101);
    const retainedStale = next();
    expectFound(retainedStale, true);
    expect(retainedStale.row?.count).toBe(999);
    expectApplied(next(), true, 4102);
    const newestStale = next();
    expectFound(newestStale, true);
    expect(newestStale.row?.count).toBe(999);
    expectApplied(next(), true, 4102);
    const finalState = next();
    expect(finalState.ok).toBe(true);
    expect(finalState.appliedIndex).toBe(4102);
  }, 310_000);

  it('round-trips rows, ledger and applied index through export and import', () => {
    const steps: Step[] = [];

    applyTo(steps, 'primary', 1, create('accounts', 'a1', { id: 'a1', code: 'ACME' }, { group: 'identity' }));
    applyTo(steps, 'primary', 2, create('entries', 'e1', { id: 'e1', email: 'e1@example.test', count: 1, accountCode: 'ACME' }, { group: 'forms', writeId: 'W' }));
    applyTo(steps, 'primary', 3, update('entries', 'e1', { id: 'e1', email: 'e1@example.test', count: 42, accountCode: 'ACME' }, { group: 'forms' }));
    const primaryExport = exportOf(steps, 'primary');
    importOf(steps, 'secondary', 'full', { snapshotRef: primaryExport });
    readOf(steps, 'secondary', 'entries', 'e1');
    readOf(steps, 'secondary', 'accounts', 'a1');
    appliedIndexOf(steps, 'secondary');
    importOf(steps, 'tertiary', 'stream', { snapshotRef: primaryExport });
    readOf(steps, 'tertiary', 'entries', 'e1');
    appliedIndexOf(steps, 'tertiary');
    const secondaryExport = exportOf(steps, 'secondary');
    applyTo(steps, 'secondary', 4, update('entries', 'e1', { id: 'e1', email: 'e1@example.test', count: 43, accountCode: 'ACME' }, { group: 'forms', writeId: 'W' }));
    readOf(steps, 'secondary', 'entries', 'e1');
    applyTo(steps, 'tertiary', 5, create('accounts', 'a2', { id: 'a2', code: 'ACME2' }, { group: 'identity', writeId: 'W' }));
    readOf(steps, 'tertiary', 'accounts', 'a2');

    const results = runProbe(steps, 120_000);
    const next = cursorOver(results);

    expectApplied(next(), false, 1);
    expectApplied(next(), false, 2);
    expectApplied(next(), false, 3);
    expectAccepted(next());
    expectAccepted(next());
    const importedEntry = next();
    expectFound(importedEntry, true);
    expect(importedEntry.row?.count).toBe(42);
    expectFound(next(), true);
    expect(next().appliedIndex).toBe(3);
    expectAccepted(next());
    const streamedEntry = next();
    expectFound(streamedEntry, true);
    expect(streamedEntry.row?.count).toBe(42);
    expect(next().appliedIndex).toBe(3);
    expect(results[secondaryExport].snapshot).toBe(results[primaryExport].snapshot);
    expectAccepted(next());
    expectApplied(next(), true, 4);
    const stillFortyTwo = next();
    expectFound(stillFortyTwo, true);
    expect(stillFortyTwo.row?.count).toBe(42);
    expectApplied(next(), true, 5);
    expectFound(next(), false);
  }, 130_000);

  it('versions snapshots and rejects legacy, unsupported and invalid ledger payloads', () => {
    const legacyModel = {
      schemaVersion: '1',
      entities: [
        {
          name: 'entries',
          ownerGroup: 'forms',
          fields: [
            { name: 'id', type: 'text', primaryKey: true, required: true },
            { name: 'title', type: 'text', required: true },
          ],
        },
      ],
    };
    const legacySnapshot = JSON.stringify({
      model: legacyModel,
      epoch: 7,
      raftAppliedIndex: 42,
      rows: [{ tenant: 't1', site: 's1', entity: 'entries', id: 'old', data: { id: 'old', title: 'kept' } }],
      snapshotRows: [],
    });
    const unsupportedVersionSnapshot = JSON.stringify({
      formatVersion: 3,
      model: legacyModel,
      epoch: 7,
      raftAppliedIndex: 42,
      rows: [],
      snapshotRows: [],
    });
    const invalidLedgerSnapshot = JSON.stringify({
      formatVersion: 2,
      model: legacyModel,
      epoch: 7,
      raftAppliedIndex: 42,
      rows: [],
      ledger: [
        { tenant: 't1', site: 's1', writeId: 'bad', outcome: { kind: 'create', entity: 'entries', id: 'bad', applied: 1 }, seq: 0 },
      ],
      snapshotRows: [],
    });
    const noAppliedIndexSnapshot = JSON.stringify({
      model: legacyModel,
      epoch: 7,
      rows: [],
      snapshotRows: [],
    });
    const largeLedgerSnapshot = JSON.stringify({
      formatVersion: 2,
      model: legacyModel,
      epoch: 3,
      raftAppliedIndex: 10,
      rows: [{ tenant: 't1', site: 's1', entity: 'entries', id: 'k-row', data: { id: 'k-row', title: 'kept' } }],
      ledger: Array.from({ length: 4097 }, (_, n) => ({
        tenant: 't1',
        site: 's1',
        writeId: `k${n}`,
        outcome: { kind: 'create', entity: 'entries', id: `k${n}`, applied: 1 },
        seq: n + 1,
      })),
      snapshotRows: [],
    });

    const steps: Step[] = [];
    importOf(steps, 'secondary', 'full', { snapshot: legacySnapshot });
    readOf(steps, 'secondary', 'entries', 'old');
    appliedIndexOf(steps, 'secondary');
    const secondaryExport = exportOf(steps, 'secondary');
    importOf(steps, 'tertiary', 'full', { snapshot: unsupportedVersionSnapshot });
    importOf(steps, 'tertiary', 'stream', { snapshot: unsupportedVersionSnapshot });
    importOf(steps, 'tertiary', 'full', { snapshot: invalidLedgerSnapshot });
    importOf(steps, 'tertiary', 'stream', { snapshot: invalidLedgerSnapshot });
    importOf(steps, 'tertiary', 'full', { snapshot: noAppliedIndexSnapshot });
    importOf(steps, 'tertiary', 'full', { snapshot: largeLedgerSnapshot });
    const tertiaryExport = exportOf(steps, 'tertiary');

    const results = runProbe(steps, 150_000);
    const next = cursorOver(results);

    expectAccepted(next());
    const legacyRow = next();
    expectFound(legacyRow, true);
    expect(legacyRow.row?.title).toBe('kept');
    expect(next().appliedIndex).toBe(42);
    const relabelled = parseSnapshot(results[secondaryExport].snapshot);
    expect(relabelled.formatVersion).toBe(2);
    expect(relabelled.ledger).toHaveLength(0);
    expect(relabelled.raftAppliedIndex).toBe(42);
    expectAccepted(next());
    expectRefused(next(), 'invalid_request');
    expectRefused(next(), 'invalid_request');
    expectRefused(next(), 'invalid_request');
    expectRefused(next(), 'invalid_request');
    expectRefused(next(), 'invalid_request');
    expectAccepted(next());
    expectAccepted(next());
    const pruned = parseSnapshot(results[tertiaryExport].snapshot);
    expect(pruned.formatVersion).toBe(2);
    expect(pruned.ledger).toHaveLength(4096);
    const writeIds = new Set((pruned.ledger ?? []).map((entry) => entry.writeId));
    expect(writeIds.has('k0')).toBe(false);
    expect(writeIds.has('k1')).toBe(true);
    expect(writeIds.has('k4096')).toBe(true);
    expect(pruned.raftAppliedIndex).toBe(10);
  }, 180_000);
});
