import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-cluster-migration-'));
  probeBinary = join(binaryDirectory, 'cluster-migration-probe');
  execFileSync('go', ['build', '-o', probeBinary, './tests/fixtures/migration/cluster'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    timeout: 120_000,
  });
}, 180_000);

afterAll(() => {
  if (binaryDirectory) rmSync(binaryDirectory, { recursive: true, force: true });
}, 180_000);

const FORMS = 'spiffe://liapoldus/domain/product/owner-forms';
const OTHER = 'spiffe://liapoldus/domain/product/other-scope';

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
};

type RunOutcome = {
  crashed: boolean;
  results: CallResult[];
};

type Field = {
  name: string;
  type: string;
  primaryKey?: boolean;
  required?: boolean;
  unique?: boolean;
  default?: unknown;
  references?: { entity: string; field: string };
};

type Entity = { name: string; ownerGroup: string; fields: Field[] };

type Model = {
  schemaVersion: string;
  entities: Entity[];
  migrations?: { entity: string; from: string; to: string; conversion?: string }[];
};

type AuditEntry = {
  kind: string;
  writeId: string;
  epochBefore: number;
  epochAfter: number;
  fingerprintBefore: string;
  fingerprintAfter: string;
};

type StateRow = {
  entity: string;
  id: string;
  found: boolean;
  document?: Record<string, unknown>;
};

type StateData = {
  epoch: number;
  appliedIndex: number;
  model: Model;
  fingerprint: string;
  rows: StateRow[];
  snapshotAvailable: boolean;
  snapshotFingerprint?: string;
  audit: AuditEntry[];
  epochs: number[];
  appliedIndexes: number[];
};

type PlanData = {
  safe: boolean;
  planError?: string;
  steps?: Step[];
  epoch: number;
  appliedIndex: number;
  fingerprint: string;
  fingerprintAfter: string;
};

type WriteData = {
  writeId: string;
  duplicate: boolean;
  applied: boolean;
  epochBefore: number;
  epochAfter: number;
  fingerprintBefore: string;
  fingerprintAfter: string;
  appliedIndex: number;
};

type StatusData = {
  epoch: number;
  appliedIndex: number;
  fingerprint: string;
  snapshotAvailable: boolean;
  snapshotFingerprint?: string;
  audit: AuditEntry[];
};

type ImportData = {
  model: Model;
  epoch: number;
  fingerprint: string;
  snapshotAvailable: boolean;
  snapshotFingerprint?: string;
  audit: AuditEntry[];
  rows: StateRow[];
};

const baseModel = (): Model => ({
  schemaVersion: '1',
  entities: [{
    name: 'records',
    ownerGroup: 'forms',
    fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'title', type: 'text', required: true },
      { name: 'slug', type: 'text', required: true, unique: true },
      { name: 'count', type: 'int64' },
    ],
  }],
});

const migratedModel = (): Model => ({
  schemaVersion: '1',
  entities: [{
    name: 'records',
    ownerGroup: 'forms',
    fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'heading', type: 'text', required: true },
      { name: 'slug', type: 'text', required: true, unique: true },
      { name: 'count', type: 'int64' },
      { name: 'note', type: 'text' },
    ],
  }],
  migrations: [{ entity: 'records', from: 'title', to: 'heading' }],
});

const destructiveModel = (): Model => ({
  schemaVersion: '1',
  entities: [{
    name: 'other',
    ownerGroup: 'forms',
    fields: [{ name: 'id', type: 'text', primaryKey: true, required: true }],
  }],
});

const unmappedModel = (): Model => {
  const model = baseModel();
  model.entities[0].fields[1].name = 'heading';
  return model;
};

const ownershipModel = (): Model => {
  const model = baseModel();
  model.entities[0].ownerGroup = 'records';
  return model;
};

const violationModel = (): Model => {
  const model = migratedModel();
  model.entities[0].fields[3].unique = true;
  return model;
};

const R1_BASE_DOC = { id: 'r1', title: 'hello', slug: 's1', count: 1 };
const R1_MIGRATED_DOC = { id: 'r1', heading: 'hello', slug: 's1', count: 1 };
const R1_ROW: StateRow = { entity: 'records', id: 'r1', found: true, document: R1_BASE_DOC };
const R1_MIGRATED_ROW: StateRow = { entity: 'records', id: 'r1', found: true, document: R1_MIGRATED_DOC };
const R2_ABSENT: StateRow = { entity: 'records', id: 'r2', found: false };

const migrationSteps = [
  { kind: 'renameField', entity: 'records', field: 'title', target: 'heading' },
  { kind: 'addField', entity: 'records', field: 'note' },
];

const call = (target: string, method: string, identity: string, payload: Step): Step => ({
  action: 'call',
  target,
  method,
  identity,
  payload,
});

const createRow = (id: string, row: Step): Step =>
  call('leader', 'domain.create', FORMS, { entity: 'records', id, row });

const planCall = (model: Model, epoch?: number): Step =>
  call('leader', 'domain.migration.plan', FORMS, epoch === undefined ? { next: model } : { next: model, epoch });

const applyCall = (model: Model, writeId: string, identity = FORMS, target = 'leader'): Step => ({
  action: 'reload',
  target,
  identity,
  payload: { next: model, writeId },
});

const rollbackCall = (writeId: string, identity = FORMS, target = 'leader'): Step => ({
  action: 'reload',
  target,
  identity,
  mode: 'coreRollback',
  payload: { writeId },
});

const statusCall = (identity = FORMS): Step =>
  call('leader', 'domain.migration.status', identity, {});

const runProbe = (steps: Step[], workRoot: string, timeout = 120_000): RunOutcome => {
  try {
    const output = execFileSync(probeBinary, {
      input: JSON.stringify({ model: baseModel(), steps }),
      env: { ...process.env, PROBE_ROOT: workRoot },
      encoding: 'utf8',
      timeout,
    });
    const parsed = JSON.parse(output) as { results: CallResult[] };
    return { crashed: false, results: parsed.results };
  } catch (error) {
    const failure = error as { status?: number | null; stderr?: string };
    if (failure.status === 42) return { crashed: true, results: [] };
    const detail = failure.stderr ? `: ${failure.stderr.trim()}` : '';
    throw new Error(`cluster-migration probe failed (status ${failure.status})${detail}`);
  }
};

const dataOf = (result: CallResult): Record<string, unknown> => {
  expect(result.protocolError).toBeUndefined();
  expect(result.envelope?.ok).toBe(true);
  return result.envelope?.data ?? {};
};

const failureOf = (result: CallResult, code: string): EnvelopeError => {
  expect(result.protocolError).toBeUndefined();
  expect(result.envelope?.ok).toBe(false);
  const error = result.envelope?.error;
  expect(error?.code).toBe(code);
  return error as EnvelopeError;
};

const stateOf = (result: CallResult): StateData => dataOf(result) as unknown as StateData;
const planOf = (result: CallResult): PlanData => dataOf(result) as unknown as PlanData;
const writeOf = (result: CallResult): WriteData => dataOf(result) as unknown as WriteData;
const statusOf = (result: CallResult): StatusData => dataOf(result) as unknown as StatusData;
const importOf = (result: CallResult): ImportData => dataOf(result) as unknown as ImportData;

const withRoot = (body: (workRoot: string) => void): void => {
  const workRoot = mkdtempSync(join(tmpdir(), 'domain-cluster-migration-run-'));
  try {
    body(workRoot);
  } finally {
    rmSync(workRoot, { recursive: true, force: true });
  }
};

describe('Domain cluster migration lifecycle', () => {
  it('rejects direct peer calls that attempt to apply or roll back the ER model', () => {
    withRoot((workRoot) => {
      const run = runProbe([
        call('leader', 'domain.migration.apply', FORMS, { next: migratedModel(), writeId: 'direct' }),
        call('leader', 'domain.migration.rollback', FORMS, { writeId: 'direct' }),
      ], workRoot);

      expect(run.results).toHaveLength(2);
      expect(run.results[0].protocolError).toContain('unknown domain product method');
      expect(run.results[1].protocolError).toContain('unknown domain product method');
    });
  });

  it('plans and reports status publicly; model changes are applied only by the configuration reload path', () => {
    withRoot((workRoot) => {
      const steps: Step[] = [
        createRow('r1', { id: 'r1', title: 'hello', slug: 's1', count: 1 }),
        planCall(migratedModel()),
        applyCall(migratedModel(), 'mig-1'),
        applyCall(migratedModel(), 'mig-1'),
        call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
        createRow('r2', { id: 'r2', heading: 'world', slug: 's2', count: 2 }),
        call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r2' }),
        planCall(migratedModel(), 1),
        rollbackCall('rb-1'),
        call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
        call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r2' }),
        rollbackCall('rb-2'),
        { action: 'state' },
      ];
      const run = runProbe(steps, workRoot);
      expect(run.crashed).toBe(false);
      expect(run.results).toHaveLength(13);

      dataOf(run.results[0]);

      const plan = planOf(run.results[1]);
      expect(plan.safe).toBe(true);
      expect(plan.epoch).toBe(1);
      expect(plan.steps).toEqual(migrationSteps);

      const applied = writeOf(run.results[2]);
      expect(applied).toMatchObject({
        writeId: 'mig-1',
        duplicate: false,
        applied: true,
        epochBefore: 1,
        epochAfter: 2,
      });
      expect(applied.fingerprintBefore).not.toBe(applied.fingerprintAfter);
      expect(plan.fingerprint).toBe(applied.fingerprintBefore);
      expect(plan.fingerprintAfter).toBe(applied.fingerprintAfter);

      const replayed = writeOf(run.results[3]);
      expect(replayed).toMatchObject({
        writeId: 'mig-1',
        duplicate: true,
        applied: false,
        epochBefore: 2,
        epochAfter: 2,
        fingerprintBefore: applied.fingerprintAfter,
        fingerprintAfter: applied.fingerprintAfter,
      });

      const migratedRead = dataOf(run.results[4]);
      expect(migratedRead.found).toBe(true);
      expect(migratedRead.row).toEqual(R1_MIGRATED_DOC);
      expect(migratedRead.row).not.toHaveProperty('title');

      dataOf(run.results[5]);
      const createdRead = dataOf(run.results[6]);
      expect(createdRead.found).toBe(true);

      failureOf(run.results[7], 'epoch_mismatch');

      const rolled = writeOf(run.results[8]);
      expect(rolled).toMatchObject({
        writeId: 'rb-1',
        duplicate: false,
        applied: true,
        epochBefore: 2,
        epochAfter: 3,
        fingerprintBefore: applied.fingerprintAfter,
        fingerprintAfter: applied.fingerprintBefore,
      });

      const restoredRead = dataOf(run.results[9]);
      expect(restoredRead.found).toBe(true);
      expect(restoredRead.row).toEqual(R1_BASE_DOC);

      const removedRead = dataOf(run.results[10]);
      expect(removedRead.found).toBe(false);

      failureOf(run.results[11], 'conflict');

      const state = stateOf(run.results[12]);
      expect(state.epoch).toBe(3);
      expect(state.model).toEqual(baseModel());
      expect(state.fingerprint).toBe(applied.fingerprintBefore);
      expect(state.snapshotAvailable).toBe(false);
      expect(state.snapshotFingerprint).toBeUndefined();
      expect(state.audit).toEqual([
        {
          kind: 'rollback',
          writeId: 'rb-1',
          epochBefore: 2,
          epochAfter: 3,
          fingerprintBefore: applied.fingerprintAfter,
          fingerprintAfter: applied.fingerprintBefore,
        },
        {
          kind: 'migrate',
          writeId: 'mig-1',
          epochBefore: 1,
          epochAfter: 2,
          fingerprintBefore: applied.fingerprintBefore,
          fingerprintAfter: applied.fingerprintAfter,
        },
      ]);
      expect(state.epochs).toEqual([3, 3, 3]);
      expect(new Set(state.appliedIndexes).size).toBe(1);
      expect(state.rows).toEqual([R1_ROW, R2_ABSENT]);
    });
  }, 120_000);

  it('rejects unsafe plans and scopes migration status', () => {
    withRoot((workRoot) => {
      const steps: Step[] = [
        createRow('r1', { id: 'r1', title: 'hello', slug: 's1', count: 1 }),
        createRow('r3', { id: 'r3', title: 'third', slug: 's3', count: 1 }),
        planCall(destructiveModel()),
        planCall(unmappedModel()),
        planCall(ownershipModel()),
        planCall(violationModel()),
        applyCall(destructiveModel(), 'w-d'),
        applyCall(violationModel(), 'w-u'),
        call('follower', 'domain.migration.plan', FORMS, { next: migratedModel() }),
        applyCall(migratedModel(), 'w-f', FORMS, 'follower'),
        rollbackCall('w-f', FORMS, 'follower'),
        call('follower', 'domain.migration.status', FORMS, {}),
        call('leader', 'domain.migration.apply', FORMS, { next: migratedModel() }),
        planCall(migratedModel(), 42),
        applyCall(migratedModel(), 'w1'),
        statusCall(FORMS),
        statusCall(OTHER),
      ];
      const run = runProbe(steps, workRoot);
      expect(run.crashed).toBe(false);
      expect(run.results).toHaveLength(17);

      dataOf(run.results[0]);
      dataOf(run.results[1]);

      expect(planOf(run.results[2])).toMatchObject({
        safe: false,
        planError: 'destructive_change',
      });
      expect(planOf(run.results[3])).toMatchObject({
        safe: false,
        planError: 'mapping_required',
      });
      expect(planOf(run.results[4])).toMatchObject({
        safe: false,
        planError: 'ownership_change',
      });
      const violationPlan = planOf(run.results[5]);
      expect(violationPlan.safe).toBe(true);
      expect(violationPlan.steps).toEqual(migrationSteps);
      expect(violationPlan.planError).toBeUndefined();

      expect(failureOf(run.results[6], 'invalid_request').message).toContain('destructive_change');
      expect(failureOf(run.results[7], 'invalid_request').message).toContain('row_unique_violation');

      failureOf(run.results[8], 'not_leader');
      failureOf(run.results[9], 'not_leader');
      failureOf(run.results[10], 'not_leader');

      const followerStatus = statusOf(run.results[11]);
      expect(followerStatus).toMatchObject({
        epoch: 1,
        snapshotAvailable: false,
        audit: [],
      });

      expect(run.results[12].protocolError).toContain('unknown domain product method');
      failureOf(run.results[13], 'epoch_mismatch');

      const applied = writeOf(run.results[14]);
      expect(applied).toMatchObject({
        writeId: 'w1',
        duplicate: false,
        applied: true,
        epochBefore: 1,
        epochAfter: 2,
      });

      const ownerStatus = statusOf(run.results[15]);
      expect(ownerStatus).toMatchObject({
        epoch: 2,
        snapshotAvailable: true,
        snapshotFingerprint: applied.fingerprintBefore,
      });
      expect(ownerStatus.audit).toHaveLength(1);
      expect(ownerStatus.audit[0]).toMatchObject({
        kind: 'migrate',
        writeId: 'w1',
        epochBefore: 1,
        epochAfter: 2,
      });

      const otherStatus = statusOf(run.results[16]);
      expect(otherStatus).toMatchObject({ epoch: 2, audit: [] });
    });
  }, 120_000);

  it('recovers durable migration state after a crash before apply', () => {
    withRoot((workRoot) => {
      const crashed = runProbe([
        createRow('r1', { id: 'r1', title: 'hello', slug: 's1', count: 1 }),
        planCall(migratedModel()),
        { action: 'crash' },
      ], workRoot);
      expect(crashed.crashed).toBe(true);

      const run = runProbe([
        { action: 'state' },
        planCall(migratedModel()),
        applyCall(migratedModel(), 'mig-1'),
        { action: 'state' },
      ], workRoot);
      expect(run.crashed).toBe(false);
      expect(run.results).toHaveLength(4);

      const before = stateOf(run.results[0]);
      expect(before.epoch).toBe(1);
      expect(before.model).toEqual(baseModel());
      expect(before.snapshotAvailable).toBe(false);
      expect(before.audit).toEqual([]);
      expect(before.epochs).toEqual([1, 1, 1]);
      expect(new Set(before.appliedIndexes).size).toBe(1);
      expect(before.rows).toEqual([R1_ROW, R2_ABSENT]);

      const plan = planOf(run.results[1]);
      expect(plan.safe).toBe(true);
      expect(plan.epoch).toBe(1);
      expect(plan.steps).toEqual(migrationSteps);
      expect(plan.fingerprint).toBe(before.fingerprint);

      const applied = writeOf(run.results[2]);
      expect(applied).toMatchObject({
        writeId: 'mig-1',
        duplicate: false,
        applied: true,
        epochBefore: 1,
        epochAfter: 2,
        fingerprintBefore: before.fingerprint,
      });

      const after = stateOf(run.results[3]);
      expect(after.epoch).toBe(2);
      expect(after.model).toEqual(migratedModel());
      expect(after.fingerprint).toBe(applied.fingerprintAfter);
      expect(after.snapshotAvailable).toBe(true);
      expect(after.snapshotFingerprint).toBe(applied.fingerprintBefore);
      expect(after.audit).toHaveLength(1);
      expect(after.audit[0]).toMatchObject({
        kind: 'migrate',
        writeId: 'mig-1',
        epochBefore: 1,
        epochAfter: 2,
      });
      expect(after.epochs).toEqual([2, 2, 2]);
      expect(after.rows).toEqual([R1_MIGRATED_ROW, R2_ABSENT]);
    });
  }, 120_000);

  it('converges to exactly one outcome when a crash interrupts apply', () => {
    withRoot((workRoot) => {
      const crashed = runProbe([
        createRow('r1', { id: 'r1', title: 'hello', slug: 's1', count: 1 }),
        {
          action: 'crashDuringReload',
          target: 'leader',
          identity: FORMS,
          payload: { next: migratedModel(), writeId: 'mig-1' },
          delayMs: 75,
        },
      ], workRoot);
      expect(crashed.crashed).toBe(true);

      const run = runProbe([
        { action: 'state' },
        applyCall(migratedModel(), 'mig-1'),
        { action: 'state' },
      ], workRoot);
      expect(run.crashed).toBe(false);
      expect(run.results).toHaveLength(3);

      const before = stateOf(run.results[0]);
      expect([1, 2]).toContain(before.epoch);
      const alreadyApplied = before.epoch === 2;
      expect(before.model).toEqual(alreadyApplied ? migratedModel() : baseModel());
      expect(before.audit).toHaveLength(alreadyApplied ? 1 : 0);

      const retried = writeOf(run.results[1]);
      expect(retried.writeId).toBe('mig-1');
      expect(retried.duplicate).toBe(alreadyApplied);
      if (alreadyApplied) {
        expect(retried).toMatchObject({
          applied: false,
          epochBefore: 2,
          epochAfter: 2,
          fingerprintBefore: before.fingerprint,
          fingerprintAfter: before.fingerprint,
        });
      } else {
        expect(retried).toMatchObject({
          applied: true,
          epochBefore: 1,
          epochAfter: 2,
          fingerprintBefore: before.fingerprint,
        });
        expect(retried.fingerprintAfter).not.toBe(retried.fingerprintBefore);
      }

      const after = stateOf(run.results[2]);
      expect(after.epoch).toBe(2);
      expect(after.model).toEqual(migratedModel());
      expect(after.audit).toHaveLength(1);
      expect(after.snapshotAvailable).toBe(true);
      expect(after.epochs).toEqual([2, 2, 2]);
      expect(after.rows[0]).toEqual(R1_MIGRATED_ROW);
    });
  }, 120_000);

  it('deduplicates an apply retried after a successful crash', () => {
    withRoot((workRoot) => {
      const crashed = runProbe([
        createRow('r1', { id: 'r1', title: 'hello', slug: 's1', count: 1 }),
        applyCall(migratedModel(), 'mig-1'),
        { action: 'crash' },
      ], workRoot);
      expect(crashed.crashed).toBe(true);

      const run = runProbe([
        { action: 'state' },
        applyCall(migratedModel(), 'mig-1'),
        { action: 'state' },
      ], workRoot);
      expect(run.crashed).toBe(false);
      expect(run.results).toHaveLength(3);

      const before = stateOf(run.results[0]);
      expect(before.epoch).toBe(2);
      expect(before.model).toEqual(migratedModel());
      expect(before.audit).toHaveLength(1);

      const retried = writeOf(run.results[1]);
      expect(retried).toMatchObject({
        writeId: 'mig-1',
        duplicate: true,
        applied: false,
        epochBefore: 2,
        epochAfter: 2,
        fingerprintBefore: before.fingerprint,
        fingerprintAfter: before.fingerprint,
      });

      const after = stateOf(run.results[2]);
      expect(after.epoch).toBe(2);
      expect(after.fingerprint).toBe(before.fingerprint);
      expect(after.audit).toHaveLength(1);
      expect(after.epochs).toEqual([2, 2, 2]);
      expect(after.rows).toEqual([R1_MIGRATED_ROW, R2_ABSENT]);
    });
  }, 120_000);

  it('round-trips durable migration state through snapshot export and import', () => {
    withRoot((workRoot) => {
      const first = runProbe([
        createRow('r1', { id: 'r1', title: 'hello', slug: 's1', count: 1 }),
        applyCall(migratedModel(), 'mig-1'),
        { action: 'export' },
        rollbackCall('rb-1'),
        { action: 'state' },
        { action: 'export' },
      ], workRoot);
      expect(first.crashed).toBe(false);
      expect(first.results).toHaveLength(6);

      dataOf(first.results[0]);
      const applied = writeOf(first.results[1]);
      const exportApplied = dataOf(first.results[2]).data as string;
      const rolled = writeOf(first.results[3]);
      expect(rolled).toMatchObject({
        writeId: 'rb-1',
        applied: true,
        epochBefore: 2,
        epochAfter: 3,
        fingerprintBefore: applied.fingerprintAfter,
        fingerprintAfter: applied.fingerprintBefore,
      });

      const mid = stateOf(first.results[4]);
      expect(mid).toMatchObject({ epoch: 3, snapshotAvailable: false });
      expect(mid.model).toEqual(baseModel());
      expect(mid.fingerprint).toBe(applied.fingerprintBefore);
      expect(mid.audit).toHaveLength(2);

      const exportRestored = dataOf(first.results[5]).data as string;
      expect(exportApplied).not.toBe(exportRestored);

      const second = runProbe([
        { action: 'import', data: exportApplied, mode: 'full' },
        { action: 'import', data: exportRestored, mode: 'full' },
        { action: 'import', data: exportRestored, mode: 'stream' },
      ], workRoot);
      expect(second.crashed).toBe(false);
      expect(second.results).toHaveLength(3);

      const importedApplied = importOf(second.results[0]);
      expect(importedApplied).toMatchObject({
        epoch: 2,
        snapshotAvailable: true,
        fingerprint: applied.fingerprintAfter,
        snapshotFingerprint: applied.fingerprintBefore,
      });
      expect(importedApplied.model).toEqual(migratedModel());
      expect(importedApplied.audit).toHaveLength(1);
      expect(importedApplied.audit[0]).toMatchObject({
        kind: 'migrate',
        writeId: 'mig-1',
        epochBefore: 1,
        epochAfter: 2,
      });
      expect(importedApplied.rows).toEqual([R1_MIGRATED_ROW, R2_ABSENT]);

      for (const restored of [importOf(second.results[1]), importOf(second.results[2])]) {
        expect(restored).toMatchObject({
          epoch: 3,
          fingerprint: mid.fingerprint,
          snapshotAvailable: false,
        });
        expect(restored.model).toEqual(baseModel());
        expect(restored.audit).toEqual(mid.audit);
        expect(restored.rows).toEqual([R1_ROW, R2_ABSENT]);
      }
    });
  }, 120_000);
});
