import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-stale-revision-'));
  probeBinary = join(binaryDirectory, 'product-api-probe');
  execFileSync('go', ['build', '-o', probeBinary, './tests/fixtures/row/api'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    timeout: 120_000,
  });
}, 180_000);

afterAll(() => {
  if (binaryDirectory) rmSync(binaryDirectory, { recursive: true, force: true });
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

const call = (target: string, method: string, identity: string, payload: Step): Step => ({
  action: 'call',
  target,
  method,
  identity,
  payload,
});

const changeModel = (action: 'migrate' | 'rollback'): Step => ({
  action,
  payload: { refresh: false },
});

const runProbe = (steps: Step[], timeout = 120_000): CallResult[] => {
  const output = execFileSync(probeBinary, {
    input: JSON.stringify({ steps }),
    encoding: 'utf8',
    timeout,
    maxBuffer: 64 * 1024 * 1024,
  });
  const parsed = JSON.parse(output) as { results: CallResult[] };
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

const row = (id: string, overrides: Record<string, unknown> = {}): Step => ({
  id,
  title: `title-${id}`,
  slug: `slug-${id}`,
  count: 1,
  ...overrides,
});

describe('Product API against a store revision moved without a re-wrap', () => {
  it('serves the applied epoch and model after refresh:false migration and rollback', () => {
    const steps: Step[] = [
      call('leader', 'domain.cluster.status', FORMS, {}), // 0 baseline epoch 1
      changeModel('migrate'), // 1 applied epoch 2, adds mig1
      call('leader', 'domain.cluster.status', FORMS, {}), // 2 status epoch
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'fresh1', row: row('fresh1'), epoch: 2 }), // 3 fenced write
      call('leader', 'domain.create', FORMS, { entity: 'mig1', id: 'g1', row: { id: 'g1', label: 'L1' }, epoch: 2 }), // 4 introduced entity
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'fresh1', epoch: 2 }), // 5 fenced read
      changeModel('rollback'), // 6 applied epoch 3, removes mig1
      call('leader', 'domain.cluster.status', FORMS, {}), // 7 status epoch
      call('leader', 'domain.create', FORMS, { entity: 'mig1', id: 'g2', row: { id: 'g2', label: 'L2' }, epoch: 3 }), // 8 removed entity
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'fresh2', row: row('fresh2'), epoch: 3 }), // 9 fenced write
    ];
    const results = runProbe(steps);
    const [baselineStatus, migrated, statusAfterMigrate, fencedWrite, introducedEntity, fencedRead,
      rolledBack, statusAfterRollback, removedEntity, fencedWriteAfterRollback] = results;

    expect(expectData(baselineStatus).epoch).toBe(1);
    expect(migrated.epoch).toBe(2);

    expect(expectData(statusAfterMigrate).epoch).toBe(2);
    expect(expectData(fencedWrite)).toMatchObject({ duplicate: false, entity: 'records', id: 'fresh1', epoch: 2 });
    expect(expectData(introducedEntity)).toMatchObject({ duplicate: false, entity: 'mig1', id: 'g1', epoch: 2 });
    expect(expectData(fencedRead)).toMatchObject({ found: true, epoch: 2 });

    expect(rolledBack.epoch).toBe(3);
    expect(expectData(statusAfterRollback).epoch).toBe(3);
    const removed = expectFailure(removedEntity, 'invalid_request');
    expect(removed.message).toContain('unknown entity mig1');
    expect(expectData(fencedWriteAfterRollback)).toMatchObject({ duplicate: false, entity: 'records', id: 'fresh2', epoch: 3 });
  }, 130_000);
});
