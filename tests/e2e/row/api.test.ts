import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-product-api-'));
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
const RECORDS = 'spiffe://liapoldus/domain/product/owner-records';

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

type Target = 'leader' | 'follower' | 'fake-unavailable' | 'fake-internal' | 'fake-unknownOutcome' | 'fake-barrier' | 'fake-notLeader';

const call = (target: Target, method: string, identity: string, payload: Step): Step => ({
  action: 'call',
  target,
  method,
  identity,
  payload,
});

const runProbe = (steps: Step[], timeout = 120_000): CallResult[] => {
  const output = execFileSync(probeBinary, {
    input: JSON.stringify({ steps }),
    timeout,
  });
  const parsed = JSON.parse(output.toString()) as { results: CallResult[] };
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

describe('Domain product peer API', () => {
  it('serves create, get, update and delete around a leader-confirmed barrier', () => {
    const steps: Step[] = [
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1') }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'absent' }),
      call('leader', 'domain.update', FORMS, { entity: 'records', id: 'r1', row: row('r1', { count: 7 }) }),
      call('leader', 'domain.delete', FORMS, { entity: 'records', id: 'r1' }),
      call('leader', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
    ];
    const [created, read, missing, updated, removed, gone] = runProbe(steps);

    const createdData = expectData(created);
    expect(createdData).toMatchObject({ duplicate: false, entity: 'records', id: 'r1', epoch: 1 });
    expect(createdData.appliedIndex).toBeGreaterThan(0);

    const readData = expectData(read);
    expect(readData.found).toBe(true);
    expect(readData.row).toMatchObject({ id: 'r1', slug: 'slug-r1', count: 1 });

    expect(expectData(missing).found).toBe(false);

    expect(expectData(updated)).toMatchObject({ duplicate: false, entity: 'records', id: 'r1' });

    expect(expectData(removed)).toMatchObject({ duplicate: false });

    expect(expectData(gone).found).toBe(false);
  });

  it('deduplicates a repeated writeId and reports the durable applied index', () => {
    const steps: Step[] = [
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1'), writeId: 'w-1' }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1'), writeId: 'w-1' }),
    ];
    const [first, replay] = runProbe(steps);
    const firstData = expectData(first);
    const replayData = expectData(replay);

    expect(firstData).toMatchObject({ duplicate: false, writeId: 'w-1', entity: 'records', id: 'r1', epoch: 1 });
    expect(replayData).toMatchObject({ duplicate: true, writeId: 'w-1', entity: 'records', id: 'r1', epoch: 1 });
    expect(replayData.appliedIndex as number).toBeGreaterThanOrEqual(firstData.appliedIndex as number);
  });

  it('maps conflicts and scope violations to spec codes', () => {
    const steps: Step[] = [
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1') }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1'), writeId: 'other' }),
      call('leader', 'domain.create', RECORDS, { entity: 'records', id: 'r9', row: row('r9') }),
    ];
    const [created, conflict, forbidden] = runProbe(steps);
    expectData(created);
    expect(expectFailure(conflict, 'conflict').retryable).toBe(false);
    expect(expectFailure(forbidden, 'forbidden').retryable).toBe(false);
  });

  it('reports a missing row on delete as not_found', () => {
    const steps: Step[] = [
      call('leader', 'domain.delete', FORMS, { entity: 'records', id: 'ghost' }),
    ];
    expect(expectFailure(runProbe(steps)[0], 'not_found').unknownOutcome).toBe(false);
  });

  it('rejects unknown entities, malformed identifiers and schema drift as invalid_request', () => {
    const steps: Step[] = [
      call('leader', 'domain.get', FORMS, { entity: 'ghost', id: 'r1' }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'bad id!', row: row('r1') }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1'), unexpected: true }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: { id: 'r1', title: 'only-title' } }),
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1' }),
    ];
    const results = runProbe(steps);
    for (const result of results) {
      expect(expectFailure(result, 'invalid_request').unknownOutcome).toBe(false);
    }
  });

  it('resolves unknown methods and unmapped identities as protocol errors', () => {
    const steps: Step[] = [
      call('leader', 'domain.bogus', FORMS, {}),
      call('leader', 'domain.get', 'spiffe://liapoldus/unknown', { entity: 'records', id: 'r1' }),
    ];
    const [method, identity] = runProbe(steps);
    expect(method.envelope).toBeUndefined();
    expect(method.protocolError).toBeTruthy();
    expect(identity.envelope).toBeUndefined();
    expect(identity.protocolError).toBeTruthy();
  });

  it('reports not_leader with the leader address from a follower and from a stub', () => {
    const steps: Step[] = [
      call('follower', 'domain.create', FORMS, { entity: 'records', id: 'r2', row: row('r2') }),
      call('fake-notLeader', 'domain.create', FORMS, { entity: 'records', id: 'r3', row: row('r3') }),
    ];
    const [follower, stub] = runProbe(steps);
    const followerError = expectFailure(follower, 'not_leader');
    expect(followerError.retryable).toBe(true);
    expect(followerError.message).toContain('leader: node-0');
    expect(expectFailure(stub, 'not_leader').retryable).toBe(true);
  });

  it('maps proposer and barrier failures to unavailable, internal and unknown_outcome', () => {
    const steps: Step[] = [
      call('fake-unavailable', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1') }),
      call('fake-internal', 'domain.create', FORMS, { entity: 'records', id: 'r2', row: row('r2') }),
      call('fake-unknownOutcome', 'domain.create', FORMS, { entity: 'records', id: 'r3', row: row('r3') }),
      call('fake-barrier', 'domain.create', FORMS, { entity: 'records', id: 'r4', row: row('r4') }),
    ];
    const [unavailable, internal, unknown, barrier] = runProbe(steps);

    const unavailableError = expectFailure(unavailable, 'unavailable');
    expect(unavailableError.retryable).toBe(true);
    expect(unavailableError.unknownOutcome).toBe(false);

    const internalError = expectFailure(internal, 'internal');
    expect(internalError.retryable).toBe(false);

    const unknownError = expectFailure(unknown, 'unknown_outcome');
    expect(unknownError.retryable).toBe(true);
    expect(unknownError.unknownOutcome).toBe(true);

    expectFailure(barrier, 'unavailable');
  });

  it('applies a batch atomically with a single writeId and deduplicates replays', () => {
    const steps: Step[] = [
      call('leader', 'domain.batch', FORMS, {
        writeId: 'batch-1',
        ops: [
          { op: 'create', entity: 'audit', id: 'a1', row: { id: 'a1', note: 'n1' } },
          { op: 'create', entity: 'records', id: 'r2', row: row('r2') },
        ],
      }),
      call('leader', 'domain.batch', FORMS, {
        writeId: 'batch-1',
        ops: [{ op: 'create', entity: 'audit', id: 'a1', row: { id: 'a1', note: 'n1' } }],
      }),
      call('leader', 'domain.batch', FORMS, {
        writeId: 'batch-2',
        ops: [{ op: 'create', entity: 'records', id: 'r3', row: row('r3') }, { op: 'explode', entity: 'records', id: 'r4' }],
      }),
      call('leader', 'domain.batch', RECORDS, {
        writeId: 'batch-3',
        ops: [{ op: 'create', entity: 'records', id: 'r5', row: row('r5') }],
      }),
    ];
    const [applied, replay, invalidOp, forbidden] = runProbe(steps);

    expect(expectData(applied)).toMatchObject({ duplicate: false, applied: true, writeId: 'batch-1', epoch: 1 });
    expect(expectData(replay)).toMatchObject({ duplicate: true, applied: false, writeId: 'batch-1' });
    expectFailure(invalidOp, 'invalid_request');
    expectFailure(forbidden, 'forbidden');
  });

  it('runs a SELECT over owned entities and rejects other statements and non-scalar parameters', () => {
    const manyParams = Array.from({ length: 65 }, () => 1);
    const steps: Step[] = [
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1') }),
      call('leader', 'domain.query', FORMS, { sql: 'select id, title from records order by id' }),
      call('leader', 'domain.query', FORMS, { sql: 'delete from records' }),
      call('leader', 'domain.query', FORMS, { sql: 'select id from records where title = ?', params: [{ nested: true }] }),
      call('leader', 'domain.query', FORMS, { sql: 'select id from records', params: manyParams }),
      call('leader', 'domain.query', FORMS, { sql: 'select id from records', maxRows: 10001 }),
    ];
    const [created, selected, rejected, nested, tooMany, maxRows] = runProbe(steps);
    expectData(created);

    const selectedData = expectData(selected);
    expect(selectedData.columns).toEqual(['id', 'title']);
    expect(selectedData.rows).toEqual([['r1', 'title-r1']]);
    expect(selectedData.rowCount).toBe(1);
    expect(selectedData.epoch).toBe(1);

    expectFailure(rejected, 'query_rejected');
    expectFailure(nested, 'invalid_request');
    expectFailure(tooMany, 'invalid_request');
    expectFailure(maxRows, 'invalid_request');
  });

  it('returns cluster status from any node', () => {
    const steps: Step[] = [call('follower', 'domain.cluster.status', FORMS, {})];
    const status = expectData(runProbe(steps)[0]);
    expect(status.ready).toBe(true);
    expect(status.leader).toBeTruthy();
    expect(status.quorum).toBeGreaterThan(0);
    expect(status.voters).toBeInstanceOf(Array);
    expect(status.epoch).toBe(1);
  });
});
