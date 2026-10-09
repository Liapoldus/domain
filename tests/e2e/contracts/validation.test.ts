import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-schema-validation-'));
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
// Raft peer identity of the first fixture node: peers are not product
// callers, they may only present the reserved forwardedScope claim.
const PEER = 'node-0';
const CLAIM = { tenant: 'tenant-a', site: 'site-1', group: 'forms' };

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

const call = (method: string, payload: unknown): Step => ({
  action: 'call',
  target: 'leader',
  method,
  identity: FORMS,
  payload,
});

const callAs = (identity: string, method: string, payload: unknown): Step => ({
  action: 'call',
  target: 'leader',
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

const runRawPayloadProbe = (method: string, rawPayload: string, following: Step[] = []): CallResult[] => {
  const steps = [
    `{"action":"call","target":"leader","method":${JSON.stringify(method)},"identity":${JSON.stringify(FORMS)},"payload":${rawPayload}}`,
    ...following.map((step) => JSON.stringify(step)),
  ];
  const output = execFileSync(probeBinary, {
    input: `{"steps":[${steps.join(',')}]}`,
    timeout: 120_000,
  });
  const parsed = JSON.parse(output.toString()) as { results: CallResult[] };
  return parsed.results;
};

// Every schema violation must read as "<instance pointer>: <keyword>" —
// pointers and keywords only, never the submitted values.
const expectViolation = (result: CallResult, pointer: string, keyword: string): EnvelopeError => {
  expect(result.protocolError).toBeUndefined();
  expect(result.envelope?.ok).toBe(false);
  const error = result.envelope?.error;
  expect(error?.code).toBe('invalid_request');
  expect(error?.retryable).toBe(false);
  expect(error?.unknownOutcome).toBe(false);
  expect(error?.message).toContain(`${pointer}: ${keyword}`);
  return error as EnvelopeError;
};

const expectSuccess = (result: CallResult): Record<string, unknown> => {
  expect(result.protocolError).toBeUndefined();
  expect(result.envelope?.ok).toBe(true);
  return result.envelope?.data ?? {};
};

describe('domain request schema validation', () => {
  it('reports the failing instance pointer and keyword for type violations', () => {
    const [invalid] = runProbe([call('domain.create', { entity: 123, id: 'r1', row: { id: 'r1' } })]);
    const error = expectViolation(invalid, '/entity', 'type');
    expect(error.message).not.toContain('123');
  });

  it('rejects unknown top-level fields without echoing them', () => {
    const [invalid] = runProbe([call('domain.get', { entity: 'records', id: 'r1', tenant: 'tenant-a' })]);
    const error = expectViolation(invalid, '/', 'additionalProperties');
    expect(error.message).not.toContain('tenant-a');
  });

  it('rejects a writeId outside the contract pattern without echoing it', () => {
    const [invalid] = runProbe([
      call('domain.create', { entity: 'records', id: 'r1', row: { id: 'r1' }, writeId: 'bad writeId!' }),
    ]);
    const error = expectViolation(invalid, '/writeId', 'pattern');
    expect(error.message).not.toContain('bad writeId!');
  });

  it('enforces the batch ops count declared by the schema', () => {
    const ops = Array.from({ length: 101 }, (_, index) => ({
      op: 'create',
      entity: 'records',
      id: `b${index}`,
      row: { id: `b${index}` },
    }));
    const [invalid] = runProbe([call('domain.batch', { writeId: 'big-batch', ops })]);
    expectViolation(invalid, '/ops', 'maxItems');
  });

  it('enforces the batch op enum without echoing the submitted value', () => {
    const [invalid] = runProbe([
      call('domain.batch', { writeId: 'enum-check', ops: [{ op: 'explode', entity: 'records', id: 'r1' }] }),
    ]);
    const error = expectViolation(invalid, '/ops/0/op', 'enum');
    expect(error.message).not.toContain('explode');
  });

  it('reports missing required fields at the failing instance', () => {
    const [invalid] = runProbe([call('domain.create', { entity: 'records', id: 'r1' })]);
    expectViolation(invalid, '/', 'required');
  });

  it('never echoes submitted row or identity data in schema messages', () => {
    const [invalid] = runProbe([
      call('domain.create', {
        entity: 'records',
        id: 'secret-id-9',
        writeId: 'w-secret',
        row: { id: 'secret-id-9', title: { nested: 'topsecret' } },
      }),
    ]);
    const error = expectViolation(invalid, '/row/title', 'type');
    expect(error.message).not.toContain('topsecret');
    expect(error.message).not.toContain('secret-id-9');
    expect(error.message).not.toContain('w-secret');
  });

  it('rejects duplicate JSON object keys before a write can reach the FSM', () => {
    const [invalid, readback] = runRawPayloadProbe(
      'domain.create',
      '{"entity":"records","id":"first","id":"second","row":{"id":"second","title":"t","slug":"s"},"writeId":"duplicate-key"}',
      [{ action: 'call', target: 'leader', method: 'domain.get', identity: FORMS, payload: { entity: 'records', id: 'second' } }],
    );

    expect(invalid.envelope).toMatchObject({
      ok: false,
      error: { code: 'invalid_request', message: 'request payload must not contain duplicate object keys' },
    });
    expect(readback.envelope).toMatchObject({
      ok: true,
      data: { found: false },
    });
  });

  it('rejects duplicate keys at every nested object depth, including escaped aliases', () => {
    const [invalid] = runRawPayloadProbe(
      'domain.create',
      '{"entity":"records","id":"nested-duplicate","row":{"id":"nested-duplicate","title":"t","slug":"s","\\u006eote":"first","note":"second"},"writeId":"nested-duplicate"}',
    );

    expect(invalid.envelope).toMatchObject({
      ok: false,
      error: { code: 'invalid_request', message: 'request payload must not contain duplicate object keys' },
    });
  });

  it('validates every method against its own request schema', () => {
    const cases: Array<{ method: string; payload: unknown; pointer: string; keyword: string }> = [
      { method: 'domain.create', payload: {}, pointer: '/', keyword: 'required' },
      { method: 'domain.update', payload: { entity: 'records' }, pointer: '/', keyword: 'required' },
      {
        method: 'domain.delete',
        payload: { entity: 'records', id: 'r1', row: { id: 'r1' } },
        pointer: '/',
        keyword: 'additionalProperties',
      },
      { method: 'domain.batch', payload: { writeId: 'w-1', ops: [] }, pointer: '/ops', keyword: 'minItems' },
      { method: 'domain.get', payload: { sql: 'select 1' }, pointer: '/', keyword: 'required' },
      {
        method: 'domain.query',
        payload: { entity: 'records', sql: 'select 1', maxRows: 0 },
        pointer: '/maxRows',
        keyword: 'minimum',
      },
      {
        method: 'domain.query',
        payload: { sql: 'select 1', params: [{ nested: true }] },
        pointer: '/params/0',
        keyword: 'type',
      },
      {
        method: 'domain.cluster.status',
        payload: { entity: 'records' },
        pointer: '/',
        keyword: 'additionalProperties',
      },
      { method: 'domain.get', payload: 42, pointer: '/', keyword: 'type' },
    ];
    const results = runProbe(cases.map((entry) => call(entry.method, entry.payload)));
    cases.forEach((entry, index) => {
      expectViolation(results[index], entry.pointer, entry.keyword);
    });
  });

  it('still accepts valid payloads for every method', () => {
    const steps: Step[] = [
      call('domain.create', {
        entity: 'records',
        id: 's1',
        row: { id: 's1', title: 't1', slug: 's1', count: 1 },
        writeId: 'sv-1',
      }),
      call('domain.get', { entity: 'records', id: 's1' }),
      call('domain.update', {
        entity: 'records',
        id: 's1',
        row: { id: 's1', title: 't2', slug: 's1', count: 2 },
        writeId: 'sv-2',
      }),
      call('domain.batch', {
        writeId: 'sv-3',
        ops: [
          { op: 'create', entity: 'audit', id: 'sa1', row: { id: 'sa1', note: 'n' } },
          { op: 'delete', entity: 'audit', id: 'sa1' },
        ],
      }),
      call('domain.query', { sql: 'select id, title from records order by id' }),
      call('domain.delete', { entity: 'records', id: 's1', writeId: 'sv-4' }),
      call('domain.cluster.status', {}),
    ];
    const results = runProbe(steps);
    for (const result of results) {
      expectSuccess(result);
    }
  });

  it('accepts a peer-forwarded read carrying the reserved forwardedScope claim', () => {
    const [get, query] = runProbe([
      callAs(PEER, 'domain.get', { entity: 'records', id: 's1', forwardedScope: CLAIM }),
      callAs(PEER, 'domain.query', { sql: 'select id, title from records', forwardedScope: CLAIM }),
    ]);
    expectSuccess(get);
    expectSuccess(query);
  });

  it('rejects a forwardedScope claim outside the read request contract without echoing it', () => {
    const [write, malformed] = runProbe([
      callAs(PEER, 'domain.create', { entity: 'records', id: 'r1', row: { id: 'r1' }, forwardedScope: CLAIM }),
      callAs(PEER, 'domain.get', { entity: 'records', id: 'r1', forwardedScope: { tenant: 'tenant-a', site: 'site-1' } }),
    ]);
    const writeError = expectViolation(write, '/', 'additionalProperties');
    expect(writeError.message).not.toContain('tenant-a');
    const malformedError = expectViolation(malformed, '/forwardedScope', 'required');
    expect(malformedError.message).not.toContain('tenant-a');
  });
});
