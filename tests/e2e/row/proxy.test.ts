import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-follower-proxy-'));
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
// Raft peer identity of the first fixture node: accepted only for
// forwardedScope-carrying domain.get/domain.query reads.
const PEER = 'node-0';

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

type Target =
  | 'leader'
  | 'follower'
  | 'fake-notLeader'
  | 'fake-forwardFail'
  | 'fake-forwardHang'
  | 'fake-forwardRefused';

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

describe('Domain follower read proxying', () => {
  it('proxies follower domain.get and domain.query reads to the leader with the caller scope', () => {
    const steps: Step[] = [
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1') }),
      call('follower', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
      call('follower', 'domain.query', FORMS, { sql: 'select id, title from records order by id' }),
    ];
    const [created, fetched, selected] = runProbe(steps);
    expectData(created);

    const readData = expectData(fetched);
    expect(readData.found).toBe(true);
    expect(readData.row).toMatchObject({ id: 'r1', slug: 'slug-r1', count: 1 });

    const selectedData = expectData(selected);
    expect(selectedData.columns).toEqual(['id', 'title']);
    expect(selectedData.rowCount).toBe(1);
    expect(selectedData.rows).toEqual([['r1', 'title-r1']]);
  });

  it('answers entity and identifier failures locally and forwards the epoch check', () => {
    const steps: Step[] = [
      call('follower', 'domain.get', FORMS, { entity: 'ghost', id: 'r1' }),
      call('follower', 'domain.get', FORMS, { entity: 'records', id: 'bad id!' }),
      call('follower', 'domain.get', FORMS, { entity: 'records', id: 'r1', epoch: 99 }),
    ];
    const [ghost, malformed, stale] = runProbe(steps);
    expect(expectFailure(ghost, 'invalid_request').unknownOutcome).toBe(false);
    expect(expectFailure(malformed, 'invalid_request').unknownOutcome).toBe(false);
    expectFailure(stale, 'epoch_mismatch');
  });

  it('rejects a caller-supplied forwardedScope claim from a mapped identity', () => {
    const [spoofed] = runProbe([
      call('follower', 'domain.get', FORMS, {
        entity: 'records',
        id: 'r1',
        forwardedScope: { tenant: 'tenant-a', site: 'site-1', group: 'records' },
      }),
    ]);
    const error = expectFailure(spoofed, 'invalid_request');
    expect(error.retryable).toBe(false);
    expect(error.message).toContain('forwardedScope');
    expect(error.message).not.toContain('additionalProperties');
    expect(error.message).not.toContain('tenant-a');
    expect(error.message).not.toContain('site-1');
  });

  it('requires a peer-forwarded read to carry the forwardedScope claim', () => {
    const [unclaimed] = runProbe([
      call('leader', 'domain.get', PEER, { entity: 'records', id: 'r1' }),
    ]);
    expect(unclaimed.envelope).toBeUndefined();
    expect(unclaimed.protocolError).toBeTruthy();
  });

  it('classifies forward transport failures as unavailable and forward deadlines as unknown_outcome', () => {
    const [refused, hung] = runProbe(
      [
        call('fake-forwardFail', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
        call('fake-forwardHang', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
      ],
      60_000,
    );

    const refusedError = expectFailure(refused, 'unavailable');
    expect(refusedError.retryable).toBe(true);
    expect(refusedError.unknownOutcome).toBe(false);

    const hungError = expectFailure(hung, 'unknown_outcome');
    expect(hungError.retryable).toBe(true);
    expect(hungError.unknownOutcome).toBe(true);
  }, 60_000);

  it('passes a proxied leader refusal through without re-forwarding it', () => {
    const [refused] = runProbe([
      call('fake-forwardRefused', 'domain.get', FORMS, { entity: 'records', id: 'r1' }),
    ]);
    const error = expectFailure(refused, 'not_leader');
    expect(error.retryable).toBe(true);
    expect(error.message).toContain('the proxied leader refused the read');
    expect(error.message).not.toContain('reForwarded');
  });
});

// The dispatcher caps one request document at 1 MiB (maxPayloadBytes in
// contracts/v1/plugin.json). domain.get can never legitimately reach it:
// entity and id cap at 128 bytes each and epoch is a plain integer, so the
// largest successfully-decodable get document is ~302 bytes and the real get
// window is limit − maxLegalGet ≈ 1 MiB − 302 bytes (a giant digit-run epoch
// is schema-legal but always fails the int64 decode, so it never succeeds on
// either path). domain.query params are unbounded scalar strings, so a legal
// query payload can reach the limit exactly — that is the boundary under
// test. A proxied call's document is the client payload plus the claim the
// follower injects (~60-90 bytes), so the same request must not change
// verdict depending on which node it entered through.
const PAYLOAD_LIMIT = 1048576;

const boundaryQuery = (overflow: number): Step => {
  const sql = 'select id from records where id = ?';
  const shell = JSON.stringify({ sql, params: [''] });
  return { sql, params: ['p'.repeat(PAYLOAD_LIMIT + overflow - shell.length)] };
};

describe('proxied read payload limit', () => {
  it('answers a boundary-sized read the same way on the follower as on the leader', () => {
    const largestLegalGet = Buffer.byteLength(
      JSON.stringify({ entity: 'e'.repeat(128), id: 'i'.repeat(128), epoch: Number.MAX_SAFE_INTEGER }),
    );
    expect(largestLegalGet).toBeLessThan(400);

    const payload = boundaryQuery(0);
    expect(Buffer.byteLength(JSON.stringify(payload))).toBe(PAYLOAD_LIMIT);

    const [created, viaLeader, viaFollower] = runProbe([
      call('leader', 'domain.create', FORMS, { entity: 'records', id: 'r1', row: row('r1') }),
      call('leader', 'domain.query', FORMS, payload),
      call('follower', 'domain.query', FORMS, payload),
    ]);
    expectData(created);

    const leaderData = expectData(viaLeader);
    const followerData = expectData(viaFollower);
    expect(followerData).toEqual(leaderData);
    expect(followerData.columns).toEqual(['id']);
    expect(followerData.rowCount).toBe(0);
  });

  it('still rejects a client payload over the limit by less than the claim size on both paths', () => {
    // 40 bytes sits under the smallest claim an entry node can inject
    // ({"forwardedScope":{"tenant":"a","site":"b","group":"c"}} is 53 bytes),
    // so this payload can never become a claim-shaped document on the wire.
    const payload = boundaryQuery(40);
    expect(Buffer.byteLength(JSON.stringify(payload))).toBe(PAYLOAD_LIMIT + 40);

    const [viaLeader, viaFollower] = runProbe([
      call('leader', 'domain.query', FORMS, payload),
      call('follower', 'domain.query', FORMS, payload),
    ]);
    for (const result of [viaLeader, viaFollower]) {
      const error = expectFailure(result, 'invalid_request');
      expect(error.message).toBe('payload exceeds the method size limit');
    }
  });

  it('bounds the forwardedScope claim a peer presents before its contents are decoded', () => {
    const [oversized] = runProbe([
      call('leader', 'domain.get', PEER, {
        entity: 'records',
        id: 'r1',
        forwardedScope: { tenant: 't'.repeat(4096), site: 'site-1', group: 'forms' },
      }),
    ]);
    const error = expectFailure(oversized, 'invalid_request');
    expect(error.message).toContain('claim size limit');
    expect(error.message).not.toContain('maxLength');
    expect(error.message).not.toContain('tttt');
  });
});
