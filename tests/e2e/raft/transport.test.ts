import { parseJson } from '../../support/json.js';
import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';


import { root } from '../../support/paths.js';

describe('Domain Raft peer transport', () => {
  it('declares only consumer-owned Raft RPC methods over generic peer calls and streams', () => {
    const contract = parseJson(readFileSync(resolve(root, 'contracts/v1/raft-peer.json'), 'utf8')) as {schemaVersion: string; methods: Record<string, {mode: string; maxPayloadBytes?: number; maxBytes?: number; timeoutMs: number}>};
    expect(contract.schemaVersion).toBe('liapoldus.domain.raft-peer.v1');
    expect(contract.methods.appendEntries.mode).toBe('call');
    expect(contract.methods.requestVote.mode).toBe('call');
    expect(contract.methods.requestPreVote.mode).toBe('call');
    expect(contract.methods.timeoutNow.mode).toBe('call');
    expect(contract.methods.appendEntries.maxPayloadBytes).toBe(4194304);
    expect(contract.methods.appendEntries.timeoutMs).toBe(10000);
    expect(contract.methods.installSnapshot.mode).toBe('stream');
    expect(contract.methods.installSnapshot.maxBytes).toBe(536870912);
    expect(contract.methods.installSnapshot.timeoutMs).toBe(600000);
  });

  it('carries authenticated Raft heartbeats, append entries, and bounded snapshot bytes between child processes', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/raft/transport'], {
      cwd: root,
      env: { ...process.env, GOWORK: 'off' },
      encoding: 'utf8',
      timeout: 60_000,
    });
    expect(JSON.parse(output)).toEqual({
      mTLSIdentityPinned: true,
      wrongPeerIdentityRejected: true,
      unauthorizedPeerRejected: true,
      spoofedRaftServerIDRejected: true,
      heartbeatFastPath: true,
      appendEntriesRoundTrip: true,
      snapshotStreamRoundTrip: true,
      snapshotBytesPreserved: true,
    });
  }, 65_000);
});
