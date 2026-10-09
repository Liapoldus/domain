import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';



import { root } from '../../support/paths.js';

describe('Domain Raft peer cluster conformance', () => {
  it('runs a real three-node Raft cluster over the authenticated peer adapter', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/raft/cluster'], {
      cwd: root,
      env: { ...process.env, GOWORK: 'off' },
      encoding: 'utf8',
      timeout: 90_000,
    });

    expect(JSON.parse(output)).toEqual({
      threeNodeElection: true,
      majorityCommitApplied: true,
      minorityWriteRejected: true,
      newLeaderElectedAfterFailure: true,
      postFailoverMajorityCommitApplied: true,
      peerMTLSUsed: true,
      quorumLossRejected: true,
      noStaleWriteSuccessAfterQuorumLoss: true,
      transportRecoveredAfterPeerRestart: true,
      networkPartitionFenced: true,
      networkPartitionNoStaleApply: true,
      networkPartitionRecovered: true,
      freshReadBarrierFenced: true,
      freshReadBarrierReleased: true,
      freshReadBarrierUnavailableWithoutQuorum: true,
      freshReadBarrierAllowsEmptyState: true,
    });
  }, 95_000);
});
