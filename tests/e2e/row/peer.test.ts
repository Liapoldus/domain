import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';



import { root } from '../../support/paths.js';

describe('Domain product peer cluster conformance', () => {
  it('drives a three-node product cluster over authenticated peer transport', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/row/peer'], {
      cwd: root,
      env: { ...process.env, GOWORK: 'off' },
      encoding: 'utf8',
      timeout: 180_000,
    });

    expect(JSON.parse(output)).toEqual({
      leaderCreateApplied: true,
      leaderReadFound: true,

      followerWriteNotLeader: true,
      followerWriteLeftStateUnchanged: true,
      followerReadProxiedFound: true,
      leaderReadFreshAfterProxiedFollowerRead: true,
      followerQueryProxiedRows: true,
      followerReadForeignTenantHidden: true,
      spoofedForwardedScopeRejected: true,
      leaderDownReadCode: 'unavailable',
      leaderDownReadBounded: true,

      aclOwnerGroupForbiddenWrite: true,
      aclOwnerGroupRestrictsReads: false,
      aclOwnEntityWriteAllowed: true,

      tenantScopedDeleteReportsNotFound: true,
      tenantScopedReadHidesForeignRow: true,
      tenantScopedDeleteLeftOwnerRow: true,
      rowRejectsScopeFields: true,

      writeIdReplayReportedDuplicate: true,
      writeIdReplayKeptSingleRow: true,

      batchApplied: true,
      batchReplayDeduplicated: true,
      batchUnknownEntityRejected: true,
      batchOwnerGroupForbidden: true,
      batchRejectsInvalidBeforeApply: true,

      unauthorizedCallerRejectedAtTransport: true,

      statusReadyWithQuorum: true,
      statusVotersCounted: true,
      statusQuorumCounted: true,

      quorumLossFreshReadUnavailable: true,
      quorumLossWriteCode: 'unavailable',
      quorumLossStatusUnready: true,
      quorumLossStatusVotersCounted: true,
      quorumLossStatusQuorumCounted: true,
      statusReadyAfterQuorumRestored: true,
      quorumLossRetryAccepted: true,
      quorumLossRetryDeduplicated: true,
      quorumLossRetryKeptSingleRow: true,
      quorumLossRetryConflictsOnNewWriteId: true,
    });
  }, 190_000);
});
