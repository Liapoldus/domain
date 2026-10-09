import { afterEach, describe, expect, it } from 'vitest';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';


import { root } from '../../support/paths.js';
const buildDirs: string[] = [];
const runnerAvailable = hasLinuxRunner(process.platform);
const networkPartitionTest = runnerAvailable || process.env.DOMAIN_REQUIRE_OS_PARTITION === '1' ? it : it.skip;

afterEach(() => {
  for (const directory of buildDirs.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe('Domain Raft TCP/OS partition recovery', () => {
  networkPartitionTest('recovers a live mTLS peer process after Linux drops and restores its TCP packets', () => {
    const binaryDirectory = mkdtempSync(join(root, 'tests', '.tmp-raft-os-partition-'));
    buildDirs.push(binaryDirectory);
    const binary = join(binaryDirectory, 'raft-peer-transport-linux');
    const arch = process.arch === 'arm64' ? 'arm64' : 'amd64';

    execFileSync('go', ['build', '-o', binary, './tests/fixtures/raft/transport'], {
      cwd: root,
      env: { ...process.env, CGO_ENABLED: '0', GOARCH: arch, GOOS: 'linux', GOWORK: 'off' },
      stdio: 'pipe',
      timeout: 120_000,
    });

    const result = runLinuxNetworkNamespace(binary);
    expect(JSON.parse(result)).toEqual({
      clientProcessSurvivedPartition: true,
      serverProcessSurvivedPartition: true,
      initialTcpCallSucceeded: true,
      kernelDroppedPeerPackets: true,
      partitionedTcpCallFailed: true,
      tcpCallSucceededAfterHeal: true,
      sameClientProcessReconnected: true,
    });
  }, 45_000);

  networkPartitionTest('keeps quorum and fences one live Raft process while the kernel partitions its TCP endpoint', () => {
    const binaryDirectory = mkdtempSync(join(root, 'tests', '.tmp-raft-cluster-os-partition-'));
    buildDirs.push(binaryDirectory);
    const binary = join(binaryDirectory, 'raft-peer-cluster-linux');
    const arch = process.arch === 'arm64' ? 'arm64' : 'amd64';

    execFileSync('go', ['build', '-o', binary, './tests/fixtures/raft/cluster'], {
      cwd: root,
      env: { ...process.env, CGO_ENABLED: '0', GOARCH: arch, GOOS: 'linux', GOWORK: 'off' },
      stdio: 'pipe',
      timeout: 120_000,
    });

    expect(JSON.parse(runLinuxNetworkNamespace(binary))).toEqual({
      threeNodeElection: true,
      majorityCommitDuringKernelPartition: true,
      isolatedProcessStayedAlive: true,
      isolatedWriteRejected: true,
      isolatedReadFenceRejected: true,
      isolatedRowStayedStale: true,
      kernelDroppedPeerPackets: true,
      sameIncarnationCaughtUpAfterHeal: true,
      postHealMajorityCommit: true,
    });
  }, 90_000);
});

function runLinuxNetworkNamespace(binary: string): string {
  if (process.platform === 'linux') {
    return execFileSync('unshare', ['--net', '--fork', '--', binary, 'network-partition'], {
      cwd: root,
      encoding: 'utf8',
      timeout: 80_000,
    });
  }

  if (process.platform === 'darwin') {
    return execFileSync('orb', ['-u', 'root', 'unshare', '--net', '--fork', 'sh', '-lc', `ip link set lo up && exec "$1" network-partition`, 'domain-network-partition', binary], {
      cwd: root,
      encoding: 'utf8',
      timeout: 80_000,
    });
  }

  throw new Error(`Linux network namespace runner is unavailable on ${process.platform}`);
}

describe('Domain isolated Linux runner detection', () => {
  it('requires successful network-namespace creation on Linux', () => {
    expect(hasLinuxRunner('linux', () => ({ status: 0 }), 0)).toBe(true);
    expect(hasLinuxRunner('linux', () => ({ status: 0 }), 1000)).toBe(false);
    expect(hasLinuxRunner('linux', () => ({ status: 1 }), 0)).toBe(false);
    expect(hasLinuxRunner('linux', () => ({ status: null, error: new Error('permission denied') }), 0)).toBe(false);
  });
});

function hasLinuxRunner(platform: string, linuxProbe = probeLinuxNetworkNamespace, effectiveUID = process.geteuid?.()): boolean {
  if (platform === 'linux') {
    if (effectiveUID !== 0) return false;
    const result = linuxProbe();
    return result.status === 0 && !result.error;
  }
  if (platform !== 'darwin') return false;
  const result = spawnSync('orb', ['list', '--running', '--quiet'], { encoding: 'utf8' });
  return result.status === 0 && result.stdout.trim().length > 0;
}

function probeLinuxNetworkNamespace(): { status: number | null; error?: Error | null } {
  const result = spawnSync('unshare', ['--net', '--fork', 'true'], {
    encoding: 'utf8',
    stdio: 'ignore',
    timeout: 3_000,
  });
  return { status: result.status, error: result.error };
}
