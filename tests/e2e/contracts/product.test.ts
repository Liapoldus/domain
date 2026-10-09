import { describe, expect, it } from 'vitest';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';


import { parseJson, propertiesOf, required, type Schema } from '../../support/json.js';

import { root } from '../../support/paths.js';
type Manifest = { name: string; configuration: {schemaVersion: string; schema: string}; capabilities: Capability[]; compatibility: {raftLogCommandKinds: string[]; fsmSnapshotFormatVersion: number; erSchemaVersion: string; peerContracts: string[]} };
const manifest = parseJson(readFileSync(resolve(root, 'contracts/v1/plugin.json'), 'utf8')) as Manifest;

type Capability = {
  name: string;
  mode: string;
  payloadSchema?: string;
  responseSchema?: string;
  errorSchema?: string;
  maxPayloadBytes?: number;
  timeoutMs?: number;
};

const capabilities: Capability[] = manifest.capabilities;

function loadJson(relativePath: string): unknown {
  return parseJson(readFileSync(resolve(root, relativePath), 'utf8'));
}

function schemaFiles(): string[] {
  const dir = resolve(root, 'contracts/v1/schemas');
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((entry: string) => entry.endsWith('.json'))
    .map((entry: string) => `contracts/v1/schemas/${entry}`)
    .sort();
}

function collectForbiddenScopeProperties(node: unknown, found: string[]): void {
  if (Array.isArray(node)) {
    for (const item of node) collectForbiddenScopeProperties(item, found);
    return;
  }
  if (node === null || typeof node !== 'object') return;
  const record = node as Record<string, unknown>;
  const properties = record.properties;
  const hasProperties = properties !== null && typeof properties === 'object' && !Array.isArray(properties);
  if (hasProperties) {
    for (const key of Object.keys(properties as Record<string, unknown>)) {
      if (key === 'tenant' || key === 'site' || key === 'ownerGroup') found.push(key);
    }
  }
  for (const [key, value] of Object.entries(record)) {
    // forwardedScope is the single reserved cluster-proxy claim: it carries the
    // original caller's scope for peer-forwarded reads, so its tenant/site/group
    // property names are exempt. No other schema may declare them.
    if (key === 'properties' && hasProperties) {
      for (const [propertyKey, propertyValue] of Object.entries(properties as Record<string, unknown>)) {
        if (propertyKey === 'forwardedScope') continue;
        collectForbiddenScopeProperties(propertyValue, found);
      }
      continue;
    }
    collectForbiddenScopeProperties(value, found);
  }
}

function errorObjectOf(schema: unknown): Schema | undefined {
 const record = schema as Schema;
 return record.$defs?.error ?? record.properties?.error;
}

const ERROR_CODES = [
  'invalid_request', 'forbidden', 'not_found', 'conflict', 'not_leader',
  'unavailable', 'unknown_outcome', 'query_rejected', 'internal',
  'epoch_mismatch',
];

// Spec §7 compatibility block. Spec §7 writes "...7 kinds..." while §4 enumerates
// init|put|migrate|rollback + create|update|delete|batch; the explicit §4/task
// enumeration (8 kinds, order preserved) is treated as authoritative.
const RAFT_LOG_COMMAND_KINDS = [
  'init', 'put', 'migrate', 'rollback', 'create', 'update', 'delete', 'batch',
];

const PEER_CONTRACTS = [
  'liapoldus.domain.raft-peer.v1',
  'liapoldus.domain.product-peer.v1',
];

const EXPECTED_LIMITS: Record<string, { maxPayloadBytes: number; timeoutMs: number }> = {
  'domain.create': { maxPayloadBytes: 1048576, timeoutMs: 15000 },
  'domain.get': { maxPayloadBytes: 1048576, timeoutMs: 10000 },
  'domain.update': { maxPayloadBytes: 1048576, timeoutMs: 15000 },
  'domain.delete': { maxPayloadBytes: 1048576, timeoutMs: 15000 },
  'domain.batch': { maxPayloadBytes: 2097152, timeoutMs: 30000 },
  'domain.query': { maxPayloadBytes: 1048576, timeoutMs: 10000 },
  'domain.cluster.status': { maxPayloadBytes: 1048576, timeoutMs: 3000 },
  'domain.migration.plan': { maxPayloadBytes: 1048576, timeoutMs: 15000 },
  'domain.migration.status': { maxPayloadBytes: 1048576, timeoutMs: 10000 },
};

const MANIFEST_SNAPSHOT = {
  name: 'domain',
  configuration: {
    schemaVersion: '1',
    schema: 'contracts/v1/model.schema.json',
  },
  capabilities: [
    { name: 'domain.create', mode: 'call' },
    { name: 'domain.get', mode: 'call' },
    { name: 'domain.update', mode: 'call' },
    { name: 'domain.delete', mode: 'call' },
    { name: 'domain.batch', mode: 'call' },
    { name: 'domain.query', mode: 'call' },
    { name: 'domain.cluster.status', mode: 'call' },
    { name: 'domain.migration.plan', mode: 'call' },
    { name: 'domain.migration.status', mode: 'call' },
  ],
};

describe('product contract ownership', () => {
  it('declares CRUD, batch, analytics, cluster status and read-only migration methods as product calls', () => {
    expect(manifest.capabilities.map((entry: { name: string }) => entry.name)).toEqual([
      'domain.create', 'domain.get', 'domain.update', 'domain.delete',
      'domain.batch', 'domain.query', 'domain.cluster.status',
      'domain.migration.plan', 'domain.migration.status',
    ]);
    expect(manifest.capabilities.every((entry: { mode: string }) => entry.mode === 'call')).toBe(true);
  });

  it('does not expose model mutation outside Core-owned configuration generations', () => {
    const names = manifest.capabilities.map((entry: { name: string }) => entry.name);
    expect(names).not.toContain('domain.migration.apply');
    expect(names).not.toContain('domain.migration.rollback');
  });
});

describe('manifest regression (pre-v2 fields)', () => {
  it('keeps name, configuration and capability name/mode pairs byte-identical to the pre-v2 manifest', () => {
    expect({
      name: manifest.name,
      configuration: manifest.configuration,
      capabilities: capabilities.map((entry) => ({ name: entry.name, mode: entry.mode })),
    }).toEqual(MANIFEST_SNAPSHOT);
    expect(Object.keys(manifest)).toEqual(expect.arrayContaining(['name', 'configuration', 'capabilities']));
    for (const entry of capabilities) {
      expect(Object.keys(entry)).toEqual(expect.arrayContaining(['name', 'mode']));
      expect(entry.mode).toBe('call');
    }
  });
});

describe('capability schema references (spec §7)', () => {
  it('gives every capability payload, response and error schemas that resolve to JSON Schema 2020-12 documents', () => {
    expect(capabilities.length).toBe(9);
    const seen = new Set<string>();
    for (const entry of capabilities) {
      for (const key of ['payloadSchema', 'responseSchema', 'errorSchema'] as const) {
        const reference = entry[key];
        expect(typeof reference, `${entry.name}.${key} must be a string`).toBe('string');
        expect(required(reference).startsWith('contracts/v1/schemas/'), `${entry.name}.${key}=${String(reference)}`).toBe(true);
        expect(seen.has(`${entry.name}.${key}`)).toBe(false);
        seen.add(`${entry.name}.${key}`);
        const absolute = resolve(root, required(reference));
        expect(existsSync(absolute), `missing schema file ${reference}`).toBe(true);
        const schema = loadJson(required(reference)) as Schema;
        expect(String(schema.$schema)).toContain('2020-12');
        expect(schema.type).toBe('object');
        expect(typeof schema.properties === 'object').toBe(true);
        expect(Array.isArray(schema.required)).toBe(true);
        expect(schema.additionalProperties).toBe(false);
      }
    }
  });

  it('owns runtime schemas in Go definitions', () => {
    const source = readFileSync(resolve(root, 'contracts/export.go'), 'utf8');
    expect(source).toContain('func Documents()');
    expect(source).not.toContain('go:embed');
  });
});

describe('request scope isolation (spec §2.2)', () => {
  it('keeps tenant, site and ownerGroup out of every request schema', () => {
    const found: string[] = [];
    for (const entry of capabilities) {
      collectForbiddenScopeProperties(loadJson(required(entry.payloadSchema)), found);
    }
    expect(found).toEqual([]);
  });

  it('keeps tenant, site and ownerGroup out of every schema file under contracts/v1/schemas', () => {
    const files = schemaFiles();
    expect(files.length).toBeGreaterThan(0);
    const found: string[] = [];
    for (const file of files) collectForbiddenScopeProperties(loadJson(file), found);
    expect(found).toEqual([]);
  });
});

describe('reserved forwardedScope claim (peer read proxying)', () => {
  const CLAIM_SHAPE = {
    type: 'object',
    additionalProperties: false,
    required: ['tenant', 'site', 'group'],
    properties: {
      tenant: { type: 'string', minLength: 1, maxLength: 128 },
      site: { type: 'string', minLength: 1, maxLength: 128 },
      group: { type: 'string', minLength: 1, maxLength: 128 },
    },
  };

  it('declares the forwardedScope claim only on the proxied read request schemas', () => {
    const readRequests = new Set([
      'contracts/v1/schemas/get.request.json',
      'contracts/v1/schemas/query.request.json',
    ]);
    for (const file of schemaFiles()) {
      const schema = loadJson(file) as Schema;
      const claim = schema.properties?.forwardedScope;
      if (readRequests.has(file)) {
        expect(claim, file).toEqual(CLAIM_SHAPE);
      } else {
        expect(claim, file).toBeUndefined();
      }
    }
    expect(readRequests.size).toBe(2);
  });
});

describe('error envelope (spec §2.1)', () => {
  it('defines the exact error-code enum with required retryable and unknownOutcome booleans', () => {
    const references = [...new Set(capabilities.map((entry) => required(entry.errorSchema)))];
    expect(references.length).toBeGreaterThan(0);
    for (const reference of references) {
      const error = errorObjectOf(loadJson(reference));
      expect(error, `${reference} must define the error object`).toBeTruthy();
      expect(propertiesOf(required(error)).code.enum).toEqual(ERROR_CODES);
      expect(Array.isArray(required(error).required)).toBe(true);
      expect(required(error).required).toEqual(expect.arrayContaining(
        ['code', 'retryable', 'unknownOutcome', 'message'],
      ));
      expect(propertiesOf(required(error)).retryable.type).toBe('boolean');
      expect(propertiesOf(required(error)).unknownOutcome.type).toBe('boolean');
      expect(required(error).additionalProperties).toBe(false);
    }
  });

  it('exposes success and failure variants through the envelope schema', () => {
    const envelope = loadJson('contracts/v1/schemas/envelope.json') as Schema;
    expect(Object.keys(propertiesOf(envelope))).toEqual(expect.arrayContaining(['ok', 'data', 'error']));
    expect(envelope.required).toContain('ok');
    expect(JSON.stringify(envelope)).toContain('unknown_outcome');
    expect(JSON.stringify(envelope)).toContain('query_rejected');
  });
});

describe('compatibility block (spec §7)', () => {
  it('declares raft log command kinds, snapshot format version, ER schema version and peer contracts', () => {
    expect(typeof manifest.compatibility === 'object').toBe(true);
    const compatibility = manifest.compatibility;
    expect(compatibility.raftLogCommandKinds).toEqual(RAFT_LOG_COMMAND_KINDS);
    expect(compatibility.fsmSnapshotFormatVersion).toBe(2);
    expect(compatibility.erSchemaVersion).toBe('1');
    expect(compatibility.peerContracts).toEqual(PEER_CONTRACTS);
  });
});

describe('capability limits (spec §2.2)', () => {
  it('declares maxPayloadBytes and timeoutMs per capability', () => {
    for (const entry of capabilities) {
      const expected = EXPECTED_LIMITS[entry.name];
      expect(expected, `no expected limits for ${entry.name}`).toBeTruthy();
      expect(entry.maxPayloadBytes, `${entry.name}.maxPayloadBytes`).toBe(expected.maxPayloadBytes);
      expect(entry.timeoutMs, `${entry.name}.timeoutMs`).toBe(expected.timeoutMs);
    }
  });
});
