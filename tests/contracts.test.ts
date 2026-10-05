import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifest = JSON.parse(readFileSync(resolve(root, 'contracts/v1/plugin.json'), 'utf8'));

describe('product contract ownership', () => {
  it('declares CRUD, batch, analytics and cluster status as product call methods', () => {
    expect(manifest.capabilities.map((entry: { name: string }) => entry.name)).toEqual([
      'domain.create', 'domain.get', 'domain.update', 'domain.delete',
      'domain.batch', 'domain.query', 'domain.cluster.status',
    ]);
    expect(manifest.capabilities.every((entry: { mode: string }) => entry.mode === 'call')).toBe(true);
  });
});
