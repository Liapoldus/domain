import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';


import { parseJson } from '../../support/json.js';

type QueryResponse = { columns?: string[]; rows: unknown[][]; error?: {code: string; message: string} };

import { root } from '../../support/paths.js';

let binaryDirectory: string;
let probeBinary: string;

beforeAll(() => {
  binaryDirectory = mkdtempSync(join(tmpdir(), 'domain-query-planner-'));
  probeBinary = join(binaryDirectory, 'query-planner-probe');
  execFileSync('go', ['build', '-o', probeBinary, './tests/fixtures/query/select'], {
    cwd: root,
    env: { ...process.env, GOWORK: 'off' },
    timeout: 120_000,
  });
}, 180_000);

afterAll(() => {
  if (binaryDirectory) rmSync(binaryDirectory, { recursive: true, force: true });
}, 180_000);

type QueryRequest = {
  model: unknown;
  rows: Record<string, unknown[]>;
  sql: string;
  params?: unknown[];
  maxRows?: number;
};

function runQuery(request: QueryRequest): QueryResponse {
  const output = execFileSync(probeBinary, {
    input: JSON.stringify(request),
    encoding: 'utf8',
    timeout: 60_000,
    maxBuffer: 32 * 1024 * 1024,
  });
  return parseJson(output) as QueryResponse;
}

function expectRejected(response: QueryResponse) {
  expect(response).toMatchObject({
    error: { code: 'query_rejected' },
  });
  if (!response.error) throw new Error('expected rejected query');
  expect(response.error.message.length).toBeGreaterThan(0);
  expect(response.columns).toBeUndefined();
  return response.error.message;
}

const model = {
  schemaVersion: '1',
  entities: [
    { name: 'accounts', ownerGroup: 'identity', fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'code', type: 'text', required: true, unique: true },
      { name: 'name', type: 'text', required: true },
    ] },
    { name: 'entries', ownerGroup: 'forms', fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'title', type: 'text', required: true },
      { name: 'score', type: 'int64' },
      { name: 'price', type: 'decimal' },
      { name: 'active', type: 'bool' },
      { name: 'accountCode', type: 'text', references: { entity: 'accounts', field: 'code' } },
    ] },
    { name: 'notes', ownerGroup: 'forms', fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'entryId', type: 'text', references: { entity: 'entries', field: 'id' } },
      { name: 'body', type: 'text' },
    ] },
    { name: 'big', ownerGroup: 'forms', fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'n', type: 'int64' },
    ] },
    { name: 'secret', ownerGroup: 'forms', fields: [
      { name: 'id', type: 'text', primaryKey: true, required: true },
      { name: 'title', type: 'text', required: true },
      { name: 'word', type: 'text' },
    ] },
  ],
};

// Input order is deliberately shuffled: without ORDER BY the planner must
// return rows lexicographically ordered by the full output row.
const entries = [
  { id: 'e5', title: 'alpha', score: 100, price: 0.5, active: false, accountCode: null },
  { id: 'e1', title: 'alpha', score: 80, price: 1.5, active: true, accountCode: 'ACME' },
  { id: 'e7', title: 'eta', score: 80, price: 2.5, active: false, accountCode: 'GLOBEX' },
  { id: 'e3', title: 'gamma', score: 70, price: 3.25, active: true, accountCode: 'GLOBEX' },
  { id: 'e8', title: 'theta', score: 90, price: 3.0, active: true, accountCode: 'GLOBEX' },
  { id: 'e2', title: 'beta', score: 90, price: 2.0, active: false, accountCode: 'ACME' },
  { id: 'e4', title: 'delta', score: 60, price: 4.0, active: true, accountCode: null },
  { id: 'e6', title: 'zeta', score: 100, price: 10.0, active: true, accountCode: 'ACME' },
];

const accounts = [
  { id: 'a2', code: 'GLOBEX', name: 'Globex' },
  { id: 'a1', code: 'ACME', name: 'Acme Corp' },
];

const notes = [
  { id: 'n1', entryId: 'e1', body: 'first note' },
  { id: 'n2', entryId: 'e1', body: 'second note' },
];

const secret = [
  { id: 's1', title: 'TOPSECRET_ROW_VALUE', word: 'TOPSECRET_ROW_VALUE' },
];

const rows = { entries, accounts, notes, secret };

function bigRows(count: number): { id: string; n: number }[] {
  return Array.from({ length: count }, (_, index) => ({ id: `x${String(index)}`, n: index }));
}

// 200001 rows is one over maxScanRows; built lazily and reused by both the
// scan-limit rejection and the error-message safety suite.
let oversizedRows: { id: string; n: number }[] | null = null;
function oversized(): { id: string; n: number }[] {
  if (!oversizedRows) oversizedRows = bigRows(200_001);
  return oversizedRows;
}

function sqlOfLength(length: number): string {
  const prefix = "SELECT id FROM entries WHERE title = '";
  const suffix = "'";
  return prefix + 'a'.repeat(length - prefix.length - suffix.length) + suffix;
}

describe('Domain SELECT planner — projection and WHERE', () => {
  it('projects columns and filters with >=, AND, bool and string literals', () => {
    expect(runQuery({ model, rows, sql: "SELECT id FROM entries WHERE active = true AND score >= 80" }))
      .toEqual({ columns: ['id'], rows: [['e1'], ['e6'], ['e8']] });
  });

  it('filters with < and returns the default lexicographic order', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id, score FROM entries WHERE score < 75' }))
      .toEqual({ columns: ['id', 'score'], rows: [['e3', 70], ['e4', 60]] });
  });

  it('filters with <=', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id, score FROM entries WHERE score <= 80' }))
      .toEqual({ columns: ['id', 'score'], rows: [['e1', 80], ['e3', 70], ['e4', 60], ['e7', 80]] });
  });

  it('filters with >', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score > 95' }))
      .toEqual({ columns: ['id'], rows: [['e5'], ['e6']] });
  });

  it('filters with both != and <>', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score != 80' }))
      .toEqual({ columns: ['id'], rows: [['e2'], ['e3'], ['e4'], ['e5'], ['e6'], ['e8']] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score <> 80' }))
      .toEqual({ columns: ['id'], rows: [['e2'], ['e3'], ['e4'], ['e5'], ['e6'], ['e8']] });
  });

  it('filters with = on a string column', () => {
    expect(runQuery({ model, rows, sql: "SELECT id FROM entries WHERE title = 'beta'" }))
      .toEqual({ columns: ['id'], rows: [['e2']] });
  });

  it('combines OR and NOT', () => {
    expect(runQuery({ model, rows, sql: "SELECT id FROM entries WHERE title = 'beta' OR score > 95" }))
      .toEqual({ columns: ['id'], rows: [['e2'], ['e5'], ['e6']] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE NOT (active = true)' }))
      .toEqual({ columns: ['id'], rows: [['e2'], ['e5'], ['e7']] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE NOT active = true' }))
      .toEqual({ columns: ['id'], rows: [['e2'], ['e5'], ['e7']] });
  });

  it('gives parentheses precedence over AND/OR', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE active = false AND (score > 95 OR score < 65)' }))
      .toEqual({ columns: ['id'], rows: [['e5']] });
  });

  it('compares numbers numerically, including decimals', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE price <= 3.25' }))
      .toEqual({ columns: ['id'], rows: [['e1'], ['e2'], ['e3'], ['e5'], ['e7'], ['e8']] });
  });

  it('treats comparisons against null as unknown so no row matches', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE accountCode = null' }))
      .toEqual({ columns: ['id'], rows: [] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE accountCode != null' }))
      .toEqual({ columns: ['id'], rows: [] });
  });

  it('binds ? parameters positionally and rejects non-matching rows for null params', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score > ? AND title = ?', params: [75, 'alpha'] }))
      .toEqual({ columns: ['id'], rows: [['e1'], ['e5']] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE accountCode = ?', params: [null] }))
      .toEqual({ columns: ['id'], rows: [] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score >= ?', params: [90] }))
      .toEqual({ columns: ['id'], rows: [['e2'], ['e5'], ['e6'], ['e8']] });
  });

  it('expands * into qualified columns of every source field', () => {
    expect(runQuery({ model, rows, sql: "SELECT * FROM entries WHERE id = 'e1'" }))
      .toEqual({
        columns: ['entries.id', 'entries.title', 'entries.score', 'entries.price', 'entries.active', 'entries.accountCode'],
        rows: [['e1', 'alpha', 80, 1.5, true, 'ACME']],
      });
  });
});

describe('Domain SELECT planner — joins', () => {
  it('joins on a declared reference field', () => {
    expect(runQuery({
      model, rows,
      sql: 'SELECT e.id, a.name FROM entries e INNER JOIN accounts a ON e.accountCode = a.code',
    })).toEqual({
      columns: ['e.id', 'a.name'],
      rows: [
        ['e1', 'Acme Corp'], ['e2', 'Acme Corp'], ['e3', 'Globex'],
        ['e6', 'Acme Corp'], ['e7', 'Globex'], ['e8', 'Globex'],
      ],
    });
  });

  it('accepts the ON condition in either column order', () => {
    expect(runQuery({
      model, rows,
      sql: 'SELECT e.id, a.name FROM entries e JOIN accounts a ON a.code = e.accountCode',
    })).toEqual({
      columns: ['e.id', 'a.name'],
      rows: [
        ['e1', 'Acme Corp'], ['e2', 'Acme Corp'], ['e3', 'Globex'],
        ['e6', 'Acme Corp'], ['e7', 'Globex'], ['e8', 'Globex'],
      ],
    });
  });

  it('keeps unmatched left rows with null right columns on LEFT JOIN', () => {
    expect(runQuery({
      model, rows,
      sql: 'SELECT e.id, a.name FROM entries e LEFT JOIN accounts a ON e.accountCode = a.code',
    })).toEqual({
      columns: ['e.id', 'a.name'],
      rows: [
        ['e1', 'Acme Corp'], ['e2', 'Acme Corp'], ['e3', 'Globex'],
        ['e4', null], ['e5', null], ['e6', 'Acme Corp'],
        ['e7', 'Globex'], ['e8', 'Globex'],
      ],
    });
  });

  it('applies WHERE to joined rows including LEFT JOIN nulls', () => {
    expect(runQuery({
      model, rows,
      sql: "SELECT e.id FROM entries e LEFT JOIN accounts a ON e.accountCode = a.code WHERE a.name = 'Acme Corp'",
    })).toEqual({ columns: ['e.id'], rows: [['e1'], ['e2'], ['e6']] });
  });

  it('rejects an ON condition that is not backed by a declared reference', () => {
    expectRejected(runQuery({
      model, rows,
      sql: 'SELECT e.id FROM entries e JOIN accounts a ON e.id = a.id',
    }));
    expectRejected(runQuery({
      model, rows,
      sql: 'SELECT e.id FROM entries e JOIN notes n ON e.title = n.body',
    }));
  });

  it('rejects unknown fields and entities used in a join', () => {
    expectRejected(runQuery({
      model, rows,
      sql: 'SELECT e.id FROM entries e JOIN accounts a ON e.accountCode = a.nope',
    }));
    expectRejected(runQuery({
      model, rows,
      sql: 'SELECT e.id FROM entries e JOIN ghosts g ON e.accountCode = g.code',
    }));
  });

  it('joins multiple sources and projects qualified columns', () => {
    expect(runQuery({
      model, rows,
      sql: "SELECT e.id, a.code, n.body FROM entries e JOIN accounts a ON e.accountCode = a.code JOIN notes n ON n.entryId = e.id WHERE e.id = 'e1'",
    })).toEqual({
      columns: ['e.id', 'a.code', 'n.body'],
      rows: [['e1', 'ACME', 'first note'], ['e1', 'ACME', 'second note']],
    });
  });
});

describe('Domain SELECT planner — aggregates, GROUP BY, HAVING', () => {
  it('groups with COUNT(*), including the null group', () => {
    expect(runQuery({ model, rows, sql: 'SELECT accountCode, COUNT(*) FROM entries GROUP BY accountCode' }))
      .toEqual({
        columns: ['accountCode', 'COUNT(*)'],
        rows: [[null, 2], ['ACME', 3], ['GLOBEX', 3]],
      });
  });

  it('computes COUNT, SUM, MIN, MAX and AVG per group', () => {
    expect(runQuery({
      model, rows,
      sql: 'SELECT accountCode, COUNT(*), SUM(score), MIN(score), MAX(score), AVG(score) FROM entries GROUP BY accountCode',
    })).toEqual({
      columns: ['accountCode', 'COUNT(*)', 'SUM(score)', 'MIN(score)', 'MAX(score)', 'AVG(score)'],
      rows: [
        [null, 2, 160, 60, 100, 80],
        ['ACME', 3, 270, 80, 100, 90],
        ['GLOBEX', 3, 240, 70, 90, 80],
      ],
    });
  });

  it('renders AVG as an exact decimal, rounding non-terminating results deterministically', () => {
    expect(runQuery({ model, rows, sql: 'SELECT accountCode, AVG(price) FROM entries GROUP BY accountCode' }))
      .toEqual({
        columns: ['accountCode', 'AVG(price)'],
        rows: [[null, 2.25], ['ACME', 4.5], ['GLOBEX', 2.916666666667]],
      });
  });

  it('counts non-null values with COUNT(column)', () => {
    expect(runQuery({ model, rows, sql: 'SELECT COUNT(accountCode) FROM entries' }))
      .toEqual({ columns: ['COUNT(accountCode)'], rows: [[6]] });
  });

  it('aggregates the whole table without GROUP BY', () => {
    expect(runQuery({ model, rows, sql: 'SELECT COUNT(*), SUM(score), MIN(score), MAX(score), AVG(score) FROM entries' }))
      .toEqual({ columns: ['COUNT(*)', 'SUM(score)', 'MIN(score)', 'MAX(score)', 'AVG(score)'], rows: [[8, 670, 60, 100, 83.75]] });
  });

  it('returns a zero-count single row for aggregates over no rows', () => {
    expect(runQuery({ model, rows, sql: 'SELECT COUNT(*), SUM(score), AVG(score) FROM entries WHERE score > 1000' }))
      .toEqual({ columns: ['COUNT(*)', 'SUM(score)', 'AVG(score)'], rows: [[0, null, null]] });
  });

  it('filters groups with HAVING, including a bound parameter', () => {
    expect(runQuery({ model, rows, sql: 'SELECT accountCode, COUNT(*) FROM entries GROUP BY accountCode HAVING COUNT(*) > 2' }))
      .toEqual({ columns: ['accountCode', 'COUNT(*)'], rows: [['ACME', 3], ['GLOBEX', 3]] });
    // >= 3 excludes the null group (count 2) while keeping ACME and GLOBEX.
    expect(runQuery({ model, rows, sql: 'SELECT accountCode, COUNT(*) FROM entries GROUP BY accountCode HAVING COUNT(*) >= ?', params: [3] }))
      .toEqual({ columns: ['accountCode', 'COUNT(*)'], rows: [['ACME', 3], ['GLOBEX', 3]] });
  });

  it('groups without aggregates', () => {
    expect(runQuery({ model, rows, sql: 'SELECT accountCode FROM entries GROUP BY accountCode' }))
      .toEqual({ columns: ['accountCode'], rows: [[null], ['ACME'], ['GLOBEX']] });
  });

  it('rejects a star projection combined with aggregates', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT *, COUNT(*) FROM entries' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT COUNT(*), * FROM entries' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT * FROM entries GROUP BY accountCode' }));
  });

  it('rejects an ungrouped projection column and aggregates in WHERE', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT title, COUNT(*) FROM entries GROUP BY accountCode' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE COUNT(*) > 1' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT accountCode FROM entries HAVING COUNT(*) > 1' }));
  });
});

describe('Domain SELECT planner — DISTINCT, ordering, LIMIT and OFFSET', () => {
  it('projects distinct values in lexicographic order', () => {
    expect(runQuery({ model, rows, sql: 'SELECT DISTINCT title FROM entries' }))
      .toEqual({
        columns: ['title'],
        rows: [['alpha'], ['beta'], ['delta'], ['eta'], ['gamma'], ['theta'], ['zeta']],
      });
  });

  it('returns rows lexicographically ordered by the full output row without ORDER BY', () => {
    expect(runQuery({ model, rows, sql: 'SELECT title, score FROM entries' }))
      .toEqual({
        columns: ['title', 'score'],
        rows: [
          ['alpha', 80], ['alpha', 100], ['beta', 90], ['delta', 60],
          ['eta', 80], ['gamma', 70], ['theta', 90], ['zeta', 100],
        ],
      });
  });

  it('orders by multiple columns with mixed ASC and DESC', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id, score FROM entries ORDER BY score DESC, id ASC' }))
      .toEqual({
        columns: ['id', 'score'],
        rows: [
          ['e5', 100], ['e6', 100], ['e2', 90], ['e8', 90],
          ['e1', 80], ['e7', 80], ['e3', 70], ['e4', 60],
        ],
      });
  });

  it('orders joined output by a right-side column', () => {
    expect(runQuery({
      model, rows,
      sql: 'SELECT e.id, a.name FROM entries e JOIN accounts a ON e.accountCode = a.code ORDER BY a.name DESC, e.id ASC',
    })).toEqual({
      columns: ['e.id', 'a.name'],
      rows: [
        ['e3', 'Globex'], ['e7', 'Globex'], ['e8', 'Globex'],
        ['e1', 'Acme Corp'], ['e2', 'Acme Corp'], ['e6', 'Acme Corp'],
      ],
    });
  });

  it('orders grouped output by an aggregate', () => {
    expect(runQuery({
      model, rows,
      sql: 'SELECT accountCode, COUNT(*) FROM entries GROUP BY accountCode ORDER BY COUNT(*) DESC, accountCode ASC',
    })).toEqual({
      columns: ['accountCode', 'COUNT(*)'],
      rows: [['ACME', 3], ['GLOBEX', 3], [null, 2]],
    });
  });

  it('applies LIMIT and OFFSET', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries ORDER BY id LIMIT 3' }))
      .toEqual({ columns: ['id'], rows: [['e1'], ['e2'], ['e3']] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries ORDER BY id LIMIT 3 OFFSET 2' }))
      .toEqual({ columns: ['id'], rows: [['e3'], ['e4'], ['e5']] });
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries ORDER BY id LIMIT 0' }))
      .toEqual({ columns: ['id'], rows: [] });
  });

  it('defaults to LIMIT 100 when the query omits LIMIT', () => {
    const response = runQuery({ model, rows: { big: bigRows(150) }, sql: 'SELECT id FROM big' });
    expect(response.columns).toEqual(['id']);
    expect(response.rows).toHaveLength(100);
    expect(response.rows[0]).toEqual(['x0']);
    // Ordering is lexicographic by the full output row, not natural: after
    // x0, x1 come x10, x100..x109, so the 100th row is x53.
    expect(response.rows[99]).toEqual(['x53']);
  });

  it('lowers the limit via maxRows but never raises it', () => {
    expect(runQuery({ model, rows, sql: 'SELECT id FROM entries ORDER BY id LIMIT 5', maxRows: 2 }))
      .toEqual({ columns: ['id'], rows: [['e1'], ['e2']] });
    const response = runQuery({ model, rows: { big: bigRows(150) }, sql: 'SELECT id FROM big', maxRows: 5000 });
    expect(response.rows).toHaveLength(100);
    expect(response.rows[0]).toEqual(['x0']);
  });

  it('accepts a LIMIT equal to the maximum', () => {
    const response = runQuery({ model, rows, sql: 'SELECT id FROM entries ORDER BY id LIMIT 10000' });
    expect(response.rows).toHaveLength(8);
  });

  it('accepts positional row arrays from the caller', () => {
    const positional = {
      entries: [
        ['e1', 'alpha', 80, 1.5, true, 'ACME'],
        ['e2', 'beta', 90, 2.0, false, 'ACME'],
      ],
    };
    expect(runQuery({ model, rows: positional, sql: 'SELECT id, score FROM entries' }))
      .toEqual({ columns: ['id', 'score'], rows: [['e1', 80], ['e2', 90]] });
  });
});

describe('Domain SELECT planner — rejections', () => {
  it('rejects a second statement or trailing semicolon', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries;' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries; DELETE FROM entries' }));
    expectRejected(runQuery({ model, rows, sql: "SELECT id FROM entries WHERE id = 'e1'; UPDATE entries SET title = 'x'" }));
  });

  it('rejects subqueries and UNION', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE id IN (SELECT id FROM accounts)' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries UNION SELECT id FROM accounts' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE EXISTS (SELECT 1 FROM accounts)' }));
  });

  it('rejects write and administrative verbs', () => {
    for (const sql of [
      "INSERT INTO entries (id) VALUES ('x')",
      "UPDATE entries SET title = 'x'",
      'DELETE FROM entries',
      'DROP TABLE entries',
      'ALTER TABLE entries ADD COLUMN x text',
      'PRAGMA table_info',
      "ATTACH DATABASE 'other.sqlite' AS other",
    ]) {
      expectRejected(runQuery({ model, rows, sql }));
    }
  });

  it('rejects unknown entities and fields', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM ghosts' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT nope FROM entries' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE nope = 1' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT g.nope FROM entries g' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE id = ? ', params: [] }));
  });

  it('rejects LIMIT above the maximum', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries LIMIT 10001' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries LIMIT 999999999999' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries LIMIT -1' }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries LIMIT 1.5' }));
  });

  it('rejects parameter count mismatches', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score > ?', params: [] }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE score > ?', params: [1, 2] }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries', params: [1] }));
  });

  it('rejects SQL above maxSQLBytes and accepts exactly maxSQLBytes', () => {
    expectRejected(runQuery({ model, rows, sql: sqlOfLength(8193) }));
    expect(runQuery({ model, rows, sql: sqlOfLength(8192) })).toEqual({ columns: ['id'], rows: [] });
  });

  it('rejects more than maxParams parameters', () => {
    const sql = 'SELECT id FROM entries WHERE ' + Array.from({ length: 65 }, () => 'score >= ?').join(' AND ');
    expectRejected(runQuery({ model, rows, sql, params: Array.from({ length: 65 }, () => 0) }));
  });

  it('rejects non-scalar parameters', () => {
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE title = ?', params: [['a', 'b']] }));
    expectRejected(runQuery({ model, rows, sql: 'SELECT id FROM entries WHERE title = ?', params: [{ a: 1 }] }));
  });

  it('rejects scanning more than maxScanRows rows', () => {
    expectRejected(runQuery({ model, rows: { big: oversized() }, sql: 'SELECT id FROM big' }));
  });
});

describe('Domain SELECT planner — error messages never leak row or parameter values', () => {
  const leaked = ['TOPSECRET_ROW_VALUE', 'TOPSECRET_PARAM_VALUE'];

  function expectSafe(response: QueryResponse) {
    const message = expectRejected(response);
    for (const sentinel of leaked) {
      expect(message).not.toContain(sentinel);
    }
    return message;
  }

  it('keeps parameter values out of validation errors', () => {
    expectSafe(runQuery({
      model, rows,
      sql: 'SELECT nope FROM secret WHERE title = ?',
      params: ['TOPSECRET_PARAM_VALUE'],
    }));
    expectSafe(runQuery({
      model, rows,
      sql: 'SELECT id FROM secret WHERE title = ? LIMIT 10001',
      params: ['TOPSECRET_PARAM_VALUE'],
    }));
    expectSafe(runQuery({
      model, rows,
      sql: 'SELECT id FROM secret WHERE title = ?',
      params: ['TOPSECRET_PARAM_VALUE', 7],
    }));
  });

  it('keeps row values out of execution errors', () => {
    expectSafe(runQuery({ model, rows, sql: 'SELECT SUM(word) FROM secret' }));
    expectSafe(runQuery({ model, rows, sql: 'SELECT MIN(word), AVG(word) FROM secret' }));
  });

  it('keeps parameter values out of limit violations on large scans', () => {
    expectSafe(runQuery({
      model, rows: { big: oversized() },
      sql: 'SELECT id FROM big WHERE id = ?',
      params: ['TOPSECRET_PARAM_VALUE'],
    }));
  });
});
