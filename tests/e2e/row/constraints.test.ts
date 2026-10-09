import { execFileSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';



import { root } from '../../support/paths.js';

describe('Domain committed row constraints', () => {
  it('validates field types, required fields, unique values, references and primary key before advancing the FSM', () => {
    const output = execFileSync('go', ['run', './tests/fixtures/row/constraints'], {
      cwd: root,
      env: { ...process.env, GOWORK: 'off' },
      encoding: 'utf8',
      timeout: 60_000,
    });

    expect(JSON.parse(output)).toEqual({
      validRowsAccepted: true,
      typedPrimaryKeyAccepted: true,
      missingRequiredRejected: true,
      wrongTypeRejected: true,
      uniqueConflictRejected: true,
      missingReferenceRejected: true,
      unknownFieldRejected: true,
      primaryKeyMismatchRejected: true,
      typedPrimaryKeyMismatchRejected: true,
      rejectedEntriesDidNotAdvanceAppliedIndex: true,
    });
  }, 65_000);
});
