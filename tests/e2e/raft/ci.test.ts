import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { rootURL as root } from '../../support/paths.js';

describe("required Linux Raft partition CI gate", () => {
  it("runs the real OS-level partition suite as root and fails instead of skipping", () => {
    const workflow = readFileSync(new URL(".github/workflows/verify.yml", root), "utf8");

    expect(workflow).toContain("runs-on: ubuntu-24.04");
    expect(workflow).toContain("DOMAIN_REQUIRE_OS_PARTITION: \"1\"");
    expect(workflow).toContain("sudo -E");
    expect(workflow).toContain("tests/e2e/raft/partition.test.ts");
  });
});
