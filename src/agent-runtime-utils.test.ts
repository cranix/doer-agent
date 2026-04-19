import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { filterValidRunImagePaths } from "./agent-runtime-utils.js";

const invalidTinyPngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aK1cAAAAASUVORK5CYII=";

function buildValidTinyPng(): Buffer {
  const bytes = Buffer.from(invalidTinyPngBase64, "base64");
  bytes.writeUInt32BE(0xefa2a75b, 0x34);
  return bytes;
}

test("filterValidRunImagePaths drops PNGs with CRC mismatches", async () => {
  const workspaceRoot = await mkdtemp(path.join(os.tmpdir(), "doer-agent-image-filter-"));
  try {
    const invalidPngPath = path.join(workspaceRoot, "bad.png");
    const validPngPath = path.join(workspaceRoot, "good.png");
    const validJpgPath = path.join(workspaceRoot, "photo.jpg");
    await writeFile(invalidPngPath, Buffer.from(invalidTinyPngBase64, "base64"));
    await writeFile(validPngPath, buildValidTinyPng());
    await writeFile(validJpgPath, Buffer.from([0xff, 0xd8, 0xff, 0xd9]));

    const invalid: Array<{ imagePath: string; reason: string }> = [];
    const result = await filterValidRunImagePaths({
      workspaceRoot,
      imagePaths: ["bad.png", "good.png", "photo.jpg"],
      onInvalidImage: (imagePath, reason) => {
        invalid.push({ imagePath, reason });
      },
    });

    assert.deepEqual(result, ["good.png", "photo.jpg"]);
    assert.equal(invalid.length, 1);
    assert.equal(invalid[0]?.imagePath, "bad.png");
    assert.match(invalid[0]?.reason ?? "", /CRC mismatch/i);
  } finally {
    await rm(workspaceRoot, { recursive: true, force: true });
  }
});
