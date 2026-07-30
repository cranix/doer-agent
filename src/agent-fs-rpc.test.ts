import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { StringCodec, type Msg } from "nats";
import { handleFsRpcMessage, normalizeFsRpcPath } from "./agent-fs-rpc.js";

const workspaceRoot = path.resolve("/tmp/doer-workspace");
const outsidePath = path.resolve("/tmp/outside/file.csv");

test("normalizes workspace paths as workspace-relative paths", () => {
  const target = normalizeFsRpcPath(workspaceRoot, "folder/file.txt");

  assert.equal(target.abs, path.join(workspaceRoot, "folder/file.txt"));
  assert.equal(target.formatPath(target.abs), "folder/file.txt");
});

test("allows and preserves absolute paths outside the workspace for read operations", () => {
  const target = normalizeFsRpcPath(workspaceRoot, outsidePath, {
    allowOutsideWorkspace: true,
  });

  assert.equal(target.abs, outsidePath);
  assert.equal(target.formatPath(target.abs), outsidePath.split(path.sep).join("/"));
});

test("allows parent traversal outside the workspace for read operations", () => {
  const target = normalizeFsRpcPath(workspaceRoot, "../outside/file.csv", {
    allowOutsideWorkspace: true,
  });

  assert.equal(target.abs, outsidePath);
  assert.equal(target.formatPath(target.abs), outsidePath.split(path.sep).join("/"));
});

test("keeps paths outside the workspace blocked by default", () => {
  assert.throws(
    () => normalizeFsRpcPath(workspaceRoot, outsidePath),
    /path escapes workspace root/,
  );
  assert.throws(
    () => normalizeFsRpcPath(workspaceRoot, "../outside/file.csv"),
    /path escapes workspace root/,
  );
});

test("filesystem RPC lists and reads files outside the workspace but does not delete them", async () => {
  const tempRoot = await mkdtemp(path.join(os.tmpdir(), "doer-fs-rpc-"));
  const rpcWorkspaceRoot = path.join(tempRoot, "workspace");
  const externalDir = path.join(tempRoot, "external");
  const externalFile = path.join(externalDir, "example.txt");
  const codec = StringCodec();

  await mkdir(rpcWorkspaceRoot);
  await mkdir(externalDir);
  await writeFile(externalFile, "outside workspace");

  async function request(action: string, targetPath: string): Promise<Record<string, unknown>> {
    let responseData: Uint8Array | undefined;
    const msg = {
      data: codec.encode(JSON.stringify({ action, path: targetPath })),
      respond(data: Uint8Array) {
        responseData = data;
        return true;
      },
    } as unknown as Msg;

    await handleFsRpcMessage({
      msg,
      workspaceRoot: rpcWorkspaceRoot,
      serverBaseUrl: "https://example.com",
      agentId: "agent-1",
      agentToken: "token",
      onError: () => undefined,
    });

    assert.ok(responseData);
    return JSON.parse(codec.decode(responseData)) as Record<string, unknown>;
  }

  try {
    const listing = await request("list", externalDir);
    assert.equal(listing.ok, true);
    assert.equal(listing.path, externalDir.split(path.sep).join("/"));
    assert.deepEqual(
      (listing.items as Array<Record<string, unknown>>).map((item) => item.path),
      [externalFile.split(path.sep).join("/")],
    );

    const text = await request("read_text", externalFile);
    assert.equal(text.ok, true);
    assert.equal(text.text, "outside workspace");

    const deletion = await request("delete_path", externalFile);
    assert.equal(deletion.ok, false);
    assert.match(String(deletion.error), /path escapes workspace root/);
    assert.equal(await readFile(externalFile, "utf8"), "outside workspace");
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
});
