import assert from "node:assert/strict";
import test from "node:test";
import {
  parseSuggestedThreadTags,
  parseThreadTagClassificationResponse,
} from "./agent-thread-tags-rpc.js";

test("parses only requested valid thread tag assignments", () => {
  const result = parseThreadTagClassificationResponse(
    JSON.stringify({
      assignments: [
        { threadId: "a", activityTag: "feature", confidence: 0.91 },
        { threadId: "b", activityTag: "invalid", confidence: 0.8 },
        { threadId: "unknown", activityTag: "bug", confidence: 1 },
      ],
    }),
    new Set(["a", "b"]),
  );
  assert.deepEqual(result, [{
    threadId: "a",
    activityTag: "feature",
    confidence: 0.91,
    source: "ai",
  }]);
});

test("accepts fenced JSON and clamps confidence", () => {
  const result = parseThreadTagClassificationResponse(
    "```json\n{\"assignments\":[{\"threadId\":\"a\",\"activityTag\":\"bug\",\"confidence\":2}]}\n```",
    new Set(["a"]),
  );
  assert.equal(result[0]?.confidence, 1);
});

test("accepts classifications from a custom configured range", () => {
  const result = parseThreadTagClassificationResponse(
    "{\"assignments\":[{\"threadId\":\"a\",\"activityTag\":\"customer-request\",\"confidence\":0.8}]}",
    new Set(["a"]),
    new Set(["customer-request", "other"]),
  );
  assert.equal(result[0]?.activityTag, "customer-request");
});

test("normalizes AI suggested tags and always adds other", () => {
  const result = parseSuggestedThreadTags(JSON.stringify({
    tags: [
      {
        id: "Customer Request",
        label: "고객 요청",
        description: "고객이 요청한 제품 변경",
        color: "#ABCDEF",
      },
    ],
  }));
  assert.deepEqual(result.map((tag) => tag.id), ["customer-request", "other"]);
  assert.equal(result[0]?.color, "#abcdef");
});

test("keeps other when an oversized AI proposal omits it", () => {
  const result = parseSuggestedThreadTags(JSON.stringify({
    tags: Array.from({ length: 12 }, (_, index) => ({
      id: `tag-${index}`,
      label: `Tag ${index}`,
      description: `Description ${index}`,
      color: "#123456",
    })),
  }));
  assert.equal(result.length, 12);
  assert.equal(result.at(-1)?.id, "other");
});
