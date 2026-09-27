import type { LoginTab } from "./browser-login-cdp.js";

export function browserUrl(value: unknown): URL {
  if (typeof value !== "string" || value.length > 8192) throw new Error("Invalid browser address");
  const url = new URL(value);
  if (url.username || url.password || !(url.protocol === "https:" || (url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))) throw new Error("Unsupported browser address");
  return url;
}
export const browserKeys = ["Tab", "Shift+Tab", "Enter", "Escape", "ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight", "PageDown", "PageUp", "Home", "End", "Backspace"] as const;

/** Small shared action set. No arbitrary JavaScript, filesystem access or credential values. */
export async function performBrowserAction(page: LoginTab, size: { width: number; height: number }, command: Record<string, unknown>) {
  if (command.action === "navigate") {
    const result = await page.call("Page.navigate", { url: browserUrl(command.url).href });
    if (result.errorText) throw new Error("Navigation failed");
  } else if (command.action === "back" || command.action === "forward") {
    const history = await page.call("Page.getNavigationHistory");
    const entry = history.entries[history.currentIndex + (command.action === "back" ? -1 : 1)];
    if (!entry) return;
    browserUrl(entry.url);
    await page.call("Page.navigateToHistoryEntry", { entryId: entry.id });
  } else if (command.action === "reload") {
    await page.call("Page.reload");
  } else if (command.action === "click") {
    const { x, y } = command;
    if (typeof x !== "number" || typeof y !== "number" || !Number.isFinite(x) || !Number.isFinite(y) || x < 0 || y < 0 || x > size.width || y > size.height) throw new Error("Invalid click position");
    await page.click(x, y);
  } else if (command.action === "key") {
    if (!(browserKeys as readonly unknown[]).includes(command.key)) throw new Error("Unsupported browser key");
    await page.key(String(command.key));
  } else if (command.action === "scroll") {
    if (typeof command.delta !== "number" || !Number.isInteger(command.delta) || Math.abs(command.delta) > 2000) throw new Error("Invalid scroll");
    await page.call("Input.dispatchMouseEvent", { type: "mouseWheel", x: size.width / 2, y: size.height / 2, deltaX: 0, deltaY: command.delta });
  } else throw new Error("Unsupported browser action");
}
