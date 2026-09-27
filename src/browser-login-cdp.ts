import WebSocket from "ws";

// A single tab connection. Never enable Network, tracing or console event capture.
export class LoginTab {
  private nextId = 0;
  private readonly pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  private constructor(private readonly ws: WebSocket) {
    ws.on("message", (data) => {
      let message: { id?: number; result?: unknown; error?: unknown };
      try { message = JSON.parse(data.toString()); } catch { return; }
      if (typeof message.id !== "number") return; // Drop all events, never log them.
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id); clearTimeout(pending.timer);
      if (message.error) pending.reject(new Error("Browser command failed"));
      else pending.resolve(message.result);
    });
    ws.on("close", () => this.rejectPending());
    ws.on("error", () => this.rejectPending());
  }
  static async connect(url: string): Promise<LoginTab> {
    const ws = new WebSocket(url, { handshakeTimeout: 5_000, maxPayload: 8 * 1024 * 1024 });
    const tab = new LoginTab(ws);
    await new Promise<void>((resolve, reject) => { ws.once("open", resolve); ws.once("error", () => reject(new Error("Browser connection failed"))); });
    return tab;
  }
  get connected() { return this.ws.readyState === WebSocket.OPEN; }
  private rejectPending() {
    for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error("Browser connection closed")); }
    this.pending.clear();
  }
  call<T = any>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    if (!this.connected) return Promise.reject(new Error("Browser connection closed"));
    const id = ++this.nextId;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error("Browser command timed out")); }, 5_000);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ id, method, params }), (error) => {
        if (error) { clearTimeout(timer); this.pending.delete(id); reject(new Error("Browser command failed")); }
      });
    });
  }
  async evaluate<T>(expression: string, contextId: number, returnByValue = true): Promise<T> {
    const response = await this.call("Runtime.evaluate", { expression, contextId, returnByValue, silent: true });
    if (response.exceptionDetails) throw new Error("Browser evaluation failed");
    return returnByValue ? response.result.value : response.result.objectId;
  }
  async onObject<T>(objectId: string, functionDeclaration: string, values: unknown[] = []): Promise<T> {
    const response = await this.call("Runtime.callFunctionOn", { objectId, functionDeclaration, arguments: values.map((value) => ({ value })), returnByValue: true, silent: true });
    if (response.exceptionDetails) throw new Error("Browser evaluation failed");
    return response.result.value;
  }
  async snapshot(capturePreview = true) {
    const { frameTree } = await this.call("Page.getFrameTree");
    const { executionContextId: contextId } = await this.call("Page.createIsolatedWorld", { frameId: frameTree.frame.id, worldName: "doer-login" });
    const origin = new URL(frameTree.frame.url).origin;
    const documentId = await this.evaluate<string>("document.documentElement", contextId, false);
    const metadata = await this.evaluate<Array<{ index: number; type: string; label: string; purpose: "username" | "password" | "other" }>>(`Array.from(document.querySelectorAll('input')).map((input,index)=>({input,index})).filter(({input})=>!['hidden','submit','button','checkbox','radio','file'].includes(input.type)&&!input.disabled&&!input.readOnly&&input.getBoundingClientRect().width>0&&input.getBoundingClientRect().height>0&&getComputedStyle(input).visibility!=='hidden').slice(0,20).map(({input,index})=>{
      const autocomplete=(input.autocomplete||'').split(/\\s+/), names=[input.name,input.id];
      let purpose='other';
      if(!autocomplete.includes('one-time-code')&&!autocomplete.includes('new-password')) {
        if(input.type==='password'||autocomplete.includes('current-password')||names.some(n=>/^(pw|passwd|password)$/i.test(n))) purpose='password';
        else if(autocomplete.includes('username')||input.type==='email'||names.some(n=>/^(id|email|username|user_name|userid|user_id|loginid|login_id)$/i.test(n))) purpose='username';
      }
      return {index,type:input.type,purpose,label:(input.labels?.[0]?.textContent||input.getAttribute('aria-label')||input.placeholder||input.name||input.type).slice(0,100)};
    })`, contextId);
    const fields = [];
    for (const field of metadata) {
      const objectId = await this.evaluate<string>(`document.querySelectorAll('input')[${field.index}]`, contextId, false);
      fields.push({ ...field, objectId, origin });
    }
    const size = await this.evaluate<{ width: number; height: number }>("({width:innerWidth,height:innerHeight})", contextId);
    if (!capturePreview) return { origin, documentId, contextId, fields, screenshot: "", ...size };
    // Opaque covers hide all input fields and embedded frames, not just password-type fields.
    // Covers exist only during capture, and are removed in finally even if capture fails.
    const coverId = await this.evaluate<string>(`(()=>{const host=document.createElement('div');host.style.cssText='position:fixed;inset:0;z-index:2147483647;pointer-events:none';const shadow=host.attachShadow({mode:'closed'});for(const input of document.querySelectorAll('input,iframe')){const r=input.getBoundingClientRect();if(!r.width||!r.height)continue;const cover=document.createElement('div');cover.style.cssText='position:fixed;background:#d4d4d8;left:'+r.left+'px;top:'+r.top+'px;width:'+r.width+'px;height:'+r.height+'px';shadow.appendChild(cover)}document.documentElement.appendChild(host);return host})()`, contextId, false);
    let screenshot: string;
    try {
      screenshot = (await this.call("Page.captureScreenshot", { format: "jpeg", quality: 45, captureBeyondViewport: false })).data;
    } finally {
      await this.onObject(coverId, "function(){this.remove()}").catch(() => undefined);
      await this.call("Runtime.releaseObject", { objectId: coverId }).catch(() => undefined);
    }
    if (!await this.onObject<boolean>(documentId, "function(){return this.isConnected}").catch(() => false)) throw new Error("Page changed");
    return { origin, documentId, contextId, fields, screenshot, ...size };
  }
  async fill(objectId: string, origin: string, value: string) {
    return this.onObject<boolean>(objectId, `function(input){
      if(!this.isConnected||this.ownerDocument.location.origin!==input.origin||this.disabled||this.readOnly)return false;
      if(this.form&&new URL(this.form.action||location.href,location.href).origin!==input.origin)return false;
      const setter=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value')?.set;
      if(!setter)return false;setter.call(this,input.value);
      this.dispatchEvent(new Event('input',{bubbles:true}));this.dispatchEvent(new Event('change',{bubbles:true}));return true;
    }`, [{ origin, value }]);
  }
  async canAutoSubmit(passwordId: string, usernameId: string, origin: string) {
    const result = await this.call("Runtime.callFunctionOn", { objectId: passwordId,
      functionDeclaration: `function(username,origin){
        const form=this.form;
        if(!this.isConnected||!username.isConnected||this.ownerDocument.location.origin!==origin||!form||username.form!==form||form.method.toLowerCase()!=='post'||new URL(form.action||location.href,location.href).origin!==origin)return false;
        const buttons=Array.from(form.querySelectorAll('button,input[type=submit]')).filter(e=>e.type==='submit'&&!e.disabled&&e.getBoundingClientRect().width>0);
        return buttons.length===1&&/sign[ -]?in|log[ -]?in|로그인/i.test(buttons[0].innerText||buttons[0].value||'');
      }`, arguments: [{ objectId: usernameId }, { value: origin }], returnByValue: true, silent: true });
    return result.result?.value === true;
  }
  async autoSubmit(passwordId: string, usernameId: string, origin: string) {
    if (!await this.canAutoSubmit(passwordId, usernameId, origin)) return false;
    // Recheck and submit in a single browser task after input handlers have run.
    return this.onObject<boolean>(passwordId, `function(origin){
      const form=this.form;
      if(!this.isConnected||location.origin!==origin||!form||form.method.toLowerCase()!=='post'||new URL(form.action||location.href,location.href).origin!==origin)return false;
      const buttons=Array.from(form.querySelectorAll('button,input[type=submit]')).filter(e=>e.type==='submit'&&!e.disabled&&e.getBoundingClientRect().width>0);
      if(buttons.length!==1||!/sign[ -]?in|log[ -]?in|로그인/i.test(buttons[0].innerText||buttons[0].value||''))return false;
      buttons[0].click();return true;
    }`, [origin]);
  }
  async click(x: number, y: number) {
    await this.call("Input.dispatchMouseEvent", { type: "mousePressed", x, y, button: "left", clickCount: 1 });
    await this.call("Input.dispatchMouseEvent", { type: "mouseReleased", x, y, button: "left", clickCount: 1 });
  }
  async key(key: string) {
    const codes: Record<string, number> = { Tab: 9, Enter: 13, Escape: 27, ArrowDown: 40, ArrowUp: 38 };
    const actual = key === "Shift+Tab" ? "Tab" : key;
    const params = { key: actual, code: actual, windowsVirtualKeyCode: codes[actual], modifiers: key === "Shift+Tab" ? 8 : 0 };
    await this.call("Input.dispatchKeyEvent", { ...params, type: "keyDown", ...(actual === "Enter" ? { text: "\r" } : {}) });
    await this.call("Input.dispatchKeyEvent", { ...params, type: "keyUp" });
  }
  close() { this.ws.close(); this.rejectPending(); }
}
