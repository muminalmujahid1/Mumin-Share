// Mumin Share — Cloudflare Worker + Durable Object signaling
// Files travel over WebRTC DataChannel. This Worker only relays connection signals.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/room\/([0-9]{6})$/);
    if (!match) return new Response("Mumin Share signaling server is running.", { headers: { "content-type": "text/plain" } });
    if (request.headers.get("Upgrade") !== "websocket") return new Response("WebSocket connection required.", { status: 426 });
    const id = env.ROOMS.idFromName(match[1]);
    return env.ROOMS.get(id).fetch(request);
  }
};

export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Set();
    this.state.acceptWebSocket; // Durable Object WebSocket support
  }
  async fetch(request) {
    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.state.acceptWebSocket(server);
    const sockets = this.state.getWebSockets();
    if (sockets.length > 2) {
      server.send(JSON.stringify({ type: "error", message: "এই রুমে সর্বোচ্চ ২টি ডিভাইস যুক্ত হতে পারবে।" }));
      server.close(1008, "Room full");
      return new Response(null, { status: 101, webSocket: client });
    }
    if (sockets.length === 2) {
      for (const socket of sockets) {
        if (socket !== server) {
          try { socket.send(JSON.stringify({ type: "peer-joined" })); } catch {}
        }
      }
      server.send(JSON.stringify({ type: "peer-joined" }));
    }
    return new Response(null, { status: 101, webSocket: client });
  }
  webSocketMessage(ws, message) {
    let parsed;
    try { parsed = JSON.parse(message); } catch { return; }
    if (parsed.type !== "signal" || !parsed.data) return;
    for (const socket of this.state.getWebSockets()) {
      if (socket !== ws) {
        try { socket.send(JSON.stringify({ type: "signal", data: parsed.data })); } catch {}
      }
    }
  }
  webSocketClose(ws) {
    try { ws.close(); } catch {}
    for (const socket of this.state.getWebSockets()) {
      try { socket.send(JSON.stringify({ type: "peer-left" })); } catch {}
    }
  }
  webSocketError(ws) {
    try { ws.close(); } catch {}
  }
}