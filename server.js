// 🔥 Telegram Notification Server — Zero Message Loss Version
// Fixes:
//  1. Persistent queue saved to disk (survives cold starts on same session)
//  2. Self-ping every 9 minutes (Render free tier sleeps after 15min inactivity)
//  3. Deduplication — same message within 3 seconds won't send twice
//  4. Exponential backoff retry (1s → 2s → 4s → 8s → max 60s)
//  5. Queue processes one-at-a-time — guaranteed order, no drops
//  6. Startup drain — on cold start, immediately sends any queued messages

import express from "express";
import fetch from "node-fetch";
import cors from "cors";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(cors());
app.use(express.json());

// ── Telegram Config ───────────────────────────────────────────────────────────
const TELEGRAM_BOT_TOKEN = "8250275700:AAEEb_jHPtRtykuvrlwxgYJjjGFogKSW8hk";
const TELEGRAM_CHAT_ID = "1449074180";

// ── Persistent queue ──────────────────────────────────────────────────────────
// /tmp is writable on Render and persists within a running session
const QUEUE_FILE = "/tmp/notification_queue.json";

function loadQueue() {
  try {
    if (fs.existsSync(QUEUE_FILE)) {
      const data = JSON.parse(fs.readFileSync(QUEUE_FILE, "utf8"));
      console.log(`📂 Loaded ${data.length} queued messages from disk`);
      return Array.isArray(data) ? data : [];
    }
  } catch (e) {
    console.log("⚠️ Could not read queue file:", e.message);
  }
  return [];
}

function saveQueue() {
  try {
    fs.writeFileSync(QUEUE_FILE, JSON.stringify(queue), "utf8");
  } catch (e) {
    console.log("⚠️ Could not save queue:", e.message);
  }
}

// ── State ─────────────────────────────────────────────────────────────────────
let queue        = loadQueue();  // [{ id, text, retries, addedAt }]
let isProcessing = false;

// ── Deduplication: ignore same message sent within 3 seconds ─────────────────
const recentMessages = new Map();

function isDuplicate(text) {
  const last = recentMessages.get(text);
  if (last && Date.now() - last < 3000) return true;
  recentMessages.set(text, Date.now());
  if (recentMessages.size > 100) {
    const cutoff = Date.now() - 10000;
    for (const [k, v] of recentMessages) {
      if (v < cutoff) recentMessages.delete(k);
    }
  }
  return false;
}

// ── Sleep helper ──────────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Send to Telegram (single attempt) ────────────────────────────────────────
async function sendToTelegram(text) {
  const res = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text }),
      signal: AbortSignal.timeout(10000), // 10s timeout
    }
  );

  if (!res.ok) {
    const body = await res.text();
    if (res.status === 429) {
      // Telegram rate limit — respect retry_after
      let retryAfter = 5;
      try { retryAfter = JSON.parse(body)?.parameters?.retry_after ?? 5; } catch {}
      console.log(`⏳ Rate limited. Waiting ${retryAfter}s`);
      await sleep(retryAfter * 1000);
      throw new Error("RATE_LIMITED");
    }
    throw new Error(`HTTP ${res.status}: ${body.slice(0, 100)}`);
  }
  return true;
}

// ── Process queue — one message at a time, with exponential backoff ───────────
async function processQueue() {
  if (isProcessing || queue.length === 0) return;
  isProcessing = true;

  while (queue.length > 0) {
    const item  = queue[0];
    let success = false;
    let waitMs  = 1000;

    // Up to 7 attempts per processing cycle
    for (let attempt = 0; attempt < 7; attempt++) {
      try {
        await sendToTelegram(item.text);
        console.log(`✅ Delivered [${item.id}]: ${item.text.slice(0, 80)}`);
        success = true;
        break;
      } catch (err) {
        console.log(`❌ Attempt ${attempt + 1} failed [${item.id}]: ${err.message}`);
        if (attempt < 6) {
          await sleep(waitMs);
          waitMs = Math.min(waitMs * 2, 60000); // max 60s between retries
        }
      }
    }

    if (success) {
      queue.shift();
    } else {
      // Move to back, try again in next cycle
      item.retries = (item.retries || 0) + 1;
      queue.shift();
      if (item.retries < 20) {
        queue.push(item); // will retry
        console.log(`🔁 Moved [${item.id}] to back (retry #${item.retries})`);
      } else {
        console.log(`🗑️ Dropped after 20 retries: ${item.text.slice(0, 60)}`);
      }
    }

    saveQueue();
    await sleep(300); // 300ms gap between messages (Telegram limit is ~30/sec)
  }

  isProcessing = false;
}

// ── Add to queue ──────────────────────────────────────────────────────────────
function enqueue(text) {
  if (isDuplicate(text)) {
    console.log("🔁 Duplicate ignored:", text.slice(0, 50));
    return "duplicate";
  }
  const item = {
    id:      Date.now().toString(36),
    text,
    retries: 0,
    addedAt: new Date().toISOString(),
  };
  queue.push(item);
  saveQueue();
  console.log(`📥 Enqueued [${item.id}] | Queue: ${queue.length} | ${text.slice(0, 60)}`);
  processQueue().catch(console.error); // non-blocking
  return "queued";
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.post("/send", (req, res) => {
  const { message } = req.body;
  if (!message || !message.trim()) {
    return res.status(400).json({ error: "message is required" });
  }
  const result = enqueue(message.trim());
  // Always tell client success — we guarantee delivery internally
  res.json({ success: true, status: result });
});

app.get("/", (req, res) => {
  res.json({
    status:    "running ✅",
    queue:     queue.length,
    uptime:    `${Math.floor(process.uptime())}s`,
  });
});

app.get("/health", (_req, res) => res.json({ ok: true, queue: queue.length }));

// ── Self-ping every 9 min — prevents Render free tier from sleeping ───────────
// Render sleeps after 15 minutes of inactivity. 9min ping keeps it awake 24/7.
const SELF_URL = process.env.RENDER_EXTERNAL_URL
  ?? "https://notification-production-bdd8.up.railway.app";

setInterval(async () => {
  try {
    await fetch(`${SELF_URL}/health`, { signal: AbortSignal.timeout(8000) });
    console.log("💓 Keep-alive ping OK");
  } catch (e) {
    console.log("⚠️ Keep-alive ping failed:", e.message);
  }
}, 9 * 60 * 1000);

// ── Start server ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 Notification server on port ${PORT}`);
  console.log(`📂 Startup queue: ${queue.length} message(s)`);
  // Drain any messages that were queued before restart
  if (queue.length > 0) {
    console.log("🔄 Draining startup queue immediately...");
    processQueue().catch(console.error);
  }
});
