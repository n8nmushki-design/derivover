/**
 * Cloudflare Worker – free-tier keep-alive cron for Derivover
 *
 * Pings your bot's /health endpoint on a schedule so free hosts
 * (Render, Railway free, Fly free, etc.) do not sleep the process.
 *
 * Setup (free Cloudflare account):
 *   1. Workers & Pages → Create Worker → paste this file
 *   2. Settings → Variables → add BOT_URL = https://your-app.onrender.com
 *      (no trailing slash)
 *   3. Settings → Triggers → Cron Triggers → Add:
 *        * /5 * * * *     (every 5 minutes – free tier allows this)
 *   4. Deploy. Check Logs to confirm 200 responses.
 *
 * Free-tier limits (as of 2026): 100k requests/day, plenty for a 5-min ping.
 */

export default {
  async scheduled(event, env, ctx) {
    const base = (env.BOT_URL || "").replace(/\/$/, "");
    if (!base) {
      console.error("BOT_URL secret is not set");
      return;
    }
    const url = base + "/health";
    try {
      const res = await fetch(url, {
        method: "GET",
        headers: { "User-Agent": "derivover-cf-cron/1.0" },
        cf: { cacheTtl: 0, cacheEverything: false },
      });
      const body = await res.text();
      console.log(`ping ${url} → ${res.status} ${body.slice(0, 120)}`);
    } catch (err) {
      console.error(`ping failed: ${err}`);
    }
  },

  // Optional: hit the worker URL in a browser to test manually
  async fetch(request, env) {
    const base = (env.BOT_URL || "").replace(/\/$/, "");
    if (!base) {
      return new Response("Set BOT_URL secret first", { status: 500 });
    }
    const url = base + "/health";
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": "derivover-cf-cron/1.0" },
      });
      const text = await res.text();
      return new Response(
        JSON.stringify({ ok: res.ok, status: res.status, body: text }, null, 2),
        { headers: { "content-type": "application/json" } }
      );
    } catch (err) {
      return new Response(String(err), { status: 502 });
    }
  },
};
