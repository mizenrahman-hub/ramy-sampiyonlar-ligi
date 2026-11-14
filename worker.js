// Ramy Sampiyonlar Ligi - Cloudflare Worker (staging)
// D1 binding: DB (also supports Veritabani / Veritabanı).
// Secrets: YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN.
// Configure a cron trigger (* * * * *) only after testing.
// This file is NOT deployed automatically.
const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, OPTIONS",
  "access-control-allow-headers": "content-type"
};
const out = (data, status = 200) => new Response(JSON.stringify(data), {status, headers: JSON_HEADERS});
const dbFor = env => env.DB || env.Veritabani || env["Veritabanı"];
const google = async (url, options) => {
  const res = await fetch(url, options);
  const data = await res.json();
  if (!res.ok) throw new Error("YouTube API " + res.status + ": " + JSON.stringify(data).slice(0, 400));
  return data;
};
async function accessToken(env) {
  if (!env.YOUTUBE_CLIENT_ID || !env.YOUTUBE_CLIENT_SECRET || !env.YOUTUBE_REFRESH_TOKEN) {
    throw new Error("Missing YouTube OAuth secrets");
  }
  const body = new URLSearchParams({
    client_id: env.YOUTUBE_CLIENT_ID,
    client_secret: env.YOUTUBE_CLIENT_SECRET,
    refresh_token: env.YOUTUBE_REFRESH_TOKEN,
    grant_type: "refresh_token"
  });
  const token = await google("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {"content-type": "application/x-www-form-urlencoded"},
    body
  });
  if (!token.access_token) throw new Error("Missing access token");
  return token.access_token;
}
const ytGet = (path, token) => google("https://www.googleapis.com/youtube/v3/" + path, {
  headers: {authorization: "Bearer " + token}
});
async function ensureState(db) {
  await db.prepare("CREATE TABLE IF NOT EXISTS ingestion_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)").run();
}
async function stateGet(db, key) {
  const r = await db.prepare("SELECT value FROM ingestion_state WHERE key=?").bind(key).first();
  return r?.value || null;
}
async function statePut(db, key, value) {
  await db.prepare("INSERT INTO ingestion_state (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(key, value).run();
}
function amountTry(item, env) {
  const details = item.snippet?.superChatDetails;
  if (!details) return null;
  const amount = Number(details.amountMicros) / 1e6;
  if (!Number.isFinite(amount) || amount <= 0) return null;
  const currency = details.currency || "";
  if (currency === "TRY") return amount;
  // No fabricated exchange rate: explicit configured rate is required.
  if (!/^[A-Z]{3}$/.test(currency)) throw new Error("Invalid currency");
  const rate = Number(env["FX_" + currency + "_TRY"]);
  if (!Number.isFinite(rate) || rate <= 0) throw new Error("Missing FX_" + currency + "_TRY rate");
  return Math.round(amount * rate * 100) / 100;
}
async function poll(env) {
  const db = dbFor(env);
  if (!db) throw new Error("D1 binding not found");
  await ensureState(db);
  const token = await accessToken(env);
  const broadcasts = await ytGet("liveBroadcasts?part=snippet&broadcastStatus=active&mine=true&maxResults=10", token);
  const active = (broadcasts.items || []).find(x => x.snippet?.liveChatId);
  if (!active) return {status:"no_active_broadcast", imported:0};
  const chatId = active.snippet.liveChatId;
  const key = "page:" + chatId;
  const pageToken = await stateGet(db, key);
  const params = new URLSearchParams({
    liveChatId: chatId, part: "snippet,authorDetails", maxResults: "200"
  });
  if (pageToken) params.set("pageToken", pageToken);
  const messages = await ytGet("liveChat/messages?" + params, token);
  let imported = 0;
  for (const item of messages.items || []) {
    if (item.snippet?.type !== "superChatEvent") continue;
    const amount = amountTry(item, env);
    if (amount === null) continue;
    const details = item.snippet.superChatDetails;
    const id = item.id;
    const supporterId = item.authorDetails?.channelId || "unknown:" + id;
    const supporterName = item.authorDetails?.displayName || "Destekçi";
    const amountOriginal = Number(details.amountMicros) / 1e6;
    const donatedAt = item.snippet.publishedAt;
    const result = await db.prepare(
      "INSERT OR IGNORE INTO donations (id,supporter_id,supporter_name,amount_original,currency,amount_try,message,donated_at) VALUES (?,?,?,?,?,?,?,?)"
    ).bind(id,supporterId,supporterName,amountOriginal,details.currency,amount,details.userComment || "",donatedAt).run();
    if (result.meta?.changes) imported++;
  }
  if (messages.nextPageToken) await statePut(db, key, messages.nextPageToken);
  return {status:"ok", imported, checked:(messages.items || []).length, pollingIntervalMillis:messages.pollingIntervalMillis || null};
}
async function leaderboard(db, since) {
  const result = await db.prepare(
    "SELECT supporter_id, MAX(supporter_name) AS name, ROUND(SUM(amount_try),2) AS amount FROM donations WHERE donated_at >= ? GROUP BY supporter_id ORDER BY amount DESC LIMIT 3"
  ).bind(since).all();
  return result.results || [];
}
function periodStart(timeZone, week) {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone, year:"numeric", month:"2-digit", day:"2-digit", weekday:"short", hour:"2-digit", hourCycle:"h23"
  }).formatToParts(now);
  const v = Object.fromEntries(parts.map(p=>[p.type,p.value]));
  // Date starts at midnight in Turkey (UTC+03 year-round).
  const day = new Date(Date.UTC(Number(v.year),Number(v.month)-1,Number(v.day),-3));
  if (week) {
    const offset = ({Mon:0,Tue:1,Wed:2,Thu:3,Fri:4,Sat:5,Sun:6})[v.weekday];
    day.setUTCDate(day.getUTCDate()-offset);
  }
  return day.toISOString();
}
export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") return new Response(null,{status:204,headers:JSON_HEADERS});
    const path = new URL(request.url).pathname.replace(/\\/$/,"") || "/";
    if (path === "/") return out({system:"Ramy Şampiyonlar Ligi", status:"online"});
    if (path !== "/health" && path !== "/leaders") return out({error:"not_found"},404);
    const db = dbFor(env);
    if (!db) return out({status:"error",database:"disconnected",reason:"D1 binding not found"},503);
    try {
      if (path === "/health") {
        await db.prepare("SELECT 1 AS test").first();
        return out({status:"ok",database:"connected"});
      }
      const [day,week] = await Promise.all([
        leaderboard(db,periodStart("Europe/Istanbul",false)),
        leaderboard(db,periodStart("Europe/Istanbul",true))
      ]);
      return out({day,week,currency:"TRY",updated_at:new Date().toISOString()});
    } catch(e) {
      return out({status:"error",reason:String(e.message || e)},503);
    }
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(poll(env).then(result=>console.log(JSON.stringify(result))).catch(error=>{
      console.error("Super Chat polling failed:",String(error.message || error));
      throw error;
    }));
  }
};
