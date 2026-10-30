// Seeds Spotify-style test data the way production produces it:
//   users, tracks  → Mongo music.* (Flink CDC carries them to Kafka cdc.music → ClickHouse)
//   plays, clicks  → Kafka events.plays / events.clicks, key tenant_id:user_id (contract: contracts/events.md)
//
//   node seed.js              !! FULL RESET !! wipes Mongo users/tracks AND TRUNCATES ClickHouse music.events_raw,
//                             plays, clicks, users, tracks, ingest_errors and every table fed from them (rollups),
//                             then loads users, tracks and ~60 days of plays/clicks (epoch-ms timestamps)
//   node seed.js --live [n]   keep producing ~n events/sec (default 20) until Ctrl-C; ~10% arrive late,
//                             ~10% carry ISO-8601 timestamps instead of epoch ms
// Env: MONGO_URL (default mongodb://localhost:27017/?directConnection=true), KAFKA_BROKERS (default localhost:19092),
//      CLICKHOUSE_URL (default http://localhost:8123), CLICKHOUSE_USER / CLICKHOUSE_PASSWORD (default default / clickhouse)
import { MongoClient } from "mongodb";
import { Kafka, Partitioners, logLevel } from "kafkajs";
import { randomUUID } from "node:crypto";

const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017/?directConnection=true";
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:19092").split(",");
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || "http://localhost:8123";
const CLICKHOUSE_AUTH = Buffer.from(`${process.env.CLICKHOUSE_USER || "default"}:${process.env.CLICKHOUSE_PASSWORD ?? "clickhouse"}`).toString("base64");
const DAY = 86_400_000;
const HOUR = 3_600_000;

// Deterministic RNG so every bulk seed produces the same catalog and users.
let seed = 42;
const rand = () => {
  seed |= 0; seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const pick = (a) => a[Math.floor(rand() * a.length)];
const between = (lo, hi) => lo + rand() * (hi - lo);
const int = (lo, hi) => Math.floor(between(lo, hi + 1));
const clamp = (x) => Math.round(Math.min(1, Math.max(0, x)) * 1000) / 1000;
const b62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const spotifyId = () => Array.from({ length: 22 }, () => pick([...b62])).join("");

// genre → [danceability, energy, valence, acousticness, tempo] centre, plus tracks [title, artist, album, year]
const CATALOG = {
  "pop": [[0.72, 0.70, 0.62, 0.15, 118], [
    ["Blinding Lights", "The Weeknd", "After Hours", 2020], ["Levitating", "Dua Lipa", "Future Nostalgia", 2020],
    ["As It Was", "Harry Styles", "Harry's House", 2022], ["Anti-Hero", "Taylor Swift", "Midnights", 2022],
    ["Flowers", "Miley Cyrus", "Endless Summer Vacation", 2023], ["Espresso", "Sabrina Carpenter", "Short n' Sweet", 2024],
    ["bad guy", "Billie Eilish", "WHEN WE ALL FALL ASLEEP", 2019], ["Watermelon Sugar", "Harry Styles", "Fine Line", 2019]]],
  "hip-hop": [[0.80, 0.65, 0.48, 0.10, 98], [
    ["HUMBLE.", "Kendrick Lamar", "DAMN.", 2017], ["SICKO MODE", "Travis Scott", "ASTROWORLD", 2018],
    ["God's Plan", "Drake", "Scorpion", 2018], ["Not Like Us", "Kendrick Lamar", "Not Like Us", 2024],
    ["Lose Yourself", "Eminem", "8 Mile", 2002], ["Money Trees", "Kendrick Lamar", "good kid, m.A.A.d city", 2012],
    ["N95", "Kendrick Lamar", "Mr. Morale", 2022]]],
  "rock": [[0.48, 0.82, 0.45, 0.08, 128], [
    ["Bohemian Rhapsody", "Queen", "A Night at the Opera", 1975], ["Smells Like Teen Spirit", "Nirvana", "Nevermind", 1991],
    ["Mr. Brightside", "The Killers", "Hot Fuss", 2004], ["Do I Wanna Know?", "Arctic Monkeys", "AM", 2013],
    ["Seven Nation Army", "The White Stripes", "Elephant", 2003], ["Back In Black", "AC/DC", "Back In Black", 1980],
    ["Everlong", "Foo Fighters", "The Colour and the Shape", 1997]]],
  "indie": [[0.58, 0.55, 0.50, 0.35, 112], [
    ["Electric Feel", "MGMT", "Oracular Spectacular", 2007], ["The Less I Know The Better", "Tame Impala", "Currents", 2015],
    ["Midnight City", "M83", "Hurry Up, We're Dreaming", 2011], ["Sweater Weather", "The Neighbourhood", "I Love You.", 2013],
    ["Ribs", "Lorde", "Pure Heroine", 2013], ["Myth", "Beach House", "Bloom", 2012], ["Heat Waves", "Glass Animals", "Dreamland", 2020]]],
  "electronic": [[0.75, 0.85, 0.45, 0.05, 124], [
    ["Strobe", "deadmau5", "For Lack of a Better Name", 2009], ["One More Time", "Daft Punk", "Discovery", 2000],
    ["Titanium", "David Guetta", "Nothing but the Beat", 2011], ["Levels", "Avicii", "Levels", 2011],
    ["Midnight", "Lane 8", "Little by Little", 2018], ["Opus", "Eric Prydz", "Opus", 2016], ["Breathe", "The Prodigy", "The Fat of the Land", 1997]]],
  "bollywood": [[0.68, 0.66, 0.60, 0.25, 110], [
    ["Kesariya", "Arijit Singh", "Brahmastra", 2022], ["Tum Hi Ho", "Arijit Singh", "Aashiqui 2", 2013],
    ["Chaleya", "Arijit Singh", "Jawan", 2023], ["Jhoome Jo Pathaan", "Arijit Singh", "Pathaan", 2023],
    ["Kala Chashma", "Badshah", "Baar Baar Dekho", 2016], ["Apna Bana Le", "Arijit Singh", "Bhediya", 2022],
    ["Raataan Lambiyan", "Jubin Nautiyal", "Shershaah", 2021]]],
  "jazz": [[0.52, 0.32, 0.55, 0.80, 105], [
    ["So What", "Miles Davis", "Kind of Blue", 1959], ["Take Five", "Dave Brubeck", "Time Out", 1959],
    ["Fly Me To The Moon", "Frank Sinatra", "It Might as Well Be Swing", 1964], ["Autumn Leaves", "Cannonball Adderley", "Somethin' Else", 1958],
    ["My Favorite Things", "John Coltrane", "My Favorite Things", 1961], ["Feeling Good", "Nina Simone", "I Put a Spell on You", 1965]]],
  "lofi": [[0.62, 0.30, 0.42, 0.70, 82], [
    ["Snowman", "WYS", "Snowman", 2019], ["Coffee", "beabadoobee", "Coffee", 2017], ["Affection", "Jinsang", "Life", 2016],
    ["Sunday Morning", "Idealism", "Hiraeth", 2017], ["Lullaby", "Kupla", "Kingdom in Blue", 2019], ["Glow", "Tomppabeats", "Harbor", 2016]]],
};

function buildTracks() {
  const now = new Date();
  const artistIds = {};
  const tracks = [];
  for (const [genre, [[d, e, v, a, tempo], songs]] of Object.entries(CATALOG)) {
    for (const [title, artist, album, year] of songs) {
      artistIds[artist] ??= `spotify:artist:${spotifyId()}`;
      tracks.push({
        track_id: `spotify:track:${spotifyId()}`, track_name: title,
        artist_id: artistIds[artist], artist_name: artist, album_name: album, genre,
        duration_ms: int(150_000, 330_000), popularity: int(45, 98), release_year: year,
        danceability: clamp(d + between(-0.12, 0.12)), energy: clamp(e + between(-0.12, 0.12)),
        valence: clamp(v + between(-0.15, 0.15)), acousticness: clamp(a + between(-0.1, 0.1)),
        tempo: Math.round((tempo + between(-12, 12)) * 10) / 10, updated_at: now,
      });
    }
  }
  return tracks;
}

const TENANTS = ["t_acme", "t_globex", "t_initech"];
const NAMES = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Sara", "Vikram", "Zoya", "Liam", "Emma", "Noah", "Olivia"];
const COUNTRIES = ["IN", "US", "GB", "DE", "SG"];
const PLATFORMS = ["web", "android", "ios", "desktop"];

function buildUsers() {
  const users = [];
  TENANTS.forEach((tenant, ti) => {
    for (let i = 1; i <= 8; i++) {
      const name = `${pick(NAMES)} ${String.fromCharCode(65 + ti)}${i}`;
      users.push({
        user_id: `u_${tenant.slice(2)}_${String(i).padStart(2, "0")}`, tenant_id: tenant, display_name: name,
        email: `${name.toLowerCase().replace(/\s+/g, ".")}@${tenant.slice(2)}.example`,
        country: pick(COUNTRIES), plan: rand() < 0.4 ? "premium" : "free",
        created_at: new Date(Date.now() - int(60, 400) * DAY), updated_at: new Date(),
      });
    }
  });
  return users;
}

// Each user has 2 favourite genres and a "rotation" of ~12 tracks from them (leaves room for recommendations).
function tasteFor(user, tracks) {
  const genres = Object.keys(CATALOG);
  const favs = [pick(genres), pick(genres)];
  const pool = tracks.filter((t) => favs.includes(t.genre));
  const rotation = [...pool].sort(() => rand() - 0.5).slice(0, Math.min(12, pool.length - 3));
  return { user, favs, rotation, platform: pick(PLATFORMS), country: user.country };
}

// One listening session → plays + the clicks around them. `late` delays received_at by hours (offline sync).
function session(taste, start, tracks, late = false) {
  const { user, rotation, platform, country } = taste;
  const sid = randomUUID();
  const plays = [], clicks = [];
  let t = start;
  const recv = (ct) => new Date(Math.min(Date.now(), ct.getTime() + (late ? between(HOUR, 72 * HOUR) : between(200, 4000))));
  const click = (click_type, target_type, target_id, extra = {}) => {
    const ct = new Date(t);
    clicks.push({ event_id: randomUUID(), tenant_id: user.tenant_id, user_id: user.user_id, session_id: sid,
      click_type, target_type, target_id, page: extra.page || "/home", platform,
      client_time: ct, received_at: recv(ct), schema_version: 1, ...extra });
  };
  click("page_view", "page", "/home", { page: "/home" });
  if (rand() < 0.4) {
    const q = pick(rotation);
    click("search", "search", q.artist_name, { page: "/search", search_query: q.artist_name.toLowerCase() });
  }
  const n = int(3, 15);
  for (let i = 0; i < n; i++) {
    if (t > Date.now() - 5_000) break; // never emit events from the future
    const tr = rand() < 0.85 ? pick(rotation) : pick(tracks); // mostly rotation, some exploration
    const skipped = rand() < (rotation.includes(tr) ? 0.12 : 0.45);
    const ms = skipped ? int(4_000, 30_000) : Math.floor(tr.duration_ms * between(0.85, 1));
    const ct = new Date(t);
    const v2 = rand() < 0.1; // newer clients send schema v2 with extra fields
    if (i === 0) click("play", "track", tr.track_id, { page: "/home" });
    plays.push({
      event_id: randomUUID(), tenant_id: user.tenant_id, user_id: user.user_id, track_id: tr.track_id,
      track_name: tr.track_name, artist_name: tr.artist_name, genre: tr.genre,
      client_time: ct, received_at: recv(ct), ms_played: ms,
      reason_start: i === 0 ? "clickrow" : "trackdone", reason_end: skipped ? "fwdbtn" : "trackdone",
      skipped, shuffle: rand() < 0.3, platform, conn_country: country, session_id: sid,
      schema_version: v2 ? 2 : 1,
      ...(v2 ? { audio_quality: pick(["normal", "high", "lossless"]), device_model: pick(["Pixel 8", "iPhone 15", "MacBook Air"]) } : {}),
    });
    if (skipped) click("skip", "track", tr.track_id, { page: "/player" });
    else if (rand() < 0.08) click("like", "track", tr.track_id, { page: "/player" });
    else if (rand() < 0.03) click("add_to_playlist", "track", tr.track_id, { page: "/player" });
    if (rand() < 0.02) click("follow_artist", "artist", tr.artist_id, { page: `/artist/${tr.artist_id}` });
    t += ms + int(500, 3000);
  }
  return { plays, clicks };
}

// ── Kafka ──────────────────────────────────────────────────────────────
// kafkajs 2.2.4 passes a negative delay to setTimeout on Node >= 23 (harmless, Node clamps it to 1 ms):
// hide that one warning type, keep all others.
const { emitWarning } = process;
process.emitWarning = (warning, ...args) => {
  if ((args[0]?.type ?? args[0]) !== "TimeoutNegativeWarning") emitWarning.call(process, warning, ...args);
};
const kafka = new Kafka({ clientId: "music-seed", brokers: KAFKA_BROKERS, logLevel: logLevel.WARN });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner }); // Java-compatible murmur2

// One contract message: key tenant_id:user_id, timestamps as epoch ms, or ISO-8601 strings when `iso`.
function toMessage(e, iso = false) {
  const ts = (d) => (iso ? d.toISOString() : d.getTime());
  return { key: `${e.tenant_id}:${e.user_id}`, value: JSON.stringify({ ...e, client_time: ts(e.client_time), received_at: ts(e.received_at) }) };
}

async function publish(topic, events) {
  for (let i = 0; i < events.length; i += 2000) {
    await producer.send({ topic, acks: -1, messages: events.slice(i, i + 2000).map((e) => toMessage(e)) });
  }
  return events.length;
}

// ── ClickHouse reset ───────────────────────────────────────────────────
async function ch(sql) {
  const res = await fetch(CLICKHOUSE_URL, { method: "POST", body: sql, headers: { Authorization: `Basic ${CLICKHOUSE_AUTH}` } });
  if (!res.ok) throw new Error(`ClickHouse: ${(await res.text()).trim()}`);
  return res.text();
}

// events_raw, every table downstream of it (typed tables, rollups: found by following materialized views),
// and the dead-letter table.
async function truncateClickHouse() {
  const tables = JSON.parse(await ch(
    "SELECT name, create_table_query, dependencies_table FROM system.tables WHERE database = 'music' FORMAT JSON")).data;
  const byName = new Map(tables.map((t) => [t.name, t]));
  const targets = new Set();
  const queue = ["events_raw", "ingest_errors"];
  while (queue.length) {
    const name = queue.shift();
    if (targets.has(name) || !byName.has(name)) continue;
    targets.add(name);
    for (const view of byName.get(name).dependencies_table) {
      // a view `TO music.x` writes into x; a view without TO stores rows itself (TRUNCATE on it clears them)
      queue.push(byName.get(view)?.create_table_query.match(/ TO music\.(\w+)/)?.[1] ?? view);
    }
  }
  for (const t of targets) await ch(`TRUNCATE TABLE music.${t}`);
  console.log(`truncated ClickHouse: ${[...targets].join(", ")}`);
}

async function waitFor(what, check, timeoutMs = 30_000) {
  for (const end = Date.now() + timeoutMs; Date.now() < end; await new Promise((r) => setTimeout(r, 500))) {
    if (await check()) return true;
  }
  console.warn(`warning: timed out waiting for ${what}`);
  return false;
}

// ── modes ──────────────────────────────────────────────────────────────
async function bulk(db) {
  // Wipe Mongo first and let the CDC deletes reach ClickHouse, so none land after the truncate.
  for (const c of ["users", "tracks"]) await db.collection(c).deleteMany({});
  await waitFor("CDC deletes to reach ClickHouse (is the Flink job running?)", async () =>
    (await ch("SELECT (SELECT count() FROM music.users FINAL WHERE is_deleted = 0) + (SELECT count() FROM music.tracks FINAL WHERE is_deleted = 0)")).trim() === "0");
  await truncateClickHouse();

  const tracks = buildTracks();
  const users = buildUsers();
  await db.collection("tracks").insertMany(tracks);
  await db.collection("users").insertMany(users);

  let plays = [], clicks = [], late = 0;
  for (const u of users) {
    const taste = tasteFor(u, tracks);
    for (let day = 60; day >= 0; day--) {
      const sessions = rand() < 0.75 ? int(1, 3) : 0;
      for (let s = 0; s < sessions; s++) {
        const start = Date.now() - day * DAY - int(0, 20) * HOUR;
        const isLate = rand() < 0.05;
        late += isLate;
        const r = session(taste, Math.min(start, Date.now() - HOUR), tracks, isLate);
        plays.push(...r.plays); clicks.push(...r.clicks);
      }
    }
  }
  const nPlays = await publish("events.plays", plays);
  const nClicks = await publish("events.clicks", clicks);
  console.log(`seeded Mongo: ${tracks.length} tracks, ${users.length} users (${TENANTS.length} tenants)`);
  console.log(`produced to Kafka: ${nPlays} plays -> events.plays, ${nClicks} clicks -> events.clicks (${late} late sessions)`);
}

async function live(db, perSec) {
  const tracks = await db.collection("tracks").find().toArray();
  const users = await db.collection("users").find().toArray();
  if (!tracks.length || !users.length) throw new Error("run a bulk seed first");
  seed = Date.now();
  const tastes = users.map((u) => tasteFor(u, tracks));
  let total = 0;
  console.log(`live: ~${perSec} events/sec to events.plays / events.clicks, Ctrl-C to stop`);

  async function tick() {
    const taste = pick(tastes);
    const isLate = rand() < 0.1; // event happened 1–72h ago, only arriving now
    // sessions run forward from `start`, so begin far enough back for most of one to fit before now
    const start = isLate ? Date.now() - int(1, 72) * HOUR : Date.now() - int(10, 45) * 60_000;
    const { plays, clicks } = session(taste, start, tracks, false);
    if (isLate) [...plays, ...clicks].forEach((e) => (e.received_at = new Date()));
    const messages = (events) => events.map((e) => toMessage(e, rand() < 0.1)); // some producers send ISO strings
    await producer.sendBatch({ acks: -1, topicMessages: [
      { topic: "events.plays", messages: messages(plays) },
      { topic: "events.clicks", messages: messages(clicks) },
    ].filter((t) => t.messages.length) });
    total += plays.length + clicks.length;
    process.stdout.write(`\rproduced ${total} events`);
  }

  const pending = new Set();
  const timer = setInterval(() => {
    const p = tick()
      .catch((e) => console.error(`\nproduce failed, session skipped: ${e.message}`))
      .finally(() => pending.delete(p));
    pending.add(p);
  }, Math.max(50, Math.floor(10_000 / perSec))); // a session is ~10 events

  // On Ctrl-C / SIGTERM finish in-flight sends, so the final count is exactly what reached Kafka.
  const stop = async () => {
    clearInterval(timer);
    await Promise.all(pending);
    console.log(`\nstopped: produced ${total} events`);
    await producer.disconnect();
    await client.close();
    process.exit(0);
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}

const client = await MongoClient.connect(MONGO_URL);
const db = client.db("music");
await producer.connect();
const liveIdx = process.argv.indexOf("--live");
if (liveIdx > -1) {
  await live(db, Number(process.argv[liveIdx + 1]) || 20);
} else {
  await bulk(db);
  await producer.disconnect();
  await client.close();
}
