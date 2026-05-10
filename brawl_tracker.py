import asyncio
import aiohttp
import json
import time
import os
import datetime
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6Ijk2ZjAxOWQzLTQ3ZjctNGI0Zi04NzFhLTIyMzkzMzRjZDQ5ZiIsImlhdCI6MTc3ODM1NjE1OSwic3ViIjoiZGV2ZWxvcGVyL2U2OWM2MTkyLThiMDctZDg2NC03NmQyLTg4MDNiNzRiYzYxZiIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiNzAuMjIuMjEyLjQ4Il0sInR5cGUiOiJjbGllbnQifV19.kNTsH9CZtiSzKmHfcw68O1jtiApzQgl53HAvd8uQiOSiomjHCyMnQlCDH88cnBqoKTrMo8wkzptrRfwCWnQTtA"

SEED_TAGS = [
    "#882282JQ0",
    "#9PL902CVL", "#9Y090YJV0", "#29JYUJPCC", "#2CJJJGUJ20",
    "#GLPJTCLYL", "#PCC9G9PQG", "#G88882GJ",  "#29V22GC8C",
    "#GRUQCJQ8V", "#J9G8L2CPG", "#2UQVQ8YYCL","#YPP28UL9",
    "#98UPCYYQQ", "#29UGLJV2G", "#8JQC9CPGJ", "#2V0P0CLRP2",
    "#2J2LCRCQCJ","#29L9220C8", "#2GYC08JC80","#RRQ2QJQ0",
    "#2RR2VG28L", "#R2LR2QLG",  "#8JP280V2Y", "#Y29P0JYYL",
    "#Q22YCP8J",  "#9QRR8GGG8", "#GCY88G0CR", "#L9C98U2UL",
    "#PYCC99UU9", "#2YGU9VVRV", "#2LGQJLYVU8","#8UC080RPL",
    "#R9VYUL9J8", "#LVQR8CLY0", "#UUJY28UJ",  "#2GGYG08GP0",
    "#2CYVV8P9UY","#8LJJ0890C", "#2J9LL2RRLC","#2Y0V8YYCL",
    "#LRGLC9VJY", "#82CPVQGY9", "#9PV02C0L",  "#RYUCU2RJJ",
    "#22CL00PG0", "#2QP8U809C8","#2JQJQC099", "#2LVGCJ2UQR",
    "#8VGQV88CV", "#9GYQGRJ80",
]

BASE_URL     = "https://api.brawlstars.com/v1"
SAVE_FILE    = "brawl_stats.json"
OUTPUT_DIR   = "data"
DEPTH        = 2
CONCURRENCY  = 6
RATE_DELAY   = 0.18
RANKED_TYPES = {"soloRanked", "ranked"}
HEADERS      = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def norm_tag(tag):
    return tag.lstrip("#").upper()

def comp_key(brawlers):
    return "|".join(sorted(brawlers))

def battle_uid(battle):
    b = battle.get("battle", {})
    tags = []
    if "teams" in b:
        for team in b["teams"]:
            for p in team:
                tags.append(norm_tag(p["tag"]))
    elif "players" in b:
        for p in b["players"]:
            tags.append(norm_tag(p["tag"]))
    return battle.get("battleTime", "") + "|" + ",".join(sorted(tags))

def is_ranked(battle):
    return battle.get("battle", {}).get("type") in RANKED_TYPES

def make_record():
    return {"wins": 0, "losses": 0, "picks": 0}

def make_wl():
    return {"wins": 0, "losses": 0}

def make_brawler_stats():
    return defaultdict(make_record)

def make_matchup_stats():
    return defaultdict(lambda: defaultdict(make_wl))

def make_comp_stats():
    return defaultdict(make_record)

# ── PROCESSING ────────────────────────────────────────────────────────────────

def process_battle(battle, queried_tag, brawler_stats, matchup_stats, team_comp_stats, seen_ids):
    if not is_ranked(battle):
        return set()

    uid = battle_uid(battle)
    if uid in seen_ids:
        return set()

    b = battle.get("battle", {})
    if "teams" not in b or len(b["teams"]) != 2:
        seen_ids.add(uid)
        return set()

    teams  = b["teams"]
    result = b.get("result")

    if not all(len(t) > 0 for t in teams):
        seen_ids.add(uid)
        return set()

    # Determine absolute winning team index from queried player perspective
    winning_idx = None
    if result in ("victory", "defeat"):
        qtag = norm_tag(queried_tag)
        for ti, team in enumerate(teams):
            for p in team:
                if norm_tag(p["tag"]) == qtag:
                    winning_idx = ti if result == "victory" else 1 - ti
                    break
            if winning_idx is not None:
                break

    # Cannot determine winner — skip to keep picks == wins + losses
    if winning_idx is None:
        seen_ids.add(uid)
        return set()

    seen_ids.add(uid)

    mode = b.get("mode", "unknown")
    map_ = battle.get("event", {}).get("map") or "unknown"

    # Track at 3 levels: specific map, mode-wide, overall
    keys = [(mode, map_), (mode, "all"), ("all", "all")]

    # Collect brawlers per team
    team_brawlers = [[], []]
    all_tags = set()
    for ti, team in enumerate(teams):
        for p in team:
            all_tags.add(p["tag"])
            name = p.get("brawler", {}).get("name")
            if name:
                team_brawlers[ti].append(name)

    winning = team_brawlers[winning_idx]
    losing  = team_brawlers[1 - winning_idx]

    if not winning or not losing:
        return all_tags

    # Per-brawler stats
    for key in keys:
        bs = brawler_stats[key]
        for name in winning:
            bs[name]["picks"] += 1
            bs[name]["wins"]  += 1
        for name in losing:
            bs[name]["picks"]  += 1
            bs[name]["losses"] += 1

    # Matchup stats: winner vs loser, both directions
    for key in keys:
        ms = matchup_stats[key]
        for w in winning:
            for l in losing:
                ms[w][l]["wins"]   += 1
                ms[l][w]["losses"] += 1

    # Team comp stats (full 3v3 only)
    if len(winning) == 3 and len(losing) == 3:
        win_comp  = comp_key(winning)
        lose_comp = comp_key(losing)
        for key in keys:
            cs = team_comp_stats[key]
            cs[win_comp]["picks"]   += 1
            cs[win_comp]["wins"]    += 1
            cs[lose_comp]["picks"]  += 1
            cs[lose_comp]["losses"] += 1

    return all_tags


def process_battles(battles, queried_tag, brawler_stats, matchup_stats, team_comp_stats, seen_ids):
    all_tags = set()
    for battle in battles:
        tags = process_battle(battle, queried_tag, brawler_stats, matchup_stats, team_comp_stats, seen_ids)
        all_tags.update(tags)
    return all_tags

# ── API ───────────────────────────────────────────────────────────────────────

async def fetch_battlelog(session, semaphore, tag):
    encoded = tag.replace("#", "%23")
    url = f"{BASE_URL}/players/{encoded}/battlelog"
    async with semaphore:
        for attempt in range(4):
            try:
                async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    await asyncio.sleep(RATE_DELAY)
                    if r.status == 200:
                        return (await r.json()).get("items", [])
                    elif r.status == 429:
                        wait = 10 * (attempt + 1)
                        print(f"  429 — sleeping {wait}s")
                        await asyncio.sleep(wait)
                    elif r.status == 404:
                        return []
                    else:
                        return []
            except asyncio.TimeoutError:
                await asyncio.sleep(3)
            except Exception as e:
                print(f"  error {tag}: {e}")
                await asyncio.sleep(3)
        return []


async def fetch_leaderboard_tags(n=500):
    endpoints = [
        "/rankings/global/players", "/rankings/US/players",
        "/rankings/KR/players",     "/rankings/BR/players",
        "/rankings/DE/players",     "/rankings/JP/players",
        "/rankings/GB/players",
    ]
    tags, seen = [], set()
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for ep in endpoints:
            if len(tags) >= n:
                break
            try:
                async with session.get(f"{BASE_URL}{ep}?limit=200", headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 200:
                        batch = [p["tag"] for p in (await r.json()).get("items", [])]
                        new = [t for t in batch if t not in seen]
                        tags.extend(new); seen.update(new)
                        print(f"  {ep} -> {len(new)} new tags")
                    else:
                        print(f"  {ep} -> HTTP {r.status}")
            except Exception as e:
                print(f"  {ep} -> {e}")
            await asyncio.sleep(0.5)
    return tags[:n]

# ── SERIALISATION ─────────────────────────────────────────────────────────────

def _ser_bs(brawler_stats):
    out = {}
    for (mode, map_), brawlers in brawler_stats.items():
        out[f"{mode}|{map_}"] = {b: dict(r) for b, r in brawlers.items()}
    return out

def _deser_bs(d):
    out = defaultdict(make_brawler_stats)
    for k, brawlers in d.items():
        mode, map_ = k.split("|", 1)
        for b, r in brawlers.items():
            out[(mode, map_)][b] = dict(r)
    return out

def _ser_ms(matchup_stats):
    out = {}
    for (mode, map_), ms in matchup_stats.items():
        out[f"{mode}|{map_}"] = {a: {b: dict(r) for b, r in vs.items()} for a, vs in ms.items()}
    return out

def _deser_ms(d):
    out = defaultdict(make_matchup_stats)
    for k, ms in d.items():
        mode, map_ = k.split("|", 1)
        for a, vs in ms.items():
            for b, r in vs.items():
                out[(mode, map_)][a][b] = dict(r)
    return out

def _ser_cs(team_comp_stats):
    out = {}
    for (mode, map_), cs in team_comp_stats.items():
        out[f"{mode}|{map_}"] = {comp: dict(r) for comp, r in cs.items()}
    return out

def _deser_cs(d):
    out = defaultdict(make_comp_stats)
    for k, cs in d.items():
        mode, map_ = k.split("|", 1)
        for comp, r in cs.items():
            out[(mode, map_)][comp] = dict(r)
    return out

# ── SAVE / LOAD ───────────────────────────────────────────────────────────────

def load_save():
    if os.path.exists(SAVE_FILE):
        with open(SAVE_FILE) as f:
            data = json.load(f)
        print(f"Loaded: {len(data.get('seen_battle_ids',[]))} battles, "
              f"{len(data.get('processed_tags',[]))} tags")
        return data
    return {"brawler_stats": {}, "matchup_stats": {}, "team_comp_stats": {},
            "processed_tags": [], "seen_battle_ids": []}


def export_data(brawler_stats, matchup_stats, seen_ids, processed_tags):
    """
    Writes lean split files into OUTPUT_DIR/:

      meta.json        -- brawler list, modes, maps, battle count
      overall.json     -- all|all stats (loaded on page start)
      mode_X.json      -- per-mode + per-map stats (lazy loaded)

    Encoding:
      - Brawler names replaced by integer IDs (see meta.json brawlers array)
      - Stats stored as arrays [picks, wins] not objects
      - Matchup entries with < 15 games omitted
      - Per-map contexts with < 300 total picks omitted
      - No pre-computed rates (client does the division)
      - Team comp data omitted (not used by website yet)
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Build brawler ID table
    all_names = set()
    for brawlers in brawler_stats.values():
        all_names.update(brawlers.keys())
    brawler_list = sorted(all_names)
    bid = {name: i for i, name in enumerate(brawler_list)}

    # Build mode/map index
    modes_set    = set()
    maps_by_mode = defaultdict(set)
    for (mode, map_) in brawler_stats.keys():
        if mode == "all":
            continue
        modes_set.add(mode)
        if map_ not in ("all", "unknown", None):
            maps_by_mode[mode].add(map_)

    def encode_ctx(mode, map_):
        b_data = brawler_stats.get((mode, map_), {})
        m_data = matchup_stats.get((mode, map_), {})
        total  = sum(v["picks"] for v in b_data.values())

        # Skip sparse per-map contexts
        if map_ not in ("all", "unknown") and total < 50:
            return None

        # Brawler stats: {id: [picks, wins]}
        bs = {}
        for name, r in b_data.items():
            i = bid.get(name)
            if i is not None:
                bs[i] = [r["picks"], r["wins"]]

        # Matchup stats: {id: {id: [wins, losses]}}  min 15 games
        ms = {}
        for a, vs in m_data.items():
            ai = bid.get(a)
            if ai is None:
                continue
            inner = {}
            for b_name, r in vs.items():
                bi = bid.get(b_name)
                if bi is None:
                    continue
                dec = r["wins"] + r["losses"]
                if dec < 5:
                    continue
                inner[bi] = [r["wins"], r["losses"]]
            if inner:
                ms[ai] = inner

        return {"bs": bs, "ms": ms}

    # meta.json
    meta = {
        "battles":  len(seen_ids),
        "players":  len(processed_tags),
        "updated":  datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "brawlers": brawler_list,
        "modes":    sorted(modes_set),
        "maps":     {m: sorted(x for x in maps if x) for m, maps in maps_by_mode.items()},
    }
    with open(f"{OUTPUT_DIR}/meta.json", "w") as f:
        json.dump(meta, f, separators=(",", ":"))

    # overall.json
    overall = encode_ctx("all", "all")
    with open(f"{OUTPUT_DIR}/overall.json", "w") as f:
        json.dump(overall, f, separators=(",", ":"))
    sz_overall = os.path.getsize(f"{OUTPUT_DIR}/overall.json") / 1024

    # mode_X.json
    total_sz = sz_overall
    for mode in sorted(modes_set):
        mode_data = {}
        ctx = encode_ctx(mode, "all")
        if ctx:
            mode_data["all"] = ctx
        for map_ in sorted(maps_by_mode.get(mode, [])):
            ctx = encode_ctx(mode, map_)
            if ctx:
                mode_data[map_] = ctx
        if not mode_data:
            continue
        fname = f"{OUTPUT_DIR}/mode_{mode}.json"
        with open(fname, "w") as f:
            json.dump(mode_data, f, separators=(",", ":"))
        sz = os.path.getsize(fname) / 1024
        total_sz += sz

    print(f"  data/ written — {total_sz:.0f}KB total "
          f"({sz_overall:.0f}KB overall, {len(modes_set)} mode files)")


def save_data(brawler_stats, matchup_stats, team_comp_stats, processed_tags, seen_ids):
    with open(SAVE_FILE, "w") as f:
        json.dump({
            "brawler_stats":   _ser_bs(brawler_stats),
            "matchup_stats":   _ser_ms(matchup_stats),
            "team_comp_stats": _ser_cs(team_comp_stats),
            "processed_tags":  list(processed_tags),
            "seen_battle_ids": list(seen_ids),
        }, f, separators=(",", ":"))
    print(f"  saved: {len(seen_ids)} battles | {len(processed_tags)} tags")
    export_data(brawler_stats, matchup_stats, seen_ids, processed_tags)

# ── INTEGRITY CHECK ───────────────────────────────────────────────────────────

def verify_integrity(brawler_stats):
    errors = 0
    for (mode, map_), brawlers in brawler_stats.items():
        if mode != "all" or map_ != "all":
            continue
        for name, s in brawlers.items():
            if s["wins"] + s["losses"] != s["picks"]:
                print(f"  INTEGRITY ERROR: {name} picks={s['picks']} w={s['wins']} l={s['losses']}")
                errors += 1
    if errors == 0:
        print("  Integrity OK: wins + losses == picks for all brawlers")

# ── ASYNC RUNNER ──────────────────────────────────────────────────────────────

async def fetch_and_process_all(tags, brawler_stats, matchup_stats, team_comp_stats, processed_tags, seen_ids):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 4, ssl=False)
    next_frontier = set()
    done, total, start = 0, len(tags), time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = {tag: asyncio.create_task(fetch_battlelog(session, semaphore, tag)) for tag in tags}
        for tag, task in tasks.items():
            battles = await task
            ranked  = sum(1 for b in battles if is_ranked(b))
            new_tags = process_battles(battles, tag, brawler_stats, matchup_stats, team_comp_stats, seen_ids)
            next_frontier.update(new_tags)
            processed_tags.add(tag)
            done += 1
            elapsed = time.time() - start
            rate = done / elapsed
            eta  = (total - done) / rate if rate > 0 else 0
            print(f"  [{done}/{total}] {tag}  {ranked}r/{len(battles)}  | {rate:.1f}/s  ETA {eta/60:.1f}m")
            if done % 50 == 0:
                save_data(brawler_stats, matchup_stats, team_comp_stats, processed_tags, seen_ids)

    return next_frontier

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    save = load_save()

    brawler_stats   = _deser_bs(save.get("brawler_stats", {}))
    matchup_stats   = _deser_ms(save.get("matchup_stats", {}))
    team_comp_stats = _deser_cs(save.get("team_comp_stats", {}))
    processed_tags  = set(save.get("processed_tags", []))
    seen_ids        = set(save.get("seen_battle_ids", []))

    print("\nFetching leaderboard tags...")
    lb_tags = await fetch_leaderboard_tags(500)
    print(f"  {len(lb_tags)} leaderboard tags")

    all_seeds = list(dict.fromkeys(SEED_TAGS + lb_tags))
    print(f"\nBrawl Stars Ranked Meta Tracker")
    print(f"  Seeds: {len(all_seeds)} | Depth: {DEPTH} | Concurrency: {CONCURRENCY}")
    print(f"  Already done: {len(processed_tags)} tags, {len(seen_ids)} battles\n")

    current_frontier = set(all_seeds) - processed_tags
    all_discovered   = set(all_seeds) | processed_tags

    for depth in range(1, DEPTH + 1):
        if not current_frontier:
            print(f"Depth {depth}: nothing new, skipping.")
            continue
        print(f"{'='*62}")
        print(f"  DEPTH {depth}  --  {len(current_frontier)} tags")
        print(f"{'='*62}")
        t0 = time.time()
        next_tags = await fetch_and_process_all(
            sorted(current_frontier),
            brawler_stats, matchup_stats, team_comp_stats, processed_tags, seen_ids
        )
        print(f"\n  Depth {depth} done in {(time.time()-t0)/60:.1f}min")
        save_data(brawler_stats, matchup_stats, team_comp_stats, processed_tags, seen_ids)
        new_for_next = next_tags - all_discovered
        all_discovered |= next_tags
        current_frontier = new_for_next - processed_tags
        print(f"  -> Depth {depth+1} frontier: {len(current_frontier)} tags\n")

    save_data(brawler_stats, matchup_stats, team_comp_stats, processed_tags, seen_ids)

    print("\nIntegrity check...")
    verify_integrity(brawler_stats)

    overall = brawler_stats.get(("all", "all"), {})
    total_picks = sum(s["picks"] for s in overall.values())
    print(f"\nDone! {len(seen_ids)} battles | {len(processed_tags)} players\n")
    print(f"{'BRAWLER':<22} {'PICKS':>7} {'PICK%':>6} {'WIN%':>6}  {'W':>6} {'L':>6}")
    print(f"{'─'*62}")
    for name, s in sorted(overall.items(), key=lambda x: x[1]["picks"], reverse=True)[:30]:
        pp = s["picks"] / total_picks * 100 if total_picks else 0
        wr = s["wins"]  / s["picks"]  * 100 if s["picks"]  else 0
        print(f"{name:<22} {s['picks']:>7} {pp:>5.1f}% {wr:>5.1f}%  {s['wins']:>6} {s['losses']:>6}")


if __name__ == "__main__":
    asyncio.run(main())
