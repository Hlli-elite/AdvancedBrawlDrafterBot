"""
Brawl Stars Ranked Meta Tracker
================================
Collects ranked battle data and outputs data.json for the draft website.

Tracks per (mode, map):
  - brawler_stats:   picks, wins, losses per brawler
  - matchup_stats:   brawler A's wins/losses when facing brawler B
  - team_comps:      3-brawler team composition wins/losses vs opponent comps

Key correctness guarantees:
  - Every battle counted exactly once (battle_uid deduplication)
  - wins + losses == picks always (skip if winner can't be determined)
  - result field interpreted relative to queried player's tag
"""

import asyncio
import aiohttp
import json
import time
import os
from collections import defaultdict
from itertools import combinations

# ─── CONFIG ───────────────────────────────────────────────────────────────────
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
SAVE_FILE    = "brawl_stats.json"   # internal incremental save (raw counts)
OUTPUT_FILE  = "data.json"          # what the website reads
DEPTH        = 2
CONCURRENCY  = 6
RATE_DELAY   = 0.18
RANKED_TYPES = {"soloRanked", "ranked"}
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm_tag(tag: str) -> str:
    return tag.lstrip("#").upper()

def comp_key(brawlers: list) -> str:
    """Canonical sorted key for a team composition e.g. 'CROW|EDGAR|MORTIS'."""
    return "|".join(sorted(brawlers))

def battle_uid(battle: dict) -> str:
    """Stable unique ID: battleTime + sorted player tags."""
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

def is_ranked(battle: dict) -> bool:
    return battle.get("battle", {}).get("type") in RANKED_TYPES

def make_record():
    return {"wins": 0, "losses": 0, "picks": 0}

def make_wl():
    return {"wins": 0, "losses": 0}


# ── Data structures ───────────────────────────────────────────────────────────
#
# All three main dicts are keyed by (mode, map) tuples so every stat is
# automatically split by game context. "overall" key also maintained.
#
# brawler_stats[(mode,map)][brawler_name] = {picks, wins, losses}
# matchup_stats[(mode,map)][brawler_a][brawler_b] = {wins, losses}
#   → a's record when facing b on the opposing team
# team_comp_stats[(mode,map)][comp_key] = {wins, losses, picks}
#   → comp_key = "BRAWLER_A|BRAWLER_B|BRAWLER_C" (sorted)

def make_brawler_stats():
    return defaultdict(make_record)

def make_matchup_stats():
    return defaultdict(lambda: defaultdict(make_wl))

def make_comp_stats():
    return defaultdict(make_record)


# ── Core processing ───────────────────────────────────────────────────────────

def process_battle(battle: dict, queried_tag: str,
                   brawler_stats: dict,
                   matchup_stats: dict,
                   team_comp_stats: dict,
                   seen_ids: set) -> set:
    """
    Process one battle. Returns set of all player tags found.

    Skips if:
      - Not ranked
      - Already seen (dedup)
      - Not exactly 2 teams
      - Can't determine winner (queried player not found in either team)
    """
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
    result = b.get("result")   # "victory" | "defeat" | None

    # Each team must have at least 1 brawler recorded
    if not all(len(t) > 0 for t in teams):
        seen_ids.add(uid)
        return set()

    # ── Find winning team index ────────────────────────────────────────────
    # result is from the queried player's perspective.
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

    if winning_idx is None:
        # Can't determine winner — skip entirely so picks == wins + losses
        seen_ids.add(uid)
        return set()

    seen_ids.add(uid)

    # ── Extract context ────────────────────────────────────────────────────
    mode = b.get("mode", "unknown")
    map_ = battle.get("event", {}).get("map", "unknown")

    keys = [
        (mode, map_),    # specific: this mode on this map
        (mode, "all"),   # this mode across all maps
        ("all", "all"),  # overall across everything
    ]

    # ── Collect brawler names and player tags ──────────────────────────────
    team_brawlers = [[], []]
    all_tags = set()
    for ti, team in enumerate(teams):
        for p in team:
            all_tags.add(p["tag"])
            name = p.get("brawler", {}).get("name")
            if name:
                team_brawlers[ti].append(name)

    winning  = team_brawlers[winning_idx]
    losing   = team_brawlers[1 - winning_idx]

    # Skip if brawler names are missing
    if not winning or not losing:
        return all_tags

    # ── Per-brawler stats ──────────────────────────────────────────────────
    for key in keys:
        bs = brawler_stats[key]
        for name in winning:
            bs[name]["picks"] += 1
            bs[name]["wins"]  += 1
        for name in losing:
            bs[name]["picks"]  += 1
            bs[name]["losses"] += 1

    # ── Matchup stats ──────────────────────────────────────────────────────
    # Every winning brawler gets a win vs every losing brawler, and vice versa.
    for key in keys:
        ms = matchup_stats[key]
        for w in winning:
            for l in losing:
                ms[w][l]["wins"]   += 1
                ms[l][w]["losses"] += 1

    # ── Team composition stats ─────────────────────────────────────────────
    # Only track full 3-brawler comps to keep data clean.
    if len(winning) == 3 and len(losing) == 3:
        win_comp  = comp_key(winning)
        lose_comp = comp_key(losing)
        for key in keys:
            cs = team_comp_stats[key]
            cs[win_comp]["picks"]  += 1
            cs[win_comp]["wins"]   += 1
            cs[lose_comp]["picks"] += 1
            cs[lose_comp]["losses"] += 1

    return all_tags


def process_battles(battles: list, queried_tag: str,
                    brawler_stats, matchup_stats,
                    team_comp_stats, seen_ids) -> set:
    all_tags = set()
    for battle in battles:
        tags = process_battle(
            battle, queried_tag,
            brawler_stats, matchup_stats, team_comp_stats, seen_ids
        )
        all_tags.update(tags)
    return all_tags


# ── API fetching ──────────────────────────────────────────────────────────────

async def fetch_battlelog(session, semaphore, tag: str) -> list:
    encoded = tag.replace("#", "%23")
    url = f"{BASE_URL}/players/{encoded}/battlelog"
    async with semaphore:
        for attempt in range(4):
            try:
                async with session.get(
                    url, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    await asyncio.sleep(RATE_DELAY)
                    if r.status == 200:
                        return (await r.json()).get("items", [])
                    elif r.status == 429:
                        wait = 10 * (attempt + 1)
                        print(f"  ⚠️  429 — sleeping {wait}s")
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


async def fetch_leaderboard_tags(n: int = 500) -> list:
    endpoints = [
        "/rankings/global/players",
        "/rankings/US/players",
        "/rankings/KR/players",
        "/rankings/BR/players",
        "/rankings/DE/players",
        "/rankings/JP/players",
        "/rankings/GB/players",
    ]
    tags, seen = [], set()
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for ep in endpoints:
            if len(tags) >= n:
                break
            try:
                async with session.get(
                    f"{BASE_URL}{ep}?limit=200", headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status == 200:
                        batch = [p["tag"] for p in (await r.json()).get("items", [])]
                        new = [t for t in batch if t not in seen]
                        tags.extend(new); seen.update(new)
                        print(f"  {ep} → {len(new)} new tags")
                    else:
                        print(f"  {ep} → HTTP {r.status}")
            except Exception as e:
                print(f"  {ep} → {e}")
            await asyncio.sleep(0.5)
    return tags[:n]


# ── Save / Load (internal format) ─────────────────────────────────────────────

def _ser_brawler_stats(brawler_stats):
    """Convert {(mode,map): {brawler: record}} to JSON-safe dict."""
    out = {}
    for (mode, map_), brawlers in brawler_stats.items():
        k = f"{mode}|{map_}"
        out[k] = {b: dict(r) for b, r in brawlers.items()}
    return out

def _deser_brawler_stats(d):
    out = defaultdict(make_brawler_stats)
    for k, brawlers in d.items():
        mode, map_ = k.split("|", 1)
        for b, r in brawlers.items():
            out[(mode, map_)][b] = dict(r)
    return out

def _ser_matchup_stats(matchup_stats):
    out = {}
    for (mode, map_), ms in matchup_stats.items():
        k = f"{mode}|{map_}"
        out[k] = {a: {b: dict(r) for b, r in vs.items()} for a, vs in ms.items()}
    return out

def _deser_matchup_stats(d):
    out = defaultdict(make_matchup_stats)
    for k, ms in d.items():
        mode, map_ = k.split("|", 1)
        for a, vs in ms.items():
            for b, r in vs.items():
                out[(mode, map_)][a][b] = dict(r)
    return out

def _ser_comp_stats(team_comp_stats):
    out = {}
    for (mode, map_), cs in team_comp_stats.items():
        k = f"{mode}|{map_}"
        out[k] = {comp: dict(r) for comp, r in cs.items()}
    return out

def _deser_comp_stats(d):
    out = defaultdict(make_comp_stats)
    for k, cs in d.items():
        mode, map_ = k.split("|", 1)
        for comp, r in cs.items():
            out[(mode, map_)][comp] = dict(r)
    return out

def load_save() -> dict:
    if os.path.exists(SAVE_FILE):
        with open(SAVE_FILE) as f:
            data = json.load(f)
        print(f"📂 Loaded: {len(data.get('seen_battle_ids',[]))} battles, "
              f"{len(data.get('processed_tags',[]))} tags")
        return data
    return {
        "brawler_stats":   {},
        "matchup_stats":   {},
        "team_comp_stats": {},
        "processed_tags":  [],
        "seen_battle_ids": [],
    }

def save_data(brawler_stats, matchup_stats, team_comp_stats,
              processed_tags, seen_ids):
    with open(SAVE_FILE, "w") as f:
        json.dump({
            "brawler_stats":   _ser_brawler_stats(brawler_stats),
            "matchup_stats":   _ser_matchup_stats(matchup_stats),
            "team_comp_stats": _ser_comp_stats(team_comp_stats),
            "processed_tags":  list(processed_tags),
            "seen_battle_ids": list(seen_ids),
        }, f, separators=(",", ":"))
    print(f"  💾 {len(seen_ids)} battles | {len(processed_tags)} tags")


# ── Output: data.json for the website ─────────────────────────────────────────

def export_data_json(brawler_stats, matchup_stats, team_comp_stats,
                     seen_ids, processed_tags):
    """
    Converts raw counts into the final data.json the website reads.

    Structure:
    {
      "meta": { "battles": N, "players": N, "updated": "ISO timestamp" },
      "modes": ["brawlBall", "gemGrab", ...],
      "maps":  { "brawlBall": ["Sneaky Fields", ...], ... },
      "brawler_stats": {
        "brawlBall|Sneaky Fields": {
          "EDGAR": { "picks": 100, "wins": 52, "losses": 48,
                     "pick_rate": 0.054, "win_rate": 0.52 }
        },
        ...
      },
      "matchup_stats": {
        "brawlBall|Sneaky Fields": {
          "EDGAR": { "MORTIS": { "wins": 20, "losses": 15, "win_rate": 0.571 } }
        }
      },
      "team_comp_stats": {
        "brawlBall|Sneaky Fields": {
          "CROW|EDGAR|MORTIS": { "picks": 10, "wins": 6, "losses": 4,
                                  "win_rate": 0.6 }
        }
      }
    }
    """
    import datetime

    modes_set = set()
    maps_by_mode = defaultdict(set)
    for (mode, map_) in brawler_stats.keys():
        if mode != "all":
            modes_set.add(mode)
        if map_ != "all" and mode != "all":
            maps_by_mode[mode].add(map_)

    # ── brawler_stats output ───────────────────────────────────────────────
    bs_out = {}
    for (mode, map_), brawlers in brawler_stats.items():
        # Total picks for this context (for pick_rate calculation)
        total = sum(v["picks"] for v in brawlers.values())
        key = f"{mode}|{map_}"
        bs_out[key] = {}
        for name, r in brawlers.items():
            dec = r["wins"] + r["losses"]
            bs_out[key][name] = {
                "picks":     r["picks"],
                "wins":      r["wins"],
                "losses":    r["losses"],
                "pick_rate": round(r["picks"] / total, 5) if total else 0,
                "win_rate":  round(r["wins"] / dec, 5) if dec else 0,
            }

    # ── matchup_stats output ───────────────────────────────────────────────
    ms_out = {}
    for (mode, map_), ms in matchup_stats.items():
        key = f"{mode}|{map_}"
        ms_out[key] = {}
        for a, vs in ms.items():
            ms_out[key][a] = {}
            for b, r in vs.items():
                dec = r["wins"] + r["losses"]
                ms_out[key][a][b] = {
                    "wins":     r["wins"],
                    "losses":   r["losses"],
                    "win_rate": round(r["wins"] / dec, 5) if dec else 0,
                }

    # ── team_comp_stats output ─────────────────────────────────────────────
    # Only include comps with at least 3 appearances to reduce noise
    cs_out = {}
    for (mode, map_), cs in team_comp_stats.items():
        key = f"{mode}|{map_}"
        cs_out[key] = {}
        for comp, r in cs.items():
            if r["picks"] < 3:
                continue
            dec = r["wins"] + r["losses"]
            cs_out[key][comp] = {
                "picks":    r["picks"],
                "wins":     r["wins"],
                "losses":   r["losses"],
                "win_rate": round(r["wins"] / dec, 5) if dec else 0,
            }

    out = {
        "meta": {
            "battles":  len(seen_ids),
            "players":  len(processed_tags),
            "updated":  datetime.datetime.utcnow().isoformat() + "Z",
        },
        "modes": sorted(modes_set),
        "maps":  {m: sorted(maps) for m, maps in maps_by_mode.items()},
        "brawler_stats":   bs_out,
        "matchup_stats":   ms_out,
        "team_comp_stats": cs_out,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(out, f, separators=(",", ":"))

    total_battles = len(seen_ids)
    total_brawlers = len(brawler_stats.get(("all", "all"), {}))
    print(f"  📊 data.json written — {total_battles} battles, "
          f"{total_brawlers} brawlers, "
          f"{len(modes_set)} modes")


# ── Integrity check ───────────────────────────────────────────────────────────

def verify_integrity(brawler_stats):
    errors = 0
    for (mode, map_), brawlers in brawler_stats.items():
        if mode != "all" or map_ != "all":
            continue   # only check overall to avoid spam
        for name, s in brawlers.items():
            if s["wins"] + s["losses"] != s["picks"]:
                diff = (s["wins"] + s["losses"]) - s["picks"]
                print(f"  ⚠️  {name}: picks={s['picks']} "
                      f"w={s['wins']} l={s['losses']} diff={diff}")
                errors += 1
    if errors == 0:
        print("  ✅ Integrity OK: wins + losses == picks for all brawlers")
    else:
        print(f"  ❌ {errors} integrity errors found")


# ── Async batch runner ────────────────────────────────────────────────────────

async def fetch_and_process_all(tags, brawler_stats, matchup_stats,
                                 team_comp_stats, processed_tags, seen_ids):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 4, ssl=False)
    next_frontier = set()
    done, total = 0, len(tags)
    start = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = {
            tag: asyncio.create_task(fetch_battlelog(session, semaphore, tag))
            for tag in tags
        }
        for tag, task in tasks.items():
            battles = await task
            ranked_count = sum(1 for b in battles if is_ranked(b))

            new_tags = process_battles(
                battles, tag,
                brawler_stats, matchup_stats, team_comp_stats, seen_ids
            )
            next_frontier.update(new_tags)
            processed_tags.add(tag)
            done += 1

            elapsed = time.time() - start
            rate = done / elapsed
            eta  = (total - done) / rate if rate > 0 else 0
            print(f"  [{done}/{total}] {tag}  {ranked_count}r/{len(battles)}"
                  f"  | {rate:.1f}/s  ETA {eta/60:.1f}m")

            if done % 50 == 0:
                save_data(brawler_stats, matchup_stats, team_comp_stats,
                          processed_tags, seen_ids)

    return next_frontier


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    save = load_save()

    brawler_stats   = _deser_brawler_stats(save.get("brawler_stats", {}))
    matchup_stats   = _deser_matchup_stats(save.get("matchup_stats", {}))
    team_comp_stats = _deser_comp_stats(save.get("team_comp_stats", {}))
    processed_tags  = set(save.get("processed_tags", []))
    seen_ids        = set(save.get("seen_battle_ids", []))

    print("\n🏆  Fetching leaderboard tags...")
    lb_tags = await fetch_leaderboard_tags(500)
    print(f"  {len(lb_tags)} leaderboard tags")

    all_seeds = list(dict.fromkeys(SEED_TAGS + lb_tags))
    print(f"\n🎮  Brawl Stars Ranked Meta Tracker")
    print(f"    Seeds: {len(all_seeds)} | Depth: {DEPTH} | Concurrency: {CONCURRENCY}")
    print(f"    Already done: {len(processed_tags)} tags, {len(seen_ids)} battles\n")

    current_frontier = set(all_seeds) - processed_tags
    all_discovered   = set(all_seeds) | processed_tags

    for depth in range(1, DEPTH + 1):
        if not current_frontier:
            print(f"Depth {depth}: nothing new, skipping.")
            continue

        print(f"{'='*62}")
        print(f"  DEPTH {depth}  —  {len(current_frontier)} tags")
        print(f"{'='*62}")

        t0 = time.time()
        next_tags = await fetch_and_process_all(
            sorted(current_frontier),
            brawler_stats, matchup_stats, team_comp_stats,
            processed_tags, seen_ids
        )
        print(f"\n  ✓ Depth {depth} in {(time.time()-t0)/60:.1f}min")

        save_data(brawler_stats, matchup_stats, team_comp_stats,
                  processed_tags, seen_ids)

        new_for_next = next_tags - all_discovered
        all_discovered |= next_tags
        current_frontier = new_for_next - processed_tags
        print(f"  → Depth {depth+1} frontier: {len(current_frontier)} tags\n")

    save_data(brawler_stats, matchup_stats, team_comp_stats,
              processed_tags, seen_ids)

    print("\n🔍  Integrity check...")
    verify_integrity(brawler_stats)

    print("\n📤  Exporting data.json...")
    export_data_json(brawler_stats, matchup_stats, team_comp_stats,
                     seen_ids, processed_tags)

    # Summary
    overall = brawler_stats.get(("all", "all"), {})
    total_picks = sum(s["picks"] for s in overall.values())
    print(f"\n✅  Done! {len(seen_ids)} battles | {len(processed_tags)} players\n")
    print(f"{'─'*62}")
    print(f"{'BRAWLER':<22} {'PICKS':>7} {'PICK%':>6} {'WIN%':>6}  {'W':>6} {'L':>6}")
    print(f"{'─'*62}")
    for name, s in sorted(overall.items(),
                           key=lambda x: x[1]["picks"], reverse=True)[:30]:
        pp  = s["picks"] / total_picks * 100 if total_picks else 0
        wr  = s["wins"] / s["picks"] * 100 if s["picks"] else 0
        print(f"{name:<22} {s['picks']:>7} {pp:>5.1f}% {wr:>5.1f}%  "
              f"{s['wins']:>6} {s['losses']:>6}")

    print(f"\n📊  Website data ready in {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
