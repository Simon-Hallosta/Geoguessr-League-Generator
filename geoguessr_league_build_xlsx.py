from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


# ============================================================
# Regex / constants
# ============================================================

TOKEN_RE = re.compile(r"/challenge/([A-Za-z0-9_-]+)")
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
EPOCH_RE = re.compile(r"^\d{10,13}$")

DEFAULT_TZ = "Europe/Stockholm"
DEFAULT_INFORMATION_CONFIG_NAME = "information_config.json"

# Fixed weekly map slots (index -> category)
MAP_SLOT_KEY_BY_INDEX = {
    1: "moving_1",
    2: "moving_2",
    3: "no_move_1",
    4: "no_move_2",
    5: "nmpz_1",
    6: "nmpz_2",
}

SLOT_KEYS_ORDER = ["moving_1", "moving_2", "no_move_1", "no_move_2", "nmpz_1", "nmpz_2"]

SLOT_LABEL_BY_KEY = {
    "moving_1": "Moving 1",
    "moving_2": "Moving 2",
    "no_move_1": "No move 1",
    "no_move_2": "No move 2",
    "nmpz_1": "NMPZ 1",
    "nmpz_2": "NMPZ 2",
}

SUBLEAGUE_SLOT_KEYS = {
    "Moving": ["moving_1", "moving_2"],
    "No move": ["no_move_1", "no_move_2"],
    "NMPZ": ["nmpz_1", "nmpz_2"],
    "Sverige": ["moving_1", "no_move_2"],
}

DEFAULT_INFORMATION_ROWS = [
    "Ingen anmälan krävs - det är bara att spela veckans challenges!",
    "För att öppna länken: klicka på den understrukna raden i varje kolumn. Exempelvis \"🔗 Moving 1 | Moving - 3 min\".",
    "Preliminära poäng utdelas under veckan. De kan gå upp beroende på hur många spelare som placerar sig under dig.",
    "Poäng delas ut enligt pro league-systemet: sista plats får 1 poäng, näst sista 2 poäng, tredje sista 3 poäng osv.",
    "Tiebreaker vid samma poäng är tid. Om två spelare delar plats får båda poäng för den delade placeringen.",
    "Varje vecka avslutas onsdag kl 20.00. Om poängen inte är ihopräknade då kan du spela tills poängen är ihopräknade.",
    "Vid frågor, skriv i #ligan.",
]

# Excel styling
DARK = PatternFill("solid", fgColor="2B2B2B")
MID = PatternFill("solid", fgColor="3A3A3A")
ROW_A = PatternFill("solid", fgColor="D9EAD3")
ROW_B = PatternFill("solid", fgColor="C9E2BC")
WHITE = PatternFill("solid", fgColor="FFFFFF")

FONT_HDR = Font(color="FFFFFF", bold=True)
FONT_HDR_BIG = Font(color="FFFFFF", bold=True, size=16)
FONT_HDR_MED = Font(color="FFFFFF", bold=True, size=12)
FONT_BODY = Font(color="000000", bold=False)

THIN = Side(style="thin", color="1F1F1F")
BORDER_THIN = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)


# ============================================================
# Data classes
# ============================================================

@dataclass
class WeekSpec:
    label: str
    urls_path: Path
    deadline: Optional[str] = None  # user string, parsed later


@dataclass
class Entry:
    week_label: str
    map_index: int
    map_url: str
    map_token: str
    map_name: str
    rule_text: str

    player: str
    total_pts: int
    total_time: int  # tie-breaker within map
    played_at_epoch: Optional[int]  # optional, for deadline filtering


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser()

    # repeatable week spec:
    #   --week "Vecka 1|urls_week1.txt|2026-02-18 20:00"
    # deadline is optional
    ap.add_argument(
        "--week",
        action="append",
        default=[],
        help='Repeatable. Format: "LABEL|URLS_FILE|DEADLINE". Deadline optional. Example: --week "Vecka 2|urls_week2.txt|2026-02-25 20:00"',
    )

    ap.add_argument("--out-base", default="Liga_overview", help="Base filename without extension.")
    ap.add_argument(
        "--information-config",
        default="",
        help=f"Path to JSON config for Information sheet. Default behavior uses ./{DEFAULT_INFORMATION_CONFIG_NAME} when present.",
    )
    ap.add_argument("--tz", default=DEFAULT_TZ, help="Timezone for deadlines, e.g. Europe/Stockholm")
    ap.add_argument("--ncfa", default="", help="Override GEOGUESSR_NCFA env var")
    ap.add_argument("--timeout", type=float, default=30.0)

    # scoring / tie handling (only for exact ties on points+time; rare)
    ap.add_argument("--tie", default="average", choices=["average", "dense", "min", "max"])

    # highscores pagination
    ap.add_argument("--page-size", type=int, default=200)
    ap.add_argument("--max-players", type=int, default=5000)

    # played-at filtering
    ap.add_argument("--fetch-played-at", action="store_true", help="Try to fetch played timestamp per entry via extra API calls.")
    ap.add_argument("--keep-missing-time", action="store_true", help="When filtering, keep entries where played_at cannot be determined (default: exclude).")

    # debug
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--dump-json", action="store_true", help="Dump first highscores payload per map into ./debug_json/")

    return ap.parse_args(argv)


# ============================================================
# Core helpers
# ============================================================

def debug_print(debug: bool, *args):
    if debug:
        print(*args)


def extract_token(url: str) -> str:
    m = TOKEN_RE.search(url)
    if m:
        return m.group(1)
    p = urlparse(url).path.rstrip("/").split("/")
    if not p or not p[-1]:
        raise ValueError(f"Could not extract token from URL: {url}")
    return p[-1]


def load_urls(path: Path) -> List[str]:
    txt = path.read_text(encoding="utf-8")
    out: List[str] = []
    for line in txt.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def make_session(ncfa: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.geoguessr.com/",
            "Origin": "https://www.geoguessr.com",
        }
    )
    s.cookies.set("_ncfa", ncfa, domain=".geoguessr.com", path="/")
    return s


def http_get_json(session: requests.Session, url: str, timeout: float, debug: bool) -> Any:
    r = session.get(url, timeout=timeout)
    debug_print(debug, f"[HTTP] GET {url} -> {r.status_code} len={len(r.text)}")
    if r.status_code >= 400:
        snippet = r.text[:300].replace("\n", "\\n")
        raise RuntimeError(f"HTTP {r.status_code} for {url}: {snippet}")
    return r.json()


def _parse_int_maybe(x: Any) -> Optional[int]:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if s.isdigit():
            return int(s)
    return None


def map_slot_key(map_index: Any) -> str:
    idx = _parse_int_maybe(map_index)
    if idx is None:
        return "unknown"
    return MAP_SLOT_KEY_BY_INDEX.get(idx, f"map_{idx}")


def map_slot_label(slot_key: str) -> str:
    return SLOT_LABEL_BY_KEY.get(slot_key, slot_key.replace("_", " ").title())


def default_information_rows() -> List[str]:
    return list(DEFAULT_INFORMATION_ROWS)


def _normalize_information_rows(rows: Any) -> List[str]:
    if not isinstance(rows, list):
        return default_information_rows()
    out: List[str] = []
    for row in rows:
        if not isinstance(row, str):
            continue
        txt = row.strip()
        if txt:
            out.append(txt)
    return out or default_information_rows()


def load_information_rows(config_path: Optional[Path], debug: bool = False) -> List[str]:
    if config_path is None or not config_path.exists():
        return default_information_rows()
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        debug_print(debug, f"[INFO-CONFIG] failed to parse {config_path}: {e}")
        return default_information_rows()

    if isinstance(payload, dict):
        return _normalize_information_rows(payload.get("information_rows"))
    if isinstance(payload, list):
        return _normalize_information_rows(payload)
    return default_information_rows()


# ============================================================
# Highscores parsing (schema based on your payload)
# ============================================================

def extract_items(payload: Any) -> List[dict]:
    if isinstance(payload, dict):
        v = payload.get("items")
        if isinstance(v, list) and (not v or isinstance(v[0], dict)):
            return v  # type: ignore
    if isinstance(payload, list) and (not payload or isinstance(payload[0], dict)):
        return payload  # type: ignore
    raise RuntimeError("Could not locate highscores items list.")


def rule_text_from_game(game: dict) -> str:
    forbid_moving = game.get("forbidMoving")
    forbid_zooming = game.get("forbidZooming")
    forbid_rotating = game.get("forbidRotating")
    time_limit = game.get("timeLimit")

    parts: List[str] = []

    if forbid_moving is True and forbid_zooming is True and forbid_rotating is True:
        parts.append("NMPZ")
    elif forbid_moving is True:
        # could be NM or NMP depending on other flags
        if forbid_rotating is True and forbid_zooming is False:
            parts.append("NMP")
        elif forbid_rotating is True and forbid_zooming is True:
            parts.append("NMPZ")
        else:
            parts.append("NM")
    else:
        parts.append("Moving")

    if isinstance(time_limit, int) and time_limit > 0:
        if time_limit % 60 == 0:
            parts.append(f"{time_limit//60} min")
        else:
            parts.append(f"{time_limit}s")

    return " - ".join(parts)


def player_name_from_item(item: dict) -> str:
    try:
        nick = item["game"]["player"].get("nick")
        if isinstance(nick, str) and nick.strip():
            return nick.strip()
    except Exception:
        pass
    return "UNKNOWN"


def total_points_from_item(item: dict) -> int:
    # prefer totalScore.amount (string)
    try:
        amt = item["game"]["player"]["totalScore"].get("amount")
        v = _parse_int_maybe(amt)
        if v is not None:
            return v
    except Exception:
        pass

    # fallback numeric variants
    try:
        v = _parse_int_maybe(item["game"]["player"].get("totalScoreInPoints"))
        if v is not None:
            return v
    except Exception:
        pass

    return 0


def total_time_from_item(item: dict) -> int:
    # tie-break: lower is better
    try:
        v = _parse_int_maybe(item["game"]["player"].get("totalTime"))
        if v is not None:
            return v
    except Exception:
        pass
    return 10**12


def map_name_from_item(item: dict) -> str:
    try:
        name = item["game"].get("mapName")
        if isinstance(name, str) and name.strip():
            return name.strip()
    except Exception:
        pass
    return ""


# ============================================================
# Ranking + Borda with time tie-break
# ============================================================

def compute_rank_and_borda_with_time(
    pts_by_player: Dict[str, int],
    time_by_player: Dict[str, int],
    tie_mode: str,
) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Ranking:
      - higher points is better
      - if equal points: lower totalTime is better
      - if equal points AND time: tie_mode decides rank for exact ties

    Returns:
      rank_best: 1.0 = best
      borda: N = best ... 1 = worst (fractional if average tie)
    """
    if not pts_by_player:
        return {}, {}

    players = list(pts_by_player.keys())
    players_sorted = sorted(players, key=lambda p: (-pts_by_player[p], time_by_player.get(p, 10**12)))

    groups: Dict[Tuple[int, int], List[str]] = {}
    for p in players_sorted:
        key = (pts_by_player[p], time_by_player.get(p, 10**12))
        groups.setdefault(key, []).append(p)

    keys_sorted = sorted(groups.keys(), key=lambda k: (-k[0], k[1]))

    rank_best: Dict[str, float] = {}
    current_rank = 1

    for key in keys_sorted:
        names = groups[key]
        k = len(names)
        occupied = list(range(current_rank, current_rank + k))  # +k (NOT +k+1)

        if k == 1:
            rank_best[names[0]] = float(current_rank)
            current_rank += 1
            continue

        if tie_mode == "average":
            val = sum(occupied) / len(occupied)
        elif tie_mode == "dense":
            val = float(current_rank)
        elif tie_mode == "min":
            val = float(min(occupied))
        elif tie_mode == "max":
            val = float(max(occupied))
        else:
            raise ValueError(tie_mode)

        for n in names:
            rank_best[n] = float(val)

        current_rank = current_rank + (1 if tie_mode == "dense" else k)

    N = len(pts_by_player)
    borda = {p: float(N - rank_best[p] + 1) for p in rank_best}
    return rank_best, borda


# ============================================================
# Played-at extraction (best-effort)
# ============================================================

def _iter_all_dicts(obj: Any) -> Iterable[dict]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_all_dicts(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _iter_all_dicts(it)


def _try_parse_epoch(val: Any) -> Optional[int]:
    # epoch seconds or ms, or ISO string
    if val is None:
        return None

    if isinstance(val, (int, float)) and not isinstance(val, bool):
        x = int(val)
        # if ms
        if x > 10_000_000_000:
            return x // 1000
        # seconds
        if x > 1_000_000_000:
            return x
        return None

    if isinstance(val, str):
        s = val.strip()
        if EPOCH_RE.match(s):
            x = int(s)
            if x > 10_000_000_000:
                return x // 1000
            if x > 1_000_000_000:
                return x
        if ISO_RE.match(s):
            # very light ISO parsing without extra deps: handle "YYYY-MM-DDTHH:MM:SSZ" or with offset
            try:
                # Python can parse many ISO formats via fromisoformat, but "Z" needs replacement
                ss = s.replace("Z", "+00:00")
                dt = pd.to_datetime(ss, utc=True)
                if pd.isna(dt):
                    return None
                return int(dt.timestamp())
            except Exception:
                return None
    return None


def extract_played_at_epoch(game_payload: Any) -> Optional[int]:
    """
    Best-effort scan for typical timestamp keys:
      createdAt, created, updatedAt, finishedAt, endedAt, startTime, endTime, completedAt, etc.
    Prioritizes end/finish timestamps over generic updated/timestamp fields.
    """
    def key_priority(lk: str) -> Optional[int]:
        if lk in {"finishedat", "endedat", "endtime", "completedat", "completed", "finished", "ended"}:
            return 0
        if lk in {"createdat", "created", "startedat", "starttime", "started"}:
            return 1
        if any(x in lk for x in ["finished", "ended", "completed", "end"]):
            return 2
        if any(x in lk for x in ["created", "started", "start"]):
            return 3
        if lk in {"timestamp", "time"}:
            return 4
        if lk in {"updatedat", "updated"} or "updated" in lk:
            return 5
        return None

    best: Optional[Tuple[int, int]] = None  # (priority, epoch)

    for d in _iter_all_dicts(game_payload):
        for k, v in d.items():
            lk = str(k).lower()
            prio = key_priority(lk)
            if prio is None:
                continue

            ep = _try_parse_epoch(v)
            if ep is None:
                continue

            # Prefer lower priority class; within same class pick latest epoch.
            if best is None or prio < best[0] or (prio == best[0] and ep > best[1]):
                best = (prio, ep)

    return best[1] if best is not None else None


def fetch_game_details_for_played_at(
    session: requests.Session,
    game_token: str,
    timeout: float,
    debug: bool,
) -> Optional[int]:
    """
    Try a few endpoints. GeoGuessr may change schemas.
    We keep this tolerant: if an endpoint fails, try next.
    """
    endpoints = [
        f"https://www.geoguessr.com/api/v3/games/{game_token}",
        f"https://www.geoguessr.com/api/v3/results/{game_token}",
    ]
    for url in endpoints:
        try:
            payload = http_get_json(session, url, timeout=timeout, debug=debug)
            ep = extract_played_at_epoch(payload)
            if ep is not None:
                return ep
        except Exception as e:
            debug_print(debug, f"[played_at] endpoint failed: {url} -> {e}")
            continue
    return None


# ============================================================
# Highscores fetch
# ============================================================

def fetch_highscores_items(
    session: requests.Session,
    challenge_token: str,
    timeout: float,
    debug: bool,
    page_size: int,
    max_players: int,
) -> List[dict]:
    all_items: List[dict] = []
    offset = 0
    while True:
        url = (
            f"https://www.geoguessr.com/api/v3/results/highscores/{challenge_token}"
            f"?friends=false&limit={page_size}&offset={offset}"
        )
        payload = http_get_json(session, url, timeout=timeout, debug=debug)
        items = extract_items(payload)
        if not items:
            break
        all_items.extend(items)
        if len(items) < page_size:
            break
        offset += page_size
        if offset >= max_players:
            break
    return all_items


# ============================================================
# Deadline parsing
# ============================================================

def parse_deadline_epoch(deadline_str: str, tz_name: str) -> int:
    """
    Accepts e.g. "2026-02-25 20:00" or ISO.
    Interprets as tz_name local time.
    """
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo not available. Use Python 3.9+ or install backports.zoneinfo.")

    try:
        tz = ZoneInfo(tz_name)
    except Exception as e:
        raise ValueError(
            f'Unknown timezone "{tz_name}". Example valid value: "Europe/Stockholm".'
        ) from e

    # Use pandas only for string parsing. Do timezone attachment/conversion via stdlib
    # to avoid pandas+zoneinfo incompatibilities in some environments.
    dt = pd.to_datetime(deadline_str)
    if pd.isna(dt):
        raise ValueError(f"Could not parse deadline: {deadline_str}")

    py_dt = dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt
    if getattr(py_dt, "tzinfo", None) is None:
        py_dt = py_dt.replace(tzinfo=tz)
    else:
        py_dt = py_dt.astimezone(tz)
    # convert to epoch seconds
    return int(py_dt.timestamp())


# ============================================================
# Build entries per week
# ============================================================

def build_week_entries(
    session: requests.Session,
    week: WeekSpec,
    tz_name: str,
    timeout: float,
    debug: bool,
    dump_json: bool,
    page_size: int,
    max_players: int,
    fetch_played_at: bool,
) -> Tuple[List[Entry], List[dict], bool, int]:
    """
    Returns (entries, map_meta_rows, has_any_played_at, failed_maps_count).
    """
    urls = load_urls(week.urls_path)
    if not urls:
        raise RuntimeError(f"{week.urls_path} is empty")

    out_entries: List[Entry] = []
    map_meta_rows: List[dict] = []
    has_any_played_at = False
    failed_maps_count = 0

    debug_dir = week.urls_path.parent / "debug_json"
    if dump_json:
        debug_dir.mkdir(parents=True, exist_ok=True)

    played_at_cache: Dict[str, Optional[int]] = {}

    for map_idx, url in enumerate(urls, start=1):
        token = extract_token(url)
        map_name = f"Map {map_idx}"
        rule_text = ""
        try:
            items = fetch_highscores_items(
                session=session,
                challenge_token=token,
                timeout=timeout,
                debug=debug,
                page_size=page_size,
                max_players=max_players,
            )
        except Exception as e:
            failed_maps_count += 1
            print(f"[WARN] {week.label} map {map_idx}: kunde inte hämta resultat för {url} ({e})")
            map_meta_rows.append(
                {
                    "week": week.label,
                    "map_index": map_idx,
                    "map_url": url,
                    "map_name": map_name,
                    "rule_text": rule_text,
                }
            )
            continue

        if dump_json:
            p = debug_dir / f"{week.label.replace(' ', '_')}_map{map_idx}_highscores.json"
            p.write_text(json.dumps({"token": token, "items": items}, ensure_ascii=False, indent=2), encoding="utf-8")

        # map info from first item (stable in your payload)
        if items:
            try:
                game0 = items[0]["game"]
                map_name = str(game0.get("mapName") or "").strip()
                rule_text = rule_text_from_game(game0)
            except Exception:
                rule_text = ""

        map_meta_rows.append(
            {
                "week": week.label,
                "map_index": map_idx,
                "map_url": url,
                "map_name": map_name or f"Map {map_idx}",
                "rule_text": rule_text or "",
            }
        )

        for it in items:
            if not isinstance(it, dict) or "game" not in it:
                continue
            name = player_name_from_item(it)
            if name == "UNKNOWN":
                continue
            pts = total_points_from_item(it)
            ttime = total_time_from_item(it)

            # played_at: requires extra call using game token
            played_at: Optional[int] = None
            if fetch_played_at:
                try:
                    game_token = it["game"].get("token")
                except Exception:
                    game_token = None

                if isinstance(game_token, str) and game_token:
                    if game_token in played_at_cache:
                        played_at = played_at_cache[game_token]
                    else:
                        played_at = fetch_game_details_for_played_at(session, game_token, timeout=timeout, debug=debug)
                        played_at_cache[game_token] = played_at

            if played_at is not None:
                has_any_played_at = True

            out_entries.append(
                Entry(
                    week_label=week.label,
                    map_index=map_idx,
                    map_url=url,
                    map_token=token,
                    map_name=map_name or f"Map {map_idx}",
                    rule_text=rule_text or "",
                    player=name,
                    total_pts=pts,
                    total_time=ttime,
                    played_at_epoch=played_at,
                )
            )

    return out_entries, map_meta_rows, has_any_played_at, failed_maps_count


# ============================================================
# Filtering + scoring aggregation
# ============================================================

def filter_entries_by_deadlines(
    entries: List[Entry],
    deadlines_epoch_by_week: Dict[str, int],
    keep_missing_time: bool,
    now_epoch: Optional[int] = None,
) -> List[Entry]:
    if now_epoch is None:
        now_epoch = int(time.time())

    out: List[Entry] = []
    for e in entries:
        dl = deadlines_epoch_by_week.get(e.week_label)
        if dl is None:
            # no deadline specified for this week => keep
            out.append(e)
            continue

        # Ongoing/future week: never filter out yet.
        if dl > now_epoch:
            out.append(e)
            continue

        if e.played_at_epoch is None:
            if keep_missing_time:
                out.append(e)
            continue

        if e.played_at_epoch <= dl:
            out.append(e)
    return out


def compute_week_tables(entries: List[Entry], tie_mode: str, map_meta_rows: Optional[List[dict]] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      df_overview_rows: per entry with rank/borda within each (week,map)
      df_weekly: weekly summary per player (sum of borda)
      df_week_meta: map meta per (week,map) for headers
    """
    if not entries:
        if map_meta_rows:
            df_week_meta = pd.DataFrame(map_meta_rows, columns=["week", "map_index", "map_url", "map_name", "rule_text"])
            if not df_week_meta.empty:
                df_week_meta = (
                    df_week_meta.drop_duplicates(subset=["week", "map_index"], keep="last")
                    .sort_values(["week", "map_index"])
                    .reset_index(drop=True)
                )
        else:
            df_week_meta = pd.DataFrame(columns=["week", "map_index", "map_url", "map_name", "rule_text"])
        return (
            pd.DataFrame(columns=["week", "map_index", "map_url", "map_name", "rule_text", "slot_key", "slot_label", "player", "total_pts", "total_time", "rank_best", "borda_points", "played_at_epoch"]),
            pd.DataFrame(columns=["week", "player", "weekly_borda", "weekly_total_pts", "maps_counted"]),
            df_week_meta,
        )

    df = pd.DataFrame([{
        "week": e.week_label,
        "map_index": e.map_index,
        "map_url": e.map_url,
        "map_token": e.map_token,
        "map_name": e.map_name,
        "rule_text": e.rule_text,
        "player": e.player,
        "total_pts": e.total_pts,
        "total_time": e.total_time,
        "played_at_epoch": e.played_at_epoch,
    } for e in entries])
    df["slot_key"] = df["map_index"].apply(map_slot_key)
    df["slot_label"] = df["slot_key"].apply(map_slot_label)

    # discovered meta per map from result payloads
    df_week_meta_seen = (
        df[["week", "map_index", "map_url", "map_name", "rule_text"]]
        .drop_duplicates()
        .sort_values(["week", "map_index"])
        .reset_index(drop=True)
    )

    if map_meta_rows:
        df_week_meta_base = pd.DataFrame(map_meta_rows, columns=["week", "map_index", "map_url", "map_name", "rule_text"])
        if not df_week_meta_base.empty:
            df_week_meta_base = (
                df_week_meta_base.drop_duplicates(subset=["week", "map_index"], keep="last")
                .sort_values(["week", "map_index"])
                .reset_index(drop=True)
            )
    else:
        df_week_meta_base = pd.DataFrame(columns=["week", "map_index", "map_url", "map_name", "rule_text"])

    if df_week_meta_base.empty:
        df_week_meta = df_week_meta_seen
    else:
        df_week_meta = df_week_meta_base.merge(
            df_week_meta_seen.rename(
                columns={
                    "map_url": "seen_map_url",
                    "map_name": "seen_map_name",
                    "rule_text": "seen_rule_text",
                }
            ),
            on=["week", "map_index"],
            how="left",
        )

        def _prefer_non_empty(base: Any, seen: Any, fallback: str) -> str:
            b = str(base).strip() if isinstance(base, str) else ""
            s = str(seen).strip() if isinstance(seen, str) else ""
            if s:
                return s
            if b:
                return b
            return fallback

        df_week_meta["map_url"] = [
            _prefer_non_empty(b, s, "")
            for b, s in zip(df_week_meta.get("map_url", pd.Series(dtype=str)), df_week_meta.get("seen_map_url", pd.Series(dtype=str)))
        ]
        df_week_meta["map_name"] = [
            _prefer_non_empty(b, s, f"Map {int(mi)}")
            for b, s, mi in zip(
                df_week_meta.get("map_name", pd.Series(dtype=str)),
                df_week_meta.get("seen_map_name", pd.Series(dtype=str)),
                df_week_meta["map_index"],
            )
        ]
        df_week_meta["rule_text"] = [
            _prefer_non_empty(b, s, "")
            for b, s in zip(df_week_meta.get("rule_text", pd.Series(dtype=str)), df_week_meta.get("seen_rule_text", pd.Series(dtype=str)))
        ]
        df_week_meta = df_week_meta[["week", "map_index", "map_url", "map_name", "rule_text"]]

    # compute rank/borda within each week+map
    out_rows: List[dict] = []

    for (w, mi), g in df.groupby(["week", "map_index"], sort=True):
        pts_map = {row["player"]: int(row["total_pts"]) for _, row in g.iterrows()}
        time_map = {row["player"]: int(row["total_time"]) for _, row in g.iterrows()}
        rank_best, borda = compute_rank_and_borda_with_time(pts_map, time_map, tie_mode=tie_mode)

        for _, row in g.iterrows():
            p = row["player"]
            out_rows.append({
                **row.to_dict(),
                "rank_best": rank_best.get(p),
                "borda_points": borda.get(p),
            })

    df_overview = pd.DataFrame(out_rows)

    # weekly summary: sum borda across maps (and keep raw points sum too)
    df_weekly = (
        df_overview.groupby(["week", "player"], as_index=False)
        .agg(
            weekly_borda=("borda_points", "sum"),
            weekly_total_pts=("total_pts", "sum"),
            maps_counted=("map_index", "nunique"),
        )
        .sort_values(["week", "weekly_borda", "weekly_total_pts"], ascending=[True, False, False])
        .reset_index(drop=True)
    )

    return df_overview, df_weekly, df_week_meta


def compute_total_tables(df_overview: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Total standings + stats.
    """
    base_cols_total = [
        "player",
        "total_borda",
        "total_pts",
        "maps_counted",
        "weeks_counted",
        "avg_borda_per_map",
        "avg_borda_per_week",
        "avg_pts_per_map",
        "cat_moving_1",
        "cat_moving_2",
        "cat_no_move_1",
        "cat_no_move_2",
        "cat_nmpz_1",
        "cat_nmpz_2",
        "cat_moving",
        "cat_no_move",
        "cat_nmpz",
        "cat_sverige",
    ]
    base_cols_stats = base_cols_total + ["best_week", "best_week_borda", "best_week_pts"]

    if df_overview.empty:
        total = pd.DataFrame(columns=base_cols_total)
        stats = pd.DataFrame(columns=base_cols_stats)
        return total, stats

    dfo = df_overview.copy()
    dfo["week_map_key"] = dfo["week"].astype(str) + "::" + dfo["map_index"].astype(str)
    dfo["slot_key"] = dfo["map_index"].apply(map_slot_key)

    by_player = (
        dfo.groupby("player", as_index=False)
        .agg(
            total_borda=("borda_points", "sum"),
            total_pts=("total_pts", "sum"),
            maps_counted=("week_map_key", "nunique"),
            weeks_counted=("week", "nunique"),
        )
    )
    by_player["avg_borda_per_map"] = by_player["total_borda"] / by_player["maps_counted"].clip(lower=1)
    by_player["avg_borda_per_week"] = by_player["total_borda"] / by_player["weeks_counted"].clip(lower=1)
    by_player["avg_pts_per_map"] = by_player["total_pts"] / by_player["maps_counted"].clip(lower=1)

    # Slot totals
    slot_scores = (
        dfo[dfo["slot_key"].isin(SLOT_KEYS_ORDER)]
        .groupby(["player", "slot_key"], as_index=False)
        .agg(slot_borda=("borda_points", "sum"))
    )
    if not slot_scores.empty:
        slot_pivot = (
            slot_scores.pivot_table(index="player", columns="slot_key", values="slot_borda", aggfunc="sum")
            .fillna(0.0)
            .reset_index()
        )
        by_player = by_player.merge(slot_pivot, on="player", how="left")

    for key in SLOT_KEYS_ORDER:
        if key not in by_player.columns:
            by_player[key] = 0.0

    by_player = by_player.rename(columns={
        "moving_1": "cat_moving_1",
        "moving_2": "cat_moving_2",
        "no_move_1": "cat_no_move_1",
        "no_move_2": "cat_no_move_2",
        "nmpz_1": "cat_nmpz_1",
        "nmpz_2": "cat_nmpz_2",
    })

    by_player["cat_moving"] = by_player["cat_moving_1"] + by_player["cat_moving_2"]
    by_player["cat_no_move"] = by_player["cat_no_move_1"] + by_player["cat_no_move_2"]
    by_player["cat_nmpz"] = by_player["cat_nmpz_1"] + by_player["cat_nmpz_2"]
    by_player["cat_sverige"] = by_player["cat_moving_1"] + by_player["cat_no_move_2"]

    total = by_player.sort_values(["total_borda", "total_pts"], ascending=[False, False]).reset_index(drop=True)
    total = total.reindex(columns=base_cols_total)

    # extra stats: best week, avg per week, etc.
    per_week = (
        dfo.groupby(["player", "week"], as_index=False)
        .agg(
            week_borda=("borda_points", "sum"),
            week_pts=("total_pts", "sum"),
            week_maps=("week_map_key", "nunique"),
        )
    )
    best_week = per_week.sort_values(["player", "week_borda", "week_pts"], ascending=[True, False, False]).groupby("player").head(1)
    best_week = best_week[["player", "week", "week_borda", "week_pts"]].rename(columns={"week": "best_week", "week_borda": "best_week_borda", "week_pts": "best_week_pts"})

    stats = total.merge(best_week, on="player", how="left")
    stats = stats.sort_values(["total_borda", "total_pts"], ascending=[False, False]).reset_index(drop=True)
    stats = stats.reindex(columns=base_cols_stats)

    return total, stats


def compute_subleague_tables(df_overview: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    if df_overview.empty:
        for league_name in SUBLEAGUE_SLOT_KEYS:
            out[league_name] = pd.DataFrame(columns=["player", "league_points", "total_pts", "maps_counted", "weeks_counted"])
        return out

    dfo = df_overview.copy()
    dfo["slot_key"] = dfo["map_index"].apply(map_slot_key)
    dfo["week_map_key"] = dfo["week"].astype(str) + "::" + dfo["map_index"].astype(str)

    for league_name, slots in SUBLEAGUE_SLOT_KEYS.items():
        part = dfo[dfo["slot_key"].isin(slots)]
        if part.empty:
            out[league_name] = pd.DataFrame(columns=["player", "league_points", "total_pts", "maps_counted", "weeks_counted"])
            continue

        table = (
            part.groupby("player", as_index=False)
            .agg(
                league_points=("borda_points", "sum"),
                total_pts=("total_pts", "sum"),
                maps_counted=("week_map_key", "nunique"),
                weeks_counted=("week", "nunique"),
            )
            .sort_values(["league_points", "total_pts"], ascending=[False, False])
            .reset_index(drop=True)
        )
        out[league_name] = table
    return out


# ============================================================
# Excel output
# ============================================================

def set_col_widths(ws, widths: Dict[int, float]) -> None:
    for col_idx, w in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = w


def style_cell(ws, r: int, c: int, *, fill=None, font=None, align=None, border=True) -> None:
    cell = ws.cell(r, c)
    if fill is not None:
        cell.fill = fill
    if font is not None:
        cell.font = font
    if align is not None:
        cell.alignment = align
    if border:
        cell.border = BORDER_THIN


def merge_and_style(ws, r1: int, c1: int, r2: int, c2: int, value: str, *, fill, font, align) -> None:
    ws.merge_cells(start_row=r1, start_column=c1, end_row=r2, end_column=c2)
    cell = ws.cell(r1, c1)
    cell.value = value
    cell.fill = fill
    cell.font = font
    cell.alignment = align
    for r in range(r1, r2 + 1):
        for c in range(c1, c2 + 1):
            ws.cell(r, c).border = BORDER_THIN
            ws.cell(r, c).fill = fill


def write_week_sheet(
    wb: Workbook,
    week_label: str,
    deadline_str: str,
    df_week: pd.DataFrame,
    df_overview: pd.DataFrame,
    df_meta: pd.DataFrame,
) -> None:
    """
    Layout similar to your Google-sheet:
      Row 1: Week label / Map 1..N
      Row 2: Deadline / Map names
      Row 3: headers (#, Spelare, Poäng, links)
      Rows : ranking table with per-map borda points
    """
    ws = wb.create_sheet(week_label)

    # Determine map columns
    meta = df_meta[df_meta["week"] == week_label].sort_values("map_index")
    maps = meta.to_dict("records")
    n_maps = len(maps)

    col_rank = 1
    col_player = 2
    col_total = 3
    col_map_start = 4

    r1, r2, r3, r_data_start = 1, 2, 3, 4

    merge_and_style(ws, r1, 1, r1, 3, week_label, fill=DARK, font=FONT_HDR_BIG, align=CENTER)
    merge_and_style(ws, r2, 1, r2, 3, deadline_str, fill=DARK, font=FONT_HDR_MED, align=CENTER)

    for i in range(n_maps):
        c = col_map_start + i
        merge_and_style(ws, r1, c, r1, c, f"Map {i+1}", fill=DARK, font=FONT_HDR_MED, align=CENTER)
        merge_and_style(ws, r2, c, r2, c, str(maps[i].get("map_name") or f"Map {i+1}"), fill=DARK, font=FONT_HDR_MED, align=CENTER)

    ws.cell(r3, col_rank).value = "#"
    ws.cell(r3, col_player).value = "Spelare"
    ws.cell(r3, col_total).value = "Poäng"
    for c in (col_rank, col_player, col_total):
        style_cell(ws, r3, c, fill=MID, font=FONT_HDR, align=CENTER)

    # header links per map
    for i in range(n_maps):
        c = col_map_start + i
        slot_key = map_slot_key(maps[i].get("map_index"))
        slot_label = map_slot_label(slot_key)
        rule = str(maps[i].get("rule_text") or "")
        url = str(maps[i].get("map_url") or "")
        txt = f"🔗 {slot_label}"
        if rule:
            txt = f"{txt} | {rule}"

        cell = ws.cell(r3, c)
        cell.value = txt
        if url:
            cell.hyperlink = url
        cell.fill = MID
        cell.alignment = CENTER
        cell.border = BORDER_THIN
        cell.font = Font(color="FFFFFF", bold=True, underline="single")

    ws.freeze_panes = ws["A4"]
    ws.row_dimensions[r2].height = 38
    ws.row_dimensions[r3].height = 34

    # column widths
    widths = {
        col_rank: 4.5,
        col_player: 22.0,
        col_total: 8.0,
    }
    for i in range(n_maps):
        widths[col_map_start + i] = 20.0
    set_col_widths(ws, widths)

    # per-map pivot for this week
    dwo = df_overview[df_overview["week"] == week_label]
    pivot = pd.DataFrame()
    if not dwo.empty:
        pivot = dwo.pivot_table(index="player", columns="map_index", values="borda_points", aggfunc="max")

    # weekly order
    dw = df_week[df_week["week"] == week_label].sort_values(["weekly_borda", "weekly_total_pts"], ascending=[False, False])
    ordered = dw["player"].tolist()

    for idx, player in enumerate(ordered, start=1):
        r = r_data_start + (idx - 1)
        fill = ROW_A if (idx % 2 == 1) else ROW_B

        ws.cell(r, col_rank).value = idx
        ws.cell(r, col_player).value = player
        pts = float(dw.loc[dw["player"] == player, "weekly_borda"].iloc[0])
        ws.cell(r, col_total).value = int(pts) if abs(pts - int(pts)) < 1e-9 else pts

        style_cell(ws, r, col_rank, fill=fill, font=FONT_BODY, align=CENTER)
        style_cell(ws, r, col_player, fill=fill, font=FONT_BODY, align=LEFT)
        style_cell(ws, r, col_total, fill=fill, font=Font(color="000000", bold=True), align=CENTER)

        for i in range(n_maps):
            c = col_map_start + i
            map_idx = int(_parse_int_maybe(maps[i].get("map_index")) or (i + 1))
            val = None
            if not pivot.empty and player in pivot.index and map_idx in pivot.columns:
                v = pivot.loc[player, map_idx]
                if pd.notna(v):
                    val = float(v)
            ws.cell(r, c).value = "" if val is None else (int(val) if abs(val - int(val)) < 1e-9 else val)
            style_cell(ws, r, c, fill=fill, font=FONT_BODY, align=CENTER)


def write_total_sheet(wb: Workbook, df_total: pd.DataFrame, df_overview: pd.DataFrame, weeks: List[str]) -> None:
    ws = wb.create_sheet("Total")

    # Header
    merge_and_style(ws, 1, 1, 1, 7 + len(weeks), "Totalställning", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    headers = ["#", "Spelare", "Poäng", "Total pts", "Snitt pts/karta", "Kartor", "Veckor"] + [f"{w}" for w in weeks]
    for c, h in enumerate(headers, start=1):
        ws.cell(2, c).value = h
        style_cell(ws, 2, c, fill=MID, font=FONT_HDR, align=CENTER)

    ws.freeze_panes = "A3"

    widths = {1: 4.5, 2: 22.0, 3: 14.0, 4: 10.0, 5: 14.0, 6: 8.0, 7: 8.0}
    for i in range(len(weeks)):
        widths[8 + i] = 12.0
    set_col_widths(ws, widths)

    # per-week totals pivot (borda)
    per_week = (
        df_overview.groupby(["player", "week"], as_index=False)
        .agg(week_borda=("borda_points", "sum"))
    )
    pivot = per_week.pivot_table(index="player", columns="week", values="week_borda", aggfunc="sum")

    for idx, row in enumerate(df_total.itertuples(index=False), start=1):
        r = 2 + idx
        fill = ROW_A if (idx % 2 == 1) else ROW_B

        ws.cell(r, 1).value = idx
        ws.cell(r, 2).value = row.player
        ws.cell(r, 3).value = float(row.total_borda)
        ws.cell(r, 4).value = int(row.total_pts)
        ws.cell(r, 5).value = float(getattr(row, "avg_pts_per_map", 0) or 0)
        ws.cell(r, 6).value = int(row.maps_counted)
        ws.cell(r, 7).value = int(row.weeks_counted)

        for c in range(1, 8):
            style_cell(ws, r, c, fill=fill, font=FONT_BODY if c != 3 else Font(color="000000", bold=True), align=CENTER if c != 2 else LEFT)

        # week columns
        for j, w in enumerate(weeks):
            c = 8 + j
            val = ""
            if not pivot.empty and row.player in pivot.index and w in pivot.columns:
                v = pivot.loc[row.player, w]
                if pd.notna(v):
                    val = float(v)
                    val = int(val) if abs(val - int(val)) < 1e-9 else val
            ws.cell(r, c).value = val
            style_cell(ws, r, c, fill=fill, font=FONT_BODY, align=CENTER)


def write_stats_sheet(wb: Workbook, df_stats: pd.DataFrame) -> None:
    ws = wb.create_sheet("Stats")

    merge_and_style(ws, 1, 1, 1, 22, "Statistik", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    cols = [
        "#", "Spelare",
        "Poäng", "Total pts",
        "Kartor", "Veckor",
        "Moving 1", "Moving 2",
        "No move 1", "No move 2",
        "NMPZ 1", "NMPZ 2",
        "Moving", "No move", "NMPZ", "Sverige",
        "Snitt poäng / karta", "Snitt poäng / vecka", "Snitt pts / karta",
        "Bästa vecka", "Bästa vecka poäng", "Bästa vecka pts",
    ]

    for c, h in enumerate(cols, start=1):
        ws.cell(2, c).value = h
        style_cell(ws, 2, c, fill=MID, font=FONT_HDR, align=CENTER)

    ws.freeze_panes = "A3"

    widths = {
        1: 4.5, 2: 22.0,
        3: 12.0, 4: 10.0,
        5: 8.0, 6: 8.0,
        7: 10.0, 8: 10.0,
        9: 10.0, 10: 10.0,
        11: 10.0, 12: 10.0,
        13: 10.0, 14: 10.0, 15: 10.0, 16: 10.0,
        17: 14.0, 18: 14.0, 19: 12.0,
        20: 14.0, 21: 18.0, 22: 14.0,
    }
    set_col_widths(ws, widths)

    for idx, row in enumerate(df_stats.itertuples(index=False), start=1):
        r = 2 + idx
        fill = ROW_A if (idx % 2 == 1) else ROW_B

        ws.cell(r, 1).value = idx
        ws.cell(r, 2).value = row.player
        ws.cell(r, 3).value = float(row.total_borda)
        ws.cell(r, 4).value = int(row.total_pts)
        ws.cell(r, 5).value = int(row.maps_counted)
        ws.cell(r, 6).value = int(row.weeks_counted)
        ws.cell(r, 7).value = float(getattr(row, "cat_moving_1", 0) or 0)
        ws.cell(r, 8).value = float(getattr(row, "cat_moving_2", 0) or 0)
        ws.cell(r, 9).value = float(getattr(row, "cat_no_move_1", 0) or 0)
        ws.cell(r, 10).value = float(getattr(row, "cat_no_move_2", 0) or 0)
        ws.cell(r, 11).value = float(getattr(row, "cat_nmpz_1", 0) or 0)
        ws.cell(r, 12).value = float(getattr(row, "cat_nmpz_2", 0) or 0)
        ws.cell(r, 13).value = float(getattr(row, "cat_moving", 0) or 0)
        ws.cell(r, 14).value = float(getattr(row, "cat_no_move", 0) or 0)
        ws.cell(r, 15).value = float(getattr(row, "cat_nmpz", 0) or 0)
        ws.cell(r, 16).value = float(getattr(row, "cat_sverige", 0) or 0)
        ws.cell(r, 17).value = float(row.avg_borda_per_map)
        ws.cell(r, 18).value = float(row.avg_borda_per_week)
        ws.cell(r, 19).value = float(row.avg_pts_per_map)
        ws.cell(r, 20).value = getattr(row, "best_week", "")
        ws.cell(r, 21).value = float(getattr(row, "best_week_borda", 0) or 0)
        ws.cell(r, 22).value = float(getattr(row, "best_week_pts", 0) or 0)

        for c in range(1, 23):
            align = LEFT if c == 2 else CENTER
            font = Font(color="000000", bold=True) if c in (3,) else FONT_BODY
            style_cell(ws, r, c, fill=fill, font=font, align=align)


def write_underligor_sheet(wb: Workbook, df_overview: pd.DataFrame) -> None:
    ws = wb.create_sheet("Underligor")
    merge_and_style(ws, 1, 1, 1, 6, "Underligor", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    widths = {1: 4.5, 2: 22.0, 3: 12.0, 4: 10.0, 5: 8.0, 6: 8.0}
    set_col_widths(ws, widths)

    tables = compute_subleague_tables(df_overview)
    row_cursor = 3

    for league_name in ["Moving", "No move", "NMPZ", "Sverige"]:
        merge_and_style(ws, row_cursor, 1, row_cursor, 6, league_name, fill=MID, font=FONT_HDR_MED, align=CENTER)
        row_cursor += 1

        headers = ["#", "Spelare", "Poäng", "Total pts", "Kartor", "Veckor"]
        for c, h in enumerate(headers, start=1):
            ws.cell(row_cursor, c).value = h
            style_cell(ws, row_cursor, c, fill=MID, font=FONT_HDR, align=CENTER)
        row_cursor += 1

        table = tables.get(league_name, pd.DataFrame())
        for idx, row in enumerate(table.itertuples(index=False), start=1):
            r = row_cursor + (idx - 1)
            fill = ROW_A if (idx % 2 == 1) else ROW_B
            ws.cell(r, 1).value = idx
            ws.cell(r, 2).value = row.player
            ws.cell(r, 3).value = float(row.league_points)
            ws.cell(r, 4).value = int(row.total_pts)
            ws.cell(r, 5).value = int(row.maps_counted)
            ws.cell(r, 6).value = int(row.weeks_counted)

            for c in range(1, 7):
                align = LEFT if c == 2 else CENTER
                font = Font(color="000000", bold=True) if c == 3 else FONT_BODY
                style_cell(ws, r, c, fill=fill, font=font, align=align)

        row_cursor += max(len(table), 1) + 1

    ws.freeze_panes = "A4"


def write_information_sheet(wb: Workbook, info_rows: Optional[List[str]] = None) -> None:
    ws = wb.create_sheet("Information")

    merge_and_style(ws, 1, 1, 2, 2, "Information", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    rows = _normalize_information_rows(info_rows if info_rows is not None else default_information_rows())

    set_col_widths(ws, {1: 4.5, 2: 185.0})
    ws.row_dimensions[1].height = 40
    ws.row_dimensions[2].height = 40

    for i, text in enumerate(rows, start=0):
        r = 3 + i
        fill = ROW_A if (i % 2 == 0) else ROW_B
        ws.cell(r, 1).value = "•"
        ws.cell(r, 2).value = text
        style_cell(ws, r, 1, fill=fill, font=FONT_BODY, align=CENTER)
        style_cell(ws, r, 2, fill=fill, font=FONT_BODY, align=LEFT)
        ws.row_dimensions[r].height = 34


def write_raw_sheet(wb: Workbook, df_overview: pd.DataFrame) -> None:
    ws = wb.create_sheet("Raw")
    if df_overview.empty:
        ws["A1"].value = "No data"
        return

    headers = list(df_overview.columns)
    for c, h in enumerate(headers, start=1):
        ws.cell(1, c).value = h
        style_cell(ws, 1, c, fill=DARK, font=FONT_HDR, align=CENTER)

    for r_idx, (_, row) in enumerate(df_overview.iterrows(), start=2):
        for c_idx, h in enumerate(headers, start=1):
            v = row[h]
            if pd.isna(v):
                v = ""
            ws.cell(r_idx, c_idx).value = v
            style_cell(ws, r_idx, c_idx, fill=WHITE, font=FONT_BODY, align=LEFT if h == "player" else CENTER)

    ws.freeze_panes = "A2"
    for c_idx, h in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(c_idx)].width = min(max(len(str(h)) + 2, 10), 40)


# ============================================================
# Main
# ============================================================

def parse_week_specs(week_args: List[str]) -> List[WeekSpec]:
    if not week_args:
        raise SystemExit(
            'No weeks specified. Example:\n'
            '  python geoguessr_league_build_xlsx.py --week "Vecka 1|urls_week1.txt|2026-02-18 20:00" --week "Vecka 2|urls_week2.txt|2026-02-25 20:00"'
        )

    out: List[WeekSpec] = []
    for s in week_args:
        parts = [p.strip() for p in s.split("|")]
        if len(parts) < 2:
            raise SystemExit(f'Bad --week "{s}". Expected "LABEL|URLS_FILE|DEADLINE(optional)".')
        label = parts[0]
        urls_path = Path(parts[1]).expanduser()
        deadline = parts[2] if len(parts) >= 3 and parts[2] else None
        out.append(WeekSpec(label=label, urls_path=urls_path, deadline=deadline))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    weeks = parse_week_specs(args.week)

    ncfa = (args.ncfa or os.environ.get("GEOGUESSR_NCFA", "")).strip()
    if not ncfa:
        raise SystemExit("Missing _ncfa. Set GEOGUESSR_NCFA or pass --ncfa.")

    print("[START] python:", sys.executable)
    print("[START] cwd   :", Path.cwd())
    print("[START] weeks :", [(w.label, str(w.urls_path), w.deadline) for w in weeks])
    print("[START] fetch_played_at:", bool(args.fetch_played_at))
    print("[START] tz:", args.tz)

    if args.information_config.strip():
        info_config_path = Path(args.information_config.strip()).expanduser()
    else:
        info_config_path = Path(DEFAULT_INFORMATION_CONFIG_NAME)
    info_rows = load_information_rows(info_config_path, debug=args.debug)
    print("[START] information_config:", info_config_path if info_config_path.exists() else "(default built-in)")

    session = make_session(ncfa)

    # collect deadlines (epoch)
    deadlines_epoch: Dict[str, int] = {}
    for w in weeks:
        if w.deadline:
            deadlines_epoch[w.label] = parse_deadline_epoch(w.deadline, args.tz)

    # Build all entries
    all_entries: List[Entry] = []
    all_map_meta: List[dict] = []
    any_played_at = False
    successful_weeks: List[WeekSpec] = []
    failed_weeks: List[Tuple[str, str]] = []

    for w in weeks:
        try:
            entries, week_map_meta, has_any, failed_maps = build_week_entries(
                session=session,
                week=w,
                tz_name=args.tz,
                timeout=args.timeout,
                debug=args.debug,
                dump_json=args.dump_json,
                page_size=args.page_size,
                max_players=args.max_players,
                fetch_played_at=bool(args.fetch_played_at and (w.deadline is not None)),
            )
            all_entries.extend(entries)
            all_map_meta.extend(week_map_meta)
            any_played_at = any_played_at or has_any
            successful_weeks.append(w)
            print(f"[OK] built entries for {w.label}: {len(entries)} rows")
            if failed_maps > 0:
                print(f"[WARN] {w.label}: {failed_maps} map(ar) kunde inte hämtas och hoppades över.")
        except Exception as e:
            failed_weeks.append((w.label, str(e)))
            print(f"[WARN] hoppar över vecka {w.label}: {e}")
            continue

    if not successful_weeks:
        raise SystemExit("Kunde inte bearbeta någon vecka. Kontrollera URL-filer, _ncfa och nätverksåtkomst.")

    if failed_weeks:
        print("[WARN] följande veckor kunde inte bearbetas:")
        for label, err in failed_weeks:
            print(f"  - {label}: {err}")

    # Compute tables for ALL (unfiltered)
    df_overview_all, df_weekly_all, df_meta_all = compute_week_tables(all_entries, tie_mode=args.tie, map_meta_rows=all_map_meta)
    df_total_all, df_stats_all = compute_total_tables(df_overview_all)

    # Decide filtering
    can_filter = bool(deadlines_epoch) and bool(args.fetch_played_at) and any_played_at

    # Build filtered data (if possible)
    df_overview_f = df_weekly_f = df_meta_f = df_total_f = df_stats_f = None
    if can_filter:
        now_epoch = int(time.time())
        open_weeks = [w for w, dl in deadlines_epoch.items() if dl > now_epoch]
        if open_weeks:
            print(f"[FILTER] open week(s), deadline not reached yet: {open_weeks}. Those weeks are not filtered.")

        filtered_entries = filter_entries_by_deadlines(
            all_entries,
            deadlines_epoch,
            keep_missing_time=bool(args.keep_missing_time),
            now_epoch=now_epoch,
        )
        df_overview_f, df_weekly_f, df_meta_f = compute_week_tables(filtered_entries, tie_mode=args.tie, map_meta_rows=all_map_meta)
        df_total_f, df_stats_f = compute_total_tables(df_overview_f)
        print(f"[FILTER] enabled. Filtered rows: {len(filtered_entries)} (from {len(all_entries)})")
    else:
        if deadlines_epoch and args.fetch_played_at:
            print("[FILTER] deadlines provided, but could not extract any played_at timestamps from API. Will write only ALL file.")
        elif deadlines_epoch and not args.fetch_played_at:
            print("[FILTER] deadlines provided, but --fetch-played-at not enabled. Will write only ALL file.")
        else:
            print("[FILTER] no deadlines -> will write only ALL file.")

    # Write ALL workbook
    out_all = Path(f"{args.out_base}_all.xlsx")
    wb_all = Workbook()
    # remove default sheet
    wb_all.remove(wb_all.active)
    write_information_sheet(wb_all, info_rows)

    week_labels = [w.label for w in successful_weeks]

    # Week tabs (ALL)
    for w in successful_weeks:
        dl_str = w.deadline or ""
        write_week_sheet(wb_all, w.label, f"Deadline {dl_str}" if dl_str else "Deadline", df_weekly_all, df_overview_all, df_meta_all)

    write_total_sheet(wb_all, df_total_all, df_overview_all, week_labels)
    write_stats_sheet(wb_all, df_stats_all)
    write_underligor_sheet(wb_all, df_overview_all)
    write_raw_sheet(wb_all, df_overview_all)

    wb_all.save(out_all)
    print("[DONE] wrote:", out_all)

    # Write FILTERED workbook (if available)
    if can_filter and df_overview_f is not None and df_weekly_f is not None and df_meta_f is not None and df_total_f is not None and df_stats_f is not None:
        out_f = Path(f"{args.out_base}_filtered.xlsx")
        wb_f = Workbook()
        wb_f.remove(wb_f.active)
        write_information_sheet(wb_f, info_rows)

        for w in successful_weeks:
            dl_str = w.deadline or ""
            write_week_sheet(wb_f, w.label, f"Deadline {dl_str}" if dl_str else "Deadline", df_weekly_f, df_overview_f, df_meta_f)

        write_total_sheet(wb_f, df_total_f, df_overview_f, week_labels)
        write_stats_sheet(wb_f, df_stats_f)
        write_underligor_sheet(wb_f, df_overview_f)
        write_raw_sheet(wb_f, df_overview_f)

        wb_f.save(out_f)
        print("[DONE] wrote:", out_f)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        print("[FATAL] Unhandled exception")
        traceback.print_exc()
        raise
