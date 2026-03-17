from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import sys
import time
import traceback
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.worksheet.table import Table, TableStyleInfo
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
SETTING_LABEL_RE = re.compile(r'game-settings-list_settingLabel[^"]*">(.*?)</div>', re.S)
URL_RE = re.compile(r"https?://\S+")

DEFAULT_TZ = "Europe/Stockholm"
DEFAULT_INFORMATION_CONFIG_NAME = "information_config_v2.json"
DEFAULT_SWEDEN_MAPS = (1, 4)

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
    "Sverige Moving": ["moving_1"],
    "Sverige No Move": ["no_move_2"],
}

DEFAULT_INFORMATION_ROWS = [
    "Ingen anmälan krävs - det är bara att spela veckans challenges!",
    "För att öppna länken: klicka på den understrukna raden i varje kolumn. Exempelvis \"🔗 Moving 1 | Moving - 3 min\".",
    "Preliminära poäng utdelas under veckan. De kan gå upp beroende på hur många spelare som placerar sig under dig.",
    "Poäng delas ut enligt pro league-systemet: sista plats får 1 poäng, näst sista 2 poäng, tredje sista 3 poäng osv.",
    "Tiebreaker vid samma poäng är tid. Om två spelare delar plats får båda poäng för den delade placeringen.",
    "Varje vecka avslutas onsdag kl 20.00. Om poängen inte är ihopräknade då kan du spela tills poängen är ihopräknade.",
    "Vid frågor, skriv i #ligan.",
    "Mer info: Testa att skapa Excel-filen själv via appen: https://drive.google.com/file/d/1wcj0CyYKskqJcD8KjDv2rG4VGvSS5Q7A/view?usp=drive_link",
    "Mer info: GitHub, README och senaste uppdateringarna: https://github.com/Simon-Hallosta/Geoguessr-League-Generator",
]

STYLE_MIN_MAPS = 6
STYLE_MIN_WEEKS = 2
STYLE_SIMILARITY_MAX_PLAYERS = 20
STYLE_FEATURE_SPECS = [
    ("fivek_rate_raw", "5k-frekvens", 1.30, "Andel kartor med minst en 5k-runda."),
    ("fivek_speed_raw", "5k-hastighet", 1.15, "Hur snabbt 5k tas nar den kommer."),
    ("fivek_steps_moving_raw", "5k-steg i moving", 1.05, "Fa steg for att ta 5k i moving."),
    ("best_round_efficiency_raw", "Basta-runda-effektivitet", 1.20, "Hog rundpoang i forhallande till rundtid."),
    ("map_time_efficiency_raw", "Karttidseffektivitet", 1.00, "Laga karttider utan att bara spegla totalpoang."),
    ("no_move_strength_raw", "No move-styrka", 1.25, "Relativ styrka i no move."),
    ("nmpz_strength_raw", "NMPZ-styrka", 1.25, "Relativ styrka i NMPZ."),
    ("moving_strength_raw", "Moving-styrka", 0.75, "Relativ styrka i moving."),
    ("specialization_raw", "Specialisering", 1.00, "Hur tydligt spelaren avviker mellan modes."),
    ("consistency_raw", "Konsistens", 1.00, "Jamnhet inom spelarens prestationer."),
    ("clutch_profile_raw", "Clutchprofil", 0.90, "Formaga att hitta toppresultat i enskilda rundor."),
    ("precision_support_raw", "Precisionstod", 0.45, "Latt precisionstillskott via map-relativa resultat."),
    ("total_pts_support_raw", "Totalpoang-stod", 0.20, "Svag stodsignal for total resultatniva."),
]
STYLE_VECTOR_COLUMNS = [key for key, _, _, _ in STYLE_FEATURE_SPECS]

# Excel styling
DARK = PatternFill("solid", fgColor="2B2B2B")
MID = PatternFill("solid", fgColor="3A3A3A")
ROW_A = PatternFill("solid", fgColor="D9EAD3")
ROW_B = PatternFill("solid", fgColor="C9E2BC")
WHITE = PatternFill("solid", fgColor="FFFFFF")
MEDAL_GOLD = PatternFill("solid", fgColor="FFD966")
MEDAL_SILVER = PatternFill("solid", fgColor="D9D9D9")
MEDAL_BRONZE = PatternFill("solid", fgColor="F4B183")

FONT_HDR = Font(color="FFFFFF", bold=True)
FONT_HDR_BIG = Font(color="FFFFFF", bold=True, size=16)
FONT_HDR_MED = Font(color="FFFFFF", bold=True, size=12)
FONT_BODY = Font(color="000000", bold=False)
FONT_BODY_SUBTLE = Font(color="667085", bold=False, italic=True, size=10)

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
    sweden_maps: Tuple[int, ...] = DEFAULT_SWEDEN_MAPS


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
    best_round_pts: int
    best_round_time: int
    fastest_5000_round_time: Optional[int]
    total_distance_m: Optional[float]
    total_steps: Optional[int]
    avg_steps_per_round: Optional[float]
    count_5000_rounds: Optional[int]
    fastest_5000_round_steps: Optional[int]
    fastest_5000_round_distance_m: Optional[float]
    played_at_epoch: Optional[int]  # optional, for deadline filtering


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser()

    # repeatable week spec:
    #   --week "Vecka 1|urls_week1.txt|2026-02-18 20:00|1,4"
    # deadline and sweden-map indexes are optional
    ap.add_argument(
        "--week",
        action="append",
        default=[],
        help='Repeatable. Format: "LABEL|URLS_FILE|DEADLINE(optional)|SWEDEN_MAPS(optional)". Example: --week "Vecka 2|urls_week2.txt|2026-02-25 20:00|2,4"',
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
    ap.add_argument(
        "--sort-by",
        default="default",
        help=(
            "Sortering för tabeller. Stöder bl.a: default, points, total_pts, maps, weeks, "
            "avg_pts, avg_points (samt svenska alias)."
        ),
    )

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


def http_get_text(session: requests.Session, url: str, timeout: float, debug: bool) -> str:
    r = session.get(url, timeout=timeout)
    debug_print(debug, f"[HTTP] GET {url} -> {r.status_code} len={len(r.text)}")
    if r.status_code >= 400:
        snippet = r.text[:300].replace("\n", "\\n")
        raise RuntimeError(f"HTTP {r.status_code} for {url}: {snippet}")
    return r.text


def _parse_int_maybe(x: Any) -> Optional[int]:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        if not math.isfinite(x):
            return None
        return int(x)
    try:
        if not isinstance(x, str):
            xf = float(x)
            if math.isfinite(xf):
                return int(xf)
    except Exception:
        pass
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if s.isdigit():
            return int(s)
    return None


def _parse_time_limit_seconds(label: str) -> Optional[int]:
    txt = label.strip().lower()
    if not txt:
        return None
    m = re.search(r"(\d+)\s*min", txt)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*(?:s|sec|secs|second|seconds)\b", txt)
    if m:
        return int(m.group(1))
    return None


def _clean_setting_label(raw: str) -> str:
    txt = re.sub(r"<!--.*?-->", "", raw, flags=re.S)
    txt = re.sub(r"<[^>]+>", "", txt)
    txt = html.unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def map_slot_key(map_index: Any) -> str:
    idx = _parse_int_maybe(map_index)
    if idx is None:
        return "unknown"
    return MAP_SLOT_KEY_BY_INDEX.get(idx, f"map_{idx}")


def map_slot_label(slot_key: str) -> str:
    return SLOT_LABEL_BY_KEY.get(slot_key, slot_key.replace("_", " ").title())


def normalize_sweden_map_indexes(raw: Any) -> Tuple[int, ...]:
    if raw is None:
        return DEFAULT_SWEDEN_MAPS
    if isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        txt = str(raw).strip()
        if not txt:
            return DEFAULT_SWEDEN_MAPS
        items = re.split(r"[\s,;]+", txt)
    out: List[int] = []
    for item in items:
        iv = _parse_int_maybe(item)
        if iv is None or iv <= 0:
            continue
        if iv not in out:
            out.append(int(iv))
    return tuple(sorted(out)) if out else DEFAULT_SWEDEN_MAPS


def mode_category_from_rule_text(rule_text: Any) -> str:
    txt = str(rule_text or "").strip().lower()
    if not txt:
        return "unknown"
    if "nmpz" in txt:
        return "nmpz"
    if txt.startswith("nm") or "no move" in txt or "nmp" in txt:
        return "no_move"
    if "moving" in txt:
        return "moving"
    return "unknown"


def mode_category_from_game(game: dict) -> str:
    forbid_moving = game.get("forbidMoving")
    forbid_zooming = game.get("forbidZooming")
    forbid_rotating = game.get("forbidRotating")
    if forbid_moving is True:
        if forbid_zooming is True and forbid_rotating is True:
            return "nmpz"
        return "no_move"
    return "moving"


def mode_category_label(mode_category: str) -> str:
    return {
        "moving": "Moving",
        "no_move": "No move",
        "nmpz": "NMPZ",
    }.get(str(mode_category or "").strip().lower(), "Unknown")


def build_slot_key_from_mode(mode_category: str, occurrence_index: int) -> str:
    cat = str(mode_category or "").strip().lower()
    if cat == "moving":
        return f"moving_{occurrence_index}"
    if cat == "no_move":
        return f"no_move_{occurrence_index}"
    if cat == "nmpz":
        return f"nmpz_{occurrence_index}"
    return f"{cat or 'unknown'}_{occurrence_index}"


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


def _extract_information_link(text: str) -> Tuple[str, Optional[str]]:
    match = URL_RE.search(str(text or ""))
    if not match:
        return str(text or ""), None

    url = match.group(0).rstrip(".,);]")
    label = (str(text or "")[:match.start()] + str(text or "")[match.end():]).strip()
    label = re.sub(r"\s+", " ", label).rstrip(":").strip()
    if not label:
        label = url
    return label, url


def _excel_hyperlink_formula(url: str, label: str) -> str:
    safe_url = str(url).replace('"', '""')
    safe_label = str(label).replace('"', '""')
    return f'=HYPERLINK("{safe_url}","{safe_label}")'


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


def slot_metadata_from_week_maps(df_meta: pd.DataFrame) -> pd.DataFrame:
    if df_meta.empty:
        out = df_meta.copy()
        out["slot_key"] = []
        out["slot_label"] = []
        return out

    parts: List[pd.DataFrame] = []
    for week_name, grp in df_meta.groupby("week", sort=False):
        sub = grp.sort_values("map_index").copy()
        counters = {"moving": 0, "no_move": 0, "nmpz": 0, "unknown": 0}
        slot_keys: List[str] = []
        slot_labels: List[str] = []
        for _, row in sub.iterrows():
            mode_category = str(row.get("mode_category") or "unknown").strip().lower()
            counters[mode_category] = counters.get(mode_category, 0) + 1
            key = build_slot_key_from_mode(mode_category, counters[mode_category])
            slot_keys.append(key)
            if mode_category in {"moving", "no_move", "nmpz"}:
                slot_labels.append(map_slot_label(key))
            else:
                slot_labels.append(f"Map {int(_parse_int_maybe(row.get('map_index')) or 0)}")
        sub["slot_key"] = slot_keys
        sub["slot_label"] = slot_labels
        parts.append(sub)
    return pd.concat(parts, ignore_index=True) if parts else df_meta.copy()


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


def _extract_score_amount(obj: Any) -> Optional[int]:
    if not isinstance(obj, dict):
        return None
    for k in ("amount", "value", "points", "score"):
        if k in obj:
            v = _parse_int_maybe(obj.get(k))
            if v is not None:
                return v
    return None


def _normalize_round_time_seconds(v: Optional[int]) -> Optional[int]:
    if v is None or v < 0:
        return None
    # Heuristic: very large values are likely milliseconds.
    if v >= 10000:
        return max(0, int(round(v / 1000.0)))
    return v


def _extract_round_points_from_guess(guess: dict) -> Optional[int]:
    candidates = [
        guess.get("roundScoreInPoints"),
        guess.get("scoreInPoints"),
        guess.get("points"),
        guess.get("score"),
        guess.get("roundScore"),
        guess.get("totalScoreInPoints"),
    ]
    for cand in candidates:
        v = _parse_int_maybe(cand)
        if v is not None:
            return v
        v2 = _extract_score_amount(cand)
        if v2 is not None:
            return v2

    # Some schemas may nest values one level below.
    for key in ("player", "guess", "result"):
        sub = guess.get(key)
        if isinstance(sub, dict):
            v = _extract_round_points_from_guess(sub)
            if v is not None:
                return v
    return None


def _extract_round_time_from_guess(guess: dict) -> Optional[int]:
    candidates = [
        guess.get("time"),
        guess.get("timeTaken"),
        guess.get("timeSpent"),
        guess.get("timeInSeconds"),
        guess.get("roundTime"),
        guess.get("duration"),
        guess.get("elapsedTime"),
        guess.get("seconds"),
    ]
    for cand in candidates:
        v = _parse_int_maybe(cand)
        if v is not None:
            return _normalize_round_time_seconds(v)
        v2 = _extract_score_amount(cand)
        if v2 is not None:
            return _normalize_round_time_seconds(v2)

    for key in ("player", "guess", "result"):
        sub = guess.get(key)
        if isinstance(sub, dict):
            v = _extract_round_time_from_guess(sub)
            if v is not None:
                return v
    return None


def extract_round_stats_from_item(item: dict) -> Tuple[int, int, Optional[int]]:
    """
    Returns:
      (best_round_pts, best_round_time_sec, fastest_5000_round_time_sec_or_none)
    """
    game = item.get("game") if isinstance(item, dict) else None
    if not isinstance(game, dict):
        return 0, 10**12, None

    player_obj = game.get("player")
    candidate_lists: List[Any] = []
    if isinstance(player_obj, dict):
        candidate_lists.extend(
            [
                player_obj.get("guesses"),
                player_obj.get("rounds"),
                player_obj.get("guessResults"),
            ]
        )
    candidate_lists.extend([game.get("rounds"), game.get("guesses"), item.get("rounds")])

    rounds: List[Tuple[int, int]] = []
    for cand in candidate_lists:
        if not isinstance(cand, list):
            continue
        for guess in cand:
            if not isinstance(guess, dict):
                continue
            pts = _extract_round_points_from_guess(guess)
            if pts is None:
                continue
            t = _extract_round_time_from_guess(guess)
            if t is None:
                t = 10**12
            rounds.append((int(pts), int(t)))
        if rounds:
            break

    if not rounds:
        return 0, 10**12, None

    best_pts = max(p for p, _ in rounds)
    best_time = min(t for p, t in rounds if p == best_pts)
    times_5000 = [t for p, t in rounds if p >= 5000]
    fastest_5000 = min(times_5000) if times_5000 else None
    return int(best_pts), int(best_time), (int(fastest_5000) if fastest_5000 is not None else None)


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


def fetch_game_details_payload(
    session: requests.Session,
    game_token: str,
    timeout: float,
    debug: bool,
) -> Optional[dict]:
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
            if isinstance(payload, dict):
                return payload
        except Exception as e:
            debug_print(debug, f"[played_at] endpoint failed: {url} -> {e}")
            continue
    return None


def fetch_game_details_for_played_at(
    session: requests.Session,
    game_token: str,
    timeout: float,
    debug: bool,
) -> Optional[int]:
    payload = fetch_game_details_payload(session, game_token, timeout=timeout, debug=debug)
    if payload is None:
        return None
    return extract_played_at_epoch(payload)


def fetch_challenge_landing_meta(
    session: requests.Session,
    challenge_token: str,
    timeout: float,
    debug: bool,
) -> Tuple[str, str]:
    """
    Best-effort parse from challenge landing page HTML.
    Useful when no highscores exist yet.
    Returns: (map_name, rule_text)
    """
    url = f"https://www.geoguessr.com/challenge/{challenge_token}"
    try:
        html_txt = http_get_text(session, url, timeout=timeout, debug=debug)
    except Exception as e:
        debug_print(debug, f"[landing-meta] failed for {challenge_token}: {e}")
        return "", ""

    labels_raw = SETTING_LABEL_RE.findall(html_txt)
    labels = [_clean_setting_label(x) for x in labels_raw if _clean_setting_label(x)]
    if not labels:
        return "", ""

    map_name = labels[0] if len(labels) >= 1 else ""
    time_label = labels[1] if len(labels) >= 2 else ""
    labels_lc = [x.lower() for x in labels]

    moving_allowed = any("moving allowed" in t for t in labels_lc)
    moving_not_allowed = any("moving not allowed" in t for t in labels_lc)
    panning_allowed = any("panning allowed" in t for t in labels_lc)
    panning_not_allowed = any("panning not allowed" in t for t in labels_lc)
    zooming_allowed = any("zooming allowed" in t for t in labels_lc)
    zooming_not_allowed = any("zooming not allowed" in t for t in labels_lc)

    mode = ""
    if moving_not_allowed:
        if panning_not_allowed and zooming_not_allowed:
            mode = "NMPZ"
        elif panning_not_allowed:
            mode = "NMP"
        else:
            mode = "NM"
    elif moving_allowed:
        mode = "Moving"

    secs = _parse_time_limit_seconds(time_label)
    time_part = ""
    if secs is not None:
        if secs % 60 == 0:
            time_part = f"{secs // 60} min"
        else:
            time_part = f"{secs}s"
    elif time_label:
        time_part = time_label

    parts = [p for p in [mode, time_part] if p]
    rule_text = " - ".join(parts)

    # If flags were present but mode wasn't resolved, infer from negation labels.
    if not rule_text and (panning_allowed or panning_not_allowed or zooming_allowed or zooming_not_allowed):
        forbid_moving = moving_not_allowed
        forbid_rotating = panning_not_allowed
        forbid_zooming = zooming_not_allowed
        pseudo_game = {
            "forbidMoving": forbid_moving,
            "forbidRotating": forbid_rotating,
            "forbidZooming": forbid_zooming,
            "timeLimit": secs,
        }
        rule_text = rule_text_from_game(pseudo_game)

    return map_name, rule_text


def _clean_html_text(fragment: str) -> str:
    txt = re.sub(r"<[^>]+>", "", str(fragment or ""))
    return " ".join(html.unescape(txt).replace("\xa0", " ").split()).strip()


def _parse_distance_meters(text: str) -> Optional[float]:
    s = _clean_html_text(text).lower().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*(km|m)\b", s)
    if not m:
        return None
    value = float(m.group(1))
    return value * 1000.0 if m.group(2) == "km" else value


def _parse_duration_seconds(text: str) -> Optional[int]:
    s = _clean_html_text(text).lower()
    mins = 0
    secs = 0
    m_min = re.search(r"(\d+)\s*min", s)
    m_sec = re.search(r"(\d+)\s*sec", s)
    if m_min:
        mins = int(m_min.group(1))
    if m_sec:
        secs = int(m_sec.group(1))
    if m_min or m_sec:
        return mins * 60 + secs
    return None


def _parse_steps_count(text: str) -> Optional[int]:
    s = _clean_html_text(text).lower()
    m = re.search(r"(\d[\d,]*)\s*steps?\b", s)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def _parse_score_points_text(text: str) -> Optional[int]:
    s = _clean_html_text(text)
    m = re.search(r"(\d[\d,]*)\s*pts\b", s, re.I)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def _build_player_metrics_from_round_rows(round_rows: List[dict]) -> dict:
    rounds_5000 = [row for row in round_rows if int(row.get("pts") or 0) >= 5000]
    fastest_5000 = None
    if rounds_5000:
        fastest_5000 = min(
            rounds_5000,
            key=lambda row: (int(row.get("time_s") or 10**12), int(row.get("steps") or 10**12)),
        )

    step_values = [int(row["steps"]) for row in round_rows if row.get("steps") is not None]
    distance_values = [float(row["distance_m"]) for row in round_rows if row.get("distance_m") is not None]
    avg_steps = (sum(step_values) / len(step_values)) if step_values else None

    return {
        "round_rows": round_rows,
        "total_distance_m": (sum(distance_values) if distance_values else None),
        "total_steps": (sum(step_values) if step_values else None),
        "avg_steps_per_round": avg_steps,
        "count_5000_rounds": len(rounds_5000),
        "fastest_5000_round_steps": (int(fastest_5000["steps"]) if fastest_5000 and fastest_5000.get("steps") is not None else None),
        "fastest_5000_round_distance_m": (float(fastest_5000["distance_m"]) if fastest_5000 and fastest_5000.get("distance_m") is not None else None),
    }


def _extract_next_data_payload(html_txt: str) -> Optional[dict]:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', str(html_txt or ""), re.S)
    if not m:
        return None
    try:
        return json.loads(html.unescape(m.group(1)))
    except Exception:
        return None


def _extract_round_distance_from_guess(guess: dict) -> Optional[float]:
    candidates = [
        guess.get("distance"),
        guess.get("distanceInMeters"),
        guess.get("distanceMeters"),
        guess.get("distanceInMetres"),
        guess.get("distanceMetres"),
        guess.get("meters"),
        guess.get("metres"),
    ]
    for cand in candidates:
        if cand is None or isinstance(cand, bool):
            continue
        if isinstance(cand, (int, float)):
            if math.isfinite(float(cand)):
                return float(cand)
            continue
        try:
            return float(str(cand).replace(",", "").strip())
        except Exception:
            pass

    for key in ("player", "guess", "result"):
        sub = guess.get(key)
        if isinstance(sub, dict):
            v = _extract_round_distance_from_guess(sub)
            if v is not None:
                return v
    return None


def _extract_round_steps_from_guess(guess: dict) -> Optional[int]:
    candidates = [
        guess.get("steps"),
        guess.get("stepsCount"),
        guess.get("moveCount"),
        guess.get("moves"),
    ]
    for cand in candidates:
        v = _parse_int_maybe(cand)
        if v is not None:
            return v

    for key in ("player", "guess", "result"):
        sub = guess.get(key)
        if isinstance(sub, dict):
            v = _extract_round_steps_from_guess(sub)
            if v is not None:
                return v
    return None


def extract_round_metrics_from_payload(game_payload: Any) -> dict:
    if not isinstance(game_payload, dict):
        return {}

    candidate_lists: List[Any] = []
    player_obj = game_payload.get("player")
    if isinstance(player_obj, dict):
        candidate_lists.extend(
            [
                player_obj.get("guesses"),
                player_obj.get("rounds"),
                player_obj.get("guessResults"),
            ]
        )
    candidate_lists.extend([game_payload.get("rounds"), game_payload.get("guesses")])

    round_rows: List[dict] = []
    for cand in candidate_lists:
        if not isinstance(cand, list):
            continue
        for guess in cand:
            if not isinstance(guess, dict):
                continue
            pts = _extract_round_points_from_guess(guess)
            time_s = _extract_round_time_from_guess(guess)
            distance_m = _extract_round_distance_from_guess(guess)
            steps = _extract_round_steps_from_guess(guess)
            if pts is None and time_s is None and distance_m is None and steps is None:
                continue
            round_rows.append(
                {
                    "pts": pts,
                    "distance_m": distance_m,
                    "time_s": time_s,
                    "steps": steps,
                }
            )
        if round_rows:
            break

    return _build_player_metrics_from_round_rows(round_rows) if round_rows else {}


def _iter_next_data_player_guess_lists(obj: Any) -> Iterable[Tuple[str, List[dict]]]:
    stack = [obj]
    seen: set[int] = set()
    yielded: set[Tuple[str, int]] = set()
    while stack:
        cur = stack.pop()
        cur_id = id(cur)
        if cur_id in seen:
            continue
        seen.add(cur_id)

        if isinstance(cur, dict):
            nick = cur.get("nick")
            guesses = cur.get("guesses")
            if isinstance(nick, str) and nick.strip() and isinstance(guesses, list):
                key = (nick.strip(), id(guesses))
                if key not in yielded:
                    yielded.add(key)
                    yield nick.strip(), guesses
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)


def parse_result_metrics_next_data(html_txt: str) -> Dict[str, dict]:
    payload = _extract_next_data_payload(html_txt)
    if not isinstance(payload, dict):
        return {}

    out: Dict[str, dict] = {}
    page_props = (
        payload.get("props", {}).get("pageProps", {})
        if isinstance(payload.get("props"), dict)
        else {}
    )
    root = page_props if isinstance(page_props, dict) else payload

    for player, guesses in _iter_next_data_player_guess_lists(root):
        round_rows: List[dict] = []
        for guess in guesses:
            if not isinstance(guess, dict):
                continue
            pts = _extract_round_points_from_guess(guess)
            time_s = _extract_round_time_from_guess(guess)
            distance_m = _extract_round_distance_from_guess(guess)
            steps = _extract_round_steps_from_guess(guess)
            if pts is None and time_s is None and distance_m is None and steps is None:
                continue
            round_rows.append(
                {
                    "pts": pts,
                    "distance_m": distance_m,
                    "time_s": time_s,
                    "steps": steps,
                }
            )

        if not round_rows:
            continue
        out[player] = _build_player_metrics_from_round_rows(round_rows)

    return out


def _merge_result_metrics(primary: Dict[str, dict], secondary: Dict[str, dict]) -> Dict[str, dict]:
    if not primary:
        return dict(secondary)
    if not secondary:
        return dict(primary)

    merged: Dict[str, dict] = {}
    for player in set(primary) | set(secondary):
        base = dict(secondary.get(player, {}))
        base.update({k: v for k, v in primary.get(player, {}).items() if v is not None})
        merged[player] = base
    return merged


def parse_result_metrics_html(html_txt: str) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    if "coordinate-results_player__" not in str(html_txt or ""):
        return out

    segments = re.split(r'<div class="coordinate-results_rowDivider__[^"]*"></div>', html_txt)
    for seg in segments:
        if "coordinate-results_player__" not in seg:
            continue

        nick_match = re.search(r'<div class="user-nick_nick__[^"]*">(.*?)</div>', seg, re.S)
        if not nick_match:
            continue
        player = _clean_html_text(nick_match.group(1)).strip()
        if not player:
            continue

        blocks = re.findall(
            r'<div class="score-cell_score__[^"]*">(.*?)</div>\s*<div class="score-cell_scoreDetails__[^"]*">(.*?)</div>',
            seg,
            re.S,
        )
        if not blocks:
            continue

        round_rows: List[dict] = []
        for score_html, details_html in blocks[:-1]:
            spans = re.findall(r"<span>(.*?)</span>", details_html, re.S)
            round_rows.append(
                {
                    "pts": _parse_score_points_text(score_html),
                    "distance_m": _parse_distance_meters(spans[0]) if len(spans) >= 1 else None,
                    "time_s": _parse_duration_seconds(spans[1]) if len(spans) >= 2 else None,
                    "steps": _parse_steps_count(spans[2]) if len(spans) >= 3 else None,
                }
            )

        total_spans = re.findall(r"<span>(.*?)</span>", blocks[-1][1], re.S)
        total_distance_m = _parse_distance_meters(total_spans[0]) if len(total_spans) >= 1 else None
        total_steps = _parse_steps_count(total_spans[2]) if len(total_spans) >= 3 else None
        metrics = _build_player_metrics_from_round_rows(round_rows)
        metrics["total_distance_m"] = total_distance_m if total_distance_m is not None else metrics.get("total_distance_m")
        metrics["total_steps"] = total_steps if total_steps is not None else metrics.get("total_steps")
        out[player] = metrics
    return out


def fetch_result_metrics_for_map(
    session: requests.Session,
    challenge_token: str,
    timeout: float,
    debug: bool,
) -> Dict[str, dict]:
    endpoints = [
        f"https://www.geoguessr.com/results/{challenge_token}",
        f"https://www.geoguessr.com/challenge/{challenge_token}",
    ]
    for url in endpoints:
        try:
            html_txt = http_get_text(session, url, timeout=timeout, debug=debug)
            parsed_next_data = parse_result_metrics_next_data(html_txt)
            parsed_html = parse_result_metrics_html(html_txt)
            parsed = _merge_result_metrics(parsed_html, parsed_next_data)
            if parsed:
                return parsed
        except Exception as e:
            debug_print(debug, f"[result-metrics] endpoint failed: {url} -> {e}")
            continue
    return {}


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

    game_payload_cache: Dict[str, Optional[dict]] = {}

    for map_idx, url in enumerate(urls, start=1):
        token = extract_token(url)
        map_name = f"Map {map_idx}"
        rule_text = ""
        mode_category = "unknown"
        is_sweden = map_idx in set(week.sweden_maps)
        html_metrics_by_player: Dict[str, dict] = {}
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
            landing_name, landing_rule = fetch_challenge_landing_meta(session, token, timeout=timeout, debug=debug)
            if landing_name:
                map_name = landing_name
            if landing_rule:
                rule_text = landing_rule
                mode_category = mode_category_from_rule_text(landing_rule)
            map_meta_rows.append(
                {
                    "week": week.label,
                    "map_index": map_idx,
                    "map_url": url,
                    "map_name": map_name,
                    "rule_text": rule_text,
                    "mode_category": mode_category,
                    "is_sweden": bool(is_sweden),
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
                mode_category = mode_category_from_game(game0)
                html_metrics_by_player = fetch_result_metrics_for_map(session, token, timeout=timeout, debug=debug)
            except Exception:
                rule_text = ""
        else:
            landing_name, landing_rule = fetch_challenge_landing_meta(session, token, timeout=timeout, debug=debug)
            if landing_name:
                map_name = landing_name
            if landing_rule:
                rule_text = landing_rule
                mode_category = mode_category_from_rule_text(landing_rule)

        map_meta_rows.append(
            {
                "week": week.label,
                "map_index": map_idx,
                "map_url": url,
                "map_name": map_name or f"Map {map_idx}",
                "rule_text": rule_text or "",
                "mode_category": mode_category,
                "is_sweden": bool(is_sweden),
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
            best_round_pts, best_round_time, fastest_5000_round_time = extract_round_stats_from_item(it)
            html_metrics = html_metrics_by_player.get(name, {})

            try:
                game_token = it["game"].get("token")
            except Exception:
                game_token = None

            game_payload: Optional[dict] = None
            api_metrics: Dict[str, Any] = {}
            if isinstance(game_token, str) and game_token:
                if game_token in game_payload_cache:
                    game_payload = game_payload_cache[game_token]
                else:
                    game_payload = fetch_game_details_payload(session, game_token, timeout=timeout, debug=debug)
                    game_payload_cache[game_token] = game_payload
                if isinstance(game_payload, dict):
                    api_metrics = extract_round_metrics_from_payload(game_payload)

            merged_metrics = dict(html_metrics)
            if api_metrics:
                merged_metrics = _merge_result_metrics({"_player": api_metrics}, {"_player": html_metrics}).get("_player", {})

            # played_at: reuse fetched game payload when available
            played_at: Optional[int] = None
            if fetch_played_at:
                if isinstance(game_token, str) and game_token:
                    if isinstance(game_payload, dict):
                        played_at = extract_played_at_epoch(game_payload)
                    elif game_token in game_payload_cache and isinstance(game_payload_cache[game_token], dict):
                        played_at = extract_played_at_epoch(game_payload_cache[game_token])
                    else:
                        played_at = fetch_game_details_for_played_at(session, game_token, timeout=timeout, debug=debug)

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
                    best_round_pts=best_round_pts,
                    best_round_time=best_round_time,
                    fastest_5000_round_time=fastest_5000_round_time,
                    total_distance_m=(float(merged_metrics.get("total_distance_m")) if merged_metrics.get("total_distance_m") is not None else None),
                    total_steps=(int(merged_metrics.get("total_steps")) if merged_metrics.get("total_steps") is not None else None),
                    avg_steps_per_round=(float(merged_metrics.get("avg_steps_per_round")) if merged_metrics.get("avg_steps_per_round") is not None else None),
                    count_5000_rounds=(int(merged_metrics.get("count_5000_rounds")) if merged_metrics.get("count_5000_rounds") is not None else None),
                    fastest_5000_round_steps=(int(merged_metrics.get("fastest_5000_round_steps")) if merged_metrics.get("fastest_5000_round_steps") is not None else None),
                    fastest_5000_round_distance_m=(float(merged_metrics.get("fastest_5000_round_distance_m")) if merged_metrics.get("fastest_5000_round_distance_m") is not None else None),
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
    meta_cols = ["week", "map_index", "map_url", "map_name", "rule_text", "mode_category", "is_sweden"]
    if not entries:
        if map_meta_rows:
            df_week_meta = pd.DataFrame(map_meta_rows, columns=meta_cols)
            if not df_week_meta.empty:
                df_week_meta = (
                    df_week_meta.drop_duplicates(subset=["week", "map_index"], keep="last")
                    .sort_values(["week", "map_index"])
                    .reset_index(drop=True)
                )
                df_week_meta = slot_metadata_from_week_maps(df_week_meta)
        else:
            df_week_meta = pd.DataFrame(columns=meta_cols + ["slot_key", "slot_label"])
        return (
            pd.DataFrame(
                columns=[
                    "week",
                    "map_index",
                    "map_url",
                    "map_name",
                    "rule_text",
                    "mode_category",
                    "is_sweden",
                    "slot_key",
                    "slot_label",
                    "player",
                    "total_pts",
                    "total_time",
                    "best_round_pts",
                    "best_round_time",
                    "fastest_5000_round_time",
                    "total_distance_m",
                    "total_steps",
                    "avg_steps_per_round",
                    "count_5000_rounds",
                    "fastest_5000_round_steps",
                    "fastest_5000_round_distance_m",
                    "rank_best",
                    "borda_points",
                    "played_at_epoch",
                ]
            ),
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
        "best_round_pts": e.best_round_pts,
        "best_round_time": e.best_round_time,
        "fastest_5000_round_time": e.fastest_5000_round_time,
        "total_distance_m": e.total_distance_m,
        "total_steps": e.total_steps,
        "avg_steps_per_round": e.avg_steps_per_round,
        "count_5000_rounds": e.count_5000_rounds,
        "fastest_5000_round_steps": e.fastest_5000_round_steps,
        "fastest_5000_round_distance_m": e.fastest_5000_round_distance_m,
        "played_at_epoch": e.played_at_epoch,
    } for e in entries])

    # discovered meta per map from result payloads
    df_week_meta_seen = (
        df[["week", "map_index", "map_url", "map_name", "rule_text"]]
        .drop_duplicates()
        .sort_values(["week", "map_index"])
        .reset_index(drop=True)
    )
    df_week_meta_seen["mode_category"] = df_week_meta_seen["rule_text"].apply(mode_category_from_rule_text)
    df_week_meta_seen["is_sweden"] = False

    if map_meta_rows:
        df_week_meta_base = pd.DataFrame(map_meta_rows, columns=meta_cols)
        if not df_week_meta_base.empty:
            df_week_meta_base = (
                df_week_meta_base.drop_duplicates(subset=["week", "map_index"], keep="last")
                .sort_values(["week", "map_index"])
                .reset_index(drop=True)
            )
    else:
        df_week_meta_base = pd.DataFrame(columns=meta_cols)

    if df_week_meta_base.empty:
        df_week_meta = df_week_meta_seen
    else:
        df_week_meta = df_week_meta_base.merge(
            df_week_meta_seen.rename(
                columns={
                    "map_url": "seen_map_url",
                    "map_name": "seen_map_name",
                    "rule_text": "seen_rule_text",
                    "mode_category": "seen_mode_category",
                    "is_sweden": "seen_is_sweden",
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
        df_week_meta["mode_category"] = [
            str(b).strip() if str(b).strip() else str(s).strip()
            for b, s in zip(df_week_meta.get("mode_category", pd.Series(dtype=str)), df_week_meta.get("seen_mode_category", pd.Series(dtype=str)))
        ]
        df_week_meta["is_sweden"] = [
            bool(b) if pd.notna(b) else bool(s)
            for b, s in zip(df_week_meta.get("is_sweden", pd.Series(dtype=bool)), df_week_meta.get("seen_is_sweden", pd.Series(dtype=bool)))
        ]
        df_week_meta = df_week_meta[meta_cols]

    df_week_meta = slot_metadata_from_week_maps(df_week_meta)
    df = df.merge(
        df_week_meta[["week", "map_index", "mode_category", "is_sweden", "slot_key", "slot_label"]],
        on=["week", "map_index"],
        how="left",
    )

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
        "cat_sverige_moving",
        "cat_sverige_no_move",
    ]
    base_cols_stats = base_cols_total + ["best_week", "best_week_borda", "best_week_pts"]

    if df_overview.empty:
        total = pd.DataFrame(columns=base_cols_total)
        stats = pd.DataFrame(columns=base_cols_stats)
        return total, stats

    dfo = df_overview.copy()
    dfo["week_map_key"] = dfo["week"].astype(str) + "::" + dfo["map_index"].astype(str)
    if "slot_key" not in dfo.columns:
        dfo["slot_key"] = dfo["map_index"].apply(map_slot_key)
    dfo["mode_category"] = dfo.get("mode_category", pd.Series(index=dfo.index, dtype=object)).fillna("unknown").astype(str)
    dfo["is_sweden"] = dfo.get("is_sweden", pd.Series(index=dfo.index, dtype=bool)).fillna(False).astype(bool)

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

    mode_scores = (
        dfo.groupby(["player", "mode_category"], as_index=False)
        .agg(mode_borda=("borda_points", "sum"))
    )
    if not mode_scores.empty:
        mode_pivot = (
            mode_scores.pivot_table(index="player", columns="mode_category", values="mode_borda", aggfunc="sum")
            .fillna(0.0)
            .reset_index()
        )
        by_player = by_player.merge(mode_pivot, on="player", how="left")
    for key in ["moving", "no_move", "nmpz"]:
        if key not in by_player.columns:
            by_player[key] = 0.0

    sverige_scores = (
        dfo[dfo["is_sweden"]]
        .groupby(["player", "mode_category"], as_index=False)
        .agg(mode_borda=("borda_points", "sum"))
    )
    sverige_pivot = pd.DataFrame(columns=["player", "moving", "no_move"])
    if not sverige_scores.empty:
        sverige_pivot = (
            sverige_scores.pivot_table(index="player", columns="mode_category", values="mode_borda", aggfunc="sum")
            .fillna(0.0)
            .reset_index()
        )
        by_player = by_player.merge(
            sverige_pivot.rename(columns={"moving": "sverige_moving", "no_move": "sverige_no_move"}),
            on="player",
            how="left",
        )
    for key in ["sverige_moving", "sverige_no_move"]:
        if key not in by_player.columns:
            by_player[key] = 0.0

    by_player["cat_moving"] = by_player["moving"]
    by_player["cat_no_move"] = by_player["no_move"]
    by_player["cat_nmpz"] = by_player["nmpz"]
    by_player["cat_sverige"] = by_player["sverige_moving"] + by_player["sverige_no_move"]
    by_player["cat_sverige_moving"] = by_player["sverige_moving"]
    by_player["cat_sverige_no_move"] = by_player["sverige_no_move"]

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
            out[league_name] = pd.DataFrame(columns=["player", "league_points", "avg_pts_per_map", "maps_counted", "weeks_counted"])
        return out

    dfo = df_overview.copy()
    dfo["week_map_key"] = dfo["week"].astype(str) + "::" + dfo["map_index"].astype(str)
    dfo["mode_category"] = dfo.get("mode_category", pd.Series(index=dfo.index, dtype=object)).fillna("unknown").astype(str)
    dfo["is_sweden"] = dfo.get("is_sweden", pd.Series(index=dfo.index, dtype=bool)).fillna(False).astype(bool)

    league_filters = {
        "Moving": (dfo["mode_category"] == "moving"),
        "No move": (dfo["mode_category"] == "no_move"),
        "NMPZ": (dfo["mode_category"] == "nmpz"),
        "Sverige": (dfo["is_sweden"] & dfo["mode_category"].isin(["moving", "no_move"])),
        "Sverige Moving": (dfo["is_sweden"] & (dfo["mode_category"] == "moving")),
        "Sverige No Move": (dfo["is_sweden"] & (dfo["mode_category"] == "no_move")),
    }

    for league_name in SUBLEAGUE_SLOT_KEYS:
        part = dfo[league_filters.get(league_name, pd.Series(False, index=dfo.index))]
        if part.empty:
            out[league_name] = pd.DataFrame(columns=["player", "league_points", "avg_pts_per_map", "maps_counted", "weeks_counted"])
            continue

        table = (
            part.groupby("player", as_index=False)
            .agg(
                league_points=("borda_points", "sum"),
                avg_pts_per_map=("total_pts", "mean"),
                maps_counted=("week_map_key", "nunique"),
                weeks_counted=("week", "nunique"),
            )
            .sort_values(["league_points", "avg_pts_per_map"], ascending=[False, False])
            .reset_index(drop=True)
        )
        out[league_name] = table
    return out


def normalize_sort_key(raw: str) -> str:
    s = (raw or "").strip().lower()
    aliases = {
        "": "default",
        "default": "default",
        "standard": "default",
        "poang": "points",
        "poäng": "points",
        "points": "points",
        "liga_poang": "points",
        "ligapoang": "points",
        "liga_poäng": "points",
        "ligapoäng": "points",
        "total_pts": "total_pts",
        "total pts": "total_pts",
        "total": "total_pts",
        "kartor": "maps",
        "maps": "maps",
        "veckor": "weeks",
        "weeks": "weeks",
        "snitt_pts": "avg_pts",
        "snitt pts": "avg_pts",
        "snitt_pts_per_karta": "avg_pts",
        "avg_pts": "avg_pts",
        "avg_pts_per_map": "avg_pts",
        "snitt_poang": "avg_points",
        "snitt poang": "avg_points",
        "snitt_poäng": "avg_points",
        "snitt poäng": "avg_points",
        "snitt_ligapoang": "avg_points",
        "snitt_ligapoäng": "avg_points",
        "avg_points": "avg_points",
        "avg_borda_per_map": "avg_points",
    }
    return aliases.get(s, "default")


def _num_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return pd.Series([0.0] * len(df), index=df.index, dtype=float)


def sort_total_table(df_total: pd.DataFrame, sort_by: str) -> pd.DataFrame:
    if df_total.empty:
        return df_total
    key = normalize_sort_key(sort_by)
    t = df_total.copy()
    t["_points"] = _num_col(t, "total_borda")
    t["_total_pts"] = _num_col(t, "total_pts")
    t["_maps"] = _num_col(t, "maps_counted")
    t["_weeks"] = _num_col(t, "weeks_counted")
    t["_avg_pts"] = _num_col(t, "avg_pts_per_map")
    t["_avg_points"] = _num_col(t, "avg_borda_per_map")

    by_key: Dict[str, Tuple[List[str], List[bool]]] = {
        "default": (["_points", "_total_pts", "_maps", "player"], [False, False, False, True]),
        "points": (["_points", "_total_pts", "_maps", "player"], [False, False, False, True]),
        "total_pts": (["_total_pts", "_points", "_maps", "player"], [False, False, False, True]),
        "maps": (["_maps", "_points", "_total_pts", "player"], [False, False, False, True]),
        "weeks": (["_weeks", "_points", "_total_pts", "player"], [False, False, False, True]),
        "avg_pts": (["_avg_pts", "_total_pts", "_points", "player"], [False, False, False, True]),
        "avg_points": (["_avg_points", "_points", "_total_pts", "player"], [False, False, False, True]),
    }
    cols, asc = by_key.get(key, by_key["default"])
    return t.sort_values(cols, ascending=asc).drop(columns=[c for c in t.columns if c.startswith("_")]).reset_index(drop=True)


def sort_subleague_table(table: pd.DataFrame, sort_by: str) -> pd.DataFrame:
    if table.empty:
        return table
    key = normalize_sort_key(sort_by)
    t = table.copy()
    t["_points"] = _num_col(t, "league_points")
    t["_maps"] = _num_col(t, "maps_counted")
    t["_weeks"] = _num_col(t, "weeks_counted")
    t["_avg_pts"] = _num_col(t, "avg_pts_per_map")
    t["_avg_points"] = t["_points"] / t["_maps"].clip(lower=1.0)

    by_key: Dict[str, Tuple[List[str], List[bool]]] = {
        "default": (["_points", "_avg_pts", "_maps", "player"], [False, False, False, True]),
        "points": (["_points", "_avg_pts", "_maps", "player"], [False, False, False, True]),
        "total_pts": (["_avg_pts", "_points", "_maps", "player"], [False, False, False, True]),
        "maps": (["_maps", "_points", "_avg_pts", "player"], [False, False, False, True]),
        "weeks": (["_weeks", "_points", "_avg_pts", "player"], [False, False, False, True]),
        "avg_pts": (["_avg_pts", "_points", "_maps", "player"], [False, False, False, True]),
        "avg_points": (["_avg_points", "_points", "_avg_pts", "player"], [False, False, False, True]),
    }
    cols, asc = by_key.get(key, by_key["default"])
    return t.sort_values(cols, ascending=asc).drop(columns=[c for c in t.columns if c.startswith("_")]).reset_index(drop=True)


def _clean_round_time_for_table(v: Any) -> Optional[int]:
    iv = _parse_int_maybe(v)
    if iv is None:
        return None
    if iv >= 10**11:
        return None
    return int(iv)


def compute_fast_round_tables(df_overview: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Per player/category:
      - if player has at least one 5000 round: pick fastest such round
      - otherwise pick player's highest single-round score
    """
    out: Dict[str, pd.DataFrame] = {}
    categories = ["Sverige", "Världen"]
    base_cols = ["player", "round_pts", "round_time", "result_type"]

    if df_overview.empty:
        for k in categories:
            out[k] = pd.DataFrame(columns=base_cols)
        return out

    dfo = df_overview.copy()
    dfo["is_sweden"] = dfo.get("is_sweden", pd.Series(index=dfo.index, dtype=bool)).fillna(False).astype(bool)

    dfo["total_pts"] = pd.to_numeric(dfo.get("total_pts"), errors="coerce").fillna(0.0)
    dfo["best_round_pts"] = pd.to_numeric(dfo.get("best_round_pts"), errors="coerce").fillna(0.0)
    dfo["best_round_time"] = pd.to_numeric(dfo.get("best_round_time"), errors="coerce").fillna(10**12)
    dfo["fastest_5000_round_time"] = pd.to_numeric(dfo.get("fastest_5000_round_time"), errors="coerce")

    for cat_name in categories:
        part = dfo[dfo["is_sweden"]] if cat_name == "Sverige" else dfo[~dfo["is_sweden"]]
        if part.empty:
            out[cat_name] = pd.DataFrame(columns=base_cols)
            continue

        rows: List[dict] = []
        for player, grp in part.groupby("player", sort=False):
            g = grp.copy()
            g5000 = g[g["fastest_5000_round_time"].notna()].copy()

            if not g5000.empty:
                g5000 = g5000.sort_values(
                    ["fastest_5000_round_time", "best_round_time", "total_pts"],
                    ascending=[True, True, False],
                )
                pick = g5000.iloc[0]
                rows.append(
                    {
                        "player": str(player),
                        "round_pts": 5000,
                        "round_time": _clean_round_time_for_table(pick.get("fastest_5000_round_time")),
                        "result_type": "5000",
                    }
                )
                continue

            g = g.sort_values(
                ["best_round_pts", "best_round_time", "total_pts"],
                ascending=[False, True, False],
            )
            pick = g.iloc[0]
            rows.append(
                {
                    "player": str(player),
                    "round_pts": int(_parse_int_maybe(pick.get("best_round_pts")) or 0),
                    "round_time": _clean_round_time_for_table(pick.get("best_round_time")),
                    "result_type": "Högsta runda",
                }
            )

        table = pd.DataFrame(rows, columns=base_cols)
        if not table.empty:
            table["has_5000"] = table["result_type"].eq("5000")
            table["round_time_sort"] = pd.to_numeric(table["round_time"], errors="coerce").fillna(10**12)
            table = (
                table.sort_values(
                    ["has_5000", "round_pts", "round_time_sort", "player"],
                    ascending=[False, False, True, True],
                )
                .drop(columns=["has_5000", "round_time_sort"])
                .reset_index(drop=True)
            )
        out[cat_name] = table

    return out


def format_seconds_compact(v: Any) -> str:
    secs = _parse_int_maybe(v)
    if secs is None or secs < 0:
        return ""
    mins = secs // 60
    rem = secs % 60
    if mins <= 0:
        return f"{secs} s"
    return f"{mins} min {rem} s"


def _series_zscore(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    if s.empty:
        return pd.Series(dtype=float, index=s.index)
    mean = float(s.mean())
    std = float(s.std(ddof=0))
    if std <= 1e-9 or math.isnan(std):
        return pd.Series([0.0] * len(s), index=s.index, dtype=float)
    return (s - mean) / std


def _series_index_0_100(series: pd.Series, *, higher_is_better: bool = True) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    if s.empty:
        return pd.Series(dtype=float, index=s.index)
    lo = float(s.min())
    hi = float(s.max())
    if math.isnan(lo) or math.isnan(hi) or abs(hi - lo) <= 1e-9:
        return pd.Series([50.0] * len(s), index=s.index, dtype=float)
    scaled = 100.0 * (s - lo) / (hi - lo)
    if not higher_is_better:
        scaled = 100.0 - scaled
    return scaled.clip(lower=0.0, upper=100.0)


def _fill_color_from_scale(value: Any, *, low_rgb: Tuple[int, int, int], high_rgb: Tuple[int, int, int]) -> PatternFill:
    v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(v):
        return WHITE
    ratio = min(1.0, max(0.0, float(v) / 100.0))
    rgb = tuple(
        int(round(low + (high - low) * ratio))
        for low, high in zip(low_rgb, high_rgb)
    )
    hex_color = "".join(f"{part:02X}" for part in rgb)
    return PatternFill("solid", fgColor=hex_color)


def _similarity_fill(value: Any) -> PatternFill:
    v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(v):
        return WHITE
    ratio = min(1.0, max(0.0, (float(v) + 1.0) / 2.0))
    start = (244, 247, 251)
    end = (42, 119, 212)
    rgb = tuple(int(round(a + (b - a) * ratio)) for a, b in zip(start, end))
    return PatternFill("solid", fgColor="".join(f"{part:02X}" for part in rgb))


def style_feature_meta_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"feature_key": key, "feature_label": label, "weight": float(weight), "description": desc}
            for key, label, weight, desc in STYLE_FEATURE_SPECS
        ]
    )


def _style_player_subset(df_style: pd.DataFrame) -> pd.DataFrame:
    if df_style.empty:
        return df_style.copy()
    subset = df_style[df_style.get("is_qualified", pd.Series(dtype=bool)).fillna(False)].copy()
    if len(subset) >= 3:
        return subset
    return df_style.copy()


def _weighted_style_feature_frame(df_style: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    players_df = _style_player_subset(df_style)
    meta = style_feature_meta_df()
    if players_df.empty:
        return players_df, pd.DataFrame(columns=STYLE_VECTOR_COLUMNS), meta

    frame = players_df[["player"] + STYLE_VECTOR_COLUMNS].copy()
    for key, _, weight, _ in STYLE_FEATURE_SPECS:
        frame[key] = _series_zscore(frame[key]).fillna(0.0) * float(weight)
    return players_df, frame[["player"] + STYLE_VECTOR_COLUMNS], meta


def compute_style_pca(df_style: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    import numpy as np

    players_df, weighted_frame, meta = _weighted_style_feature_frame(df_style)
    empty_loadings = meta.assign(pc1_loading=0.0, pc2_loading=0.0)
    if weighted_frame.empty or len(weighted_frame) < 3:
        return pd.DataFrame(columns=["player", "pc1", "pc2"]), empty_loadings, {"pc1_pct": 0.0, "pc2_pct": 0.0}

    x_mat = weighted_frame[STYLE_VECTOR_COLUMNS].to_numpy(dtype=float)
    x_center = x_mat - x_mat.mean(axis=0, keepdims=True)
    _, svals, vt = np.linalg.svd(x_center, full_matrices=False)
    pcs = x_center @ vt.T[:, :2]

    player_pca = players_df.copy()
    player_pca["pc1"] = pcs[:, 0]
    player_pca["pc2"] = pcs[:, 1]

    exp = svals * svals
    exp_ratio = exp / exp.sum() if float(exp.sum()) > 0 else exp
    pc1_pct = float(exp_ratio[0] * 100.0) if len(exp_ratio) > 0 else 0.0
    pc2_pct = float(exp_ratio[1] * 100.0) if len(exp_ratio) > 1 else 0.0

    loadings = meta.copy()
    loadings["pc1_loading"] = [float(vt[0, idx]) if vt.shape[0] > 0 else 0.0 for idx in range(len(loadings))]
    loadings["pc2_loading"] = [float(vt[1, idx]) if vt.shape[0] > 1 else 0.0 for idx in range(len(loadings))]
    return player_pca, loadings, {"pc1_pct": pc1_pct, "pc2_pct": pc2_pct}


def _dominant_style_label(row: pd.Series) -> str:
    values = {
        "fivek_rate": float(pd.to_numeric(pd.Series([row.get("fivek_rate_index")]), errors="coerce").iloc[0] or 0.0),
        "fivek_speed": float(pd.to_numeric(pd.Series([row.get("fivek_speed_index")]), errors="coerce").iloc[0] or 0.0),
        "fivek_steps": float(pd.to_numeric(pd.Series([row.get("fivek_steps_moving_index")]), errors="coerce").iloc[0] or 0.0),
        "best_eff": float(pd.to_numeric(pd.Series([row.get("best_round_efficiency_index")]), errors="coerce").iloc[0] or 0.0),
        "time_eff": float(pd.to_numeric(pd.Series([row.get("map_time_efficiency_index")]), errors="coerce").iloc[0] or 0.0),
        "consistency": float(pd.to_numeric(pd.Series([row.get("consistency_index")]), errors="coerce").iloc[0] or 0.0),
        "clutch": float(pd.to_numeric(pd.Series([row.get("clutch_profile_index")]), errors="coerce").iloc[0] or 0.0),
        "moving": float(pd.to_numeric(pd.Series([row.get("moving_strength")]), errors="coerce").iloc[0] or 0.0),
        "no_move": float(pd.to_numeric(pd.Series([row.get("no_move_strength")]), errors="coerce").iloc[0] or 0.0),
        "nmpz": float(pd.to_numeric(pd.Series([row.get("nmpz_strength")]), errors="coerce").iloc[0] or 0.0),
        "specialization": float(pd.to_numeric(pd.Series([row.get("specialization_index")]), errors="coerce").iloc[0] or 0.0),
    }

    if values["fivek_rate"] >= 72.0 and values["fivek_speed"] >= 62.0 and values["fivek_steps"] >= 55.0:
        return "5k-jagare"
    if values["time_eff"] >= 70.0 and values["best_eff"] >= 62.0:
        return "Effektiv avslutare"
    if values["consistency"] >= 70.0 and values["time_eff"] < 62.0:
        return "Metodisk & stabil"
    if values["specialization"] >= 72.0:
        if values["nmpz"] >= max(values["moving"], values["no_move"]):
            return "NMPZ-specialist"
        if values["no_move"] >= max(values["moving"], values["nmpz"]):
            return "No move-specialist"
        return "Moving-specialist"
    if values["clutch"] >= 68.0 and values["consistency"] < 52.0:
        return "Clutch & volatil"

    mode_scores = {
        "Moving-specialist": values["moving"],
        "No move-specialist": values["no_move"],
        "NMPZ-specialist": values["nmpz"],
    }
    top_mode = max(mode_scores, key=mode_scores.get)
    if mode_scores[top_mode] >= 68.0:
        return top_mode
    return "Allround"


def compute_style_tables(df_overview: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    base_style_cols = [
        "player",
        "maps_counted",
        "weeks_counted",
        "is_qualified",
        "fivek_rate_index",
        "fivek_speed_index",
        "fivek_steps_moving_index",
        "best_round_efficiency_index",
        "map_time_efficiency_index",
        "consistency_index",
        "clutch_profile_index",
        "moving_strength",
        "no_move_strength",
        "nmpz_strength",
        "specialization_index",
        "precision_support_index",
        "style_archetype",
    ] + STYLE_VECTOR_COLUMNS

    if df_overview.empty:
        return pd.DataFrame(columns=base_style_cols), pd.DataFrame()

    dfo = df_overview.copy()
    dfo["week_map_key"] = dfo["week"].astype(str) + "::" + dfo["map_index"].astype(str)
    dfo["mode3"] = dfo.get("mode_category", pd.Series(index=dfo.index, dtype=object)).fillna("unknown").astype(str).apply(mode_category_label)
    dfo["total_pts"] = pd.to_numeric(dfo.get("total_pts"), errors="coerce").fillna(0.0)
    dfo["total_time"] = pd.to_numeric(dfo.get("total_time"), errors="coerce")
    dfo["best_round_pts"] = pd.to_numeric(dfo.get("best_round_pts"), errors="coerce").fillna(0.0)
    dfo["best_round_time"] = pd.to_numeric(dfo.get("best_round_time"), errors="coerce")
    dfo["fastest_5000_round_time"] = pd.to_numeric(dfo.get("fastest_5000_round_time"), errors="coerce")

    dfo.loc[dfo["total_time"] >= 10**11, "total_time"] = pd.NA
    dfo.loc[dfo["best_round_time"] >= 10**11, "best_round_time"] = pd.NA

    map_stats = (
        dfo.groupby("week_map_key", as_index=False)
        .agg(
            map_pts_mean=("total_pts", "mean"),
            map_pts_std=("total_pts", lambda s: float(pd.to_numeric(s, errors="coerce").std(ddof=0) or 0.0)),
            map_time_mean=("total_time", "mean"),
            map_time_std=("total_time", lambda s: float(pd.to_numeric(s, errors="coerce").std(ddof=0) or 0.0)),
            best_round_pts_mean=("best_round_pts", "mean"),
            best_round_pts_std=("best_round_pts", lambda s: float(pd.to_numeric(s, errors="coerce").std(ddof=0) or 0.0)),
            best_round_time_mean=("best_round_time", "mean"),
            best_round_time_std=("best_round_time", lambda s: float(pd.to_numeric(s, errors="coerce").std(ddof=0) or 0.0)),
        )
    )
    dfo = dfo.merge(map_stats, on="week_map_key", how="left")

    pts_den = pd.to_numeric(dfo["map_pts_std"], errors="coerce").fillna(0.0).replace(0.0, 1.0)
    time_den = pd.to_numeric(dfo["map_time_std"], errors="coerce").fillna(0.0).replace(0.0, 1.0)
    best_pts_den = pd.to_numeric(dfo["best_round_pts_std"], errors="coerce").fillna(0.0).replace(0.0, 1.0)
    best_time_den = pd.to_numeric(dfo["best_round_time_std"], errors="coerce").fillna(0.0).replace(0.0, 1.0)
    dfo["pts_rel"] = (dfo["total_pts"] - pd.to_numeric(dfo["map_pts_mean"], errors="coerce").fillna(0.0)) / pts_den
    dfo["time_rel"] = (pd.to_numeric(dfo["total_time"], errors="coerce").fillna(pd.to_numeric(dfo["map_time_mean"], errors="coerce").fillna(0.0)) - pd.to_numeric(dfo["map_time_mean"], errors="coerce").fillna(0.0)) / time_den
    dfo["best_round_pts_rel"] = (dfo["best_round_pts"] - pd.to_numeric(dfo["best_round_pts_mean"], errors="coerce").fillna(0.0)) / best_pts_den
    dfo["best_round_time_rel"] = (
        pd.to_numeric(dfo["best_round_time"], errors="coerce").fillna(pd.to_numeric(dfo["best_round_time_mean"], errors="coerce").fillna(0.0))
        - pd.to_numeric(dfo["best_round_time_mean"], errors="coerce").fillna(0.0)
    ) / best_time_den
    dfo["fivek_hit"] = dfo["fastest_5000_round_time"].notna().astype(float)
    dfo["best_round_efficiency_row"] = dfo["best_round_pts_rel"] - 0.6 * dfo["best_round_time_rel"]
    dfo["clutch_signal"] = 0.65 * dfo["fivek_hit"] + 0.35 * dfo["best_round_efficiency_row"]
    dfo["fastest_5000_round_steps"] = pd.to_numeric(dfo.get("fastest_5000_round_steps"), errors="coerce")

    base = (
        dfo.groupby("player", as_index=False)
        .agg(
            maps_counted=("week_map_key", "nunique"),
            weeks_counted=("week", "nunique"),
            avg_time_sec=("total_time", "mean"),
            avg_pts=("total_pts", "mean"),
            map_time_efficiency_raw=("time_rel", lambda s: -float(pd.to_numeric(s, errors="coerce").mean() or 0.0)),
            precision_support_raw=("pts_rel", "mean"),
            fivek_rate_raw=("fivek_hit", "mean"),
            median_5k_time=("fastest_5000_round_time", "median"),
            best_round_efficiency_raw=("best_round_efficiency_row", "mean"),
            clutch_profile_raw=("clutch_signal", "mean"),
        )
    )

    mode_raw = (
        dfo[dfo["mode3"].isin(["Moving", "No move", "NMPZ"])]
        .groupby(["player", "mode3"], as_index=False)
        .agg(mode_strength=("pts_rel", "mean"))
    )
    mode_pivot = pd.DataFrame(columns=["player", "moving_strength_raw", "no_move_strength_raw", "nmpz_strength_raw"])
    if not mode_raw.empty:
        mode_pivot = (
            mode_raw.pivot_table(index="player", columns="mode3", values="mode_strength", aggfunc="mean")
            .rename(columns={"Moving": "moving_strength_raw", "No move": "no_move_strength_raw", "NMPZ": "nmpz_strength_raw"})
            .reset_index()
        )

    style_df = base.merge(mode_pivot, on="player", how="left")
    for col in ("moving_strength_raw", "no_move_strength_raw", "nmpz_strength_raw"):
        if col not in style_df.columns:
            style_df[col] = 0.0
        style_df[col] = pd.to_numeric(style_df[col], errors="coerce").fillna(0.0)

    median_times = pd.to_numeric(style_df["median_5k_time"], errors="coerce")
    if median_times.notna().any():
        valid = median_times.dropna()
        lo = float(valid.min())
        hi = float(valid.max())
        if abs(hi - lo) <= 1e-9:
            speed_component = pd.Series([1.0 if pd.notna(v) else 0.0 for v in median_times], index=style_df.index, dtype=float)
        else:
            speed_component = median_times.apply(lambda v: 0.0 if pd.isna(v) else 1.0 - ((float(v) - lo) / (hi - lo)))
    else:
        speed_component = pd.Series([0.0] * len(style_df), index=style_df.index, dtype=float)
    style_df["fivek_speed_raw"] = speed_component

    moving_5k_steps = (
        dfo[(dfo["mode3"] == "Moving") & dfo["fastest_5000_round_steps"].notna()]
        .groupby("player", as_index=False)
        .agg(moving_5k_steps_median=("fastest_5000_round_steps", "median"))
    )
    style_df = style_df.merge(moving_5k_steps, on="player", how="left")
    moving_steps_series = pd.to_numeric(style_df.get("moving_5k_steps_median"), errors="coerce")
    if moving_steps_series.notna().any():
        valid_steps = moving_steps_series.dropna()
        lo_steps = float(valid_steps.min())
        hi_steps = float(valid_steps.max())
        if abs(hi_steps - lo_steps) <= 1e-9:
            steps_component = pd.Series([1.0 if pd.notna(v) else 0.0 for v in moving_steps_series], index=style_df.index, dtype=float)
        else:
            steps_component = moving_steps_series.apply(lambda v: 0.0 if pd.isna(v) else 1.0 - ((float(v) - lo_steps) / (hi_steps - lo_steps)))
    else:
        steps_component = pd.Series([0.0] * len(style_df), index=style_df.index, dtype=float)
    style_df["fivek_steps_moving_raw"] = steps_component

    mode_strength_cols = ["moving_strength_raw", "no_move_strength_raw", "nmpz_strength_raw"]
    style_df["specialization_raw"] = style_df[mode_strength_cols].std(axis=1, ddof=0).fillna(0.0)

    mode_consistency = (
        dfo[dfo["mode3"].isin(["Moving", "No move", "NMPZ"])]
        .groupby(["player", "mode3"], as_index=False)
        .agg(mode_std=("pts_rel", lambda s: float(pd.to_numeric(s, errors="coerce").std(ddof=0) or 0.0)))
    )
    if mode_consistency.empty:
        consistency_by_player = pd.DataFrame(columns=["player", "mean_mode_std"])
    else:
        consistency_by_player = (
            mode_consistency.groupby("player", as_index=False)
            .agg(mean_mode_std=("mode_std", "mean"))
        )
    style_df = style_df.merge(consistency_by_player, on="player", how="left")
    style_df["consistency_raw"] = -pd.to_numeric(style_df.get("mean_mode_std"), errors="coerce").fillna(0.0)
    style_df["total_pts_support_raw"] = _series_zscore(style_df["avg_pts"]).fillna(0.0)

    style_df["is_qualified"] = (
        (pd.to_numeric(style_df["maps_counted"], errors="coerce").fillna(0.0) >= STYLE_MIN_MAPS)
        | (pd.to_numeric(style_df["weeks_counted"], errors="coerce").fillna(0.0) >= STYLE_MIN_WEEKS)
    )

    style_df["fivek_rate_index"] = _series_index_0_100(style_df["fivek_rate_raw"])
    style_df["fivek_speed_index"] = _series_index_0_100(style_df["fivek_speed_raw"])
    style_df["fivek_steps_moving_index"] = _series_index_0_100(style_df["fivek_steps_moving_raw"])
    style_df["best_round_efficiency_index"] = _series_index_0_100(style_df["best_round_efficiency_raw"])
    style_df["map_time_efficiency_index"] = _series_index_0_100(style_df["map_time_efficiency_raw"])
    style_df["consistency_index"] = _series_index_0_100(style_df["consistency_raw"])
    style_df["clutch_profile_index"] = _series_index_0_100(style_df["clutch_profile_raw"])
    style_df["moving_strength"] = _series_index_0_100(style_df["moving_strength_raw"])
    style_df["no_move_strength"] = _series_index_0_100(style_df["no_move_strength_raw"])
    style_df["nmpz_strength"] = _series_index_0_100(style_df["nmpz_strength_raw"])
    style_df["specialization_index"] = _series_index_0_100(style_df["specialization_raw"])
    style_df["precision_support_index"] = _series_index_0_100(style_df["precision_support_raw"])
    style_df["style_archetype"] = style_df.apply(_dominant_style_label, axis=1)

    _, weighted_frame, _ = _weighted_style_feature_frame(style_df)
    sim_df = pd.DataFrame()
    if not weighted_frame.empty:
        players = weighted_frame["player"].astype(str).tolist()
        rows: List[dict] = []
        vectors = weighted_frame[STYLE_VECTOR_COLUMNS].to_dict("records")
        for i, player_i in enumerate(players):
            vec_i = [float(vectors[i].get(col, 0.0) or 0.0) for col in STYLE_VECTOR_COLUMNS]
            norm_i = math.sqrt(sum(v * v for v in vec_i))
            row: Dict[str, Any] = {"player": player_i}
            for j, player_j in enumerate(players):
                vec_j = [float(vectors[j].get(col, 0.0) or 0.0) for col in STYLE_VECTOR_COLUMNS]
                norm_j = math.sqrt(sum(v * v for v in vec_j))
                denom = norm_i * norm_j
                sim = 0.0 if denom <= 1e-9 else sum(a * b for a, b in zip(vec_i, vec_j)) / denom
                row[player_j] = max(-1.0, min(1.0, float(sim)))
            rows.append(row)
        sim_df = pd.DataFrame(rows)

    ordered_cols = base_style_cols + [
        "avg_time_sec",
        "avg_pts",
        "median_5k_time",
        "moving_5k_steps_median",
        "mean_mode_std",
    ]
    for col in ordered_cols:
        if col not in style_df.columns:
            style_df[col] = ""
    style_df = style_df[ordered_cols].sort_values(
        ["is_qualified", "fivek_rate_index", "best_round_efficiency_index", "maps_counted", "player"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    return style_df, sim_df


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


def rank_row_fill(rank: int, fallback_fill: PatternFill) -> PatternFill:
    if rank == 1:
        return MEDAL_GOLD
    if rank == 2:
        return MEDAL_SILVER
    if rank == 3:
        return MEDAL_BRONZE
    return fallback_fill


def _sanitize_table_name(raw: str) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", str(raw or "").strip())
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "Table"
    if not base[0].isalpha():
        base = f"T_{base}"
    return base[:220]


def _collect_used_table_names(wb: Workbook) -> set[str]:
    used: set[str] = set()
    for ws in wb.worksheets:
        try:
            used.update({str(k) for k in ws.tables.keys()})
        except Exception:
            continue
    return used


def add_excel_table(
    wb: Workbook,
    ws,
    *,
    header_row: int,
    start_col: int,
    end_row: int,
    end_col: int,
    name_hint: str,
    header_horizontal: str = "left",
) -> None:
    """
    Adds an Excel table (sortable/filterable headers) for the given range.
    Requires at least one data row beneath the header.
    """
    if end_row <= header_row or end_col < start_col:
        return

    used = getattr(wb, "_table_names_used", None)
    if used is None:
        used = _collect_used_table_names(wb)
        setattr(wb, "_table_names_used", used)

    base = _sanitize_table_name(name_hint)
    name = base
    n = 1
    while name in used:
        n += 1
        name = f"{base}_{n}"

    # Give room for Excel's filter/sort dropdown glyph in header cells.
    for c in range(start_col, end_col + 1):
        col_letter = get_column_letter(c)
        dim = ws.column_dimensions[col_letter]
        current_w = float(dim.width) if dim.width is not None else 8.43
        grow = 2.2 if current_w < 12.0 else 1.0
        dim.width = current_w + grow

        hdr = ws.cell(header_row, c)
        old = hdr.alignment
        hdr.alignment = Alignment(
            horizontal=header_horizontal,
            vertical=(old.vertical if old is not None else "center"),
            wrap_text=(old.wrap_text if old is not None else False),
        )

    ref = f"{get_column_letter(start_col)}{header_row}:{get_column_letter(end_col)}{end_row}"
    tbl = Table(displayName=name, ref=ref)
    tbl.tableStyleInfo = TableStyleInfo(
        name="TableStyleLight1",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=False,
        showColumnStripes=False,
    )
    ws.add_table(tbl)
    used.add(name)


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
        slot_label = str(maps[i].get("slot_label") or f"Map {i+1}")
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
        base_fill = ROW_A if (idx % 2 == 1) else ROW_B
        fill = rank_row_fill(idx, base_fill)

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

    map_end_col = (col_map_start + n_maps - 1) if n_maps > 0 else col_total
    data_end_row = r_data_start + len(ordered) - 1
    add_excel_table(
        wb,
        ws,
        header_row=r3,
        start_col=col_rank,
        end_row=data_end_row,
        end_col=map_end_col,
        name_hint=f"Week_{week_label}",
        header_horizontal="center",
    )


def write_total_sheet(
    wb: Workbook,
    df_total: pd.DataFrame,
    df_overview: pd.DataFrame,
    weeks: List[str],
    sort_by: str = "default",
) -> None:
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

    sorted_total = sort_total_table(df_total, sort_by=sort_by)

    for idx, row in enumerate(sorted_total.itertuples(index=False), start=1):
        r = 2 + idx
        base_fill = ROW_A if (idx % 2 == 1) else ROW_B
        fill = rank_row_fill(idx, base_fill)

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

    add_excel_table(
        wb,
        ws,
        header_row=2,
        start_col=1,
        end_row=2 + len(sorted_total),
        end_col=7 + len(weeks),
        name_hint="Total",
    )


def write_stats_sheet(wb: Workbook, df_stats: pd.DataFrame, sort_by: str = "default") -> None:
    ws = wb.create_sheet("Stats")

    merge_and_style(ws, 1, 1, 1, 24, "Statistik", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    cols = [
        "#", "Spelare",
        "Poäng", "Total pts",
        "Kartor", "Veckor",
        "Moving 1", "Moving 2",
        "No move 1", "No move 2",
        "NMPZ 1", "NMPZ 2",
        "Moving", "No move", "NMPZ", "Sverige",
        "Sverige Moving", "Sverige No Move",
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
        17: 14.0, 18: 14.0,
        19: 14.0, 20: 14.0, 21: 12.0,
        22: 14.0, 23: 18.0, 24: 14.0,
    }
    set_col_widths(ws, widths)

    sorted_stats = sort_total_table(df_stats, sort_by=sort_by)

    for idx, row in enumerate(sorted_stats.itertuples(index=False), start=1):
        r = 2 + idx
        base_fill = ROW_A if (idx % 2 == 1) else ROW_B
        fill = rank_row_fill(idx, base_fill)

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
        ws.cell(r, 17).value = float(getattr(row, "cat_sverige_moving", 0) or 0)
        ws.cell(r, 18).value = float(getattr(row, "cat_sverige_no_move", 0) or 0)
        ws.cell(r, 19).value = float(row.avg_borda_per_map)
        ws.cell(r, 20).value = float(row.avg_borda_per_week)
        ws.cell(r, 21).value = float(row.avg_pts_per_map)
        ws.cell(r, 22).value = getattr(row, "best_week", "")
        ws.cell(r, 23).value = float(getattr(row, "best_week_borda", 0) or 0)
        ws.cell(r, 24).value = float(getattr(row, "best_week_pts", 0) or 0)

        for c in range(1, 25):
            align = LEFT if c == 2 else CENTER
            font = Font(color="000000", bold=True) if c in (3,) else FONT_BODY
            style_cell(ws, r, c, fill=fill, font=font, align=align)

    add_excel_table(
        wb,
        ws,
        header_row=2,
        start_col=1,
        end_row=2 + len(sorted_stats),
        end_col=24,
        name_hint="Stats",
    )


def write_visualizations_sheet(
    wb: Workbook,
    df_overview: pd.DataFrame,
    df_total: pd.DataFrame,
    df_style: pd.DataFrame,
    df_similarity: pd.DataFrame,
    weeks: List[str],
    image_dir: Optional[Path] = None,
) -> None:
    ws = wb.create_sheet("Visualiseringar")
    merge_and_style(ws, 1, 1, 1, 24, "Visualiseringar (Experiment)", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    viz_names = [
        "V1A: Tid vs poäng (Moving)",
        "V1B: Tid vs poäng (No move)",
        "V1C: Tid vs poäng (NMPZ)",
        "V2: Total råpoäng topp 20",
        "V3: Snitt GeoGuessr-poäng per karttyp och spelare",
        "V4: Aktiva spelare per vecka",
        "V5: Ligans snitt GeoGuessr-poäng per underliga-kategori",
        "V6: Topp-spelare per karttyp (snitt GeoGuessr-poäng)",
        "V7: Poängfördelning per karttyp",
        "V8: Stabilitet vs nivå (std mot snittpoäng)",
        "V9: Veckotrend-heatmap (över/under eget snitt)",
        "V10: Stil-PCA + bidrag till PC1/PC2",
        "V11: Featurevikter och PCA-komponenter",
        "V12: Placering vecka för vecka (kumulativ liga)",
        "V13: Ackumulerad ligapoäng vecka för vecka",
        "V14: Spelstilslikhet heatmap",
        "V15: 5k-effektivitet (frekvens vs fart)",
        "V16: Steg för 5k Sverige",
        "V17: Steg för 5k Världen",
    ]
    ws["A3"] = "Diagramöversikt:"
    ws["A3"].font = Font(bold=True, color="1B314B")
    for i, txt in enumerate(viz_names, start=4):
        ws[f"A{i}"] = txt
        ws[f"A{i}"].font = Font(color="1B314B")

    widths = {1: 52.0, 14: 52.0}
    for c in range(2, 14):
        widths[c] = 5.2
    set_col_widths(ws, widths)

    if df_overview.empty or df_total.empty:
        ws["A16"] = "Ingen data tillgänglig för visualiseringar."
        ws["A16"].font = Font(color="AA0000", bold=True)
        return

    if image_dir is None:
        image_dir = Path("visualizations") / "latest"
    image_dir.mkdir(parents=True, exist_ok=True)
    for old_png in image_dir.glob("V*.png"):
        try:
            old_png.unlink()
        except Exception:
            pass

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except Exception as ex:
        ws["A16"] = f"Kunde inte skapa visualiseringsbilder (matplotlib saknas): {ex}"
        ws["A16"].font = Font(color="AA0000", bold=True)
        return

    def _empty_plot(ax, text: str = "Ingen data") -> None:
        ax.text(0.5, 0.5, text, ha="center", va="center", transform=ax.transAxes, fontsize=12)
        ax.set_xticks([])
        ax.set_yticks([])

    BASE_FIG_W = 12.0
    BASE_FIG_H = 9.0  # 4:3
    plt.rcParams.update(
        {
            "font.size": 13,
            "axes.titlesize": 16,
            "axes.labelsize": 14,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
            "legend.fontsize": 11,
        }
    )

    def _save_fig(fig, filename: str, *, apply_tight_layout: bool = True) -> Path:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="This figure includes Axes that are not compatible with tight_layout",
            )
            warnings.filterwarnings(
                "ignore",
                message=r"Glyph \d+ .* missing from font\(s\) DejaVu Sans\.",
            )
            if apply_tight_layout:
                fig.tight_layout()
            out_path = image_dir / filename
            fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _insert_image(path: Path, anchor: str, width: int = 620, height: int = 320) -> None:
        img = XLImage(str(path))
        img.width = width
        img.height = height
        ws.add_image(img, anchor)

    def _detect_outlier_players(points_df: pd.DataFrame, x_col: str, y_col: str, z_thresh: float = 2.35) -> set[str]:
        if points_df.empty or len(points_df) < 5:
            return set()
        x = pd.to_numeric(points_df[x_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        y = pd.to_numeric(points_df[y_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        sx = float(np.std(x))
        sy = float(np.std(y))
        zx = np.zeros_like(x) if sx <= 1e-9 else (x - float(np.mean(x))) / sx
        zy = np.zeros_like(y) if sy <= 1e-9 else (y - float(np.mean(y))) / sy
        radial = np.sqrt(zx * zx + zy * zy)
        mask = radial >= z_thresh
        if "player" not in points_df.columns:
            return set()
        return set(points_df.loc[mask, "player"].astype(str).tolist())

    def _safe_plot_label(text: Any) -> str:
        # Drop non-BMP/control chars that DejaVu Sans often cannot render in PyInstaller builds.
        raw = str(text or "")
        cleaned = []
        for ch in raw:
            cp = ord(ch)
            if cp in (9, 10, 13):  # allow basic whitespace
                cleaned.append(ch)
            elif 32 <= cp <= 0xFFFF:
                cleaned.append(ch)
        return "".join(cleaned)

    def _annotate_all_points(ax, x_vals: List[float], y_vals: List[float], labels: List[str], fontsize: int = 9) -> None:
        offsets = [(0, 0), (4, 2), (-4, 2), (4, -2), (-4, -2), (0, 4), (0, -4)]
        for i, (xv, yv, label) in enumerate(zip(x_vals, y_vals, labels)):
            dx, dy = offsets[i % len(offsets)]
            ax.annotate(
                _safe_plot_label(label),
                (float(xv), float(yv)),
                textcoords="offset points",
                xytext=(dx, dy),
                fontsize=fontsize,
                color="#1F2D3D",
                bbox={"boxstyle": "round,pad=0.08", "fc": "white", "ec": "none", "alpha": 0.38},
            )

    def _annotate_line_endpoints(ax, xs: List[float], y_by_player: Dict[str, List[float]], *, invert_y: bool = False) -> None:
        if not xs or not y_by_player:
            return
        final_points: List[Tuple[str, float]] = []
        for player, vals in y_by_player.items():
            if not vals:
                continue
            final_points.append((str(player), float(vals[-1])))
        if not final_points:
            return

        final_points.sort(key=lambda item: item[1], reverse=invert_y)
        min_gap = 0.45
        placed: List[float] = []
        adjusted: Dict[str, float] = {}
        for player, yv in final_points:
            new_y = yv
            if placed:
                if invert_y:
                    while any(abs(new_y - py) < min_gap for py in placed):
                        new_y -= min_gap
                else:
                    while any(abs(new_y - py) < min_gap for py in placed):
                        new_y += min_gap
            placed.append(new_y)
            adjusted[player] = new_y

        x_last = float(xs[-1])
        for player, vals in y_by_player.items():
            if not vals:
                continue
            ax.scatter([x_last], [float(vals[-1])], s=18, color="white", edgecolors="none", zorder=5)
            ax.annotate(
                _safe_plot_label(player),
                (x_last, adjusted.get(player, float(vals[-1]))),
                textcoords="offset points",
                xytext=(8, 0),
                va="center",
                fontsize=9,
                color="#1F2D3D",
                bbox={"boxstyle": "round,pad=0.12", "fc": "white", "ec": "none", "alpha": 0.72},
            )

    dfo = df_overview.copy()
    if "slot_key" not in dfo.columns:
        dfo["slot_key"] = dfo["map_index"].apply(map_slot_key)
    if "slot_label" not in dfo.columns:
        dfo["slot_label"] = dfo["slot_key"].apply(map_slot_label)
    dfo["total_pts"] = pd.to_numeric(dfo["total_pts"], errors="coerce").fillna(0.0)
    dfo["total_time"] = pd.to_numeric(dfo["total_time"], errors="coerce").fillna(0.0)
    dfo["week_map_key"] = dfo["week"].astype(str) + "::" + dfo["map_index"].astype(str)
    dfo["mode_category"] = dfo.get("mode_category", pd.Series(index=dfo.index, dtype=object)).fillna("unknown").astype(str)
    dfo["is_sweden"] = dfo.get("is_sweden", pd.Series(index=dfo.index, dtype=bool)).fillna(False).astype(bool)
    dfo["mode3"] = dfo["mode_category"].apply(mode_category_label)

    weeks_seen = dfo["week"].dropna().astype(str).tolist()
    weeks_order = list(dict.fromkeys(list(weeks) + weeks_seen))

    total_maps = max(1, int(dfo["week_map_key"].nunique()))
    min_maps_for_labels = max(1, int(math.ceil(total_maps * 0.75)))
    maps_by_player = dfo.groupby("player")["week_map_key"].nunique()
    qualified_players = set(
        maps_by_player[maps_by_player >= min_maps_for_labels].index.astype(str).tolist()
    )

    week_participation = (
        dfo.groupby(["player", "week"], as_index=False)
        .agg(week_maps=("map_index", "nunique"))
    )
    expanded_rule = (
        week_participation.groupby("player", as_index=False)
        .agg(
            weeks_played=("week", "nunique"),
            max_week_maps=("week_maps", "max"),
            total_maps_played=("week_maps", "sum"),
        )
    )
    expanded_players = set(
        expanded_rule[
            (expanded_rule["max_week_maps"] >= 6)
            & (expanded_rule["weeks_played"] >= 2)
            & (expanded_rule["total_maps_played"] >= 7)
        ]["player"].astype(str).tolist()
    )

    total_pts_series = (
        df_total.set_index("player")["total_pts"]
        if ("player" in df_total.columns and "total_pts" in df_total.columns)
        else pd.Series(dtype=float)
    )
    total_pts_dict = {str(k): float(v) for k, v in total_pts_series.items()}

    # V1A/B/C: Tid vs poang per karttyp
    v1_paths: List[Path] = []
    for mode_name, color, tag in [
        ("Moving", "#2A77D4", "V1A"),
        ("No move", "#279B70", "V1B"),
        ("NMPZ", "#7A67D8", "V1C"),
    ]:
        part = dfo[dfo["mode3"] == mode_name].copy()
        fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
        if not part.empty:
            ax.scatter(part["total_time"] / 60.0, part["total_pts"], s=14, alpha=0.14, color="#808B96", label="Alla rundor")
            by_player = (
                part.groupby("player", as_index=False)
                .agg(
                    mean_time_min=("total_time", lambda s: float(np.mean(s)) / 60.0),
                    mean_pts=("total_pts", "mean"),
                    maps=("total_pts", "count"),
                )
            )

            sizes = [22 + min(60.0, float(m) * 1.2) for m in by_player["maps"].tolist()]
            ax.scatter(
                by_player["mean_time_min"].tolist(),
                by_player["mean_pts"].tolist(),
                s=sizes,
                alpha=0.82,
                color=color,
                edgecolors="white",
                linewidths=0.5,
                label="Spelarmedel",
            )
            _annotate_all_points(
                ax,
                [float(x) for x in by_player["mean_time_min"].tolist()],
                [float(x) for x in by_player["mean_pts"].tolist()],
                [str(x) for x in by_player["player"].tolist()],
                fontsize=9,
            )
            ax.set_xlabel("Tid per karta (min)")
            ax.set_ylabel("GeoGuessr-poäng")
            ax.legend(loc="best", fontsize=10, frameon=True)
        else:
            _empty_plot(ax)
        ax.set_title(f"{tag}: Tid vs poäng ({mode_name})")
        v1_paths.append(_save_fig(fig, f"{tag}_tid_vs_poang_{mode_name.lower().replace(' ', '_')}.png"))

    # V2: Total rapoang for spelare med "mer an en full vecka"
    if expanded_players:
        top_raw = (
            df_total[df_total["player"].astype(str).isin(expanded_players)]
            .sort_values(["total_pts", "total_borda"], ascending=[False, False])
            .reset_index(drop=True)
        )
    else:
        top_raw = df_total.sort_values(["total_pts", "total_borda"], ascending=[False, False]).reset_index(drop=True)
    v2_labels = [_safe_plot_label(x) for x in top_raw["player"].tolist()]
    v2_values = [float(x) for x in pd.to_numeric(top_raw["total_pts"], errors="coerce").fillna(0).tolist()]
    fig_w = max(BASE_FIG_W, min(16.0, 8.0 + 0.22 * max(1, len(v2_labels))))
    fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
    if v2_values:
        labels_rev = list(reversed(v2_labels))
        vals_rev = list(reversed(v2_values))
        ys = list(range(len(labels_rev)))
        ax.barh(ys, vals_rev, color="#279B70")
        ax.set_yticks(ys)
        ax.set_yticklabels(labels_rev, fontsize=10)
        ax.set_ylabel("Spelare")
        ax.set_xlabel("GeoGuessr-poäng")
    else:
        _empty_plot(ax)
    ax.set_title("V2: Total råpoäng (spelare med mer än en full vecka)")
    v2_path = _save_fig(fig, "V2_total_rapoang_expanded.png")

    # V3: Snitt GeoGuessr-poang per karttyp och spelare (heatmap)
    mode_avg = (
        dfo[dfo["mode3"].isin(["Moving", "No move", "NMPZ"])]
        .groupby(["player", "mode3"], as_index=False)
        .agg(avg_pts=("total_pts", "mean"))
    )
    if expanded_players:
        v3_players = sorted(expanded_players, key=lambda p: total_pts_dict.get(p, 0.0), reverse=True)
    elif qualified_players:
        v3_players = sorted(qualified_players, key=lambda p: total_pts_dict.get(p, 0.0), reverse=True)
    else:
        v3_players = (
            dfo.groupby("player", as_index=False)
            .agg(maps=("week_map_key", "nunique"))
            .sort_values("maps", ascending=False)["player"]
            .astype(str)
            .head(40)
            .tolist()
        )
    v3_pivot = (
        mode_avg.pivot_table(index="player", columns="mode3", values="avg_pts", aggfunc="mean")
        .reindex(v3_players)
        .fillna(0.0)
    )
    for col in ["Moving", "No move", "NMPZ"]:
        if col not in v3_pivot.columns:
            v3_pivot[col] = 0.0
    v3_pivot = v3_pivot[["Moving", "No move", "NMPZ"]]
    fig_w = max(BASE_FIG_W, min(16.0, 8.4 + 0.16 * max(1, len(v3_pivot.index))))
    fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
    if not v3_pivot.empty:
        im = ax.imshow(v3_pivot.values, aspect="auto", cmap="YlGnBu")
        ax.set_xticks([0, 1, 2])
        ax.set_xticklabels(["Moving", "No move", "NMPZ"])
        ax.set_yticks(list(range(len(v3_pivot.index))))
        ax.set_yticklabels([_safe_plot_label(p) for p in v3_pivot.index], fontsize=9)
        fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02, label="Snittpoäng")
    else:
        _empty_plot(ax)
    ax.set_title("V3: Snitt GeoGuessr-poäng per karttyp och spelare")
    v3_path = _save_fig(fig, "V3_snitt_ggpoang_karttyp_spelare.png")

    # V4: Aktiva spelare per vecka
    per_week_players = (
        dfo.groupby("week", as_index=False)
        .agg(active_players=("player", "nunique"))
        .set_index("week")
        .reindex(weeks_order, fill_value=0.0)
        .reset_index()
    )
    v4_labels = [str(x) for x in per_week_players["week"].tolist()]
    v4_values = [float(x) for x in pd.to_numeric(per_week_players["active_players"], errors="coerce").fillna(0).tolist()]
    fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
    if v4_values:
        xs = list(range(len(v4_values)))
        ax.bar(xs, v4_values, color="#F3B26B", edgecolor="#C56A12", linewidth=1.0, zorder=2)
        ax.plot(xs, v4_values, marker="o", color="#C56A12", linewidth=2.0, zorder=3)
        ax.set_xticks(xs)
        ax.set_xticklabels([_safe_plot_label(x) for x in v4_labels], rotation=30, ha="right")
        ax.set_ylabel("Antal spelare")
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.2, zorder=1)
    else:
        _empty_plot(ax)
    ax.set_title("V4: Aktiva spelare per vecka")
    v4_path = _save_fig(fig, "V4_aktiva_spelare_vecka.png")

    # V5: Ligans snitt GeoGuessr-poang per underliga-kategori
    v5_labels = ["Moving", "No move", "NMPZ", "Sverige", "Sverige Moving", "Sverige No Move"]
    v5_values: List[float] = []
    for cat in v5_labels:
        if cat == "Moving":
            part = dfo[dfo["mode_category"] == "moving"]
        elif cat == "No move":
            part = dfo[dfo["mode_category"] == "no_move"]
        elif cat == "NMPZ":
            part = dfo[dfo["mode_category"] == "nmpz"]
        elif cat == "Sverige":
            part = dfo[dfo["is_sweden"] & dfo["mode_category"].isin(["moving", "no_move"])]
        elif cat == "Sverige Moving":
            part = dfo[dfo["is_sweden"] & (dfo["mode_category"] == "moving")]
        else:
            part = dfo[dfo["is_sweden"] & (dfo["mode_category"] == "no_move")]
        v5_values.append(float(part["total_pts"].mean()) if not part.empty else 0.0)
    fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
    if any(v5_values):
        xs = list(range(len(v5_values)))
        ax.bar(xs, v5_values, color=["#2A77D4", "#279B70", "#7A67D8", "#E0862B", "#B4581B", "#D9A441"])
        ax.set_xticks(xs)
        ax.set_xticklabels(v5_labels, rotation=20, ha="right")
        ax.set_ylabel("Snitt GeoGuessr-poäng")
    else:
        _empty_plot(ax)
    ax.set_title("V5: Ligans snitt GeoGuessr-poäng per underliga-kategori")
    v5_path = _save_fig(fig, "V5_ligans_snitt_ggpoang_underliga.png")

    # V6: Topp-spelare per karttyp (snitt)
    by_mode_player = (
        dfo[dfo["mode3"].isin(["Moving", "No move", "NMPZ"])]
        .groupby(["player", "mode3"], as_index=False)
        .agg(avg_pts=("total_pts", "mean"))
    )
    v6_players = (
        df_total.sort_values(["total_pts", "total_borda"], ascending=[False, False])["player"]
        .astype(str).head(12).tolist()
    )
    v6_pivot = (
        by_mode_player.pivot_table(index="player", columns="mode3", values="avg_pts", aggfunc="mean")
        .reindex(v6_players)
        .fillna(0.0)
    )
    for col in ["Moving", "No move", "NMPZ"]:
        if col not in v6_pivot.columns:
            v6_pivot[col] = 0.0
    v6_pivot = v6_pivot[["Moving", "No move", "NMPZ"]]
    fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
    if not v6_pivot.empty:
        xs = np.arange(len(v6_pivot.index))
        w = 0.26
        ax.bar(xs - w, v6_pivot["Moving"].tolist(), width=w, label="Moving", color="#2A77D4")
        ax.bar(xs, v6_pivot["No move"].tolist(), width=w, label="No move", color="#279B70")
        ax.bar(xs + w, v6_pivot["NMPZ"].tolist(), width=w, label="NMPZ", color="#7A67D8")
        ax.set_xticks(xs)
        ax.set_xticklabels([_safe_plot_label(x) for x in v6_pivot.index], rotation=30, ha="right", fontsize=10)
        ax.set_ylabel("Snitt GeoGuessr-poäng")
        ax.legend(fontsize=10)
    else:
        _empty_plot(ax)
    ax.set_title("V6: Topp-spelare per karttyp (snitt GeoGuessr-poäng)")
    v6_path = _save_fig(fig, "V6_toppspelare_karttyp.png")

    # V7: Poangfordelning per karttyp (boxplot)
    fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
    box_data = [
        dfo[dfo["mode3"] == "Moving"]["total_pts"].tolist(),
        dfo[dfo["mode3"] == "No move"]["total_pts"].tolist(),
        dfo[dfo["mode3"] == "NMPZ"]["total_pts"].tolist(),
    ]
    if any(len(b) > 0 for b in box_data):
        ax.boxplot(box_data, labels=["Moving", "No move", "NMPZ"], showfliers=False)
        ax.set_ylabel("GeoGuessr-poäng")
    else:
        _empty_plot(ax)
    ax.set_title("V7: Poängfördelning per karttyp")
    v7_path = _save_fig(fig, "V7_poangfordelning_karttyp_boxplot.png")

    # V8: Stabilitet vs niva (std mot snitt)
    stab = (
        dfo.groupby("player", as_index=False)
        .agg(
            mean_pts=("total_pts", "mean"),
            std_pts=("total_pts", "std"),
            maps=("week_map_key", "nunique"),
        )
    )
    stab["std_pts"] = pd.to_numeric(stab["std_pts"], errors="coerce").fillna(0.0)
    fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
    if not stab.empty:
        ax.scatter(stab["std_pts"].tolist(), stab["mean_pts"].tolist(), color="#2A77D4", alpha=0.6, s=30)
        _annotate_all_points(
            ax,
            [float(x) for x in stab["std_pts"].tolist()],
            [float(x) for x in stab["mean_pts"].tolist()],
            [str(x) for x in stab["player"].tolist()],
            fontsize=9,
        )
        ax.set_xlabel("Standardavvikelse i poäng")
        ax.set_ylabel("Snitt GeoGuessr-poäng")
    else:
        _empty_plot(ax)
    ax.set_title("V8: Stabilitet vs nivå (std mot snittpoäng)")
    v8_path = _save_fig(fig, "V8_stabilitet_vs_niva.png")

    # V9: Veckotrend-heatmap
    week_player = (
        dfo.groupby(["player", "week"], as_index=False)
        .agg(avg_pts=("total_pts", "mean"))
    )
    if expanded_players:
        v9_players = sorted(expanded_players, key=lambda p: total_pts_dict.get(p, 0.0), reverse=True)
    elif qualified_players:
        v9_players = sorted(qualified_players, key=lambda p: total_pts_dict.get(p, 0.0), reverse=True)
    else:
        v9_players = (
            dfo.groupby("player", as_index=False)
            .agg(maps=("week_map_key", "nunique"))
            .sort_values("maps", ascending=False)["player"]
            .astype(str).head(40).tolist()
        )
    v9_pivot = (
        week_player.pivot_table(index="player", columns="week", values="avg_pts", aggfunc="mean")
        .reindex(index=v9_players, columns=weeks_order)
    )
    fig_w = max(BASE_FIG_W, min(18.0, 8.8 + 0.8 * max(1, len(weeks_order))))
    fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
    if not v9_pivot.empty:
        centered = v9_pivot.sub(v9_pivot.mean(axis=1), axis=0).fillna(0.0)
        im = ax.imshow(centered.values, aspect="auto", cmap="RdYlGn")
        ax.set_xticks(list(range(len(weeks_order))))
        ax.set_xticklabels([_safe_plot_label(w) for w in weeks_order], rotation=30, ha="right", fontsize=10)
        ax.set_yticks(list(range(len(centered.index))))
        ax.set_yticklabels([_safe_plot_label(p) for p in centered.index], fontsize=9)
        fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02, label="Över/under eget snitt")
    else:
        _empty_plot(ax)
    ax.set_title("V9: Veckotrend-heatmap (över/under eget snitt)")
    v9_path = _save_fig(fig, "V9_veckotrend_heatmap.png")

    # V10: Stil-PCA med loadings och tolkning
    style_pca_points, style_pca_loadings, style_pca_info = compute_style_pca(df_style)
    fig = plt.figure(figsize=(13.2, 9.9))
    gs = fig.add_gridspec(2, 2, width_ratios=[2.0, 1.1], hspace=0.34, wspace=0.34)
    ax_sc = fig.add_subplot(gs[:, 0])
    ax_l1 = fig.add_subplot(gs[0, 1])
    ax_l2 = fig.add_subplot(gs[1, 1])

    if not style_pca_points.empty:
        colors = style_pca_points["specialization_index"].fillna(50.0).tolist()
        sizes = [38.0 + min(58.0, float(v) * 3.0) for v in style_pca_points["maps_counted"].fillna(0.0).tolist()]
        sc = ax_sc.scatter(style_pca_points["pc1"], style_pca_points["pc2"], c=colors, cmap="viridis", s=sizes, alpha=0.86)
        fig.colorbar(sc, ax=ax_sc, fraction=0.035, pad=0.02, label="Specialiseringsindex")
        _annotate_all_points(
            ax_sc,
            [float(x) for x in style_pca_points["pc1"].tolist()],
            [float(x) for x in style_pca_points["pc2"].tolist()],
            [str(x) for x in style_pca_points["player"].tolist()],
            fontsize=9,
        )
        ax_sc.set_xlabel(f"PC1 ({style_pca_info.get('pc1_pct', 0.0):.1f}% forklarad varians)")
        ax_sc.set_ylabel(f"PC2 ({style_pca_info.get('pc2_pct', 0.0):.1f}% forklarad varians)")

        load_pc1 = style_pca_loadings.reindex(style_pca_loadings["pc1_loading"].abs().sort_values(ascending=False).index).head(6)
        load_pc2 = style_pca_loadings.reindex(style_pca_loadings["pc2_loading"].abs().sort_values(ascending=False).index).head(6)

        ax_l1.barh(list(range(len(load_pc1))), load_pc1["pc1_loading"].astype(float).tolist(), color="#2A77D4")
        ax_l1.set_yticks(list(range(len(load_pc1))))
        ax_l1.set_yticklabels([_safe_plot_label(x) for x in load_pc1["feature_label"].tolist()], fontsize=9)
        ax_l1.invert_yaxis()
        ax_l1.set_title("PC1: starkaste bidrag")

        ax_l2.barh(list(range(len(load_pc2))), load_pc2["pc2_loading"].astype(float).tolist(), color="#279B70")
        ax_l2.set_yticks(list(range(len(load_pc2))))
        ax_l2.set_yticklabels([_safe_plot_label(x) for x in load_pc2["feature_label"].tolist()], fontsize=9)
        ax_l2.invert_yaxis()
        ax_l2.set_title("PC2: starkaste bidrag")

        pc1_top = ", ".join(load_pc1["feature_label"].head(3).tolist())
        pc2_top = ", ".join(load_pc2["feature_label"].head(3).tolist())
        ax_sc.text(
            0.02,
            0.02,
            f"PC1 drivs mest av: {pc1_top}\nPC2 drivs mest av: {pc2_top}",
            transform=ax_sc.transAxes,
            fontsize=9,
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": "none", "alpha": 0.78},
        )
    else:
        _empty_plot(ax_sc, "For fa kvalificerade spelare")
        _empty_plot(ax_l1)
        _empty_plot(ax_l2)
    fig.suptitle("V10: Stil-PCA + bidrag till PC1/PC2", fontsize=12, y=0.99)
    v10_path = _save_fig(fig, "V10_stil_pca_loadings.png", apply_tight_layout=False)

    # V11: Featurevikter och komponenter
    feature_meta = style_feature_meta_df()
    feature_merge = feature_meta.merge(
        style_pca_loadings[["feature_key", "pc1_loading", "pc2_loading"]] if not style_pca_loadings.empty else pd.DataFrame(columns=["feature_key", "pc1_loading", "pc2_loading"]),
        on="feature_key",
        how="left",
    )
    fig = plt.figure(figsize=(13.2, 9.9))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.1, 1.4], wspace=0.32)
    ax_w = fig.add_subplot(gs[0, 0])
    ax_t = fig.add_subplot(gs[0, 1])
    ax_w.barh(list(range(len(feature_merge))), feature_merge["weight"].astype(float).tolist(), color="#A64D1F")
    ax_w.set_yticks(list(range(len(feature_merge))))
    ax_w.set_yticklabels([_safe_plot_label(x) for x in feature_merge["feature_label"].tolist()], fontsize=9)
    ax_w.invert_yaxis()
    ax_w.set_xlabel("Vikt i likhet/PCA")
    ax_w.set_title("Featurevikter")

    ax_t.axis("off")
    lines = [
        f"PC1 ({style_pca_info.get('pc1_pct', 0.0):.1f}%): "
        + ", ".join(feature_merge.reindex(feature_merge["pc1_loading"].abs().sort_values(ascending=False).index)["feature_label"].head(4).tolist()),
        f"PC2 ({style_pca_info.get('pc2_pct', 0.0):.1f}%): "
        + ", ".join(feature_merge.reindex(feature_merge["pc2_loading"].abs().sort_values(ascending=False).index)["feature_label"].head(4).tolist()),
        "",
        "Likheten nedviktar totalpoang och uppviktar:",
        "- 5k-frekvens och 5k-fart",
        "- basta-runda-effektivitet",
        "- no move / NMPZ / moving-profiler",
        "- specialisering och konsistens",
    ]
    ax_t.text(0.0, 0.98, "\n".join(lines), va="top", fontsize=11)
    ax_t.set_title("Vad PCA:n faktiskt visar")
    v11_path = _save_fig(fig, "V11_featurevikter_och_pca_forklaring.png", apply_tight_layout=False)

    # V12/V13: Utveckling vecka for vecka (kumulativ liga)
    weekly_player_points = (
        dfo.groupby(["player", "week"], as_index=False)
        .agg(weekly_points=("borda_points", "sum"))
    )
    all_rank_players = (
        df_total.sort_values(["total_borda", "total_pts"], ascending=[False, False])["player"]
        .astype(str).tolist()
    )
    v12_players = list(all_rank_players)
    weekly_pivot_all = (
        weekly_player_points.pivot_table(index="player", columns="week", values="weekly_points", aggfunc="sum")
        .reindex(index=all_rank_players, columns=weeks_order)
    )
    weekly_data_mask = weekly_pivot_all.notna()
    weekly_pivot_all = weekly_pivot_all.fillna(0.0)
    cumulative_all = weekly_pivot_all.cumsum(axis=1)
    cumulative_rank = cumulative_all.rank(axis=0, method="min", ascending=False)
    cumulative_selected = cumulative_all.reindex(v12_players).fillna(0.0)
    rank_selected = cumulative_rank.reindex(v12_players).fillna(len(all_rank_players) if all_rank_players else 0.0)

    fig_w = max(BASE_FIG_W, min(17.0, 9.6 + 0.7 * max(1, len(weeks_order))))
    xs = [float(i) for i in range(len(weeks_order))]
    palette = [
        "#1F5AA6", "#2E8B57", "#A64D1F", "#7A67D8", "#C0392B", "#117A65",
        "#D68910", "#884EA0", "#2E4053", "#AF601A", "#2471A3", "#1E8449",
    ]
    line_styles = ["-", "--", "-.", ":"]

    fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
    if weeks_order and v12_players and not rank_selected.empty:
        y_by_player_rank: Dict[str, List[float]] = {}
        for idx, player in enumerate(v12_players):
            vals = [float(x) for x in rank_selected.loc[player].tolist()]
            y_by_player_rank[str(player)] = vals
            color = palette[idx % len(palette)]
            linestyle = line_styles[(idx // len(palette)) % len(line_styles)]
            ax.plot(xs, vals, color=color, linestyle=linestyle, linewidth=2.6 if idx < 3 else 1.9, alpha=0.95)
            marker_mask = weekly_data_mask.loc[player].tolist() if player in weekly_data_mask.index else []
            marker_xs = [x for x, has_data in zip(xs, marker_mask) if bool(has_data)]
            marker_ys = [y for y, has_data in zip(vals, marker_mask) if bool(has_data)]
            if marker_xs:
                ax.scatter(marker_xs, marker_ys, color=color, s=18, zorder=4, edgecolors="white", linewidths=0.4)
        _annotate_line_endpoints(ax, xs, y_by_player_rank, invert_y=True)
        ax.set_xticks(xs)
        ax.set_xticklabels([_safe_plot_label(w) for w in weeks_order], rotation=30, ha="right")
        ax.set_ylabel("Placering")
        ax.set_ylim(max(1.0, float(len(all_rank_players)) + 0.75), 0.25)
        ax.grid(axis="y", alpha=0.18)
    else:
        _empty_plot(ax)
    ax.set_title("V12: Placering vecka för vecka (kumulativ liga)")
    v12_path = _save_fig(fig, "V12_placering_vecka_for_vecka.png")

    fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
    if weeks_order and v12_players and not cumulative_selected.empty:
        y_by_player_points: Dict[str, List[float]] = {}
        for idx, player in enumerate(v12_players):
            vals = [float(x) for x in cumulative_selected.loc[player].tolist()]
            y_by_player_points[str(player)] = vals
            color = palette[idx % len(palette)]
            linestyle = line_styles[(idx // len(palette)) % len(line_styles)]
            ax.plot(xs, vals, color=color, linestyle=linestyle, linewidth=2.6 if idx < 3 else 1.9, alpha=0.95)
            marker_mask = weekly_data_mask.loc[player].tolist() if player in weekly_data_mask.index else []
            marker_xs = [x for x, has_data in zip(xs, marker_mask) if bool(has_data)]
            marker_ys = [y for y, has_data in zip(vals, marker_mask) if bool(has_data)]
            if marker_xs:
                ax.scatter(marker_xs, marker_ys, color=color, s=18, zorder=4, edgecolors="white", linewidths=0.4)
        _annotate_line_endpoints(ax, xs, y_by_player_points, invert_y=False)
        ax.set_xticks(xs)
        ax.set_xticklabels([_safe_plot_label(w) for w in weeks_order], rotation=30, ha="right")
        ax.set_ylabel("Ackumulerad ligapoäng")
        ax.grid(axis="y", alpha=0.18)
        ax.set_ylim(bottom=0)
    else:
        _empty_plot(ax)
    ax.set_title("V13: Ackumulerad ligapoäng vecka för vecka")
    v13_path = _save_fig(fig, "V13_ackumulerad_ligapoang_vecka_for_vecka.png")

    # V14: Spelstilslikhet heatmap
    style_heatmap_players = (
        df_style[df_style.get("is_qualified", pd.Series(dtype=bool)).fillna(False)]
        .head(STYLE_SIMILARITY_MAX_PLAYERS)["player"]
        .astype(str).tolist()
        if not df_style.empty else []
    )
    style_heatmap = pd.DataFrame()
    if style_heatmap_players and not df_similarity.empty and "player" in df_similarity.columns:
        style_heatmap = (
            df_similarity.set_index("player")
            .reindex(index=style_heatmap_players, columns=style_heatmap_players)
        )

    fig_w = max(BASE_FIG_W, min(16.0, 8.4 + 0.22 * max(1, len(style_heatmap_players))))
    fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.75))
    if not style_heatmap.empty:
        im = ax.imshow(style_heatmap.values, aspect="auto", cmap="YlGnBu", vmin=-1.0, vmax=1.0)
        ax.set_xticks(list(range(len(style_heatmap.columns))))
        ax.set_xticklabels([_safe_plot_label(x) for x in style_heatmap.columns], rotation=35, ha="right", fontsize=9)
        ax.set_yticks(list(range(len(style_heatmap.index))))
        ax.set_yticklabels([_safe_plot_label(x) for x in style_heatmap.index], fontsize=9)
        fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02, label="Cosinuslikhet")
    else:
        _empty_plot(ax, "For fa kvalificerade spelare")
    ax.set_title("V14: Spelstilslikhet heatmap")
    v14_path = _save_fig(fig, "V14_spelstilslikhet_heatmap.png")

    # V15: 5k-effektivitet
    fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
    if not df_style.empty:
        style_plot = _style_player_subset(df_style).copy()
        if style_plot.empty:
            style_plot = df_style.copy()
        if not style_plot.empty:
            colors = style_plot["nmpz_strength"].fillna(50.0).tolist()
            sizes = [35.0 + min(55.0, float(v) * 3.0) for v in style_plot["maps_counted"].fillna(0.0).tolist()]
            sc = ax.scatter(
                style_plot["fivek_rate_index"].fillna(0.0),
                style_plot["fivek_speed_index"].fillna(0.0),
                c=colors,
                cmap="viridis",
                s=sizes,
                alpha=0.86,
            )
            fig.colorbar(sc, ax=ax, fraction=0.035, pad=0.02, label="NMPZ-styrka")
            _annotate_all_points(
                ax,
                [float(x) for x in style_plot["fivek_rate_index"].tolist()],
                [float(x) for x in style_plot["fivek_speed_index"].tolist()],
                [str(x) for x in style_plot["player"].tolist()],
                fontsize=9,
            )
            ax.set_xlabel("5k-frekvens")
            ax.set_ylabel("5k-fart")
        else:
            _empty_plot(ax, "For fa spelare")
    else:
        _empty_plot(ax, "Ingen spelstilsdata")
    ax.set_title("V15: 5k-effektivitet (frekvens vs fart)")
    v15_path = _save_fig(fig, "V15_5k_effektivitet.png")

    # V16/V17: Steg for 5k i moving, uppdelat pa Sverige / Varlden
    dfo["fastest_5000_round_steps"] = pd.to_numeric(dfo.get("fastest_5000_round_steps"), errors="coerce")
    dfo["fastest_5000_round_time"] = pd.to_numeric(dfo.get("fastest_5000_round_time"), errors="coerce")
    dfo["fastest_5000_round_distance_m"] = pd.to_numeric(dfo.get("fastest_5000_round_distance_m"), errors="coerce")
    dfo["count_5000_rounds"] = pd.to_numeric(dfo.get("count_5000_rounds"), errors="coerce").fillna(0.0)

    for tag, title, mask in [
        ("V16", "V16: Steg för 5k Sverige", dfo["is_sweden"] & (dfo["mode_category"] == "moving")),
        ("V17", "V17: Steg för 5k Världen", (~dfo["is_sweden"]) & (dfo["mode_category"] == "moving")),
    ]:
        part = dfo[mask & dfo["fastest_5000_round_steps"].notna() & dfo["fastest_5000_round_time"].notna()].copy()
        fig, ax = plt.subplots(figsize=(BASE_FIG_W, BASE_FIG_H))
        out_path: Optional[Path] = None
        if not part.empty:
            by_player = (
                part.groupby("player", as_index=False)
                .agg(
                    median_steps=("fastest_5000_round_steps", "median"),
                    median_time=("fastest_5000_round_time", "median"),
                    median_distance=("fastest_5000_round_distance_m", "median"),
                    moving_5ks=("count_5000_rounds", "sum"),
                )
                .sort_values(["moving_5ks", "median_steps", "median_time"], ascending=[False, True, True])
            )
            colors = by_player["moving_5ks"].fillna(0.0).tolist()
            sizes = [40.0 + min(90.0, float(v or 0.0) / 8.0) for v in by_player["median_distance"].fillna(0.0).tolist()]
            sc = ax.scatter(
                by_player["median_steps"].tolist(),
                by_player["median_time"].tolist(),
                c=colors,
                cmap="viridis",
                s=sizes,
                alpha=0.84,
                edgecolors="white",
                linewidths=0.6,
            )
            fig.colorbar(sc, ax=ax, fraction=0.035, pad=0.02, label="Antal moving-5k")
            _annotate_all_points(
                ax,
                [float(x) for x in by_player["median_steps"].tolist()],
                [float(x) for x in by_player["median_time"].tolist()],
                [f"{p} ({int(c)})" for p, c in zip(by_player["player"].tolist(), by_player["moving_5ks"].tolist())],
                fontsize=9,
            )
            ax.set_xlabel("Mediansteg för 5k (moving)")
            ax.set_ylabel("Mediantid för 5k (s)")
            fig.subplots_adjust(bottom=0.18)
            fig.text(
                0.5,
                0.035,
                "Färg = antal moving-5k   |   Storlek = medianavstånd till 5k",
                ha="center",
                va="bottom",
                fontsize=9,
                bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": "none", "alpha": 0.78},
            )
        else:
            _empty_plot(ax, "Ingen moving-5k-data")
        ax.set_title(title)
        out_path = _save_fig(fig, f"{tag}_steg_for_5k_{'sverige' if tag == 'V16' else 'varlden'}.png")
        if tag == "V16":
            v16_path = out_path
        else:
            v17_path = out_path

    # Place images lower and larger so overview text remains visible and plots are easier to read.
    anchors: List[str] = []
    first_row = max(26, len(viz_names) + 11)
    row_step = 31
    for i in range(10):
        r = first_row + i * row_step
        anchors.append(f"A{r}")
        anchors.append(f"N{r}")

    image_specs: List[Tuple[Path, int, int]] = [
        (v1_paths[0], 720, 540),
        (v1_paths[1], 720, 540),
        (v1_paths[2], 720, 540),
        (v2_path, 720, 540),
        (v3_path, 720, 540),
        (v4_path, 720, 540),
        (v5_path, 720, 540),
        (v6_path, 720, 540),
        (v7_path, 720, 540),
        (v8_path, 720, 540),
        (v9_path, 720, 540),
        (v10_path, 720, 540),
        (v11_path, 720, 540),
        (v12_path, 720, 540),
        (v13_path, 720, 540),
        (v14_path, 720, 540),
        (v15_path, 720, 540),
        (v16_path, 720, 540),
        (v17_path, 720, 540),
    ]

    for idx, (img_path, w, h) in enumerate(image_specs):
        if idx >= len(anchors):
            break
        _insert_image(img_path, anchors[idx], width=w, height=h)

    # Intentionally no footer text to keep the sheet compact.


def write_underligor_sheet(wb: Workbook, df_overview: pd.DataFrame, sort_by: str = "default") -> None:
    ws = wb.create_sheet("Underligor")
    league_layout = [
        ("Moving", 3, 1),
        ("No move", 3, 8),
        ("NMPZ", 3, 15),
        ("Sverige", 3, 22),
        ("Sverige Moving", None, 15),
        ("Sverige No Move", None, 22),
    ]
    block_widths = [4.5, 22.0, 12.0, 10.0, 8.0, 8.0]
    block_cols = len(block_widths)
    gap_cols = 1
    total_cols = 27
    merge_and_style(ws, 1, 1, 1, total_cols, "Underligor", fill=DARK, font=FONT_HDR_BIG, align=CENTER)

    widths: Dict[int, float] = {}
    start_cols = sorted({start_col for _, _, start_col in league_layout})
    for i, start_col in enumerate(start_cols):
        for offset, w in enumerate(block_widths):
            widths[start_col + offset] = w
        if i < len(start_cols) - 1:
            widths[start_col + block_cols] = 3.0
    set_col_widths(ws, widths)

    tables = compute_subleague_tables(df_overview)
    fast_tables = compute_fast_round_tables(df_overview)
    headers = ["#", "Spelare", "Poäng", "Snitt pts", "Kartor", "Veckor"]
    first_row_max_rows = 0
    second_row_max_rows = 0

    for idx_layout, (league_name, base_row, start_col) in enumerate(league_layout):
        if base_row is None:
            base_row = 3 + 2 + first_row_max_rows + 3
        end_col = start_col + block_cols - 1
        merge_and_style(ws, base_row, start_col, base_row, end_col, league_name, fill=MID, font=FONT_HDR_MED, align=CENTER)

        header_row = base_row + 1
        for j, h in enumerate(headers):
            c = start_col + j
            ws.cell(header_row, c).value = h
            style_cell(ws, header_row, c, fill=MID, font=FONT_HDR, align=CENTER)

        data_start_row = base_row + 2
        table = sort_subleague_table(tables.get(league_name, pd.DataFrame()), sort_by=sort_by)
        if idx_layout < 4:
            first_row_max_rows = max(first_row_max_rows, len(table))
        else:
            second_row_max_rows = max(second_row_max_rows, len(table))
        for idx, row in enumerate(table.itertuples(index=False), start=1):
            r = data_start_row + (idx - 1)
            base_fill = ROW_A if (idx % 2 == 1) else ROW_B
            fill = rank_row_fill(idx, base_fill)
            ws.cell(r, start_col + 0).value = idx
            ws.cell(r, start_col + 1).value = row.player
            ws.cell(r, start_col + 2).value = float(row.league_points)
            ws.cell(r, start_col + 3).value = float(getattr(row, "avg_pts_per_map", 0) or 0)
            ws.cell(r, start_col + 4).value = int(row.maps_counted)
            ws.cell(r, start_col + 5).value = int(row.weeks_counted)

            for c in range(start_col, end_col + 1):
                align = LEFT if c == 2 else CENTER
                if c == start_col + 1:
                    align = LEFT
                font = Font(color="000000", bold=True) if c == start_col + 2 else FONT_BODY
                style_cell(ws, r, c, fill=fill, font=font, align=align)

        add_excel_table(
            wb,
            ws,
            header_row=header_row,
            start_col=start_col,
            end_row=header_row + len(table),
            end_col=end_col,
            name_hint=f"Underliga_{league_name}",
        )

    # Extra topplistor: snabbaste 5000-rundor (fallback: högsta enskilda runda)
    fast_section_row = 3 + 2 + first_row_max_rows + 3
    fast_headers = ["#", "Spelare", "Runda pts", "Tid"]
    fast_leagues = ["Sverige", "Världen"]
    fast_block_cols = len(fast_headers)
    fast_start_cols = [1, 8]  # align with block-grid columns above (1, 8, 15, 22)

    for i, fast_name in enumerate(fast_leagues):
        start_col = fast_start_cols[i]
        end_col = start_col + fast_block_cols - 1
        merge_and_style(
            ws,
            fast_section_row,
            start_col,
            fast_section_row,
            end_col,
            f"Snabbaste 5k - {fast_name}",
            fill=MID,
            font=FONT_HDR_MED,
            align=CENTER,
        )

        header_row = fast_section_row + 1
        for j, h in enumerate(fast_headers):
            c = start_col + j
            ws.cell(header_row, c).value = h
            style_cell(ws, header_row, c, fill=MID, font=FONT_HDR, align=CENTER)

        table = fast_tables.get(fast_name, pd.DataFrame())
        data_start_row = fast_section_row + 2
        for idx, row in enumerate(table.itertuples(index=False), start=1):
            r = data_start_row + (idx - 1)
            base_fill = ROW_A if (idx % 2 == 1) else ROW_B
            fill = rank_row_fill(idx, base_fill)
            ws.cell(r, start_col + 0).value = idx
            ws.cell(r, start_col + 1).value = row.player
            ws.cell(r, start_col + 2).value = int(_parse_int_maybe(row.round_pts) or 0)
            ws.cell(r, start_col + 3).value = format_seconds_compact(row.round_time)

            for c in range(start_col, end_col + 1):
                align = LEFT if c == start_col + 1 else CENTER
                font = Font(color="000000", bold=True) if c == start_col + 2 else FONT_BODY
                style_cell(ws, r, c, fill=fill, font=font, align=align)

        add_excel_table(
            wb,
            ws,
            header_row=header_row,
            start_col=start_col,
            end_row=header_row + len(table),
            end_col=end_col,
            name_hint=f"Fast5k_{fast_name}",
        )

    ws.freeze_panes = "A5"


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
        is_subtle = text.startswith("Mer info:")
        display_text, url = _extract_information_link(text)
        ws.cell(r, 1).value = "·" if is_subtle else "•"
        ws.cell(r, 2).value = _excel_hyperlink_formula(url, display_text) if url else display_text
        if url:
            ws.cell(r, 2).hyperlink = url
        font = FONT_BODY_SUBTLE if is_subtle else FONT_BODY
        style_cell(ws, r, 1, fill=fill, font=font, align=CENTER)
        style_cell(ws, r, 2, fill=fill, font=font, align=LEFT)
        if url:
            ws.cell(r, 2).font = Font(
                color="0563C1",
                bold=font.bold,
                italic=font.italic,
                size=font.sz,
                underline="single",
            )
        ws.row_dimensions[r].height = 28 if is_subtle else 34


def write_style_sheet(wb: Workbook, df_style: pd.DataFrame, df_similarity: pd.DataFrame) -> None:
    ws = wb.create_sheet("Spelstil")
    merge_and_style(ws, 1, 1, 1, 20, "Spelstilsanalys", fill=DARK, font=FONT_HDR_BIG, align=CENTER)
    ws["A2"] = (
        "Likhet bygger pa en viktad cosinuslikhet dar 5k-formaga, effektivitet, "
        "mode-profiler och stabilitet vager tyngre an totalpoang. "
        f"Kvalificering: minst {STYLE_MIN_MAPS} kartor eller {STYLE_MIN_WEEKS} veckor."
    )
    ws["A2"].font = FONT_BODY_SUBTLE

    if df_style.empty:
        ws["A4"] = "Ingen data tillganglig for spelstilsanalys."
        ws["A4"].font = Font(color="AA0000", bold=True)
        return

    feature_meta = style_feature_meta_df()
    _, pca_loadings, pca_info = compute_style_pca(df_style)

    headers = [
        "#", "Spelare", "Arketyp", "Kartor", "Veckor", "Kval.",
        "5k freq", "5k fart", "5k steg(M)", "Best-eff", "Tid-eff", "Konsistens",
        "Moving", "No move", "NMPZ", "Spec.", "Lik 1", "Likhet", "Lik 2", "Likhet 2", "Prec.-stod",
    ]
    widths = {
        1: 4.5, 2: 22.0, 3: 18.0, 4: 8.0, 5: 8.0, 6: 8.0,
        7: 9.0, 8: 9.0, 9: 10.0, 10: 10.0, 11: 9.0, 12: 10.0,
        13: 9.0, 14: 9.0, 15: 9.0, 16: 9.0, 17: 18.0, 18: 9.0, 19: 18.0, 20: 9.0,
        21: 10.0,
    }
    set_col_widths(ws, widths)

    header_row = 4
    for c, h in enumerate(headers, start=1):
        ws.cell(header_row, c).value = h
        style_cell(ws, header_row, c, fill=MID, font=FONT_HDR, align=CENTER)

    ws.freeze_panes = "A5"

    sim_lookup: Dict[str, List[Tuple[str, float]]] = {}
    if not df_similarity.empty and "player" in df_similarity.columns:
        for _, row in df_similarity.iterrows():
            player = str(row.get("player", ""))
            pairs: List[Tuple[str, float]] = []
            for col in df_similarity.columns:
                if col == "player":
                    continue
                val = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
                if pd.isna(val) or str(col) == player:
                    continue
                pairs.append((str(col), float(val)))
            sim_lookup[player] = sorted(pairs, key=lambda item: item[1], reverse=True)

    score_cols = {
        7: "fivek_rate_index",
        8: "fivek_speed_index",
        9: "fivek_steps_moving_index",
        10: "best_round_efficiency_index",
        11: "map_time_efficiency_index",
        12: "consistency_index",
        13: "moving_strength",
        14: "no_move_strength",
        15: "nmpz_strength",
        16: "specialization_index",
    }
    sorted_style = df_style.sort_values(
        ["is_qualified", "fivek_rate_index", "best_round_efficiency_index", "maps_counted", "player"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    for idx, row in enumerate(sorted_style.itertuples(index=False), start=1):
        r = header_row + idx
        base_fill = ROW_A if idx % 2 == 1 else ROW_B
        fill = rank_row_fill(idx, base_fill)
        ws.cell(r, 1).value = idx
        ws.cell(r, 2).value = row.player
        ws.cell(r, 3).value = row.style_archetype
        ws.cell(r, 4).value = int(getattr(row, "maps_counted", 0) or 0)
        ws.cell(r, 5).value = int(getattr(row, "weeks_counted", 0) or 0)
        ws.cell(r, 6).value = "Ja" if bool(getattr(row, "is_qualified", False)) else "Nej"

        for c in range(1, 7):
            align = LEFT if c in (2, 3) else CENTER
            style_cell(ws, r, c, fill=fill, font=FONT_BODY, align=align)

        for c, key in score_cols.items():
            val = float(pd.to_numeric(pd.Series([getattr(row, key)]), errors="coerce").iloc[0] or 0.0)
            ws.cell(r, c).value = round(val, 1)
            score_fill = _fill_color_from_scale(val, low_rgb=(246, 238, 228), high_rgb=(42, 119, 212))
            style_cell(ws, r, c, fill=score_fill, font=FONT_BODY, align=CENTER)

        matches = sim_lookup.get(str(row.player), [])
        match_1 = matches[0] if len(matches) >= 1 else ("", float("nan"))
        match_2 = matches[1] if len(matches) >= 2 else ("", float("nan"))
        ws.cell(r, 17).value = match_1[0]
        ws.cell(r, 18).value = "" if math.isnan(match_1[1]) else round(match_1[1], 3)
        ws.cell(r, 19).value = match_2[0]
        ws.cell(r, 20).value = "" if math.isnan(match_2[1]) else round(match_2[1], 3)
        ws.cell(r, 21).value = round(float(pd.to_numeric(pd.Series([getattr(row, "precision_support_index")]), errors="coerce").iloc[0] or 0.0), 1)
        style_cell(ws, r, 17, fill=fill, font=FONT_BODY, align=LEFT)
        style_cell(ws, r, 18, fill=_similarity_fill(match_1[1]) if not math.isnan(match_1[1]) else fill, font=FONT_BODY, align=CENTER)
        style_cell(ws, r, 19, fill=fill, font=FONT_BODY, align=LEFT)
        style_cell(ws, r, 20, fill=_similarity_fill(match_2[1]) if not math.isnan(match_2[1]) else fill, font=FONT_BODY, align=CENTER)
        style_cell(ws, r, 21, fill=_fill_color_from_scale(ws.cell(r, 21).value, low_rgb=(246, 238, 228), high_rgb=(42, 119, 212)), font=FONT_BODY, align=CENTER)

    add_excel_table(
        wb,
        ws,
        header_row=header_row,
        start_col=1,
        end_row=header_row + len(sorted_style),
        end_col=21,
        name_hint="SpelstilOversikt",
    )

    feature_start_col = 22
    merge_and_style(ws, 1, feature_start_col, 1, feature_start_col + 4, "PCA / featureforklaring", fill=DARK, font=FONT_HDR_MED, align=CENTER)
    feature_headers = ["Feature", "Vikt", "PC1", "PC2", "Tolkning"]
    for offset, head in enumerate(feature_headers):
        style_cell(ws, 3, feature_start_col + offset, fill=MID, font=FONT_HDR, align=CENTER)
        ws.cell(3, feature_start_col + offset).value = head
    set_col_widths(ws, {
        feature_start_col: 22.0,
        feature_start_col + 1: 8.0,
        feature_start_col + 2: 8.0,
        feature_start_col + 3: 8.0,
        feature_start_col + 4: 42.0,
    })

    loadings_sorted = pca_loadings.copy()
    if not loadings_sorted.empty:
        loadings_sorted["_rank"] = loadings_sorted[["pc1_loading", "pc2_loading"]].abs().max(axis=1)
        loadings_sorted = loadings_sorted.sort_values("_rank", ascending=False).drop(columns=["_rank"])
    for idx, row in enumerate(loadings_sorted.itertuples(index=False), start=4):
        ws.cell(idx, feature_start_col).value = getattr(row, "feature_label", "")
        ws.cell(idx, feature_start_col + 1).value = round(float(getattr(row, "weight", 0.0) or 0.0), 2)
        ws.cell(idx, feature_start_col + 2).value = round(float(getattr(row, "pc1_loading", 0.0) or 0.0), 3)
        ws.cell(idx, feature_start_col + 3).value = round(float(getattr(row, "pc2_loading", 0.0) or 0.0), 3)
        ws.cell(idx, feature_start_col + 4).value = getattr(row, "description", "")
        for c in range(feature_start_col, feature_start_col + 5):
            style_cell(ws, idx, c, fill=WHITE, font=FONT_BODY, align=LEFT if c in (feature_start_col, feature_start_col + 4) else CENTER)

    pc1_top = ", ".join(loadings_sorted.sort_values("pc1_loading", ascending=False)["feature_label"].head(3).tolist()) if not loadings_sorted.empty else ""
    pc2_top = ", ".join(loadings_sorted.sort_values("pc2_loading", ascending=False)["feature_label"].head(3).tolist()) if not loadings_sorted.empty else ""
    ws.cell(2, feature_start_col).value = f"PC1 ({pca_info.get('pc1_pct', 0.0):.1f}%): {pc1_top or 'for lite data'}"
    ws.cell(2, feature_start_col).font = FONT_BODY_SUBTLE
    ws.cell(2, feature_start_col + 3).value = f"PC2 ({pca_info.get('pc2_pct', 0.0):.1f}%): {pc2_top or 'for lite data'}"
    ws.cell(2, feature_start_col + 3).font = FONT_BODY_SUBTLE

    matrix_start_row = max(header_row + len(sorted_style) + 4, 6 + len(loadings_sorted))
    ws.cell(matrix_start_row, 1).value = "Likhetsmatris"
    ws.cell(matrix_start_row, 1).font = Font(bold=True, color="1B314B")
    ws.cell(matrix_start_row + 1, 1).value = "Visar kvalificerade spelare, begransat for lasbarhet."
    ws.cell(matrix_start_row + 1, 1).font = FONT_BODY_SUBTLE

    if df_similarity.empty or "player" not in df_similarity.columns:
        ws.cell(matrix_start_row + 3, 1).value = "For fa kvalificerade spelare for likhetsmatris."
        ws.cell(matrix_start_row + 3, 1).font = Font(color="AA0000", bold=True)
        return

    qualified_players = (
        sorted_style[sorted_style["is_qualified"]]
        .head(STYLE_SIMILARITY_MAX_PLAYERS)["player"]
        .astype(str).tolist()
    )
    matrix = (
        df_similarity.set_index("player")
        .reindex(index=qualified_players, columns=qualified_players)
    )

    start_r = matrix_start_row + 3
    start_c = 2
    ws.cell(start_r, 1).value = "Spelare"
    style_cell(ws, start_r, 1, fill=MID, font=FONT_HDR, align=CENTER)
    for j, player in enumerate(qualified_players, start=start_c):
        ws.cell(start_r, j).value = player
        style_cell(ws, start_r, j, fill=MID, font=FONT_HDR, align=CENTER)
        ws.column_dimensions[get_column_letter(j)].width = 11.0

    for i, player in enumerate(qualified_players, start=1):
        r = start_r + i
        ws.cell(r, 1).value = player
        style_cell(ws, r, 1, fill=MID, font=FONT_HDR, align=LEFT)
        for j, col_player in enumerate(qualified_players, start=start_c):
            value = matrix.loc[player, col_player] if (player in matrix.index and col_player in matrix.columns) else ""
            if pd.isna(value):
                value = ""
            ws.cell(r, j).value = "" if value == "" else round(float(value), 3)
            fill = _similarity_fill(value) if value != "" else WHITE
            font = FONT_BODY if player != col_player else Font(color="FFFFFF", bold=True)
            style_cell(ws, r, j, fill=fill, font=font, align=CENTER)

    add_excel_table(
        wb,
        ws,
        header_row=start_r,
        start_col=1,
        end_row=start_r + len(qualified_players),
        end_col=start_c + len(qualified_players) - 1,
        name_hint="SpelstilLikhet",
        header_horizontal="center",
    )


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

    add_excel_table(
        wb,
        ws,
        header_row=1,
        start_col=1,
        end_row=1 + len(df_overview),
        end_col=len(headers),
        name_hint="RawData",
    )


def _output_with_suffix(path: Path, index: int) -> Path:
    if index <= 0:
        return path
    return path.with_name(f"{path.stem} ({index}){path.suffix}")


def save_workbook_with_fallback(wb: Workbook, desired_path: Path, max_attempts: int = 50) -> Path:
    """
    Save workbook to desired_path, overwriting when possible.
    If the target is locked (e.g. opened in Excel), fallback to suffixed names.
    """
    last_exc: Optional[Exception] = None
    for n in range(0, max_attempts + 1):
        candidate = _output_with_suffix(desired_path, n)
        try:
            wb.save(candidate)
            if n > 0:
                print(f"[WARN] output file locked: {desired_path.name}. Saved as {candidate.name}")
            return candidate
        except PermissionError as e:
            last_exc = e
            continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Could not save workbook: {desired_path}")


# ============================================================
# Main
# ============================================================

def parse_week_specs(week_args: List[str]) -> List[WeekSpec]:
    if not week_args:
        raise SystemExit(
            'No weeks specified. Example:\n'
            '  python geoguessr_league_build_xlsx.py --week "Vecka 1|urls_week1.txt|2026-02-18 20:00|1,4" --week "Vecka 2|urls_week2.txt|2026-02-25 20:00|2"'
        )

    out: List[WeekSpec] = []
    for s in week_args:
        parts = [p.strip() for p in s.split("|")]
        if len(parts) < 2 or len(parts) > 4:
            raise SystemExit(f'Bad --week "{s}". Expected "LABEL|URLS_FILE|DEADLINE(optional)|SWEDEN_MAPS(optional)".')
        label = parts[0]
        urls_path = Path(parts[1]).expanduser()
        deadline = parts[2] if len(parts) >= 3 and parts[2] else None
        sweden_maps = normalize_sweden_map_indexes(parts[3] if len(parts) >= 4 else "")
        out.append(WeekSpec(label=label, urls_path=urls_path, deadline=deadline, sweden_maps=sweden_maps))
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
    print("[START] sort_by:", normalize_sort_key(args.sort_by))

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
    df_style_all, df_similarity_all = compute_style_tables(df_overview_all)

    # Decide filtering
    can_filter = bool(deadlines_epoch) and bool(args.fetch_played_at) and any_played_at

    # Build filtered data (if possible)
    df_overview_f = df_weekly_f = df_meta_f = df_total_f = df_stats_f = df_style_f = df_similarity_f = None
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
        df_style_f, df_similarity_f = compute_style_tables(df_overview_f)
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

    write_total_sheet(wb_all, df_total_all, df_overview_all, week_labels, sort_by=args.sort_by)
    write_stats_sheet(wb_all, df_stats_all, sort_by=args.sort_by)
    write_underligor_sheet(wb_all, df_overview_all, sort_by=args.sort_by)
    write_style_sheet(wb_all, df_style_all, df_similarity_all)
    write_visualizations_sheet(
        wb_all,
        df_overview_all,
        df_total_all,
        df_style_all,
        df_similarity_all,
        week_labels,
        image_dir=out_all.parent / "visualizations" / out_all.stem,
    )
    write_raw_sheet(wb_all, df_overview_all)

    actual_out_all = save_workbook_with_fallback(wb_all, out_all)
    print("[DONE] wrote:", actual_out_all)

    # Write FILTERED workbook (if available)
    if can_filter and df_overview_f is not None and df_weekly_f is not None and df_meta_f is not None and df_total_f is not None and df_stats_f is not None and df_style_f is not None and df_similarity_f is not None:
        out_f = Path(f"{args.out_base}_filtered.xlsx")
        wb_f = Workbook()
        wb_f.remove(wb_f.active)
        write_information_sheet(wb_f, info_rows)

        for w in successful_weeks:
            dl_str = w.deadline or ""
            write_week_sheet(wb_f, w.label, f"Deadline {dl_str}" if dl_str else "Deadline", df_weekly_f, df_overview_f, df_meta_f)

        write_total_sheet(wb_f, df_total_f, df_overview_f, week_labels, sort_by=args.sort_by)
        write_stats_sheet(wb_f, df_stats_f, sort_by=args.sort_by)
        write_underligor_sheet(wb_f, df_overview_f, sort_by=args.sort_by)
        write_style_sheet(wb_f, df_style_f, df_similarity_f)
        write_visualizations_sheet(
            wb_f,
            df_overview_f,
            df_total_f,
            df_style_f,
            df_similarity_f,
            week_labels,
            image_dir=out_f.parent / "visualizations" / out_f.stem,
        )
        write_raw_sheet(wb_f, df_overview_f)

        actual_out_f = save_workbook_with_fallback(wb_f, out_f)
        print("[DONE] wrote:", actual_out_f)

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
