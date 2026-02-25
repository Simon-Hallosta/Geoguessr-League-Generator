# 🌍 GeoGuessr League → Excel

> Build a structured league spreadsheet from GeoGuessr challenge URLs  
> Weekly tabs · Total standings · Advanced statistics · Optional deadline filtering

---

## ✨ Features

- 📅 One sheet per week (e.g. *Vecka 1, Vecka 2, …*)
- 🏆 Automatic **Total standings**
- 📊 Advanced **Stats sheet**
- 🔍 Full **Raw data export**
- ⏱ Optional **deadline filtering**
- ⚖️ Configurable tie-breaking logic
- 🧮 Borda-based league scoring

---

## 🗂 Excel Output Structure

| Sheet | Contents |
|--------|----------|
| **Vecka X** | Weekly ranking + Borda points per map |
| **Total** | Aggregated standings across all weeks |
| **Stats** | Performance metrics & averages |
| **Raw** | Underlying structured dataset |

---

# 🚀 Installation

## Requirements

- Python **3.9+** (3.10 / 3.11 recommended)
- `requests`
- `pandas`
- `openpyxl`

Install dependencies:

```bash
pip install requests pandas openpyxl
```

---

# 📥 Input Files

For each week, create:
urls_weekX.txt


Each file contains one challenge URL per line:
https://www.geoguessr.com/challenge/a2VSPJrVz2RwATaN
https://www.geoguessr.com/challenge/xxxxxxxxxxxxxxxx


Lines starting with `#` are ignored.

---

# 🔐 Authentication (Required)

GeoGuessr blocks automated login.  
This script requires your authenticated browser session cookie: `_ncfa`.

---

## Step 1 — Log in normally

Log in to GeoGuessr in Chrome.

---

## Step 2 — Open DevTools

Navigate to:
DevTools → Application → Cookies → https://www.geoguessr.com


![DevTools Application Tab](/img/f12-application.png)

---

## Step 3 — Copy `_ncfa`

Locate the `_ncfa` cookie and copy its value.

![Copy _ncfa Cookie Value](/img/_ncfa-cookie-value.png)

---

## Step 4 — Set Environment Variable

### Windows (cmd)

```bash
set GEOGUESSR_NCFA=PASTE_VALUE_HERE
```

### PowerShell

```powershell
$env:GEOGUESSR_NCFA="PASTE_VALUE_HERE"
```

### macOS / Linux

```bash
export GEOGUESSR_NCFA="PASTE_VALUE_HERE"
```

Or pass it directly:

```bash
python geoguessr_league_build_xlsx.py --ncfa "PASTE_VALUE_HERE"
```

---

# 🧮 Scoring Logic

## Map Ranking

For each challenge:

1. Higher `total_pts` ranks higher
2. Tie-break: lower `total_time`
3. Exact ties resolved via `--tie` mode

---

## 🏁 League Points (Borda System)

If **N players** played a map:

| Rank | Points |
|------|--------|
| 1st | N |
| 2nd | N−1 |
| ... | ... |
| Last | 1 |

Weekly score = Sum of Borda points  
Total score = Sum of weekly totals

---

# ▶ Running the Script

## Without Deadline Filter

Example with two weeks:

```bash
python geoguessr_league_build_xlsx.py \
  --week "Vecka 1|urls_week1.txt" \
  --week "Vecka 2|urls_week2.txt" \
  --out-base "Liga"
```

Output:
Liga_all.xlsx


---

## ⏱ With Deadline Filtering

```bash
python geoguessr_league_build_xlsx.py \
  --week "Vecka 1|urls_week1.txt|2026-02-18 20:00" \
  --week "Vecka 2|urls_week2.txt|2026-02-25 20:00" \
  --fetch-played-at \
  --out-base "Liga"
```

Output:
Liga_all.xlsx
Liga_filtered.xlsx


Default timezone: `Europe/Stockholm`

Override:

```bash
python geoguessr_league_build_xlsx.py --tz "Europe/Stockholm"
```

---

# 🛠 Useful Flags

| Flag | Purpose |
|------|----------|
| `--debug` | Enable verbose output |
| `--dump-json` | Dump raw API JSON |
| `--tie` | Tie mode: average / dense / min / max |
| `--timeout` | HTTP timeout |
| `--page-size` | Pagination size |
| `--max-players` | Player limit |

Example:

```bash
python geoguessr_league_build_xlsx.py --tie dense --timeout 60
```

---

# 🧪 Troubleshooting

### ❌ Missing `_ncfa`
Ensure `GEOGUESSR_NCFA` is correctly set.

### ❌ Filtered file not created
- Deadline must be specified
- `--fetch-played-at` must be enabled
- API must return timestamps

### ❌ Empty results
- Verify challenge URLs
- Refresh your `_ncfa` cookie
- Run with `--debug` or `--dump-json`

---

# ⚠ Disclaimer

This is an unofficial tool.  
Use at your own risk and respect GeoGuessr's terms of service.