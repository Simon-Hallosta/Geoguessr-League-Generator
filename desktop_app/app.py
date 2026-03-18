from __future__ import annotations

import io
import json
import locale
import math
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
import uuid
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk

try:
    from tkcalendar import DateEntry
except Exception:
    DateEntry = None  # type: ignore[assignment]

try:
    from PIL import Image, ImageTk
except Exception:
    Image = None  # type: ignore[assignment]
    ImageTk = None  # type: ignore[assignment]


def _resolve_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def _resolve_resource_dir() -> Path:
    bundle_dir = getattr(sys, "_MEIPASS", None)
    if bundle_dir:
        return Path(bundle_dir)
    return Path(__file__).resolve().parents[1]


ROOT_DIR = _resolve_base_dir()
RESOURCE_DIR = _resolve_resource_dir()
WEEK_FILES_DIR = ROOT_DIR / "week_urls"
APP_STATE_PATH = ROOT_DIR / "desktop_app_state.json"
INFO_CONFIG_PATH = ROOT_DIR / "information_config_v2.json"
LEGACY_INFO_CONFIG_PATH = ROOT_DIR / "information_config.json"
INFO_CONFIG_LEGACY_DIR = ROOT_DIR / "legacy_configs" / "information"
APP_ICON_PATH = RESOURCE_DIR / "desktop_app" / "assets" / "geoleague.ico"
NCFA_HELP_IMAGES = {
    "application": RESOURCE_DIR / "img" / "f12-application.png",
    "cookie": RESOURCE_DIR / "img" / "_ncfa-cookie-value.png",
}

BG_APP = "#EEF3FA"
BG_CARD = "#FFFFFF"
BG_HERO = "#102A43"
BORDER = "#D2DEEC"
TEXT_MAIN = "#1B314B"
TEXT_MUTED = "#5E7188"
ACCENT = "#1F7AE0"
ACCENT_HOVER = "#1A66BC"
ACCENT_SOFT = "#DCEBFF"
LOG_BG = "#0D1B2A"
LOG_FG = "#E8F1FB"

TABLE_SORT_OPTIONS = [
    ("Standard (Poäng)", "default"),
    ("Total pts", "total_pts"),
    ("Kartor", "maps"),
    ("Veckor", "weeks"),
    ("Snitt pts/karta", "avg_pts"),
    ("Snitt poäng/karta", "avg_points"),
]
DEFAULT_TABLE_SORT_KEY = "default"
DEFAULT_SWEDEN_MAPS = "1,4"
LANGUAGE_OPTIONS = [
    ("Auto (system)", "auto"),
    ("Svenska", "sv"),
    ("English", "en"),
]
LANGUAGE_LABEL_TO_KEY = {label: key for label, key in LANGUAGE_OPTIONS}
LANGUAGE_KEY_TO_LABEL = {key: label for label, key in LANGUAGE_OPTIONS}
UI_TRANSLATIONS_EN = {
    "GeoGuessr League Desktop": "GeoGuessr League Desktop",
    "Desktop Edition": "Desktop Edition",
    "Challenge-insamling och ligarapport i ett klick": "Challenge collection and league reporting in one click",
    "1) Inloggning / miljövariabel": "1) Login / environment variable",
    "Sätt GEOGUESSR_NCFA i appen": "Set GEOGUESSR_NCFA in app",
    "Var hittar jag _ncfa?": "Where do I find _ncfa?",
    "Spara i Windows (setx)": "Save in Windows (setx)",
    "Behöver du hjälp första gången? Öppna guiden och följ samma steg som i README:n.": "Need help the first time? Open the guide and follow the same steps as in the README.",
    "Språk:": "Language:",
    "Auto använder operativsystemets språk. Språkvalet skickas vidare till generatorn direkt.": "Auto uses the operating system language. The selected language is passed directly to the generator.",
    "2) Veckofiler": "2) Week files",
    "Lägg till befintliga .txt-filer eller skapa nya. Varje rad i filen ska vara en challenge-länk.": "Add existing .txt files or create new ones. Each row in a file should contain a challenge link.",
    "Vecka": "Week",
    "Textfil": "Text file",
    "Deadline (valfri)": "Deadline (optional)",
    "Deadline (valfri):": "Deadline (optional):",
    "Sverige-kartor": "Sweden maps",
    "Lägg till befintliga filer": "Add existing files",
    "Skapa ny veckofil": "Create new week file",
    "Ändra deadline": "Edit deadline",
    "Ändra Sverige-kartor": "Edit Sweden maps",
    "Ta bort vald": "Remove selected",
    "3) Körning": "3) Run",
    "Output-bas:": "Output base:",
    "Tidszon:": "Timezone:",
    "Tie-läge:": "Tie mode:",
    "Sortera tabeller:": "Sort tables:",
    "Gäller Total, Stats och Underligor.": "Applies to Total, Stats, and Subleagues.",
    "Obs: Tid används alltid som tie-break vid samma poäng. Tie-läge gäller bara exakt lika poäng + tid.": "Note: Time is always used as a tiebreak for equal points. Tie mode only applies to exactly equal points + time.",
    "Hämta played_at (för deadline-filter)": "Fetch played_at (for deadline filter)",
    "Behåll poster utan tidsstämpel": "Keep rows without timestamp",
    "Hämta detaljerad moving/5k-statistik (långsammare)": "Fetch detailed moving/5k metrics (slower)",
    "Skapa avancerad spelstil/5k-analys (långsammare)": "Create advanced style/5k analysis (slower)",
    "Debug-logg": "Debug log",
    "Kör och skapa Excel": "Run and create Excel",
    "Redigera Information-flik": "Edit Information sheet",
    "Öppna projektmapp": "Open project folder",
    "Logg": "Log",
    "Appen startad.": "App started.",
    "Tips: skapa veckofiler i appen, eller lägg till befintliga .txt-filer.": "Tip: create week files in the app, or add existing .txt files.",
    "Redo": "Ready",
    "Standard (Poäng)": "Standard (Points)",
    "Kartor": "Maps",
    "Veckor": "Weeks",
    "Snitt pts/karta": "Avg pts/map",
    "Snitt poäng/karta": "Avg points/map",
    "Kör...": "Running...",
    "Körning pågår... hämtar och bearbetar data.": "Run in progress... fetching and processing data.",
    "Klart.": "Done.",
    "Klart med varningar.": "Done with warnings.",
    "Klar.": "Done.",
    "Körning misslyckades.": "Run failed.",
    "Skapa veckofil": "Create week file",
    "Vecka 1": "Week 1",
    "Veckoetikett:": "Week label:",
    "Filnamn:": "Filename:",
    "Sverige-kartor:": "Sweden maps:",
    "Exempel: 1,4": "Example: 1,4",
    "Länkar (en per rad):": "Links (one per line):",
    "Spara fil": "Save file",
    "Avbryt": "Cancel",
    "Fel": "Error",
    "Veckoetikett måste anges.": "Week label is required.",
    "Filnamn måste anges.": "Filename is required.",
    "Minst en länk måste anges.": "At least one link is required.",
    "Välj deadline": "Choose deadline",
    "Datum:": "Date:",
    "Tips: installera `tkcalendar` för popup-kalender.": "Tip: install `tkcalendar` for a popup calendar.",
    "Tid (HH:MM):": "Time (HH:MM):",
    "Rensa": "Clear",
    "Spara": "Save",
    "Ogiltigt datum eller klockslag.": "Invalid date or time.",
    "Information-flik: konfiguration": "Information sheet: configuration",
    "Ange en punkt per rad för Information-fliken.\nNär du klickar Spara skrivs config-filen över.\nNuvarande config sparas först som legacy-kopia med datum.": "Enter one bullet point per line for the Information sheet.\nWhen you click Save, the config file is overwritten.\nThe current config is first saved as a dated legacy copy.",
    "Config-fil: {config}\nLegacy-mapp: {legacy}": "Config file: {config}\nLegacy folder: {legacy}",
    "Återställ default": "Reset defaults",
    "Spara (skriver över config)": "Save (overwrite config)",
    "Lägg till minst en informationsrad.": "Add at least one information row.",
    "Bekräfta överskrivning": "Confirm overwrite",
    "Detta skriver över config-filen:\n{config}\n\nNuvarande config sparas först i:\n{legacy}\n\nVill du fortsätta?": "This will overwrite the config file:\n{config}\n\nThe current config is first saved in:\n{legacy}\n\nDo you want to continue?",
    "Hitta _ncfa": "Find _ncfa",
    "Så hittar du GeoGuessr-cookien _ncfa": "How to find the GeoGuessr _ncfa cookie",
    "Guiden återanvänder samma steg som README:n. När du har kopierat värdet klistrar du in det i fältet i appen.": "This guide follows the same steps as the README. After copying the value, paste it into the field in the app.",
    "1. Logga in i GeoGuessr": "1. Log in to GeoGuessr",
    "Öppna GeoGuessr i din vanliga webbläsare och logga in som vanligt.": "Open GeoGuessr in your normal browser and log in as usual.",
    "2. Öppna DevTools och gå till Cookies": "2. Open DevTools and go to Cookies",
    "Tryck F12 och gå till Application -> Cookies -> https://www.geoguessr.com.": "Press F12 and go to Application -> Cookies -> https://www.geoguessr.com.",
    "3. Leta upp _ncfa": "3. Find _ncfa",
    "Markera raden med namnet _ncfa och kopiera dess value.": "Select the row named _ncfa and copy its value.",
    "4. Klistra in värdet i appen": "4. Paste the value into the app",
    "Klistra in cookien i _ncfa-fältet här i appen. Du kan sedan välja antingen att bara sätta den i appen eller spara den som Windows-variabel.": "Paste the cookie into the _ncfa field here in the app. You can then choose either to set it only in the app or save it as a Windows variable.",
    "Stäng": "Close",
    "Bild kunde inte laddas: {name}": "Could not load image: {name}",
    "_ncfa saknas.": "_ncfa is missing.",
    "Denna funktion är bara tillgänglig på Windows.": "This function is only available on Windows.",
    "Kunde inte spara variabeln:\n{err}": "Could not save the variable:\n{err}",
    "Välj URL-textfiler": "Choose URL text files",
    "Textfiler": "Text files",
    "Alla filer": "All files",
    "Veckoetikett": "Week label",
    "Ange veckonamn för:\n{name}": "Enter a week name for:\n{name}",
    "Filen finns inte:\n{path}": "The file does not exist:\n{path}",
    "Info": "Info",
    "Markera en rad först.": "Select a row first.",
    "Sverige-kartor": "Sweden maps",
    "Ange kartnummer för Sverige i {label}.\nExempel: 1,4": "Enter the map numbers for Sweden in {label}.\nExample: 1,4",
    "Kunde inte spara information-config. Se loggen för detaljer.": "Could not save the information config. See the log for details.",
    "Information-config sparad.": "Information config saved.",
    "Lägg till minst en veckofil.": "Add at least one week file.",
    "Dessa veckofiler saknas:\n\n{files}": "These week files are missing:\n\n{files}",
    "Kunde inte hitta ett skrivbart filnamn för output. Stäng eventuell öppen Excel-fil och försök igen.": "Could not find a writable filename for the output. Close any open Excel file and try again.",
    "Outputfil låst": "Output file locked",
    "En eller flera outputfiler är öppna/låsta och kunde inte skrivas över.\nSparar istället med suffix: {out_base}": "One or more output files are open/locked and could not be overwritten.\nSaving instead with suffix: {out_base}",
    "Klart med varningar": "Done with warnings",
    "Klart": "Done",
    "Excel-filer skapades, men en eller flera veckor/kartor kunde inte hämtas fullt ut.\nSe loggen för detaljer.": "Excel files were created, but one or more weeks/maps could not be fully fetched.\nSee the log for details.",
    "Excel-filer skapades.": "Excel files were created.",
    "Körning misslyckades. Se loggen.": "The run failed. See the log.",
    "Kunde inte öppna mappen:\n{ex}": "Could not open the folder:\n{ex}",
    "Körning pågår": "Run in progress",
    "En körning pågår fortfarande. Vill du verkligen avsluta appen?": "A run is still in progress. Do you really want to close the app?",
}


def _detect_system_language() -> str:
    candidates = [
        os.environ.get("LANG"),
        os.environ.get("LC_ALL"),
        os.environ.get("LC_MESSAGES"),
    ]
    try:
        candidates.extend([locale.getlocale()[0], locale.getdefaultlocale()[0]])  # type: ignore[index]
    except Exception:
        pass
    for candidate in candidates:
        raw = str(candidate or "").strip().lower()
        if not raw:
            continue
        if raw.startswith("sv"):
            return "sv"
        if raw.startswith("en"):
            return "en"
    return "en"


def _scaled_window_size(root: tk.Misc, *, width_ratio: float, height_ratio: float, min_width: int, min_height: int, max_width_margin: int = 80, max_height_margin: int = 80) -> tuple[int, int]:
    screen_w = max(1, int(root.winfo_screenwidth()))
    screen_h = max(1, int(root.winfo_screenheight()))
    width = max(min_width, int(screen_w * width_ratio))
    height = max(min_height, int(screen_h * height_ratio))
    width = min(width, max(min_width, screen_w - max_width_margin))
    height = min(height, max(min_height, screen_h - max_height_margin))
    return width, height


def _initial_main_window_size(root: tk.Misc) -> tuple[int, int]:
    screen_w = max(1, int(root.winfo_screenwidth()))
    screen_h = max(1, int(root.winfo_screenheight()))
    width = min(1080, max(980, screen_w - 80))
    height = min(max(740, int(screen_h * 0.9)), max(700, screen_h - 80))
    return width, height


def _initial_help_dialog_size(root: tk.Misc) -> tuple[int, int]:
    screen_w = max(1, int(root.winfo_screenwidth()))
    screen_h = max(1, int(root.winfo_screenheight()))
    width = min(900, max(760, screen_w - 120))
    height = min(max(620, int(screen_h * 0.86)), max(620, screen_h - 80))
    return width, height

if getattr(sys, "frozen", False):
    os.chdir(ROOT_DIR)

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import geoguessr_league_build_xlsx as league_core  # noqa: E402


@dataclass
class WeekConfig:
    label: str
    file_path: Path
    deadline: str = ""
    sweden_maps: str = DEFAULT_SWEDEN_MAPS

    def effective_sweden_maps(self) -> str:
        return self.sweden_maps.strip() or DEFAULT_SWEDEN_MAPS

    def to_week_arg(self) -> str:
        deadline = self.deadline.strip()
        sweden_maps = self.effective_sweden_maps()
        if sweden_maps:
            return f"{self.label}|{self.file_path}|{deadline}|{sweden_maps}"
        if deadline:
            return f"{self.label}|{self.file_path}|{deadline}"
        return f"{self.label}|{self.file_path}"


class CreateWeekFileDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, on_save, tr=lambda x: x):
        super().__init__(master)
        self.tr = tr
        self.title(self.tr("Skapa veckofil"))
        self.geometry("760x560")
        self.minsize(680, 520)
        self.configure(bg=BG_APP)
        self.on_save = on_save

        self.label_var = tk.StringVar(value=self.tr("Vecka 1"))
        self.deadline_var = tk.StringVar(value="")
        self.filename_var = tk.StringVar(value="urls_week1.txt")
        self.sweden_maps_var = tk.StringVar(value=DEFAULT_SWEDEN_MAPS)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(4, weight=1)

        frm = ttk.Frame(self, style="Card.TFrame", padding=12)
        frm.grid(sticky="nsew")
        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(6, weight=1)

        ttk.Label(frm, text=self.tr("Veckoetikett:"), style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        label_entry = ttk.Entry(frm, textvariable=self.label_var, style="Modern.TEntry")
        label_entry.grid(row=0, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text=self.tr("Deadline (valfri):"), style="Field.TLabel").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.deadline_var, style="Modern.TEntry").grid(row=1, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text=self.tr("Filnamn:"), style="Field.TLabel").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.filename_var, style="Modern.TEntry").grid(row=2, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text=self.tr("Sverige-kartor:"), style="Field.TLabel").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.sweden_maps_var, style="Modern.TEntry").grid(row=3, column=1, sticky="ew", pady=(0, 8))
        ttk.Label(frm, text=self.tr("Exempel: 1,4"), style="Hint.TLabel").grid(row=4, column=1, sticky="w", pady=(0, 6))

        ttk.Label(frm, text=self.tr("Länkar (en per rad):"), style="Field.TLabel").grid(row=5, column=0, columnspan=2, sticky="w", pady=(8, 4))

        self.links_txt = tk.Text(
            frm,
            height=16,
            wrap="word",
            bg="#FBFCFE",
            fg=TEXT_MAIN,
            insertbackground=TEXT_MAIN,
            highlightthickness=1,
            highlightbackground=BORDER,
            relief="flat",
            font=("Segoe UI", 10),
        )
        self.links_txt.grid(row=6, column=0, columnspan=2, sticky="nsew")

        button_row = ttk.Frame(frm, style="Card.TFrame")
        button_row.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        button_row.columnconfigure(0, weight=1)

        ttk.Button(button_row, text=self.tr("Spara fil"), style="Accent.TButton", command=self.save).grid(row=0, column=1, sticky="e")
        ttk.Button(button_row, text=self.tr("Avbryt"), style="Soft.TButton", command=self.destroy).grid(row=0, column=2, sticky="e", padx=(8, 0))

        self.label_var.trace_add("write", self._on_label_changed)
        self.transient(master)
        self.grab_set()
        label_entry.focus_set()

    def _on_label_changed(self, *_args) -> None:
        label = self.label_var.get().strip()
        digits = re.findall(r"\d+", label)
        if digits:
            self.filename_var.set(f"urls_week{digits[0]}.txt")
            return

        slug = re.sub(r"[^A-Za-z0-9]+", "_", label.lower()).strip("_")
        self.filename_var.set(f"urls_{slug or 'week'}.txt")

    def save(self) -> None:
        label = self.label_var.get().strip()
        deadline = self.deadline_var.get().strip()
        filename = self.filename_var.get().strip()
        sweden_maps = self.sweden_maps_var.get().strip()
        raw_links = self.links_txt.get("1.0", "end")
        links = [line.strip() for line in raw_links.splitlines() if line.strip()]

        if not label:
            messagebox.showerror(self.tr("Fel"), self.tr("Veckoetikett måste anges."), parent=self)
            return
        if not filename:
            messagebox.showerror(self.tr("Fel"), self.tr("Filnamn måste anges."), parent=self)
            return
        if not filename.lower().endswith(".txt"):
            filename += ".txt"
        if not links:
            messagebox.showerror(self.tr("Fel"), self.tr("Minst en länk måste anges."), parent=self)
            return

        WEEK_FILES_DIR.mkdir(parents=True, exist_ok=True)
        file_path = WEEK_FILES_DIR / filename
        file_path.write_text("\n".join(links) + "\n", encoding="utf-8")

        self.on_save(WeekConfig(label=label, file_path=file_path, deadline=deadline, sweden_maps=sweden_maps))
        self.destroy()


class DeadlineDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, initial_value: str, tr=lambda x: x):
        super().__init__(master)
        self.tr = tr
        self.title(self.tr("Välj deadline"))
        self.geometry("430x220")
        self.resizable(False, False)
        self.configure(bg=BG_APP)
        self.result: Optional[str] = None

        default_dt = self._parse_initial(initial_value) or datetime.now().replace(second=0, microsecond=0)

        frm = ttk.Frame(self, style="Card.TFrame", padding=14)
        frm.pack(fill="both", expand=True, padx=10, pady=10)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text=self.tr("Datum:"), style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        if DateEntry is not None:
            self.date_picker = DateEntry(frm, date_pattern="yyyy-mm-dd", width=12)
            self.date_picker.grid(row=0, column=1, sticky="w", pady=(0, 8))
            self.date_picker.set_date(default_dt.date())
            self.year_var = None
            self.month_var = None
            self.day_var = None
        else:
            self.date_picker = None
            self.year_var = tk.IntVar(value=default_dt.year)
            self.month_var = tk.IntVar(value=default_dt.month)
            self.day_var = tk.IntVar(value=default_dt.day)
            date_row = ttk.Frame(frm, style="Card.TFrame")
            date_row.grid(row=0, column=1, sticky="w", pady=(0, 8))
            ttk.Spinbox(date_row, from_=2020, to=2100, width=6, textvariable=self.year_var).pack(side="left")
            ttk.Label(date_row, text="-", style="Field.TLabel").pack(side="left", padx=3)
            ttk.Spinbox(date_row, from_=1, to=12, width=3, textvariable=self.month_var).pack(side="left")
            ttk.Label(date_row, text="-", style="Field.TLabel").pack(side="left", padx=3)
            ttk.Spinbox(date_row, from_=1, to=31, width=3, textvariable=self.day_var).pack(side="left")
            ttk.Label(
                frm,
                text=self.tr("Tips: installera `tkcalendar` för popup-kalender."),
                style="Hint.TLabel",
            ).grid(row=1, column=1, sticky="w")

        ttk.Label(frm, text=self.tr("Tid (HH:MM):"), style="Field.TLabel").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        time_row = ttk.Frame(frm, style="Card.TFrame")
        time_row.grid(row=2, column=1, sticky="w", pady=(6, 0))
        self.hour_var = tk.IntVar(value=default_dt.hour)
        self.minute_var = tk.IntVar(value=default_dt.minute)
        ttk.Spinbox(time_row, from_=0, to=23, width=3, format="%02.0f", textvariable=self.hour_var).pack(side="left")
        ttk.Label(time_row, text=":", style="Field.TLabel").pack(side="left", padx=3)
        ttk.Spinbox(time_row, from_=0, to=59, width=3, format="%02.0f", textvariable=self.minute_var).pack(side="left")

        buttons = ttk.Frame(frm, style="Card.TFrame")
        buttons.grid(row=3, column=0, columnspan=2, sticky="e", pady=(14, 0))
        ttk.Button(buttons, text=self.tr("Rensa"), style="Outline.TButton", command=self.clear_deadline).pack(side="left")
        ttk.Button(buttons, text=self.tr("Avbryt"), style="Soft.TButton", command=self.cancel).pack(side="left", padx=(8, 0))
        ttk.Button(buttons, text=self.tr("Spara"), style="Accent.TButton", command=self.save).pack(side="left", padx=(8, 0))

        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.cancel)

    def _parse_initial(self, value: str) -> Optional[datetime]:
        txt = (value or "").strip()
        if not txt:
            return None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(txt, fmt)
            except ValueError:
                continue
        return None

    def _selected_date(self) -> datetime:
        if self.date_picker is not None:
            day = self.date_picker.get_date()
            return datetime(day.year, day.month, day.day)
        if self.year_var is None or self.month_var is None or self.day_var is None:
            raise ValueError("Date not initialized")
        return datetime(self.year_var.get(), self.month_var.get(), self.day_var.get())

    def clear_deadline(self) -> None:
        self.result = ""
        self.destroy()

    def cancel(self) -> None:
        self.result = None
        self.destroy()

    def save(self) -> None:
        try:
            base = self._selected_date()
            dt = datetime(
                year=base.year,
                month=base.month,
                day=base.day,
                hour=int(self.hour_var.get()),
                minute=int(self.minute_var.get()),
            )
        except Exception:
            messagebox.showerror(self.tr("Fel"), self.tr("Ogiltigt datum eller klockslag."), parent=self)
            return

        self.result = dt.strftime("%Y-%m-%d %H:%M")
        self.destroy()


class InformationConfigDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, initial_rows: list[str], default_rows: list[str], config_path: Path, legacy_dir: Path, tr=lambda x: x):
        super().__init__(master)
        self.tr = tr
        self.title(self.tr("Information-flik: konfiguration"))
        self.geometry("980x700")
        self.minsize(860, 600)
        self.configure(bg=BG_APP)
        self.result_rows: Optional[list[str]] = None
        self.default_rows = list(default_rows)
        self.config_path = config_path
        self.legacy_dir = legacy_dir

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        frm = ttk.Frame(self, style="Card.TFrame", padding=12)
        frm.grid(sticky="nsew")
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(2, weight=1)

        ttk.Label(
            frm,
            text=self.tr("Ange en punkt per rad för Information-fliken.\nNär du klickar Spara skrivs config-filen över.\nNuvarande config sparas först som legacy-kopia med datum."),
            style="Field.TLabel",
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        ttk.Label(
            frm,
            text=self.tr("Config-fil: {config}\nLegacy-mapp: {legacy}").format(config=self.config_path, legacy=self.legacy_dir),
            style="Hint.TLabel",
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(0, 8))

        self.text = tk.Text(
            frm,
            wrap="word",
            bg="#FBFCFE",
            fg=TEXT_MAIN,
            insertbackground=TEXT_MAIN,
            highlightthickness=1,
            highlightbackground=BORDER,
            relief="flat",
            font=("Segoe UI", 10),
        )
        self.text.grid(row=2, column=0, sticky="nsew")
        self.text.insert("1.0", "\n".join(initial_rows))

        buttons = ttk.Frame(frm, style="Card.TFrame")
        buttons.grid(row=3, column=0, sticky="e", pady=(12, 0))
        ttk.Button(buttons, text=self.tr("Återställ default"), style="Outline.TButton", command=self.reset_default).pack(side="left")
        ttk.Button(buttons, text=self.tr("Avbryt"), style="Soft.TButton", command=self.cancel).pack(side="left", padx=(8, 0))
        ttk.Button(buttons, text=self.tr("Spara (skriver över config)"), style="Accent.TButton", command=self.save).pack(side="left", padx=(8, 0))

        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.cancel)

    def reset_default(self) -> None:
        self.text.delete("1.0", "end")
        self.text.insert("1.0", "\n".join(self.default_rows))

    def cancel(self) -> None:
        self.result_rows = None
        self.destroy()

    def save(self) -> None:
        raw = self.text.get("1.0", "end")
        rows = [line.strip() for line in raw.splitlines() if line.strip()]
        if not rows:
            messagebox.showerror(self.tr("Fel"), self.tr("Lägg till minst en informationsrad."), parent=self)
            return

        should_save = messagebox.askyesno(
            self.tr("Bekräfta överskrivning"),
            self.tr("Detta skriver över config-filen:\n{config}\n\nNuvarande config sparas först i:\n{legacy}\n\nVill du fortsätta?").format(config=self.config_path, legacy=self.legacy_dir),
            parent=self,
        )
        if not should_save:
            return

        self.result_rows = rows
        self.destroy()


class NcfaHelpDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, tr=lambda x: x):
        super().__init__(master)
        self.tr = tr
        self.title(self.tr("Hitta _ncfa"))
        width, height = _initial_help_dialog_size(self)
        self.geometry(f"{width}x{height}")
        self.minsize(760, 620)
        self.configure(bg=BG_APP)
        self._image_refs: list[object] = []

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        outer = ttk.Frame(self, style="Card.TFrame", padding=12)
        outer.grid(sticky="nsew")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, bg=BG_CARD, highlightthickness=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=scrollbar.set)

        content = ttk.Frame(canvas, style="Card.TFrame", padding=(8, 4, 8, 8))
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")

        def _sync_scroll_region(_event=None) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_content_width(event) -> None:
            canvas.itemconfigure(window_id, width=event.width)

        content.bind("<Configure>", _sync_scroll_region)
        canvas.bind("<Configure>", _sync_content_width)

        def _on_mousewheel(event) -> str:
            if getattr(event, "num", None) == 4:
                canvas.yview_scroll(-3, "units")
                return "break"
            if getattr(event, "num", None) == 5:
                canvas.yview_scroll(3, "units")
                return "break"
            delta = int(getattr(event, "delta", 0))
            if delta:
                step = -max(1, int(abs(delta) / 120))
                canvas.yview_scroll(step if delta > 0 else -step, "units")
                return "break"
            return ""

        def _bind_mousewheel(_event=None) -> None:
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
            canvas.bind_all("<Button-4>", _on_mousewheel)
            canvas.bind_all("<Button-5>", _on_mousewheel)

        def _unbind_mousewheel(_event=None) -> None:
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        canvas.bind("<Enter>", _bind_mousewheel)
        canvas.bind("<Leave>", _unbind_mousewheel)
        content.bind("<Enter>", _bind_mousewheel)
        content.bind("<Leave>", _unbind_mousewheel)

        ttk.Label(
            content,
            text=self.tr("Så hittar du GeoGuessr-cookien _ncfa"),
            style="Field.TLabel",
            font=("Segoe UI Semibold", 14),
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            content,
            text=self.tr("Guiden återanvänder samma steg som README:n. När du har kopierat värdet klistrar du in det i fältet i appen."),
            style="Hint.TLabel",
            wraplength=780,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(4, 12))

        self._add_step(
            content,
            row=2,
            title=self.tr("1. Logga in i GeoGuessr"),
            body=self.tr("Öppna GeoGuessr i din vanliga webbläsare och logga in som vanligt."),
        )
        self._add_step(
            content,
            row=3,
            title=self.tr("2. Öppna DevTools och gå till Cookies"),
            body=self.tr("Tryck F12 och gå till Application -> Cookies -> https://www.geoguessr.com."),
            image_path=NCFA_HELP_IMAGES["application"],
        )
        self._add_step(
            content,
            row=4,
            title=self.tr("3. Leta upp _ncfa"),
            body=self.tr("Markera raden med namnet _ncfa och kopiera dess value."),
            image_path=NCFA_HELP_IMAGES["cookie"],
        )
        self._add_step(
            content,
            row=5,
            title=self.tr("4. Klistra in värdet i appen"),
            body=self.tr("Klistra in cookien i _ncfa-fältet här i appen. Du kan sedan välja antingen att bara sätta den i appen eller spara den som Windows-variabel."),
        )

        buttons = ttk.Frame(content, style="Card.TFrame")
        buttons.grid(row=6, column=0, sticky="e", pady=(14, 0))
        ttk.Button(buttons, text=self.tr("Stäng"), style="Accent.TButton", command=self.destroy).pack(side="left")

        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def _add_step(self, parent: ttk.Frame, row: int, title: str, body: str, image_path: Optional[Path] = None) -> None:
        card = ttk.Frame(parent, style="Card.TFrame", padding=(0, 0, 0, 10))
        card.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        card.columnconfigure(0, weight=1)

        ttk.Label(card, text=title, style="Field.TLabel", font=("Segoe UI Semibold", 11)).grid(row=0, column=0, sticky="w")
        ttk.Label(card, text=body, style="Hint.TLabel", wraplength=780, justify="left").grid(row=1, column=0, sticky="w", pady=(3, 0))

        if image_path is None:
            return

        image_label = self._build_image_label(card, image_path)
        if image_label is not None:
            image_label.grid(row=2, column=0, sticky="w", pady=(10, 0))
        else:
            ttk.Label(
                card,
                text=self.tr("Bild kunde inte laddas: {name}").format(name=image_path.name),
                style="Hint.TLabel",
            ).grid(row=2, column=0, sticky="w", pady=(8, 0))

    def _build_image_label(self, parent: ttk.Frame, image_path: Path) -> Optional[tk.Label]:
        if not image_path.exists():
            return None

        try:
            if Image is not None and ImageTk is not None:
                img = Image.open(image_path)
                max_width = 760
                max_height = 360
                scale = min(max_width / img.width, max_height / img.height, 1.0)
                new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
                if new_size != img.size:
                    img = img.resize(new_size, Image.LANCZOS)
                photo = ImageTk.PhotoImage(img)
            else:
                photo = tk.PhotoImage(file=str(image_path))
                x_step = max(1, math.ceil(photo.width() / 760))
                y_step = max(1, math.ceil(photo.height() / 360))
                step = max(x_step, y_step)
                if step > 1:
                    photo = photo.subsample(step, step)
        except Exception:
            return None

        self._image_refs.append(photo)
        return tk.Label(parent, image=photo, bg=BG_CARD, bd=0, highlightthickness=1, highlightbackground=BORDER)


class LeagueDesktopApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("GeoGuessr League Desktop")
        width, height = _initial_main_window_size(self.root)
        self.root.geometry(f"{width}x{height}")
        self.root.minsize(980, 680)
        self.root.configure(bg=BG_APP)
        self._configure_styles()
        self._try_set_window_icon()

        self.is_running = False
        self.weeks_by_id: Dict[str, WeekConfig] = {}
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._progress_job_id: Optional[str] = None
        self._log_poll_job_id: Optional[str] = None
        self._run_started_at: Optional[float] = None
        self._saw_warning = False
        self._main_scroll_bound = False
        self._nested_scroll_handoff: tuple[int, int, int] | None = None

        self.ncfa_var = tk.StringVar(value=os.environ.get("GEOGUESSR_NCFA", ""))
        self.out_base_var = tk.StringVar(value="Liga")
        self.tz_var = tk.StringVar(value="Europe/Stockholm")
        self.tie_var = tk.StringVar(value="average")
        self.language_var = tk.StringVar(value=LANGUAGE_KEY_TO_LABEL["auto"])
        self.table_sort_var = tk.StringVar(value=TABLE_SORT_OPTIONS[0][0])
        self.fetch_played_at_var = tk.BooleanVar(value=False)
        self.keep_missing_time_var = tk.BooleanVar(value=False)
        self.fetch_detailed_round_metrics_var = tk.BooleanVar(value=True)
        self.advanced_analytics_var = tk.BooleanVar(value=True)
        self.debug_var = tk.BooleanVar(value=False)
        self.progress_var = tk.StringVar(value="Redo")
        self.progress_time_var = tk.StringVar(value="")

        self._build_ui()
        self._ensure_information_config_exists()
        self._load_state()
        self._apply_language_texts()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _selected_language_key(self) -> str:
        return LANGUAGE_LABEL_TO_KEY.get(self.language_var.get().strip(), "auto")

    def _effective_language_key(self) -> str:
        selected = self._selected_language_key()
        if selected == "auto":
            return _detect_system_language()
        return selected

    def tr_ui(self, text: str) -> str:
        if self._effective_language_key() == "en":
            return UI_TRANSLATIONS_EN.get(text, text)
        return text

    def tr_uif(self, text: str, **kwargs) -> str:
        return self.tr_ui(text).format(**kwargs)

    def _table_sort_options_for_ui(self) -> list[tuple[str, str]]:
        return [(self.tr_ui(label), key) for label, key in TABLE_SORT_OPTIONS]

    def _table_sort_label_to_key(self) -> dict[str, str]:
        return {label: key for label, key in self._table_sort_options_for_ui()}

    def _table_sort_key_to_label(self) -> dict[str, str]:
        return {key: label for label, key in self._table_sort_options_for_ui()}

    def _on_language_changed(self, *_args) -> None:
        self._apply_language_texts()

    def _apply_language_texts(self) -> None:
        self.root.title(self.tr_ui("GeoGuessr League Desktop"))
        self.hero_badge_label.configure(text=self.tr_ui("Desktop Edition"))
        self.hero_sub_label.configure(text=self.tr_ui("Challenge-insamling och ligarapport i ett klick"))
        self.env_frame.configure(text=self.tr_ui("1) Inloggning / miljövariabel"))
        self.env_btn.configure(text=self.tr_ui("Sätt GEOGUESSR_NCFA i appen"))
        self.ncfa_help_btn.configure(text=self.tr_ui("Var hittar jag _ncfa?"))
        self.save_windows_env_btn.configure(text=self.tr_ui("Spara i Windows (setx)"))
        self.env_hint_label.configure(text=self.tr_ui("Behöver du hjälp första gången? Öppna guiden och följ samma steg som i README:n."))
        self.language_label.configure(text=self.tr_ui("Språk:"))
        self.language_hint_label.configure(text=self.tr_ui("Auto använder operativsystemets språk. Språkvalet skickas vidare till generatorn direkt."))
        self.weeks_frame.configure(text=self.tr_ui("2) Veckofiler"))
        self.weeks_help_label.configure(text=self.tr_ui("Lägg till befintliga .txt-filer eller skapa nya. Varje rad i filen ska vara en challenge-länk."))
        self.week_tree.heading("label", text=self.tr_ui("Vecka"))
        self.week_tree.heading("file", text=self.tr_ui("Textfil"))
        self.week_tree.heading("deadline", text=self.tr_ui("Deadline (valfri)"))
        self.week_tree.heading("sweden", text=self.tr_ui("Sverige-kartor"))
        self.add_files_btn.configure(text=self.tr_ui("Lägg till befintliga filer"))
        self.create_file_btn.configure(text=self.tr_ui("Skapa ny veckofil"))
        self.edit_deadline_btn.configure(text=self.tr_ui("Ändra deadline"))
        self.edit_sweden_btn.configure(text=self.tr_ui("Ändra Sverige-kartor"))
        self.remove_btn.configure(text=self.tr_ui("Ta bort vald"))
        self.options_frame.configure(text=self.tr_ui("3) Körning"))
        self.output_label.configure(text=self.tr_ui("Output-bas:"))
        self.tz_label.configure(text=self.tr_ui("Tidszon:"))
        self.tie_label.configure(text=self.tr_ui("Tie-läge:"))
        self.sort_label.configure(text=self.tr_ui("Sortera tabeller:"))
        self.sort_hint_label.configure(text=self.tr_ui("Gäller Total, Stats och Underligor."))
        self.tie_hint_label.configure(text=self.tr_ui("Obs: Tid används alltid som tie-break vid samma poäng. Tie-läge gäller bara exakt lika poäng + tid."))
        current_sort_key = self._table_sort_label_to_key().get(self.table_sort_var.get().strip(), DEFAULT_TABLE_SORT_KEY)
        self.sort_combo.configure(values=[label for label, _ in self._table_sort_options_for_ui()])
        self.table_sort_var.set(self._table_sort_key_to_label().get(current_sort_key, self.tr_ui("Standard (Poäng)")))
        self.fetch_chk.configure(text=self.tr_ui("Hämta played_at (för deadline-filter)"))
        self.keep_missing_chk.configure(text=self.tr_ui("Behåll poster utan tidsstämpel"))
        self.round_metrics_chk.configure(text=self.tr_ui("Hämta detaljerad moving/5k-statistik (långsammare)"))
        self.advanced_analytics_chk.configure(text=self.tr_ui("Skapa avancerad spelstil/5k-analys (långsammare)"))
        self.debug_chk.configure(text=self.tr_ui("Debug-logg"))
        self.run_btn.configure(text=self.tr_ui("Kör och skapa Excel"))
        self.info_cfg_btn.configure(text=self.tr_ui("Redigera Information-flik"))
        self.open_folder_btn.configure(text=self.tr_ui("Öppna projektmapp"))
        self.log_frame.configure(text=self.tr_ui("Logg"))
        progress_text = self.progress_var.get().strip()
        if progress_text in {"Redo", "Ready"}:
            self.progress_var.set(self.tr_ui("Redo"))
        elif progress_text in {"Kör...", "Running..."}:
            self.progress_var.set(self.tr_ui("Kör..."))
        elif progress_text in {"Klart.", "Done."}:
            self.progress_var.set(self.tr_ui("Klart."))
        elif progress_text in {"Klart med varningar.", "Done with warnings."}:
            self.progress_var.set(self.tr_ui("Klart med varningar."))

    def _configure_styles(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("App.TFrame", background=BG_APP)
        style.configure("Card.TFrame", background=BG_CARD)
        style.configure("Hero.TFrame", background=BG_HERO)

        style.configure(
            "Card.TLabelframe",
            background=BG_CARD,
            bordercolor=BORDER,
            borderwidth=1,
            relief="solid",
            lightcolor=BORDER,
            darkcolor=BORDER,
        )
        style.configure(
            "Card.TLabelframe.Label",
            background=BG_CARD,
            foreground=TEXT_MAIN,
            font=("Segoe UI Semibold", 11),
        )

        style.configure("Field.TLabel", background=BG_CARD, foreground=TEXT_MAIN, font=("Segoe UI", 10))
        style.configure("Hint.TLabel", background=BG_CARD, foreground=TEXT_MUTED, font=("Segoe UI", 9))

        style.configure("HeroTitle.TLabel", background=BG_HERO, foreground="#F7FBFF", font=("Segoe UI Semibold", 20))
        style.configure("HeroSub.TLabel", background=BG_HERO, foreground="#D2E6FF", font=("Segoe UI", 10))
        style.configure("HeroBadge.TLabel", background=BG_HERO, foreground="#8EC0FF", font=("Segoe UI Semibold", 9))

        style.configure(
            "Accent.TButton",
            background=ACCENT,
            foreground="#FFFFFF",
            borderwidth=0,
            focuscolor=ACCENT,
            padding=(14, 8),
            font=("Segoe UI Semibold", 10),
        )
        style.map(
            "Accent.TButton",
            background=[("active", ACCENT_HOVER), ("pressed", ACCENT_HOVER), ("disabled", "#A7C7F0")],
            foreground=[("disabled", "#EAF2FF")],
        )

        style.configure(
            "Soft.TButton",
            background=ACCENT_SOFT,
            foreground=TEXT_MAIN,
            borderwidth=0,
            padding=(12, 8),
            font=("Segoe UI Semibold", 10),
        )
        style.map(
            "Soft.TButton",
            background=[("active", "#CFE4FF"), ("pressed", "#C5DDFF"), ("disabled", "#EEF3F9")],
            foreground=[("disabled", "#9AAABC")],
        )

        style.configure(
            "Outline.TButton",
            background=BG_CARD,
            foreground=TEXT_MAIN,
            bordercolor=BORDER,
            borderwidth=1,
            padding=(12, 8),
            font=("Segoe UI Semibold", 10),
        )
        style.map("Outline.TButton", background=[("active", "#F5F9FF"), ("pressed", "#ECF3FF")])

        style.configure("Card.TCheckbutton", background=BG_CARD, foreground=TEXT_MAIN, font=("Segoe UI", 10))
        style.map("Card.TCheckbutton", background=[("active", BG_CARD), ("disabled", BG_CARD)])

        style.configure(
            "Modern.TEntry",
            fieldbackground="#FBFCFF",
            background="#FBFCFF",
            bordercolor=BORDER,
            lightcolor=BORDER,
            darkcolor=BORDER,
            padding=(8, 6),
        )
        style.configure(
            "Modern.TCombobox",
            fieldbackground="#FBFCFF",
            background="#FBFCFF",
            bordercolor=BORDER,
            lightcolor=BORDER,
            darkcolor=BORDER,
            padding=(6, 5),
        )

        style.configure(
            "Modern.Treeview",
            background="#FFFFFF",
            fieldbackground="#FFFFFF",
            foreground=TEXT_MAIN,
            bordercolor=BORDER,
            borderwidth=1,
            rowheight=30,
            font=("Segoe UI", 10),
        )
        style.map("Modern.Treeview", background=[("selected", "#DDECFF")], foreground=[("selected", "#123A66")])
        style.configure(
            "Modern.Treeview.Heading",
            background="#E9F1FB",
            foreground=TEXT_MAIN,
            font=("Segoe UI Semibold", 10),
            bordercolor=BORDER,
            borderwidth=1,
            padding=(8, 8),
        )
        style.map("Modern.Treeview.Heading", background=[("active", "#DFEBFA")])

    def _build_ui(self) -> None:
        shell = ttk.Frame(self.root, style="App.TFrame")
        shell.pack(fill="both", expand=True)
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(1, weight=1)

        hero_outer = ttk.Frame(shell, style="App.TFrame", padding=(18, 18, 18, 0))
        hero_outer.grid(row=0, column=0, columnspan=2, sticky="ew")
        hero_outer.columnconfigure(0, weight=1)

        logo_frame = ttk.Frame(hero_outer, style="Hero.TFrame", padding=(14, 14, 14, 14))
        logo_frame.grid(row=0, column=0, sticky="ew")
        self._build_logo_header(logo_frame)

        self.main_canvas = tk.Canvas(shell, bg=BG_APP, highlightthickness=0, bd=0)
        self.main_canvas.grid(row=1, column=0, sticky="nsew")
        self.main_scrollbar = ttk.Scrollbar(shell, orient="vertical", command=self.main_canvas.yview)
        self.main_scrollbar.grid(row=1, column=1, sticky="ns")
        self.main_canvas.configure(yscrollcommand=self.main_scrollbar.set)

        outer = ttk.Frame(self.main_canvas, style="App.TFrame", padding=(18, 12, 18, 18))
        self._main_window_id = self.main_canvas.create_window((0, 0), window=outer, anchor="nw")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(1, weight=1)
        outer.rowconfigure(3, weight=1)

        outer.bind("<Configure>", self._sync_main_scroll_region)
        self.main_canvas.bind("<Configure>", self._sync_main_content_width)
        self.main_canvas.bind("<Enter>", self._bind_main_mousewheel)
        self.main_canvas.bind("<Leave>", self._unbind_main_mousewheel)
        outer.bind("<Enter>", self._bind_main_mousewheel)
        outer.bind("<Leave>", self._unbind_main_mousewheel)

        self.env_frame = ttk.LabelFrame(outer, text="1) Inloggning / miljövariabel", style="Card.TLabelframe", padding=12)
        self.env_frame.grid(row=0, column=0, sticky="ew")
        self.env_frame.columnconfigure(1, weight=1)
        ttk.Label(self.env_frame, text="_ncfa:", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 10))
        self.ncfa_entry = ttk.Entry(self.env_frame, textvariable=self.ncfa_var, show="*", style="Modern.TEntry")
        self.ncfa_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.env_btn = ttk.Button(self.env_frame, text="Sätt GEOGUESSR_NCFA i appen", style="Accent.TButton", command=self.apply_ncfa_env)
        self.env_btn.grid(row=0, column=2, padx=(8, 0))
        self.ncfa_help_btn = ttk.Button(
            self.env_frame,
            text="Var hittar jag _ncfa?",
            style="Soft.TButton",
            command=self.open_ncfa_help,
        )
        self.ncfa_help_btn.grid(row=0, column=3, padx=(8, 0))
        self.save_windows_env_btn = ttk.Button(
            self.env_frame,
            text="Spara i Windows (setx)",
            style="Outline.TButton",
            command=self.save_ncfa_to_windows_env,
        )
        self.save_windows_env_btn.grid(row=0, column=4, padx=(8, 0))
        if not sys.platform.startswith("win"):
            self.save_windows_env_btn.configure(state="disabled")

        self.env_hint_label = ttk.Label(
            self.env_frame,
            text="Behöver du hjälp första gången? Öppna guiden och följ samma steg som i README:n.",
            style="Hint.TLabel",
        )
        self.env_hint_label.grid(row=1, column=0, columnspan=5, sticky="w", pady=(8, 0))
        self.language_label = ttk.Label(self.env_frame, text="Språk:", style="Field.TLabel")
        self.language_label.grid(row=2, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        self.language_combo = ttk.Combobox(
            self.env_frame,
            style="Modern.TCombobox",
            textvariable=self.language_var,
            values=[label for label, _ in LANGUAGE_OPTIONS],
            state="readonly",
            width=18,
        )
        self.language_combo.grid(row=2, column=1, sticky="w", pady=(10, 0))
        self.language_hint_label = ttk.Label(
            self.env_frame,
            text="Auto använder operativsystemets språk. Språkvalet skickas vidare till generatorn direkt.",
            style="Hint.TLabel",
        )
        self.language_hint_label.grid(row=2, column=2, columnspan=3, sticky="w", pady=(10, 0))

        self.weeks_frame = ttk.LabelFrame(outer, text="2) Veckofiler", style="Card.TLabelframe", padding=12)
        self.weeks_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        self.weeks_frame.columnconfigure(0, weight=1)
        self.weeks_frame.rowconfigure(1, weight=1)

        help_label = (
            "Lägg till befintliga .txt-filer eller skapa nya. "
            "Varje rad i filen ska vara en challenge-länk."
        )
        self.weeks_help_label = ttk.Label(self.weeks_frame, text=help_label, style="Hint.TLabel")
        self.weeks_help_label.grid(row=0, column=0, sticky="w", pady=(0, 10))

        cols = ("label", "file", "deadline", "sweden")
        self.week_tree = ttk.Treeview(self.weeks_frame, style="Modern.Treeview", columns=cols, show="headings", height=9)
        self.week_tree.heading("label", text="Vecka")
        self.week_tree.heading("file", text="Textfil")
        self.week_tree.heading("deadline", text="Deadline (valfri)")
        self.week_tree.heading("sweden", text="Sverige-kartor")
        self.week_tree.column("label", width=160, anchor="w")
        self.week_tree.column("file", width=520, anchor="w")
        self.week_tree.column("deadline", width=180, anchor="center")
        self.week_tree.column("sweden", width=140, anchor="center")
        self.week_tree.grid(row=1, column=0, sticky="nsew")

        tree_scroll = ttk.Scrollbar(self.weeks_frame, orient="vertical", command=self.week_tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.week_tree.configure(yscrollcommand=tree_scroll.set)

        week_buttons = ttk.Frame(self.weeks_frame, style="Card.TFrame")
        week_buttons.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        self.add_files_btn = ttk.Button(week_buttons, text="Lägg till befintliga filer", style="Accent.TButton", command=self.add_existing_files)
        self.create_file_btn = ttk.Button(week_buttons, text="Skapa ny veckofil", style="Soft.TButton", command=self.open_create_dialog)
        self.edit_deadline_btn = ttk.Button(week_buttons, text="Ändra deadline", style="Outline.TButton", command=self.edit_selected_deadline)
        self.edit_sweden_btn = ttk.Button(week_buttons, text="Ändra Sverige-kartor", style="Outline.TButton", command=self.edit_selected_sweden_maps)
        self.remove_btn = ttk.Button(week_buttons, text="Ta bort vald", style="Outline.TButton", command=self.remove_selected)
        self.add_files_btn.pack(side="left")
        self.create_file_btn.pack(side="left", padx=(8, 0))
        self.edit_deadline_btn.pack(side="left", padx=(8, 0))
        self.edit_sweden_btn.pack(side="left", padx=(8, 0))
        self.remove_btn.pack(side="left", padx=(8, 0))

        self.options_frame = ttk.LabelFrame(outer, text="3) Körning", style="Card.TLabelframe", padding=12)
        self.options_frame.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        for i in range(6):
            self.options_frame.columnconfigure(i, weight=1 if i in (1, 3) else 0)

        self.output_label = ttk.Label(self.options_frame, text="Output-bas:", style="Field.TLabel")
        self.output_label.grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.out_entry = ttk.Entry(self.options_frame, textvariable=self.out_base_var, style="Modern.TEntry")
        self.out_entry.grid(row=0, column=1, sticky="ew", padx=(0, 14))

        self.tz_label = ttk.Label(self.options_frame, text="Tidszon:", style="Field.TLabel")
        self.tz_label.grid(row=0, column=2, sticky="w", padx=(0, 8))
        self.tz_entry = ttk.Entry(self.options_frame, textvariable=self.tz_var, style="Modern.TEntry")
        self.tz_entry.grid(row=0, column=3, sticky="ew", padx=(0, 14))

        self.tie_label = ttk.Label(self.options_frame, text="Tie-läge:", style="Field.TLabel")
        self.tie_label.grid(row=0, column=4, sticky="w", padx=(0, 8))
        self.tie_combo = ttk.Combobox(
            self.options_frame,
            style="Modern.TCombobox",
            textvariable=self.tie_var,
            values=["average", "dense", "min", "max"],
            state="readonly",
            width=10,
        )
        self.tie_combo.grid(row=0, column=5, sticky="w")

        self.sort_label = ttk.Label(self.options_frame, text="Sortera tabeller:", style="Field.TLabel")
        self.sort_label.grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(8, 0))
        self.sort_combo = ttk.Combobox(
            self.options_frame,
            style="Modern.TCombobox",
            textvariable=self.table_sort_var,
            values=[label for label, _ in self._table_sort_options_for_ui()],
            state="readonly",
            width=24,
        )
        self.sort_combo.grid(row=1, column=1, sticky="w", pady=(8, 0))
        self.sort_hint_label = ttk.Label(
            self.options_frame,
            text="Gäller Total, Stats och Underligor.",
            style="Hint.TLabel",
        )
        self.sort_hint_label.grid(row=1, column=2, columnspan=4, sticky="w", pady=(8, 0))

        self.tie_hint_label = ttk.Label(
            self.options_frame,
            text="Obs: Tid används alltid som tie-break vid samma poäng. Tie-läge gäller bara exakt lika poäng + tid.",
            style="Hint.TLabel",
        )
        self.tie_hint_label.grid(row=2, column=0, columnspan=6, sticky="w", pady=(6, 0))

        self.fetch_chk = ttk.Checkbutton(
            self.options_frame,
            text="Hämta played_at (för deadline-filter)",
            style="Card.TCheckbutton",
            variable=self.fetch_played_at_var,
        )
        self.keep_missing_chk = ttk.Checkbutton(
            self.options_frame,
            text="Behåll poster utan tidsstämpel",
            style="Card.TCheckbutton",
            variable=self.keep_missing_time_var,
        )
        self.round_metrics_chk = ttk.Checkbutton(
            self.options_frame,
            text="Hämta detaljerad moving/5k-statistik (långsammare)",
            style="Card.TCheckbutton",
            variable=self.fetch_detailed_round_metrics_var,
        )
        self.advanced_analytics_chk = ttk.Checkbutton(
            self.options_frame,
            text="Skapa avancerad spelstil/5k-analys (långsammare)",
            style="Card.TCheckbutton",
            variable=self.advanced_analytics_var,
        )
        self.debug_chk = ttk.Checkbutton(self.options_frame, text="Debug-logg", style="Card.TCheckbutton", variable=self.debug_var)
        self.fetch_chk.grid(row=3, column=0, columnspan=3, sticky="w", pady=(8, 0))
        self.keep_missing_chk.grid(row=3, column=3, columnspan=2, sticky="w", pady=(8, 0))
        self.debug_chk.grid(row=3, column=5, sticky="w", pady=(8, 0))
        self.round_metrics_chk.grid(row=4, column=0, columnspan=3, sticky="w", pady=(6, 0))
        self.advanced_analytics_chk.grid(row=4, column=3, columnspan=3, sticky="w", pady=(6, 0))

        run_row = ttk.Frame(self.options_frame, style="Card.TFrame")
        run_row.grid(row=5, column=0, columnspan=6, sticky="ew", pady=(12, 0))
        self.run_btn = ttk.Button(run_row, text="Kör och skapa Excel", style="Accent.TButton", command=self.start_generation)
        self.info_cfg_btn = ttk.Button(run_row, text="Redigera Information-flik", style="Outline.TButton", command=self.open_information_config_dialog)
        self.open_folder_btn = ttk.Button(run_row, text="Öppna projektmapp", style="Soft.TButton", command=self.open_project_folder)
        self.run_btn.pack(side="left")
        self.info_cfg_btn.pack(side="left", padx=(8, 0))
        self.open_folder_btn.pack(side="left", padx=(8, 0))

        progress_row = ttk.Frame(self.options_frame, style="Card.TFrame")
        progress_row.grid(row=6, column=0, columnspan=6, sticky="ew", pady=(10, 0))
        progress_row.columnconfigure(0, weight=1)
        self.progress_bar = ttk.Progressbar(progress_row, mode="indeterminate")
        self.progress_bar.grid(row=0, column=0, sticky="ew")
        ttk.Label(progress_row, textvariable=self.progress_var, style="Hint.TLabel").grid(row=1, column=0, sticky="w", pady=(5, 0))
        ttk.Label(progress_row, textvariable=self.progress_time_var, style="Hint.TLabel").grid(row=1, column=1, sticky="e", pady=(5, 0), padx=(12, 0))

        self.log_frame = ttk.LabelFrame(outer, text="Logg", style="Card.TLabelframe", padding=12)
        self.log_frame.grid(row=3, column=0, sticky="nsew", pady=(12, 0))
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(
            self.log_frame,
            height=12,
            wrap="word",
            bg=LOG_BG,
            fg=LOG_FG,
            insertbackground=LOG_FG,
            relief="flat",
            highlightthickness=1,
            highlightbackground=BORDER,
            font=("Consolas", 10),
            padx=10,
            pady=8,
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(self.log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self.language_var.trace_add("write", self._on_language_changed)
        self._apply_language_texts()
        self.log(self.tr_ui("Appen startad."))
        self.log(self.tr_ui("Tips: skapa veckofiler i appen, eller lägg till befintliga .txt-filer."))
        self.log(f"[STATE] Sparad konfiguration: {APP_STATE_PATH}")

    def _sync_main_scroll_region(self, _event=None) -> None:
        self.main_canvas.configure(scrollregion=self.main_canvas.bbox("all"))

    def _sync_main_content_width(self, event) -> None:
        self.main_canvas.itemconfigure(self._main_window_id, width=event.width)

    def _on_main_mousewheel(self, event) -> str:
        widget = event.widget
        widget_name = widget.winfo_class() if widget is not None else ""
        direction = 0
        if getattr(event, "num", None) == 4:
            direction = -1
        elif getattr(event, "num", None) == 5:
            direction = 1
        else:
            delta = int(getattr(event, "delta", 0))
            if delta > 0:
                direction = -1
            elif delta < 0:
                direction = 1

        if widget_name in {"Text", "Treeview", "Listbox"} and widget is not None:
            try:
                first, last = widget.yview()
            except Exception:
                return ""
            at_top = first <= 0.0
            at_bottom = last >= 1.0
            widget_id = id(widget)
            if (direction < 0 and not at_top) or (direction > 0 and not at_bottom):
                self._nested_scroll_handoff = None
                return ""
            if direction != 0:
                prior = self._nested_scroll_handoff
                if prior is not None and prior[0] == widget_id and prior[1] == direction:
                    count = prior[2] + 1
                else:
                    count = 1
                self._nested_scroll_handoff = (widget_id, direction, count)
                if count < 2:
                    return "break"
        elif widget_name in {"TCombobox", "Canvas"}:
            self._nested_scroll_handoff = None
            return ""
        else:
            self._nested_scroll_handoff = None

        if direction < 0:
            self.main_canvas.yview_scroll(-1, "units")
            return "break"
        if direction > 0:
            self.main_canvas.yview_scroll(1, "units")
            return "break"
        return ""

    def _bind_main_mousewheel(self, _event=None) -> None:
        if self._main_scroll_bound:
            return
        self.root.bind_all("<MouseWheel>", self._on_main_mousewheel)
        self.root.bind_all("<Button-4>", self._on_main_mousewheel)
        self.root.bind_all("<Button-5>", self._on_main_mousewheel)
        self._main_scroll_bound = True

    def _unbind_main_mousewheel(self, _event=None) -> None:
        if not self._main_scroll_bound:
            return
        self.root.unbind_all("<MouseWheel>")
        self.root.unbind_all("<Button-4>")
        self.root.unbind_all("<Button-5>")
        self._main_scroll_bound = False

    def _build_logo_header(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(1, weight=1)

        canvas = tk.Canvas(parent, width=80, height=80, highlightthickness=0, bg=BG_HERO)
        canvas.grid(row=0, column=0, padx=(4, 10), sticky="w")
        self._draw_logo(canvas)

        txt_frame = ttk.Frame(parent, style="Hero.TFrame")
        txt_frame.grid(row=0, column=1, sticky="w")
        self.hero_badge_label = ttk.Label(txt_frame, text="Desktop Edition", style="HeroBadge.TLabel")
        self.hero_badge_label.grid(row=0, column=0, sticky="w")
        self.hero_title_label = ttk.Label(txt_frame, text="GeoLeague Builder", style="HeroTitle.TLabel")
        self.hero_title_label.grid(row=1, column=0, sticky="w")
        self.hero_sub_label = ttk.Label(
            txt_frame,
            text="Challenge-insamling och ligarapport i ett klick",
            style="HeroSub.TLabel",
        )
        self.hero_sub_label.grid(row=2, column=0, sticky="w")

    def _draw_logo(self, canvas: tk.Canvas) -> None:
        bg = BG_HERO
        canvas.configure(bg=bg)

        # Pin inspired style with map/compass colors.
        canvas.create_oval(18, 8, 62, 52, fill="#E54A3E", outline="")
        canvas.create_polygon(40, 74, 28, 41, 52, 41, fill="#E54A3E", outline="")
        canvas.create_oval(28, 18, 52, 42, fill="#FFFFFF", outline="")
        canvas.create_oval(35, 25, 45, 35, fill="#2F80ED", outline="")
        canvas.create_oval(8, 46, 26, 64, fill="#4CAF50", outline="")
        canvas.create_arc(6, 44, 74, 78, start=200, extent=120, style="arc", width=2, outline="#1F4D2E")

    def _try_set_window_icon(self) -> None:
        try:
            if APP_ICON_PATH.exists():
                self.root.iconbitmap(str(APP_ICON_PATH))
        except Exception:
            pass

    def set_controls_state(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        combo_state = "readonly" if enabled else "disabled"
        for widget in [
            self.ncfa_entry,
            self.env_btn,
            self.ncfa_help_btn,
            self.save_windows_env_btn,
            self.language_combo,
            self.add_files_btn,
            self.create_file_btn,
            self.edit_deadline_btn,
            self.edit_sweden_btn,
            self.remove_btn,
            self.out_entry,
            self.tz_entry,
            self.fetch_chk,
            self.keep_missing_chk,
            self.debug_chk,
            self.run_btn,
            self.info_cfg_btn,
            self.open_folder_btn,
        ]:
            widget.configure(state=state)
        self.tie_combo.configure(state=combo_state)
        self.sort_combo.configure(state=combo_state)

    def log(self, text: str) -> None:
        self.log_text.insert("end", text.rstrip() + "\n")
        self.log_text.see("end")

    def _append_log_chunk(self, chunk: str) -> None:
        self.log_text.insert("end", chunk)
        self.log_text.see("end")

    def _queue_log_chunk(self, chunk: str) -> None:
        if "[WARN]" in chunk:
            self._saw_warning = True
        self._log_queue.put(chunk)

    def _poll_log_queue(self) -> None:
        while True:
            try:
                chunk = self._log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log_chunk(chunk)
        if self.is_running or not self._log_queue.empty():
            self._log_poll_job_id = self.root.after(120, self._poll_log_queue)
        else:
            self._log_poll_job_id = None

    def _start_running_feedback(self) -> None:
        self._run_started_at = time.monotonic()
        self.progress_var.set(self.tr_ui("Körning pågår... hämtar och bearbetar data."))
        self.progress_time_var.set("00:00")
        self.progress_bar.start(10)
        if self._progress_job_id is None:
            self._tick_running_feedback()
        if self._log_poll_job_id is None:
            self._poll_log_queue()

    def _tick_running_feedback(self) -> None:
        if not self.is_running:
            self._progress_job_id = None
            return
        elapsed = 0
        if self._run_started_at is not None:
            elapsed = int(time.monotonic() - self._run_started_at)
        mins, secs = divmod(elapsed, 60)
        self.progress_time_var.set(f"{mins:02d}:{secs:02d}")
        self._progress_job_id = self.root.after(1000, self._tick_running_feedback)

    def _stop_running_feedback(self, ok: bool) -> None:
        self.progress_bar.stop()
        if self._progress_job_id is not None:
            self.root.after_cancel(self._progress_job_id)
            self._progress_job_id = None
        if self._log_poll_job_id is not None:
            self.root.after_cancel(self._log_poll_job_id)
            self._log_poll_job_id = None
        self.progress_var.set(self.tr_ui("Klar.") if ok else self.tr_ui("Körning misslyckades."))
        self._run_started_at = None

    def apply_ncfa_env(self) -> None:
        ncfa = self.ncfa_var.get().strip()
        if not ncfa:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("_ncfa saknas."))
            return
        os.environ["GEOGUESSR_NCFA"] = ncfa
        self.log("[OK] GEOGUESSR_NCFA satt i app-processens miljö.")
        self._save_state()

    def save_ncfa_to_windows_env(self) -> None:
        if not sys.platform.startswith("win"):
            messagebox.showinfo(self.tr_ui("Info"), self.tr_ui("Denna funktion är bara tillgänglig på Windows."))
            return
        ncfa = self.ncfa_var.get().strip()
        if not ncfa:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("_ncfa saknas."))
            return
        try:
            result = subprocess.run(
                ["setx", "GEOGUESSR_NCFA", ncfa],
                capture_output=True,
                text=True,
                check=True,
            )
            os.environ["GEOGUESSR_NCFA"] = ncfa
            if result.stdout.strip():
                self.log(result.stdout.strip())
            self.log("[OK] GEOGUESSR_NCFA sparad i Windows användarvariabler.")
            self.log("[INFO] Starta om appen om du vill läsa tillbaka värdet från systemmiljön.")
            self._save_state()
        except subprocess.CalledProcessError as ex:
            err = (ex.stderr or ex.stdout or str(ex)).strip()
            messagebox.showerror(self.tr_ui("Fel"), self.tr_uif("Kunde inte spara variabeln:\n{err}", err=err))

    def open_ncfa_help(self) -> None:
        NcfaHelpDialog(self.root, tr=self.tr_ui)

    def add_existing_files(self) -> None:
        paths = filedialog.askopenfilenames(
            title=self.tr_ui("Välj URL-textfiler"),
            initialdir=str(ROOT_DIR),
            filetypes=[(self.tr_ui("Textfiler"), "*.txt"), (self.tr_ui("Alla filer"), "*.*")],
        )
        if not paths:
            return

        for raw_path in paths:
            file_path = Path(raw_path)
            default_label = self._guess_label_from_path(file_path)
            label = simpledialog.askstring(
                self.tr_ui("Veckoetikett"),
                self.tr_uif("Ange veckonamn för:\n{name}", name=file_path.name),
                initialvalue=default_label,
                parent=self.root,
            )
            if not label:
                continue
            self._insert_week(WeekConfig(label=label.strip(), file_path=file_path, deadline="", sweden_maps=DEFAULT_SWEDEN_MAPS))

    def _guess_label_from_path(self, path: Path) -> str:
        digits = re.findall(r"\d+", path.stem)
        if digits:
            return f"{self.tr_ui('Vecka')} {digits[0]}"
        return path.stem.replace("_", " ")

    def open_create_dialog(self) -> None:
        CreateWeekFileDialog(self.root, on_save=self._insert_week, tr=self.tr_ui)

    def _insert_week(self, week: WeekConfig) -> None:
        if not week.file_path.exists():
            messagebox.showerror(self.tr_ui("Fel"), self.tr_uif("Filen finns inte:\n{path}", path=week.file_path))
            return
        row_id = self.week_tree.insert("", "end", values=(week.label, str(week.file_path), week.deadline, week.effective_sweden_maps()))
        self.weeks_by_id[row_id] = week
        self.log(f"[OK] Lade till: {week.label} -> {week.file_path}")
        self._save_state()

    def edit_selected_deadline(self) -> None:
        selected = self.week_tree.selection()
        if not selected:
            messagebox.showinfo(self.tr_ui("Info"), self.tr_ui("Markera en rad först."))
            return
        row_id = selected[0]
        week = self.weeks_by_id[row_id]
        dlg = DeadlineDialog(self.root, initial_value=week.deadline, tr=self.tr_ui)
        self.root.wait_window(dlg)
        if dlg.result is None:
            return
        week.deadline = dlg.result.strip()
        self.week_tree.item(row_id, values=(week.label, str(week.file_path), week.deadline, week.effective_sweden_maps()))
        self.log(f"[OK] Uppdaterade deadline för {week.label}: {week.deadline or '(ingen)'}")
        self._save_state()

    def edit_selected_sweden_maps(self) -> None:
        selected = self.week_tree.selection()
        if not selected:
            messagebox.showinfo(self.tr_ui("Info"), self.tr_ui("Markera en rad först."))
            return
        row_id = selected[0]
        week = self.weeks_by_id[row_id]
        value = simpledialog.askstring(
            self.tr_ui("Sverige-kartor"),
            self.tr_uif("Ange kartnummer för Sverige i {label}.\nExempel: 1,4", label=week.label),
            initialvalue=week.effective_sweden_maps(),
            parent=self.root,
        )
        if value is None:
            return
        week.sweden_maps = value.strip()
        self.week_tree.item(row_id, values=(week.label, str(week.file_path), week.deadline, week.effective_sweden_maps()))
        self.log(f"[OK] Uppdaterade Sverige-kartor för {week.label}: {week.effective_sweden_maps()}")
        self._save_state()

    def remove_selected(self) -> None:
        selected = self.week_tree.selection()
        if not selected:
            return
        for row_id in selected:
            week = self.weeks_by_id.pop(row_id, None)
            self.week_tree.delete(row_id)
            if week:
                self.log(f"[OK] Tog bort: {week.label}")
        self._save_state()

    def _collect_weeks_in_order(self) -> list[WeekConfig]:
        out: list[WeekConfig] = []
        for row_id in self.week_tree.get_children(""):
            week = self.weeks_by_id.get(row_id)
            if week:
                out.append(week)
        return out

    def _output_paths_for_base(self, out_base: str) -> tuple[Path, Path]:
        return Path(f"{out_base}_all.xlsx"), Path(f"{out_base}_filtered.xlsx")

    def _is_excel_target_writable(self, path: Path) -> bool:
        try:
            parent = path.parent if str(path.parent) != "" else Path(".")
            parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                with path.open("a+b"):
                    pass
            else:
                probe = parent / f".__probe_{uuid.uuid4().hex}.tmp"
                with probe.open("wb"):
                    pass
                probe.unlink(missing_ok=True)
            return True
        except Exception:
            return False

    def _resolve_writable_out_base(self, desired_base: str, max_attempts: int = 50) -> Optional[str]:
        base = desired_base.strip() or "Liga"
        for n in range(0, max_attempts + 1):
            candidate = base if n == 0 else f"{base} ({n})"
            out_all, out_filtered = self._output_paths_for_base(candidate)
            if self._is_excel_target_writable(out_all) and self._is_excel_target_writable(out_filtered):
                return candidate
        return None

    def _default_information_rows(self) -> list[str]:
        lang = self._effective_language_key()
        try:
            rows = league_core.default_information_rows(lang)
            if isinstance(rows, list) and rows:
                return [str(x).strip() for x in rows if str(x).strip()]
        except Exception:
            pass
        return league_core.default_information_rows(lang)

    def _information_config_payload(self, rows: list[str], existing_payload: Optional[dict] = None) -> dict:
        clean_rows = [str(line).strip() for line in rows if str(line).strip()]
        if not clean_rows:
            clean_rows = self._default_information_rows()
        lang = self._effective_language_key()
        payload = existing_payload.copy() if isinstance(existing_payload, dict) else {}
        by_lang = payload.get("information_rows_by_lang")
        if not isinstance(by_lang, dict):
            by_lang = {}
        by_lang["sv"] = [str(x).strip() for x in by_lang.get("sv", league_core.default_information_rows("sv")) if str(x).strip()]
        by_lang["en"] = [str(x).strip() for x in by_lang.get("en", league_core.default_information_rows("en")) if str(x).strip()]
        by_lang[lang] = clean_rows
        return {"version": 2, "information_rows_by_lang": by_lang}

    def _read_information_rows(self) -> list[str]:
        if not INFO_CONFIG_PATH.exists():
            return self._default_information_rows()
        try:
            payload = json.loads(INFO_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception as ex:
            self.log(f"[WARN] Kunde inte läsa {INFO_CONFIG_PATH.name} ({ex}). Använder default.")
            return self._default_information_rows()

        lang = self._effective_language_key()
        if isinstance(payload, dict):
            by_lang = payload.get("information_rows_by_lang")
            if isinstance(by_lang, dict):
                rows = by_lang.get(lang) or by_lang.get("sv") or by_lang.get("en")
            else:
                rows = payload.get("information_rows")
        elif isinstance(payload, list):
            rows = payload
        else:
            rows = None
        if not isinstance(rows, list):
            return self._default_information_rows()

        out = [str(line).strip() for line in rows if str(line).strip()]
        return out or self._default_information_rows()

    def _migrate_legacy_information_config_if_needed(self) -> None:
        if not LEGACY_INFO_CONFIG_PATH.exists():
            return
        try:
            INFO_CONFIG_LEGACY_DIR.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            legacy_target = INFO_CONFIG_LEGACY_DIR / f"information_config_legacy_migrated_{stamp}.json"
            shutil.move(str(LEGACY_INFO_CONFIG_PATH), str(legacy_target))
            self.log(f"[INFO] Migrerade gammal information-config till legacy: {legacy_target}")
        except Exception as ex:
            self.log(f"[WARN] Kunde inte migrera gammal information-config ({ex}).")

    def _ensure_information_config_exists(self) -> None:
        self._migrate_legacy_information_config_if_needed()
        if INFO_CONFIG_PATH.exists():
            return
        try:
            payload = self._information_config_payload(self._default_information_rows())
            INFO_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            self.log(f"[INFO] Skapade default information-config: {INFO_CONFIG_PATH}")
        except Exception as ex:
            self.log(f"[WARN] Kunde inte skapa information-config: {ex}")

    def _save_information_rows_with_legacy_backup(self, rows: list[str]) -> tuple[bool, Optional[Path]]:
        try:
            if INFO_CONFIG_PATH.exists():
                INFO_CONFIG_LEGACY_DIR.mkdir(parents=True, exist_ok=True)
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                legacy_path = INFO_CONFIG_LEGACY_DIR / f"information_config_{stamp}.json"
                shutil.copy2(INFO_CONFIG_PATH, legacy_path)
            else:
                legacy_path = None

            existing_payload = None
            if INFO_CONFIG_PATH.exists():
                try:
                    existing_payload = json.loads(INFO_CONFIG_PATH.read_text(encoding="utf-8"))
                except Exception:
                    existing_payload = None
            payload = self._information_config_payload(rows, existing_payload=existing_payload)
            INFO_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return True, legacy_path
        except Exception as ex:
            self.log(f"[ERROR] Kunde inte spara information-config: {ex}")
            return False, None

    def open_information_config_dialog(self) -> None:
        self._ensure_information_config_exists()
        initial_rows = self._read_information_rows()
        default_rows = self._default_information_rows()
        dlg = InformationConfigDialog(
            self.root,
            initial_rows=initial_rows,
            default_rows=default_rows,
            config_path=INFO_CONFIG_PATH,
            legacy_dir=INFO_CONFIG_LEGACY_DIR,
            tr=self.tr_ui,
        )
        self.root.wait_window(dlg)
        if dlg.result_rows is None:
            return

        ok, legacy_path = self._save_information_rows_with_legacy_backup(dlg.result_rows)
        if not ok:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("Kunde inte spara information-config. Se loggen för detaljer."), parent=self.root)
            return

        if legacy_path is not None:
            self.log(f"[OK] Sparade legacy-config: {legacy_path}")
        self.log(f"[OK] Uppdaterade information-config: {INFO_CONFIG_PATH}")
        messagebox.showinfo(self.tr_ui("Klart"), self.tr_ui("Information-config sparad."), parent=self.root)

    def start_generation(self) -> None:
        if self.is_running:
            return

        weeks = self._collect_weeks_in_order()
        if not weeks:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("Lägg till minst en veckofil."))
            return
        missing_files = [str(w.file_path) for w in weeks if not w.file_path.exists()]
        if missing_files:
            msg = self.tr_uif("Dessa veckofiler saknas:\n\n{files}", files="\n".join(missing_files))
            messagebox.showerror(self.tr_ui("Fel"), msg)
            self.log("[ERROR] Saknade filer:\n" + "\n".join(missing_files))
            return

        ncfa = self.ncfa_var.get().strip()
        if not ncfa:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("_ncfa saknas."))
            return

        requested_out_base = self.out_base_var.get().strip() or "Liga"
        out_base = self._resolve_writable_out_base(requested_out_base)
        if out_base is None:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("Kunde inte hitta ett skrivbart filnamn för output. Stäng eventuell öppen Excel-fil och försök igen."))
            return
        if out_base != requested_out_base:
            self.out_base_var.set(out_base)
            self.log(f"[INFO] Outputfil var låst/upptagen. Använder fallback-namn: {out_base}")
            messagebox.showwarning(
                self.tr_ui("Outputfil låst"),
                self.tr_uif("En eller flera outputfiler är öppna/låsta och kunde inte skrivas över.\nSparar istället med suffix: {out_base}", out_base=out_base),
                parent=self.root,
            )

        tz_name = self.tz_var.get().strip() or "Europe/Stockholm"
        table_sort_key = self._table_sort_label_to_key().get(self.table_sort_var.get().strip(), DEFAULT_TABLE_SORT_KEY)

        os.environ["GEOGUESSR_NCFA"] = ncfa
        args: list[str] = []
        for week in weeks:
            args.extend(["--week", week.to_week_arg()])
        args.extend(["--out-base", out_base, "--tz", tz_name, "--tie", self.tie_var.get(), "--ncfa", ncfa])
        args.extend(["--sort-by", table_sort_key])
        args.extend(["--lang", LANGUAGE_LABEL_TO_KEY.get(self.language_var.get().strip(), "auto")])
        args.extend(["--information-config", str(INFO_CONFIG_PATH)])
        if self.fetch_played_at_var.get():
            args.append("--fetch-played-at")
        if self.keep_missing_time_var.get():
            args.append("--keep-missing-time")
        if not self.fetch_detailed_round_metrics_var.get():
            args.append("--skip-detailed-round-metrics")
        if not self.advanced_analytics_var.get():
            args.append("--skip-advanced-analytics")
        if self.debug_var.get():
            args.append("--debug")

        self._save_state()
        self.is_running = True
        self._saw_warning = False
        self.set_controls_state(False)
        self.log("[START] " + ("Run generator..." if self._effective_language_key() == "en" else "Kör generator..."))
        self.log("[ARGS] " + " ".join(args))
        self._start_running_feedback()

        thread = threading.Thread(target=self._worker_run, args=(args,), daemon=True)
        thread.start()

    def _worker_run(self, args: list[str]) -> None:
        capture = io.StringIO()
        exit_code = 1

        class TeeWriter:
            def __init__(self, sink, mirror):
                self.sink = sink
                self.mirror = mirror

            def write(self, data):
                if not data:
                    return 0
                self.sink(data)
                self.mirror.write(data)
                return len(data)

            def flush(self):
                self.mirror.flush()

        writer = TeeWriter(self._queue_log_chunk, capture)
        try:
            with redirect_stdout(writer), redirect_stderr(writer):
                rc = league_core.main(args)
                exit_code = int(rc or 0)
        except SystemExit as ex:
            if isinstance(ex.code, int):
                exit_code = ex.code
            elif ex.code in (None, 0):
                exit_code = 0
            else:
                writer.write(str(ex.code) + "\n")
                exit_code = 1
        except Exception:
            traceback.print_exc(file=writer)
            exit_code = 1

        self.root.after(0, self._on_worker_done, exit_code)

    def _on_worker_done(self, exit_code: int) -> None:
        self.is_running = False
        self._poll_log_queue()
        self.set_controls_state(True)
        self._stop_running_feedback(ok=(exit_code == 0))
        if exit_code == 0 and self._saw_warning:
            self.progress_var.set(self.tr_ui("Klart med varningar."))
        if exit_code == 0:
            self.log("[DONE] Klart.")
            if self._saw_warning:
                messagebox.showwarning(
                    self.tr_ui("Klart med varningar"),
                    self.tr_ui("Excel-filer skapades, men en eller flera veckor/kartor kunde inte hämtas fullt ut.\nSe loggen för detaljer."),
                )
            else:
                messagebox.showinfo(self.tr_ui("Klart"), self.tr_ui("Excel-filer skapades."))
        else:
            self.log(f"[ERROR] Körning misslyckades (exit code {exit_code}).")
            messagebox.showerror(self.tr_ui("Fel"), self.tr_ui("Körning misslyckades. Se loggen."))

    def open_project_folder(self) -> None:
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(ROOT_DIR))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(ROOT_DIR)])
            else:
                subprocess.Popen(["xdg-open", str(ROOT_DIR)])
        except Exception as ex:
            messagebox.showerror(self.tr_ui("Fel"), self.tr_uif("Kunde inte öppna mappen:\n{ex}", ex=ex))

    def _save_state(self) -> None:
        state = {
            "weeks": [
                {"label": w.label, "file_path": str(w.file_path), "deadline": w.deadline, "sweden_maps": w.effective_sweden_maps()}
                for w in self._collect_weeks_in_order()
            ],
            "settings": {
                "ncfa": self.ncfa_var.get().strip(),
                "out_base": self.out_base_var.get().strip(),
                "tz": self.tz_var.get().strip(),
                "tie": self.tie_var.get().strip(),
                "language": LANGUAGE_LABEL_TO_KEY.get(self.language_var.get().strip(), "auto"),
                "sort_by": self._table_sort_label_to_key().get(self.table_sort_var.get().strip(), DEFAULT_TABLE_SORT_KEY),
                "fetch_played_at": bool(self.fetch_played_at_var.get()),
                "keep_missing_time": bool(self.keep_missing_time_var.get()),
                "fetch_detailed_round_metrics": bool(self.fetch_detailed_round_metrics_var.get()),
                "advanced_analytics": bool(self.advanced_analytics_var.get()),
                "debug": bool(self.debug_var.get()),
            },
        }
        try:
            APP_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as ex:
            self.log(f"[WARN] Kunde inte spara app-state: {ex}")

    def _load_state(self) -> None:
        if not APP_STATE_PATH.exists():
            return
        try:
            state = json.loads(APP_STATE_PATH.read_text(encoding="utf-8"))
        except Exception as ex:
            self.log(f"[WARN] Kunde inte läsa app-state: {ex}")
            return

        settings = state.get("settings", {})
        if isinstance(settings, dict):
            self.ncfa_var.set(str(settings.get("ncfa", self.ncfa_var.get())))
            self.out_base_var.set(str(settings.get("out_base", self.out_base_var.get())) or "Liga")
            self.tz_var.set(str(settings.get("tz", self.tz_var.get())) or "Europe/Stockholm")
            tie_value = str(settings.get("tie", self.tie_var.get()))
            self.tie_var.set(tie_value if tie_value in {"average", "dense", "min", "max"} else "average")
            language_value = str(settings.get("language", "auto")).strip().lower()
            if language_value not in LANGUAGE_KEY_TO_LABEL:
                language_value = "auto"
            self.language_var.set(LANGUAGE_KEY_TO_LABEL[language_value])
            sort_key = str(settings.get("sort_by", DEFAULT_TABLE_SORT_KEY)).strip().lower()
            if sort_key not in {key for _, key in TABLE_SORT_OPTIONS}:
                sort_key = DEFAULT_TABLE_SORT_KEY
            self.table_sort_var.set(self._table_sort_key_to_label()[sort_key])
            self.fetch_played_at_var.set(bool(settings.get("fetch_played_at", False)))
            self.keep_missing_time_var.set(bool(settings.get("keep_missing_time", False)))
            self.fetch_detailed_round_metrics_var.set(bool(settings.get("fetch_detailed_round_metrics", True)))
            self.advanced_analytics_var.set(bool(settings.get("advanced_analytics", True)))
            self.debug_var.set(bool(settings.get("debug", False)))

        restored = 0
        missing = 0
        for item in state.get("weeks", []):
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()
            raw_path = str(item.get("file_path", "")).strip()
            deadline = str(item.get("deadline", "")).strip()
            sweden_maps = str(item.get("sweden_maps", "")).strip()
            if not label or not raw_path:
                continue
            week = WeekConfig(label=label, file_path=Path(raw_path), deadline=deadline, sweden_maps=sweden_maps)
            if week.file_path.exists():
                row_id = self.week_tree.insert("", "end", values=(week.label, str(week.file_path), week.deadline, week.effective_sweden_maps()))
                self.weeks_by_id[row_id] = week
                restored += 1
            else:
                missing += 1

        if restored:
            self.log(f"[STATE] Återställde {restored} veckofiler.")
        if missing:
            self.log(f"[STATE] {missing} sparade filer hittades inte längre och laddades inte.")

    def on_close(self) -> None:
        if self.is_running:
            should_close = messagebox.askyesno(
                self.tr_ui("Körning pågår"),
                self.tr_ui("En körning pågår fortfarande. Vill du verkligen avsluta appen?"),
                parent=self.root,
            )
            if not should_close:
                return
        self._save_state()
        self.root.destroy()


def main() -> int:
    root = tk.Tk()
    app = LeagueDesktopApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
