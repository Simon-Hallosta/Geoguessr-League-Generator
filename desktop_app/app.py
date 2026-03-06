from __future__ import annotations

import io
import json
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


def _resolve_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


ROOT_DIR = _resolve_base_dir()
WEEK_FILES_DIR = ROOT_DIR / "week_urls"
APP_STATE_PATH = ROOT_DIR / "desktop_app_state.json"
INFO_CONFIG_PATH = ROOT_DIR / "information_config.json"
INFO_CONFIG_LEGACY_DIR = ROOT_DIR / "legacy_configs" / "information"
APP_ICON_PATH = Path(__file__).resolve().parent / "assets" / "geoleague.ico"

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

    def to_week_arg(self) -> str:
        if self.deadline.strip():
            return f"{self.label}|{self.file_path}|{self.deadline.strip()}"
        return f"{self.label}|{self.file_path}"


class CreateWeekFileDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, on_save):
        super().__init__(master)
        self.title("Skapa veckofil")
        self.geometry("760x560")
        self.minsize(680, 520)
        self.configure(bg=BG_APP)
        self.on_save = on_save

        self.label_var = tk.StringVar(value="Vecka 1")
        self.deadline_var = tk.StringVar(value="")
        self.filename_var = tk.StringVar(value="urls_week1.txt")

        self.columnconfigure(0, weight=1)
        self.rowconfigure(4, weight=1)

        frm = ttk.Frame(self, style="Card.TFrame", padding=12)
        frm.grid(sticky="nsew")
        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(4, weight=1)

        ttk.Label(frm, text="Veckoetikett:", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        label_entry = ttk.Entry(frm, textvariable=self.label_var, style="Modern.TEntry")
        label_entry.grid(row=0, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Deadline (valfri):", style="Field.TLabel").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.deadline_var, style="Modern.TEntry").grid(row=1, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Filnamn:", style="Field.TLabel").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.filename_var, style="Modern.TEntry").grid(row=2, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Länkar (en per rad):", style="Field.TLabel").grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 4))

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
        self.links_txt.grid(row=4, column=0, columnspan=2, sticky="nsew")

        button_row = ttk.Frame(frm, style="Card.TFrame")
        button_row.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        button_row.columnconfigure(0, weight=1)

        ttk.Button(button_row, text="Spara fil", style="Accent.TButton", command=self.save).grid(row=0, column=1, sticky="e")
        ttk.Button(button_row, text="Avbryt", style="Soft.TButton", command=self.destroy).grid(row=0, column=2, sticky="e", padx=(8, 0))

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
        raw_links = self.links_txt.get("1.0", "end")
        links = [line.strip() for line in raw_links.splitlines() if line.strip()]

        if not label:
            messagebox.showerror("Fel", "Veckoetikett måste anges.", parent=self)
            return
        if not filename:
            messagebox.showerror("Fel", "Filnamn måste anges.", parent=self)
            return
        if not filename.lower().endswith(".txt"):
            filename += ".txt"
        if not links:
            messagebox.showerror("Fel", "Minst en länk måste anges.", parent=self)
            return

        WEEK_FILES_DIR.mkdir(parents=True, exist_ok=True)
        file_path = WEEK_FILES_DIR / filename
        file_path.write_text("\n".join(links) + "\n", encoding="utf-8")

        self.on_save(WeekConfig(label=label, file_path=file_path, deadline=deadline))
        self.destroy()


class DeadlineDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, initial_value: str):
        super().__init__(master)
        self.title("Välj deadline")
        self.geometry("430x220")
        self.resizable(False, False)
        self.configure(bg=BG_APP)
        self.result: Optional[str] = None

        default_dt = self._parse_initial(initial_value) or datetime.now().replace(second=0, microsecond=0)

        frm = ttk.Frame(self, style="Card.TFrame", padding=14)
        frm.pack(fill="both", expand=True, padx=10, pady=10)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text="Datum:", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
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
                text="Tips: installera `tkcalendar` för popup-kalender.",
                style="Hint.TLabel",
            ).grid(row=1, column=1, sticky="w")

        ttk.Label(frm, text="Tid (HH:MM):", style="Field.TLabel").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        time_row = ttk.Frame(frm, style="Card.TFrame")
        time_row.grid(row=2, column=1, sticky="w", pady=(6, 0))
        self.hour_var = tk.IntVar(value=default_dt.hour)
        self.minute_var = tk.IntVar(value=default_dt.minute)
        ttk.Spinbox(time_row, from_=0, to=23, width=3, format="%02.0f", textvariable=self.hour_var).pack(side="left")
        ttk.Label(time_row, text=":", style="Field.TLabel").pack(side="left", padx=3)
        ttk.Spinbox(time_row, from_=0, to=59, width=3, format="%02.0f", textvariable=self.minute_var).pack(side="left")

        buttons = ttk.Frame(frm, style="Card.TFrame")
        buttons.grid(row=3, column=0, columnspan=2, sticky="e", pady=(14, 0))
        ttk.Button(buttons, text="Rensa", style="Outline.TButton", command=self.clear_deadline).pack(side="left")
        ttk.Button(buttons, text="Avbryt", style="Soft.TButton", command=self.cancel).pack(side="left", padx=(8, 0))
        ttk.Button(buttons, text="Spara", style="Accent.TButton", command=self.save).pack(side="left", padx=(8, 0))

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
            messagebox.showerror("Fel", "Ogiltigt datum eller klockslag.", parent=self)
            return

        self.result = dt.strftime("%Y-%m-%d %H:%M")
        self.destroy()


class InformationConfigDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, initial_rows: list[str], default_rows: list[str], config_path: Path, legacy_dir: Path):
        super().__init__(master)
        self.title("Information-flik: konfiguration")
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
            text=(
                "Ange en punkt per rad för Information-fliken.\n"
                "När du klickar Spara skrivs config-filen över.\n"
                "Nuvarande config sparas först som legacy-kopia med datum."
            ),
            style="Field.TLabel",
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        ttk.Label(
            frm,
            text=f"Config-fil: {self.config_path}\nLegacy-mapp: {self.legacy_dir}",
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
        ttk.Button(buttons, text="Återställ default", style="Outline.TButton", command=self.reset_default).pack(side="left")
        ttk.Button(buttons, text="Avbryt", style="Soft.TButton", command=self.cancel).pack(side="left", padx=(8, 0))
        ttk.Button(buttons, text="Spara (skriver över config)", style="Accent.TButton", command=self.save).pack(side="left", padx=(8, 0))

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
            messagebox.showerror("Fel", "Lägg till minst en informationsrad.", parent=self)
            return

        should_save = messagebox.askyesno(
            "Bekräfta överskrivning",
            (
                f"Detta skriver över config-filen:\n{self.config_path}\n\n"
                f"Nuvarande config sparas först i:\n{self.legacy_dir}\n\n"
                "Vill du fortsätta?"
            ),
            parent=self,
        )
        if not should_save:
            return

        self.result_rows = rows
        self.destroy()


class LeagueDesktopApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("GeoGuessr League Desktop")
        self.root.geometry("1080x740")
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

        self.ncfa_var = tk.StringVar(value=os.environ.get("GEOGUESSR_NCFA", ""))
        self.out_base_var = tk.StringVar(value="Liga")
        self.tz_var = tk.StringVar(value="Europe/Stockholm")
        self.tie_var = tk.StringVar(value="average")
        self.fetch_played_at_var = tk.BooleanVar(value=False)
        self.keep_missing_time_var = tk.BooleanVar(value=False)
        self.debug_var = tk.BooleanVar(value=False)
        self.progress_var = tk.StringVar(value="Redo")
        self.progress_time_var = tk.StringVar(value="")

        self._build_ui()
        self._ensure_information_config_exists()
        self._load_state()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

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
        outer = ttk.Frame(self.root, style="App.TFrame", padding=18)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(2, weight=1)
        outer.rowconfigure(4, weight=1)

        logo_frame = ttk.Frame(outer, style="Hero.TFrame", padding=(14, 14, 14, 14))
        logo_frame.grid(row=0, column=0, sticky="ew")
        self._build_logo_header(logo_frame)

        env_frame = ttk.LabelFrame(outer, text="1) Inloggning / miljövariabel", style="Card.TLabelframe", padding=12)
        env_frame.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        env_frame.columnconfigure(1, weight=1)
        ttk.Label(env_frame, text="_ncfa:", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 10))
        self.ncfa_entry = ttk.Entry(env_frame, textvariable=self.ncfa_var, show="*", style="Modern.TEntry")
        self.ncfa_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.env_btn = ttk.Button(env_frame, text="Sätt GEOGUESSR_NCFA i appen", style="Accent.TButton", command=self.apply_ncfa_env)
        self.env_btn.grid(row=0, column=2, padx=(8, 0))
        self.save_windows_env_btn = ttk.Button(
            env_frame,
            text="Spara i Windows (setx)",
            style="Outline.TButton",
            command=self.save_ncfa_to_windows_env,
        )
        self.save_windows_env_btn.grid(row=0, column=3, padx=(8, 0))
        if not sys.platform.startswith("win"):
            self.save_windows_env_btn.configure(state="disabled")

        weeks_frame = ttk.LabelFrame(outer, text="2) Veckofiler", style="Card.TLabelframe", padding=12)
        weeks_frame.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        weeks_frame.columnconfigure(0, weight=1)
        weeks_frame.rowconfigure(1, weight=1)

        help_label = (
            "Lägg till befintliga .txt-filer eller skapa nya. "
            "Varje rad i filen ska vara en challenge-länk."
        )
        ttk.Label(weeks_frame, text=help_label, style="Hint.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 10))

        cols = ("label", "file", "deadline")
        self.week_tree = ttk.Treeview(weeks_frame, style="Modern.Treeview", columns=cols, show="headings", height=9)
        self.week_tree.heading("label", text="Vecka")
        self.week_tree.heading("file", text="Textfil")
        self.week_tree.heading("deadline", text="Deadline (valfri)")
        self.week_tree.column("label", width=160, anchor="w")
        self.week_tree.column("file", width=640, anchor="w")
        self.week_tree.column("deadline", width=180, anchor="center")
        self.week_tree.grid(row=1, column=0, sticky="nsew")

        tree_scroll = ttk.Scrollbar(weeks_frame, orient="vertical", command=self.week_tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.week_tree.configure(yscrollcommand=tree_scroll.set)

        week_buttons = ttk.Frame(weeks_frame, style="Card.TFrame")
        week_buttons.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        self.add_files_btn = ttk.Button(week_buttons, text="Lägg till befintliga filer", style="Accent.TButton", command=self.add_existing_files)
        self.create_file_btn = ttk.Button(week_buttons, text="Skapa ny veckofil", style="Soft.TButton", command=self.open_create_dialog)
        self.edit_deadline_btn = ttk.Button(week_buttons, text="Ändra deadline", style="Outline.TButton", command=self.edit_selected_deadline)
        self.remove_btn = ttk.Button(week_buttons, text="Ta bort vald", style="Outline.TButton", command=self.remove_selected)
        self.add_files_btn.pack(side="left")
        self.create_file_btn.pack(side="left", padx=(8, 0))
        self.edit_deadline_btn.pack(side="left", padx=(8, 0))
        self.remove_btn.pack(side="left", padx=(8, 0))

        options_frame = ttk.LabelFrame(outer, text="3) Körning", style="Card.TLabelframe", padding=12)
        options_frame.grid(row=3, column=0, sticky="ew", pady=(12, 0))
        for i in range(5):
            options_frame.columnconfigure(i, weight=1 if i in (1, 3) else 0)

        ttk.Label(options_frame, text="Output-bas:", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.out_entry = ttk.Entry(options_frame, textvariable=self.out_base_var, style="Modern.TEntry")
        self.out_entry.grid(row=0, column=1, sticky="ew", padx=(0, 14))

        ttk.Label(options_frame, text="Tidszon:", style="Field.TLabel").grid(row=0, column=2, sticky="w", padx=(0, 8))
        self.tz_entry = ttk.Entry(options_frame, textvariable=self.tz_var, style="Modern.TEntry")
        self.tz_entry.grid(row=0, column=3, sticky="ew", padx=(0, 14))

        ttk.Label(options_frame, text="Tie-läge:", style="Field.TLabel").grid(row=0, column=4, sticky="w", padx=(0, 8))
        self.tie_combo = ttk.Combobox(
            options_frame,
            style="Modern.TCombobox",
            textvariable=self.tie_var,
            values=["average", "dense", "min", "max"],
            state="readonly",
            width=10,
        )
        self.tie_combo.grid(row=0, column=5, sticky="w")
        ttk.Label(
            options_frame,
            text="Obs: Tid används alltid som tie-break vid samma poäng. Tie-läge gäller bara exakt lika poäng + tid.",
            style="Hint.TLabel",
        ).grid(row=1, column=0, columnspan=6, sticky="w", pady=(6, 0))

        self.fetch_chk = ttk.Checkbutton(
            options_frame,
            text="Hämta played_at (för deadline-filter)",
            style="Card.TCheckbutton",
            variable=self.fetch_played_at_var,
        )
        self.keep_missing_chk = ttk.Checkbutton(
            options_frame,
            text="Behåll poster utan tidsstämpel",
            style="Card.TCheckbutton",
            variable=self.keep_missing_time_var,
        )
        self.debug_chk = ttk.Checkbutton(options_frame, text="Debug-logg", style="Card.TCheckbutton", variable=self.debug_var)
        self.fetch_chk.grid(row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))
        self.keep_missing_chk.grid(row=2, column=3, columnspan=2, sticky="w", pady=(8, 0))
        self.debug_chk.grid(row=2, column=5, sticky="w", pady=(8, 0))

        run_row = ttk.Frame(options_frame, style="Card.TFrame")
        run_row.grid(row=3, column=0, columnspan=6, sticky="ew", pady=(12, 0))
        self.run_btn = ttk.Button(run_row, text="Kör och skapa Excel", style="Accent.TButton", command=self.start_generation)
        self.info_cfg_btn = ttk.Button(run_row, text="Redigera Information-flik", style="Outline.TButton", command=self.open_information_config_dialog)
        self.open_folder_btn = ttk.Button(run_row, text="Öppna projektmapp", style="Soft.TButton", command=self.open_project_folder)
        self.run_btn.pack(side="left")
        self.info_cfg_btn.pack(side="left", padx=(8, 0))
        self.open_folder_btn.pack(side="left", padx=(8, 0))

        progress_row = ttk.Frame(options_frame, style="Card.TFrame")
        progress_row.grid(row=4, column=0, columnspan=6, sticky="ew", pady=(10, 0))
        progress_row.columnconfigure(0, weight=1)
        self.progress_bar = ttk.Progressbar(progress_row, mode="indeterminate")
        self.progress_bar.grid(row=0, column=0, sticky="ew")
        ttk.Label(progress_row, textvariable=self.progress_var, style="Hint.TLabel").grid(row=1, column=0, sticky="w", pady=(5, 0))
        ttk.Label(progress_row, textvariable=self.progress_time_var, style="Hint.TLabel").grid(row=1, column=1, sticky="e", pady=(5, 0), padx=(12, 0))

        log_frame = ttk.LabelFrame(outer, text="Logg", style="Card.TLabelframe", padding=12)
        log_frame.grid(row=4, column=0, sticky="nsew", pady=(12, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(
            log_frame,
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
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self.log("Appen startad.")
        self.log("Tips: skapa veckofiler i appen, eller lägg till befintliga .txt-filer.")
        self.log(f"[STATE] Sparad konfiguration: {APP_STATE_PATH}")

    def _build_logo_header(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(1, weight=1)

        canvas = tk.Canvas(parent, width=80, height=80, highlightthickness=0, bg=BG_HERO)
        canvas.grid(row=0, column=0, padx=(4, 10), sticky="w")
        self._draw_logo(canvas)

        txt_frame = ttk.Frame(parent, style="Hero.TFrame")
        txt_frame.grid(row=0, column=1, sticky="w")
        ttk.Label(txt_frame, text="Desktop Edition", style="HeroBadge.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(txt_frame, text="GeoLeague Builder", style="HeroTitle.TLabel").grid(row=1, column=0, sticky="w")
        ttk.Label(
            txt_frame,
            text="Challenge-insamling och ligarapport i ett klick",
            style="HeroSub.TLabel",
        ).grid(row=2, column=0, sticky="w")

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
            self.save_windows_env_btn,
            self.add_files_btn,
            self.create_file_btn,
            self.edit_deadline_btn,
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
        self.progress_var.set("Körning pågår... hämtar och bearbetar data.")
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
        self.progress_var.set("Klar." if ok else "Körning misslyckades.")
        self._run_started_at = None

    def apply_ncfa_env(self) -> None:
        ncfa = self.ncfa_var.get().strip()
        if not ncfa:
            messagebox.showerror("Fel", "_ncfa saknas.")
            return
        os.environ["GEOGUESSR_NCFA"] = ncfa
        self.log("[OK] GEOGUESSR_NCFA satt i app-processens miljö.")
        self._save_state()

    def save_ncfa_to_windows_env(self) -> None:
        if not sys.platform.startswith("win"):
            messagebox.showinfo("Info", "Denna funktion är bara tillgänglig på Windows.")
            return
        ncfa = self.ncfa_var.get().strip()
        if not ncfa:
            messagebox.showerror("Fel", "_ncfa saknas.")
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
            messagebox.showerror("Fel", f"Kunde inte spara variabeln:\n{err}")

    def add_existing_files(self) -> None:
        paths = filedialog.askopenfilenames(
            title="Välj URL-textfiler",
            initialdir=str(ROOT_DIR),
            filetypes=[("Textfiler", "*.txt"), ("Alla filer", "*.*")],
        )
        if not paths:
            return

        for raw_path in paths:
            file_path = Path(raw_path)
            default_label = self._guess_label_from_path(file_path)
            label = simpledialog.askstring(
                "Veckoetikett",
                f"Ange veckonamn för:\n{file_path.name}",
                initialvalue=default_label,
                parent=self.root,
            )
            if not label:
                continue
            self._insert_week(WeekConfig(label=label.strip(), file_path=file_path, deadline=""))

    def _guess_label_from_path(self, path: Path) -> str:
        digits = re.findall(r"\d+", path.stem)
        if digits:
            return f"Vecka {digits[0]}"
        return path.stem.replace("_", " ")

    def open_create_dialog(self) -> None:
        CreateWeekFileDialog(self.root, on_save=self._insert_week)

    def _insert_week(self, week: WeekConfig) -> None:
        if not week.file_path.exists():
            messagebox.showerror("Fel", f"Filen finns inte:\n{week.file_path}")
            return
        row_id = self.week_tree.insert("", "end", values=(week.label, str(week.file_path), week.deadline))
        self.weeks_by_id[row_id] = week
        self.log(f"[OK] Lade till: {week.label} -> {week.file_path}")
        self._save_state()

    def edit_selected_deadline(self) -> None:
        selected = self.week_tree.selection()
        if not selected:
            messagebox.showinfo("Info", "Markera en rad först.")
            return
        row_id = selected[0]
        week = self.weeks_by_id[row_id]
        dlg = DeadlineDialog(self.root, initial_value=week.deadline)
        self.root.wait_window(dlg)
        if dlg.result is None:
            return
        week.deadline = dlg.result.strip()
        self.week_tree.item(row_id, values=(week.label, str(week.file_path), week.deadline))
        self.log(f"[OK] Uppdaterade deadline för {week.label}: {week.deadline or '(ingen)'}")
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
        try:
            rows = league_core.default_information_rows()
            if isinstance(rows, list) and rows:
                return [str(x).strip() for x in rows if str(x).strip()]
        except Exception:
            pass
        return [
            "Ingen anmälan krävs - det är bara att spela veckans challenges!",
            "För att öppna länken: klicka på den understrukna raden i varje kolumn. Exempelvis \"🔗 Moving 1 | Moving - 3 min\".",
            "Preliminära poäng utdelas under veckan. De kan gå upp beroende på hur många spelare som placerar sig under dig.",
            "Poäng delas ut enligt pro league-systemet: sista plats får 1 poäng, näst sista 2 poäng, tredje sista 3 poäng osv.",
            "Tiebreaker vid samma poäng är tid. Om två spelare delar plats får båda poäng för den delade placeringen.",
            "Varje vecka avslutas onsdag kl 20.00. Om poängen inte är ihopräknade då kan du spela tills poängen är ihopräknade.",
            "Vid frågor, skriv i #ligan.",
        ]

    def _information_config_payload(self, rows: list[str]) -> dict:
        clean_rows = [str(line).strip() for line in rows if str(line).strip()]
        if not clean_rows:
            clean_rows = self._default_information_rows()
        return {"version": 1, "information_rows": clean_rows}

    def _read_information_rows(self) -> list[str]:
        if not INFO_CONFIG_PATH.exists():
            return self._default_information_rows()
        try:
            payload = json.loads(INFO_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception as ex:
            self.log(f"[WARN] Kunde inte läsa information_config.json ({ex}). Använder default.")
            return self._default_information_rows()

        if isinstance(payload, dict):
            rows = payload.get("information_rows")
        elif isinstance(payload, list):
            rows = payload
        else:
            rows = None
        if not isinstance(rows, list):
            return self._default_information_rows()

        out = [str(line).strip() for line in rows if str(line).strip()]
        return out or self._default_information_rows()

    def _ensure_information_config_exists(self) -> None:
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

            payload = self._information_config_payload(rows)
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
        )
        self.root.wait_window(dlg)
        if dlg.result_rows is None:
            return

        ok, legacy_path = self._save_information_rows_with_legacy_backup(dlg.result_rows)
        if not ok:
            messagebox.showerror("Fel", "Kunde inte spara information-config. Se loggen för detaljer.", parent=self.root)
            return

        if legacy_path is not None:
            self.log(f"[OK] Sparade legacy-config: {legacy_path}")
        self.log(f"[OK] Uppdaterade information-config: {INFO_CONFIG_PATH}")
        messagebox.showinfo("Klart", "Information-config sparad.", parent=self.root)

    def start_generation(self) -> None:
        if self.is_running:
            return

        weeks = self._collect_weeks_in_order()
        if not weeks:
            messagebox.showerror("Fel", "Lägg till minst en veckofil.")
            return
        missing_files = [str(w.file_path) for w in weeks if not w.file_path.exists()]
        if missing_files:
            msg = "Dessa veckofiler saknas:\n\n" + "\n".join(missing_files)
            messagebox.showerror("Fel", msg)
            self.log("[ERROR] Saknade filer:\n" + "\n".join(missing_files))
            return

        ncfa = self.ncfa_var.get().strip()
        if not ncfa:
            messagebox.showerror("Fel", "_ncfa saknas.")
            return

        requested_out_base = self.out_base_var.get().strip() or "Liga"
        out_base = self._resolve_writable_out_base(requested_out_base)
        if out_base is None:
            messagebox.showerror(
                "Fel",
                "Kunde inte hitta ett skrivbart filnamn för output. Stäng eventuell öppen Excel-fil och försök igen.",
            )
            return
        if out_base != requested_out_base:
            self.out_base_var.set(out_base)
            self.log(f"[INFO] Outputfil var låst/upptagen. Använder fallback-namn: {out_base}")

        tz_name = self.tz_var.get().strip() or "Europe/Stockholm"

        os.environ["GEOGUESSR_NCFA"] = ncfa
        args: list[str] = []
        for week in weeks:
            args.extend(["--week", week.to_week_arg()])
        args.extend(["--out-base", out_base, "--tz", tz_name, "--tie", self.tie_var.get(), "--ncfa", ncfa])
        args.extend(["--information-config", str(INFO_CONFIG_PATH)])
        if self.fetch_played_at_var.get():
            args.append("--fetch-played-at")
        if self.keep_missing_time_var.get():
            args.append("--keep-missing-time")
        if self.debug_var.get():
            args.append("--debug")

        self._save_state()
        self.is_running = True
        self._saw_warning = False
        self.set_controls_state(False)
        self.log("[START] Kör generator...")
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
            self.progress_var.set("Klart med varningar.")
        if exit_code == 0:
            self.log("[DONE] Klart.")
            if self._saw_warning:
                messagebox.showwarning(
                    "Klart med varningar",
                    "Excel-filer skapades, men en eller flera veckor/kartor kunde inte hämtas fullt ut.\nSe loggen för detaljer.",
                )
            else:
                messagebox.showinfo("Klart", "Excel-filer skapades.")
        else:
            self.log(f"[ERROR] Körning misslyckades (exit code {exit_code}).")
            messagebox.showerror("Fel", "Körning misslyckades. Se loggen.")

    def open_project_folder(self) -> None:
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(ROOT_DIR))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(ROOT_DIR)])
            else:
                subprocess.Popen(["xdg-open", str(ROOT_DIR)])
        except Exception as ex:
            messagebox.showerror("Fel", f"Kunde inte öppna mappen:\n{ex}")

    def _save_state(self) -> None:
        state = {
            "weeks": [
                {"label": w.label, "file_path": str(w.file_path), "deadline": w.deadline}
                for w in self._collect_weeks_in_order()
            ],
            "settings": {
                "ncfa": self.ncfa_var.get().strip(),
                "out_base": self.out_base_var.get().strip(),
                "tz": self.tz_var.get().strip(),
                "tie": self.tie_var.get().strip(),
                "fetch_played_at": bool(self.fetch_played_at_var.get()),
                "keep_missing_time": bool(self.keep_missing_time_var.get()),
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
            self.fetch_played_at_var.set(bool(settings.get("fetch_played_at", False)))
            self.keep_missing_time_var.set(bool(settings.get("keep_missing_time", False)))
            self.debug_var.set(bool(settings.get("debug", False)))

        restored = 0
        missing = 0
        for item in state.get("weeks", []):
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()
            raw_path = str(item.get("file_path", "")).strip()
            deadline = str(item.get("deadline", "")).strip()
            if not label or not raw_path:
                continue
            week = WeekConfig(label=label, file_path=Path(raw_path), deadline=deadline)
            if week.file_path.exists():
                row_id = self.week_tree.insert("", "end", values=(week.label, str(week.file_path), week.deadline))
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
                "Körning pågår",
                "En körning pågår fortfarande. Vill du verkligen avsluta appen?",
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
