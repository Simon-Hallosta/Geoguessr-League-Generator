from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import threading
import traceback
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk


def _resolve_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


ROOT_DIR = _resolve_base_dir()
WEEK_FILES_DIR = ROOT_DIR / "week_urls"
APP_STATE_PATH = ROOT_DIR / "desktop_app_state.json"
APP_ICON_PATH = Path(__file__).resolve().parent / "assets" / "geoleague.ico"

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
        self.on_save = on_save

        self.label_var = tk.StringVar(value="Vecka 1")
        self.deadline_var = tk.StringVar(value="")
        self.filename_var = tk.StringVar(value="urls_week1.txt")

        self.columnconfigure(0, weight=1)
        self.rowconfigure(4, weight=1)

        frm = ttk.Frame(self, padding=12)
        frm.grid(sticky="nsew")
        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(4, weight=1)

        ttk.Label(frm, text="Veckoetikett:").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        label_entry = ttk.Entry(frm, textvariable=self.label_var)
        label_entry.grid(row=0, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Deadline (valfri):").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.deadline_var).grid(row=1, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Filnamn:").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        ttk.Entry(frm, textvariable=self.filename_var).grid(row=2, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Länkar (en per rad):").grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 4))

        self.links_txt = tk.Text(frm, height=16, wrap="word")
        self.links_txt.grid(row=4, column=0, columnspan=2, sticky="nsew")

        button_row = ttk.Frame(frm)
        button_row.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        button_row.columnconfigure(0, weight=1)

        ttk.Button(button_row, text="Spara fil", command=self.save).grid(row=0, column=1, sticky="e")
        ttk.Button(button_row, text="Avbryt", command=self.destroy).grid(row=0, column=2, sticky="e", padx=(8, 0))

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


class LeagueDesktopApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("GeoGuessr League Desktop")
        self.root.geometry("1080x740")
        self.root.minsize(980, 680)
        self._try_set_window_icon()

        self.is_running = False
        self.weeks_by_id: Dict[str, WeekConfig] = {}

        self.ncfa_var = tk.StringVar(value=os.environ.get("GEOGUESSR_NCFA", ""))
        self.out_base_var = tk.StringVar(value="Liga")
        self.tz_var = tk.StringVar(value="Europe/Stockholm")
        self.tie_var = tk.StringVar(value="average")
        self.fetch_played_at_var = tk.BooleanVar(value=False)
        self.keep_missing_time_var = tk.BooleanVar(value=False)
        self.debug_var = tk.BooleanVar(value=False)

        self._build_ui()
        self._load_state()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=12)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(2, weight=1)
        outer.rowconfigure(4, weight=1)

        logo_frame = ttk.Frame(outer, padding=(0, 0, 0, 8))
        logo_frame.grid(row=0, column=0, sticky="ew")
        self._build_logo_header(logo_frame)

        env_frame = ttk.LabelFrame(outer, text="1) Inloggning / miljövariabel", padding=10)
        env_frame.grid(row=1, column=0, sticky="ew")
        env_frame.columnconfigure(1, weight=1)
        ttk.Label(env_frame, text="_ncfa:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.ncfa_entry = ttk.Entry(env_frame, textvariable=self.ncfa_var, show="*")
        self.ncfa_entry.grid(row=0, column=1, sticky="ew")
        self.env_btn = ttk.Button(env_frame, text="Sätt GEOGUESSR_NCFA i appen", command=self.apply_ncfa_env)
        self.env_btn.grid(row=0, column=2, padx=(8, 0))
        self.save_windows_env_btn = ttk.Button(
            env_frame,
            text="Spara i Windows (setx)",
            command=self.save_ncfa_to_windows_env,
        )
        self.save_windows_env_btn.grid(row=0, column=3, padx=(8, 0))
        if not sys.platform.startswith("win"):
            self.save_windows_env_btn.configure(state="disabled")

        weeks_frame = ttk.LabelFrame(outer, text="2) Veckofiler", padding=10)
        weeks_frame.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        weeks_frame.columnconfigure(0, weight=1)
        weeks_frame.rowconfigure(1, weight=1)

        help_label = (
            "Lägg till befintliga .txt-filer eller skapa nya. "
            "Varje rad i filen ska vara en challenge-länk."
        )
        ttk.Label(weeks_frame, text=help_label).grid(row=0, column=0, sticky="w", pady=(0, 8))

        cols = ("label", "file", "deadline")
        self.week_tree = ttk.Treeview(weeks_frame, columns=cols, show="headings", height=9)
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

        week_buttons = ttk.Frame(weeks_frame)
        week_buttons.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self.add_files_btn = ttk.Button(week_buttons, text="Lägg till befintliga filer", command=self.add_existing_files)
        self.create_file_btn = ttk.Button(week_buttons, text="Skapa ny veckofil", command=self.open_create_dialog)
        self.edit_deadline_btn = ttk.Button(week_buttons, text="Ändra deadline", command=self.edit_selected_deadline)
        self.remove_btn = ttk.Button(week_buttons, text="Ta bort vald", command=self.remove_selected)
        self.add_files_btn.pack(side="left")
        self.create_file_btn.pack(side="left", padx=(8, 0))
        self.edit_deadline_btn.pack(side="left", padx=(8, 0))
        self.remove_btn.pack(side="left", padx=(8, 0))

        options_frame = ttk.LabelFrame(outer, text="3) Körning", padding=10)
        options_frame.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        for i in range(5):
            options_frame.columnconfigure(i, weight=1 if i in (1, 3) else 0)

        ttk.Label(options_frame, text="Output-bas:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.out_entry = ttk.Entry(options_frame, textvariable=self.out_base_var)
        self.out_entry.grid(row=0, column=1, sticky="ew", padx=(0, 14))

        ttk.Label(options_frame, text="Tidszon:").grid(row=0, column=2, sticky="w", padx=(0, 8))
        self.tz_entry = ttk.Entry(options_frame, textvariable=self.tz_var)
        self.tz_entry.grid(row=0, column=3, sticky="ew", padx=(0, 14))

        ttk.Label(options_frame, text="Tie-läge:").grid(row=0, column=4, sticky="w", padx=(0, 8))
        self.tie_combo = ttk.Combobox(
            options_frame,
            textvariable=self.tie_var,
            values=["average", "dense", "min", "max"],
            state="readonly",
            width=10,
        )
        self.tie_combo.grid(row=0, column=5, sticky="w")
        ttk.Label(
            options_frame,
            text="Obs: Tid används alltid som tie-break vid samma poäng. Tie-läge gäller bara exakt lika poäng + tid.",
            foreground="#5A5A5A",
        ).grid(row=1, column=0, columnspan=6, sticky="w", pady=(6, 0))

        self.fetch_chk = ttk.Checkbutton(options_frame, text="Hämta played_at (för deadline-filter)", variable=self.fetch_played_at_var)
        self.keep_missing_chk = ttk.Checkbutton(options_frame, text="Behåll poster utan tidsstämpel", variable=self.keep_missing_time_var)
        self.debug_chk = ttk.Checkbutton(options_frame, text="Debug-logg", variable=self.debug_var)
        self.fetch_chk.grid(row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))
        self.keep_missing_chk.grid(row=2, column=3, columnspan=2, sticky="w", pady=(8, 0))
        self.debug_chk.grid(row=2, column=5, sticky="w", pady=(8, 0))

        run_row = ttk.Frame(options_frame)
        run_row.grid(row=3, column=0, columnspan=6, sticky="ew", pady=(12, 0))
        self.run_btn = ttk.Button(run_row, text="Kör och skapa Excel", command=self.start_generation)
        self.open_folder_btn = ttk.Button(run_row, text="Öppna projektmapp", command=self.open_project_folder)
        self.run_btn.pack(side="left")
        self.open_folder_btn.pack(side="left", padx=(8, 0))

        log_frame = ttk.LabelFrame(outer, text="Logg", padding=10)
        log_frame.grid(row=4, column=0, sticky="nsew", pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, height=12, wrap="word")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self.log("Appen startad.")
        self.log("Tips: skapa veckofiler i appen, eller lägg till befintliga .txt-filer.")
        self.log(f"[STATE] Sparad konfiguration: {APP_STATE_PATH}")

    def _build_logo_header(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(1, weight=1)

        canvas = tk.Canvas(parent, width=80, height=80, highlightthickness=0)
        canvas.grid(row=0, column=0, padx=(4, 10), sticky="w")
        self._draw_logo(canvas)

        txt_frame = ttk.Frame(parent)
        txt_frame.grid(row=0, column=1, sticky="w")
        ttk.Label(txt_frame, text="GeoLeague Builder", font=("Segoe UI", 18, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(
            txt_frame,
            text="Challenge-insamling och ligarapport i ett klick",
            font=("Segoe UI", 10),
        ).grid(row=1, column=0, sticky="w")

    def _draw_logo(self, canvas: tk.Canvas) -> None:
        bg = self.root.cget("bg")
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
            self.open_folder_btn,
        ]:
            widget.configure(state=state)
        self.tie_combo.configure(state=combo_state)

    def log(self, text: str) -> None:
        self.log_text.insert("end", text.rstrip() + "\n")
        self.log_text.see("end")

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
        new_deadline = simpledialog.askstring(
            "Deadline",
            "Skriv deadline (ex. 2026-02-25 20:00), lämna tomt för ingen deadline:",
            initialvalue=week.deadline,
            parent=self.root,
        )
        if new_deadline is None:
            return
        week.deadline = new_deadline.strip()
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

        out_base = self.out_base_var.get().strip() or "Liga"
        tz_name = self.tz_var.get().strip() or "Europe/Stockholm"

        os.environ["GEOGUESSR_NCFA"] = ncfa
        args: list[str] = []
        for week in weeks:
            args.extend(["--week", week.to_week_arg()])
        args.extend(["--out-base", out_base, "--tz", tz_name, "--tie", self.tie_var.get(), "--ncfa", ncfa])
        if self.fetch_played_at_var.get():
            args.append("--fetch-played-at")
        if self.keep_missing_time_var.get():
            args.append("--keep-missing-time")
        if self.debug_var.get():
            args.append("--debug")

        self._save_state()
        self.is_running = True
        self.set_controls_state(False)
        self.log("[START] Kör generator...")
        self.log("[ARGS] " + " ".join(args))

        thread = threading.Thread(target=self._worker_run, args=(args,), daemon=True)
        thread.start()

    def _worker_run(self, args: list[str]) -> None:
        capture = io.StringIO()
        exit_code = 1
        try:
            with redirect_stdout(capture), redirect_stderr(capture):
                rc = league_core.main(args)
                exit_code = int(rc or 0)
        except SystemExit as ex:
            if isinstance(ex.code, int):
                exit_code = ex.code
            elif ex.code in (None, 0):
                exit_code = 0
            else:
                capture.write(str(ex.code) + "\n")
                exit_code = 1
        except Exception:
            traceback.print_exc(file=capture)
            exit_code = 1

        output = capture.getvalue()
        self.root.after(0, self._on_worker_done, exit_code, output)

    def _on_worker_done(self, exit_code: int, output: str) -> None:
        self.is_running = False
        self.set_controls_state(True)
        if output.strip():
            self.log(output.rstrip())
        if exit_code == 0:
            self.log("[DONE] Klart.")
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
        self._save_state()
        self.root.destroy()


def main() -> int:
    root = tk.Tk()
    app = LeagueDesktopApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
