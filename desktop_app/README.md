# GeoGuessr League Desktop App

Detta är en GUI-app ovanpå `geoguessr_league_build_xlsx.py`.

## Starta appen

Från projektroten:

```bash
python desktop_app/app.py
```

För kalender-popup i deadline-dialogen vid script-körning:

```bash
pip install tkcalendar
```

På Windows kan ni även dubbelklicka:

`desktop_app/start_gui_windows.bat`

## Vad appen gör

- Sätter `GEOGUESSR_NCFA` i appens miljö
- Kan spara `GEOGUESSR_NCFA` permanent i Windows (`setx`)
- Låter dig välja vilka `.txt`-filer som ska användas
- Låter dig skapa nya veckofiler i `week_urls/`
- Har deadline-dialog med datum + klockslag (kalenderväljare om `tkcalendar` finns)
- Minns valda veckofiler och inställningar mellan starter
- Visar löpande progress + live-logg under längre körningar
- Om outputfilen är låst/upptagen väljs automatiskt nästa lediga namn (t.ex. `Liga (1)`)
- Kör generatorn och skapar samma Excel-filer som tidigare

## Veckofiler

När du skapar ny veckofil i appen:

- Ange veckoetikett (t.ex. `Vecka 5`)
- Klistra in länkar, en per rad
- Filen sparas i `week_urls/`

## Kom ihåg-lista mellan starter

Appen sparar state i:

- `desktop_app_state.json` (bredvid `.exe` eller i projektroten vid script-körning)

## Bygg .exe (valfritt)

Om ni vill dela appen som en ren Windows-app:

```bash
pip install pyinstaller
pyinstaller --onefile --windowed --icon desktop_app/assets/geoleague.ico --name GeoGuessrLeagueApp desktop_app/app.py
```

Den färdiga `.exe` hamnar i `dist/`.
