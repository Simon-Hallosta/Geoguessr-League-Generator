# GeoGuessr League → Excel (Weekly tabs + Total + Stats)

Det här scriptet bygger en liga-översikt i Excel från GeoGuessr challenge-URL:er.
Du ger en URL-fil per vecka, och scriptet genererar:

- En flik per vecka (t.ex. Vecka 1, Vecka 2, …) i ett format som liknar ligaspreadsheetet
- En flik "Total" som summerar alla veckor
- En flik "Stats" med totalsummor och snitt (per karta, per vecka, mm)
- En flik "Raw" med underliggande data

Scriptet kan också (om möjligt) filtrera bort spel som gjorts efter en deadline:
- *_all.xlsx innehåller alla spel
- *_filtered.xlsx innehåller endast spel innan deadline (om timestamps kan hämtas)


------------------------------------------------------------
KRAV
------------------------------------------------------------

- Python 3.9+ rekommenderas (3.10/3.11 fungerar)
- Paket:
  - requests
  - pandas
  - openpyxl

Installera:

pip install requests pandas openpyxl


------------------------------------------------------------
FILER DU BEHÖVER
------------------------------------------------------------

För varje vecka:

urls_weekX.txt (en challenge-URL per rad), t.ex.

https://www.geoguessr.com/challenge/a2VSPJrVz2RwATaN
https://www.geoguessr.com/challenge/xxxxxxxxxxxxxxxx

Kommentarer med # ignoreras.


------------------------------------------------------------
AUTENTISERING (_ncfa-cookie)
------------------------------------------------------------

GeoGuessr kan blockera automatiserad inloggning. Scriptet använder därför cookie-värdet `_ncfa` från en normal, inloggad webbläsarsession.

Hämta _ncfa (Chrome):

1. Öppna GeoGuessr i Chrome och logga in normalt.
2. Öppna DevTools → Application
3. Cookies → https://www.geoguessr.com
4. Leta upp cookie `_ncfa`
5. Kopiera värdet

Sätt sedan env-var:

Windows (cmd):
set GEOGUESSR_NCFA=PASTE_VALUE_HERE

PowerShell:
$env:GEOGUESSR_NCFA="PASTE_VALUE_HERE"

macOS/Linux:
export GEOGUESSR_NCFA="PASTE_VALUE_HERE"

Du kan även skicka värdet direkt:

python geoguessr_league_build_xlsx.py --ncfa "PASTE_VALUE_HERE" ...


------------------------------------------------------------
POÄNGLOGIK
------------------------------------------------------------

Per karta (challenge):
- Primärt: högre total_pts är bättre
- Tie-break: lägre total_time (tiden för 5 rundor) är bättre
- Exakt lika (poäng + tid): löses via --tie (default average)

Ligapoäng:
- Borda-poäng per karta
- Om N spelare på en karta:
  - Bästa får N
  - Tvåan får N-1
  - ...
  - Sista får 1
- Vid exakt lika (poäng + tid) används tie-mode

Veckosumma:
- Summa av Borda-poäng över veckans kartor

Totalställning:
- Summa av veckornas Borda-poäng över alla veckor


------------------------------------------------------------
KÖRNING (UTAN DEADLINE-FILTER)
------------------------------------------------------------

Exempel med två veckor:

python geoguessr_league_build_xlsx.py ^
  --week "Vecka 1|urls_week1.txt" ^
  --week "Vecka 2|urls_week2.txt" ^
  --out-base "Liga"

Output:
Liga_all.xlsx


------------------------------------------------------------
DEADLINE-FILTER (VALFRITT)
------------------------------------------------------------

python geoguessr_league_build_xlsx.py ^
  --week "Vecka 1|urls_week1.txt|2026-02-18 20:00" ^
  --week "Vecka 2|urls_week2.txt|2026-02-25 20:00" ^
  --fetch-played-at ^
  --out-base "Liga"

Output (om timestamps kan extraheras):
- Liga_all.xlsx
- Liga_filtered.xlsx

Timezone:
Deadlines tolkas i timezone från --tz (default Europe/Stockholm).

Exempel:
python geoguessr_league_build_xlsx.py ... --tz "Europe/Stockholm"


------------------------------------------------------------
VAD SOM HAMNAR I EXCEL
------------------------------------------------------------

Vecka X:
- Rank
- Spelare
- Veckopoäng
- Borda-poäng per karta
- Kartnamn (från payload)
- Regelsammanfattning (Moving/NM/NMPZ + tidsgräns)
- Klickbar länk till challenge

Total:
- Totalställning över alla veckor
- Per-vecka-poäng som egna kolumner

Stats:
- Total Borda
- Total pts
- Antal kartor
- Antal veckor
- Snitt Borda per karta
- Snitt Borda per vecka
- Snitt pts per karta
- Bästa vecka

Raw:
- Underliggande rad-data


------------------------------------------------------------
NYTTIGA FLAGGOR
------------------------------------------------------------

Debug:
python geoguessr_league_build_xlsx.py ... --debug

Dumpa JSON:
python geoguessr_league_build_xlsx.py ... --dump-json

Tie-mode:
python geoguessr_league_build_xlsx.py ... --tie average
Alternativ: dense, min, max

Timeout:
python geoguessr_league_build_xlsx.py ... --timeout 60

Pagination:
python geoguessr_league_build_xlsx.py ... --page-size 200 --max-players 5000


------------------------------------------------------------
FELSÖKNING
------------------------------------------------------------

Missing _ncfa:
Sätt GEOGUESSR_NCFA till korrekt cookie-värde.

Filterad fil skapas inte:
- Deadline måste anges
- --fetch-played-at måste vara aktiverat
- API måste returnera timestamps

Tomma resultat:
- Kontrollera URL:er
- Hämta ny _ncfa
- Kör med --debug eller --dump-json


------------------------------------------------------------
ANSVARSFRISKRIVNING
------------------------------------------------------------

Detta är ett inofficiellt verktyg.
Använd på egen risk och respektera GeoGuessrs villkor.