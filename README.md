# options-scanner

Primjeri komandi za filtriranje simbola i skeniranje opcija.

## Filtriranje simbola (`filter_symbols.py`)

1. Osnovno filtriranje (defaults):
```bash
python filter_symbols.py
``` 

2. Primjer s dodatnim pravilima + top 200 po dollar volumenu:
```bash
python filter_symbols.py --min-price 10 --max-age-days 60 --min-volume 400000 --min-dollar-volume 150000000 --min-history-days 252 --max-abs-daily-return 0.5 --lookback-days 22 --exclude-investment-vehicles --exclude-dot-symbols --top-n-dollar-volume 1000 --output symbols.txt
```

3. Ako zelis ukljuciti ETF/ETN i dot simbole:
```bash
python filter_symbols.py --no-exclude-investment-vehicles --no-exclude-dot-symbols
```

## Scanning opcija (`main.py`)

1. Pokretanje skenera (cita `config.yaml` i `symbols.txt`):
```bash
python main.py
```

2. Tipican flow:
```bash
python filter_symbols.py --top-n-dollar-volume 200 --output symbols.txt
python main.py
```

## Analiza skeniranih opcija (`analyze_scanned_options.py`)

1. Osnovno pokretanje (koristi defaulte iz `DEFAULT_ANALYSIS` bloka na vrhu skripte):
```bash
python analyze_scanned_options.py
```

2. Analiza s filtrima likvidnosti (volume/OI/spread):
```bash
python analyze_scanned_options.py --target-dte 30 --min-leg-volume 10 --min-leg-open-interest 100 --max-spread-pct-mid 0.20 --top-n 20 --output data/straddle_signal_table.csv
```

## Export QC contracts + Azure Blob (`export_qc_contracts.py`)

0. Jednokratno: napravi `.env` (npr. copy `.env.example`) i upisi credentials:
```dotenv
BLOB_ENDPOINT_SNP=https://snpmarketdata.blob.core.windows.net
BLOB_KEY_SNP=YOUR_BLOB_KEY
```

1. Kreiraj QC CSV iz `straddle_signal_table.csv` i uploadaj na blob container (`qc-backtest`):
```bash
python export_qc_contracts.py --input-csv data/straddle_signal_table.csv --container qc-backtest
```

2. Opcionalni dodatni filter prije exporta (pandas query):
```bash
python export_qc_contracts.py --where "iv_slope > 0.15 and min_leg_volume >= 5"
```

Napomena: credentials se citaju iz env varijabli `BLOB-ENDPOINT-SNP` i `BLOB-KEY-SNP` (ili underscore varijanti).
`export_qc_contracts.py` automatski ucitava `.env` (ili custom file preko `--env-file`).

3. PowerShell primjer s credentialima (underscore varijante):
```powershell
$env:BLOB_ENDPOINT_SNP="https://snpmarketdata.blob.core.windows.net"
$env:BLOB_KEY_SNP="YOUR_BLOB_KEY"
python export_qc_contracts.py --input-csv data/straddle_signal_table.csv --container qc-backtest
```

4. Svi argumenti za `export_qc_contracts.py`:
1. `--input-csv`: ulazna analiza tablica. Default: `data/straddle_signal_table.csv`
2. `--output-dir`: lokalni folder za generirani CSV prije uploada. Default: `data`
3. `--container`: Azure Blob container. Default: `qc-backtest`
4. `--blob-name`: ime izlaznog CSV-a u blobu/lokalu. Default: auto (`qc_contracts_YYYYMMDD_HHMMSS.csv`)
5. `--endpoint`: Azure Blob endpoint URL (opcionalno ako je u `.env`/env varijabli)
6. `--key`: Azure Blob account key (opcionalno ako je u `.env`/env varijabli)
7. `--env-file`: putanja do `.env` fajla. Default: `.env`
8. `--time-zone`: timezone za `date` kolonu. Default: `America/New_York`
9. `--minutes-in-future`: broj minuta unaprijed za `date`. Default: `5`
10. `--include-sides`: koje strane ukljuciti, odvojene zarezom. Default: `long_straddle,short_straddle`
11. `--where`: opcionalni `pandas query` filter nad ulaznom tablicom

5. Help komanda:
```bash
python export_qc_contracts.py -h
```

## Full pipeline (`run_full_pipeline.py`)

1. End-to-end pokretanje (filter -> scan -> analyze -> export/upload):
```bash
python run_full_pipeline.py
```

2. PowerShell primjer s Azure credentialima:
```powershell
$env:BLOB_ENDPOINT_SNP="https://snpmarketdata.blob.core.windows.net"
$env:BLOB_KEY_SNP="YOUR_BLOB_KEY"
python run_full_pipeline.py --container qc-backtest
```

## Azure Linux deployment (preporuceno)

Ako je cilj da ovo radi 24/7 i jeftinije od Windows VM-a:

1. Preporucena arhitektura:
- Azure Linux VM (`B2s` ili `B2ms`)
- Python environment + ovaj repo
- IB Gateway ili TWS + IBC (auto-login/start)
- `run_full_pipeline.py` preko cron/systemd timera
- CSV output i finalni export na Azure Blob

2. Zasto Linux:
- Nizi VM trosak nego Windows
- Jednostavniji automation (`systemd`, `cron`, `tmux`)
- Stabilnije za headless/scheduled workload

3. `.env` vs Key Vault:
- Solo projekt: `.env` je pragmaticki ok (plus stroge permisije fajla)
- Team/prod/rotacija tajni: Azure Key Vault je bolji izbor

4. Priblizni trosak VM-a (bez disk/network overheada, 24/7):
- `B2s` Linux: cca 30-40 USD/mjesec (ovisno o regiji)
- `B2ms` Linux: cca 60-80 USD/mjesec (ovisno o regiji)
- Windows je obicno skuplji zbog licence

5. Brzi setup na Linux VM:
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git tmux
git clone <YOUR_REPO_URL> options-scanner
cd options-scanner
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# upisi BLOB_ENDPOINT_SNP i BLOB_KEY_SNP u .env
```

6. Automatizirani setup (PowerShell + Azure CLI, iz ovog repoa):
```powershell
az login
.\azure\create_vm_and_install.ps1 `
  -RepoUrl "https://github.com/<ORG>/<REPO>.git" `
  -ResourceGroup "rg-options-scanner" `
  -Location "eastus" `
  -VmName "vm-options-scanner" `
  -TimerOnCalendar "Mon..Fri *-*-* 15:35:00" `
  -PipelineArgs "--container qc-backtest --skip-filter"
```

Skripta automatski:
- kreira Resource Group
- kreira Linux VM (SSH otvoren)
- pokrece `cloud-init` koji instalira `python3`, `pip`, `venv`, `git`, `tmux`
- klonira repo u `/opt/options-scanner`
- radi `.venv` i `pip install -r requirements.txt`
- kreira `.env` iz `.env.example` (ako postoji)
- kreira `systemd` `options-scanner.service` + `options-scanner.timer` i automatski enable-a timer

Napomena za repo:
- Kod mora biti dostupan VM-u preko `git clone` (GitHub, GitLab, Azure Repos, private git s SSH key-em).
- Znaci ne mora biti "tvoj GitHub", ali mora postojati remote koji VM moze dohvatiti.

Timer i pipeline parametri (skripta):
1. `-TimerOnCalendar`: systemd raspored. Default: `hourly`
2. `-PipelineArgs`: argumenti za `run_full_pipeline.py`. Default: `--container qc-backtest --skip-filter`
3. `-RunPipelineAfterProvision`: opcionalno odmah pokrene pipeline nakon prvog bootstrapa

7. Pokretanje cijelog flowa:
```bash
python run_full_pipeline.py --container qc-backtest
```

8. IBC (auto-start IB Gateway/TWS):
- Repo: `https://github.com/IbcAlpha/IBC`
- Radi na Linuxu i standardan je izbor za unattended IB automation
- Tipicno se konfigurira kao `systemd` servis, pa se nakon restarta VM-a sve samo digne

## Interaktivna analiza (`analysis.py`)

```bash
python analysis.py
```

Uredi parametre na vrhu fajla (`CONFIG` blok) i rerun za brzo igranje s filtrima, tablicama i grafovima.

## Debug jednog simbola (`debug_single_symbol.py`)

NVDA debug (status po requestu + CSV za inspekciju NULL polja):
```bash
python debug_single_symbol.py
```

Parametre uredi direktno u `CONFIG` bloku na vrhu fajla.
