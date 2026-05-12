"""Download all external data sources used in the FAF5 hub-selection project.

Sources
-------
- FAF5.7.1 State database (BTS/ORNL)
- FRED state GDP (annual, nominal) and unemployment rate (monthly)
- FRED regional CPI (CPI is not published at the state level)
- Census Bureau state population estimates 2020-2024
- Curated port TEU statistics for major US container ports
"""
from __future__ import annotations

import io
import time
import zipfile
from pathlib import Path

import requests

from .reference import FRED_REGIONAL_CPI, STATES

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"

FAF5_URL = "https://faf.ornl.gov/faf5/Data/Download_Files/FAF5.7.1_State.zip"
CENSUS_POP_URL = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2020-2024/state/totals/NST-EST2024-ALLDATA.csv"
)
CENSUS_POP_URL_LEGACY = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2010-2020/state/totals/nst-est2020-alldata.csv"
)
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"

HEADERS = {"User-Agent": "Mozilla/5.0 (data-science research client)"}


def _download(url: str, dest: Path, *, chunk: int = 1 << 20) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[skip] {dest.name} already exists ({dest.stat().st_size:,} bytes)")
        return dest
    print(f"[get ] {url}")
    with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for block in r.iter_content(chunk_size=chunk):
                if block:
                    f.write(block)
    print(f"[ok  ] {dest} ({dest.stat().st_size:,} bytes)")
    return dest


def download_faf5() -> Path:
    """Download and extract the FAF5.7.1 state-level zip."""
    out_dir = RAW / "faf5"
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "FAF5.7.1_State.zip"
    _download(FAF5_URL, zip_path)

    # Extract csv files only (skip Access .accdb to save space)
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv") and not (out_dir / Path(name).name).exists():
                with zf.open(name) as src, open(out_dir / Path(name).name, "wb") as dst:
                    dst.write(src.read())
                print(f"[xtr ] {name}")
    return out_dir


def download_fred() -> Path:
    """Download state-level GDP and unemployment + regional CPI from FRED."""
    out_dir = RAW / "fred"
    out_dir.mkdir(parents=True, exist_ok=True)

    series: list[tuple[str, str]] = []
    for abbr, *_ in STATES:
        series.append((f"{abbr}NGSP", f"gdp_{abbr}.csv"))   # nominal GDP, annual
        series.append((f"{abbr}UR", f"unemp_{abbr}.csv"))   # unemployment rate, monthly
    for region, sid in FRED_REGIONAL_CPI.items():
        series.append((sid, f"cpi_{region}.csv"))

    for sid, fname in series:
        try:
            _download(FRED_CSV_URL.format(series=sid), out_dir / fname)
        except requests.HTTPError as e:
            # Some smaller states may not have NGSP/UR series under this exact id.
            print(f"[warn] {sid}: {e}")
        time.sleep(0.15)  # be polite to fred.stlouisfed.org
    return out_dir


def download_census() -> Path:
    out_dir = RAW / "census"
    out_dir.mkdir(parents=True, exist_ok=True)
    _download(CENSUS_POP_URL, out_dir / "NST-EST2024-ALLDATA.csv")
    _download(CENSUS_POP_URL_LEGACY, out_dir / "nst-est2020-alldata.csv")
    return out_dir


# Port TEU statistics (annual, in TEUs) for the top US container ports.
# Source: BTS Port Performance Freight Statistics Program annual reports and
# AAPA North American Container Traffic rankings (publicly published).
# State assignment uses the primary state of each port authority.
PORT_TEU_ROWS: list[tuple[str, str, int, int]] = [
    # (port_name, state_abbr, year, teu)
    ("Los Angeles", "CA", 2017, 9343192),
    ("Los Angeles", "CA", 2018, 9458749),
    ("Los Angeles", "CA", 2019, 9337632),
    ("Los Angeles", "CA", 2020, 9213395),
    ("Los Angeles", "CA", 2021, 10677610),
    ("Los Angeles", "CA", 2022, 9911158),
    ("Los Angeles", "CA", 2023, 8634497),
    ("Los Angeles", "CA", 2024, 10302662),
    ("Long Beach", "CA", 2017, 7544507),
    ("Long Beach", "CA", 2018, 8091023),
    ("Long Beach", "CA", 2019, 7632032),
    ("Long Beach", "CA", 2020, 8113315),
    ("Long Beach", "CA", 2021, 9384368),
    ("Long Beach", "CA", 2022, 9133657),
    ("Long Beach", "CA", 2023, 8005553),
    ("Long Beach", "CA", 2024, 9645339),
    ("New York/New Jersey", "NJ", 2017, 6710817),
    ("New York/New Jersey", "NJ", 2018, 7179788),
    ("New York/New Jersey", "NJ", 2019, 7471131),
    ("New York/New Jersey", "NJ", 2020, 7585819),
    ("New York/New Jersey", "NJ", 2021, 8985929),
    ("New York/New Jersey", "NJ", 2022, 9493664),
    ("New York/New Jersey", "NJ", 2023, 7800852),
    ("New York/New Jersey", "NJ", 2024, 8330000),
    ("Savannah", "GA", 2017, 4046220),
    ("Savannah", "GA", 2018, 4351098),
    ("Savannah", "GA", 2019, 4599530),
    ("Savannah", "GA", 2020, 4682846),
    ("Savannah", "GA", 2021, 5614460),
    ("Savannah", "GA", 2022, 5892131),
    ("Savannah", "GA", 2023, 4925000),
    ("Savannah", "GA", 2024, 5340000),
    ("Houston", "TX", 2017, 2459107),
    ("Houston", "TX", 2018, 2700105),
    ("Houston", "TX", 2019, 2989636),
    ("Houston", "TX", 2020, 2989319),
    ("Houston", "TX", 2021, 3450000),
    ("Houston", "TX", 2022, 3974266),
    ("Houston", "TX", 2023, 3818000),
    ("Houston", "TX", 2024, 4145000),
    ("Virginia (Norfolk)", "VA", 2017, 2843391),
    ("Virginia (Norfolk)", "VA", 2018, 2856136),
    ("Virginia (Norfolk)", "VA", 2019, 2937375),
    ("Virginia (Norfolk)", "VA", 2020, 2811130),
    ("Virginia (Norfolk)", "VA", 2021, 3522260),
    ("Virginia (Norfolk)", "VA", 2022, 3702276),
    ("Virginia (Norfolk)", "VA", 2023, 3284000),
    ("Virginia (Norfolk)", "VA", 2024, 3470000),
    ("Seattle/Tacoma (NWSA)", "WA", 2017, 3661339),
    ("Seattle/Tacoma (NWSA)", "WA", 2018, 3798587),
    ("Seattle/Tacoma (NWSA)", "WA", 2019, 3775134),
    ("Seattle/Tacoma (NWSA)", "WA", 2020, 3318873),
    ("Seattle/Tacoma (NWSA)", "WA", 2021, 3736728),
    ("Seattle/Tacoma (NWSA)", "WA", 2022, 3382052),
    ("Seattle/Tacoma (NWSA)", "WA", 2023, 3022000),
    ("Seattle/Tacoma (NWSA)", "WA", 2024, 3334000),
    ("Oakland", "CA", 2017, 2421131),
    ("Oakland", "CA", 2018, 2546357),
    ("Oakland", "CA", 2019, 2499702),
    ("Oakland", "CA", 2020, 2461884),
    ("Oakland", "CA", 2021, 2447480),
    ("Oakland", "CA", 2022, 2337125),
    ("Oakland", "CA", 2023, 2057000),
    ("Oakland", "CA", 2024, 2188000),
    ("Charleston", "SC", 2017, 2177550),
    ("Charleston", "SC", 2018, 2316297),
    ("Charleston", "SC", 2019, 2440971),
    ("Charleston", "SC", 2020, 2317953),
    ("Charleston", "SC", 2021, 2785069),
    ("Charleston", "SC", 2022, 2853312),
    ("Charleston", "SC", 2023, 2440000),
    ("Charleston", "SC", 2024, 2615000),
    ("Mobile", "AL", 2017, 365100),
    ("Mobile", "AL", 2018, 396166),
    ("Mobile", "AL", 2019, 449741),
    ("Mobile", "AL", 2020, 488896),
    ("Mobile", "AL", 2021, 543361),
    ("Mobile", "AL", 2022, 580317),
    ("Mobile", "AL", 2023, 555000),
    ("Mobile", "AL", 2024, 615000),
    ("Jacksonville", "FL", 2017, 1287131),
    ("Jacksonville", "FL", 2018, 1303492),
    ("Jacksonville", "FL", 2019, 1338044),
    ("Jacksonville", "FL", 2020, 1300706),
    ("Jacksonville", "FL", 2021, 1525703),
    ("Jacksonville", "FL", 2022, 1490094),
    ("Jacksonville", "FL", 2023, 1320000),
    ("Jacksonville", "FL", 2024, 1390000),
    ("Miami", "FL", 2017, 1051291),
    ("Miami", "FL", 2018, 1083000),
    ("Miami", "FL", 2019, 1100000),
    ("Miami", "FL", 2020, 949279),
    ("Miami", "FL", 2021, 1240000),
    ("Miami", "FL", 2022, 1265000),
    ("Miami", "FL", 2023, 1110000),
    ("Miami", "FL", 2024, 1185000),
    ("Baltimore", "MD", 2017, 1023312),
    ("Baltimore", "MD", 2018, 1023850),
    ("Baltimore", "MD", 2019, 1095665),
    ("Baltimore", "MD", 2020, 921660),
    ("Baltimore", "MD", 2021, 1110000),
    ("Baltimore", "MD", 2022, 1118567),
    ("Baltimore", "MD", 2023, 1080000),
    ("Baltimore", "MD", 2024, 760000),  # Key Bridge collapse impact
    ("New Orleans", "LA", 2017, 565383),
    ("New Orleans", "LA", 2018, 591532),
    ("New Orleans", "LA", 2019, 555734),
    ("New Orleans", "LA", 2020, 471196),
    ("New Orleans", "LA", 2021, 487923),
    ("New Orleans", "LA", 2022, 506497),
    ("New Orleans", "LA", 2023, 460000),
    ("New Orleans", "LA", 2024, 490000),
    ("Philadelphia", "PA", 2017, 568575),
    ("Philadelphia", "PA", 2018, 612102),
    ("Philadelphia", "PA", 2019, 663660),
    ("Philadelphia", "PA", 2020, 685369),
    ("Philadelphia", "PA", 2021, 855886),
    ("Philadelphia", "PA", 2022, 822000),
    ("Philadelphia", "PA", 2023, 715000),
    ("Philadelphia", "PA", 2024, 790000),
    ("Port Everglades", "FL", 2017, 1075408),
    ("Port Everglades", "FL", 2018, 1057371),
    ("Port Everglades", "FL", 2019, 1019936),
    ("Port Everglades", "FL", 2020, 928726),
    ("Port Everglades", "FL", 2021, 1183091),
    ("Port Everglades", "FL", 2022, 1132099),
    ("Port Everglades", "FL", 2023, 985000),
    ("Port Everglades", "FL", 2024, 1080000),
]


def write_ports_csv() -> Path:
    out_dir = RAW / "ports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "us_port_teu.csv"
    lines = ["port,state,year,teu"]
    for port, state, year, teu in PORT_TEU_ROWS:
        lines.append(f'"{port}",{state},{year},{teu}')
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok  ] {out} ({len(PORT_TEU_ROWS)} rows)")
    return out


def main() -> None:
    download_faf5()
    download_census()
    write_ports_csv()
    download_fred()


if __name__ == "__main__":
    main()
