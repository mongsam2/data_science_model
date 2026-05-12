"""Build the modelling dataset by joining FAF5 truck flows with destination
demand indicators (pull factors) and origin port throughput (push factors).

Pipeline (matches the project spec)
-----------------------------------
Step 1  FAF5 base: keep dms_mode == 1 (Truck); aggregate over commodity,
        trade type and distance band to get one row per
        (origin_state, destination_state, year).
Step 2  Pull factors: join FRED state GDP, FRED state unemployment (annualized),
        FRED regional CPI (annualized, mapped to state via Census region) and
        Census state population on destination_state + year.
Step 3  Push factors: join curated annual US container-port TEU on
        origin_state + year (states without container ports get 0).
Step 4  Clean and validate. Write parquet + a small CSV preview.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .reference import (
    ABBR_TO_NAME,
    ABBR_TO_REGION,
    FIPS_TO_ABBR,
    FRED_REGIONAL_CPI,
    NAME_TO_ABBR,
    STATES,
)

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
INTERIM = ROOT / "data" / "interim"
PROCESSED = ROOT / "data" / "processed"

HISTORICAL_YEARS = list(range(2017, 2025))  # 2017..2024 (FAF5.7.1 historical range)
TRUCK_MODE = 1


# --------------------------------------------------------------------------- #
# Step 1: FAF5 base
# --------------------------------------------------------------------------- #
def load_faf5_truck_long() -> pd.DataFrame:
    """Read FAF5, filter trucks, aggregate to (origin, destination, year)."""
    src = RAW / "faf5" / "FAF5.7.1_State.csv"
    tons_cols = [f"tons_{y}" for y in HISTORICAL_YEARS]
    value_cols = [f"value_{y}" for y in HISTORICAL_YEARS]
    tmiles_cols = [f"tmiles_{y}" for y in HISTORICAL_YEARS]
    usecols = ["dms_origst", "dms_destst", "dms_mode", *tons_cols, *value_cols, *tmiles_cols]

    print(f"[faf5] reading {src.name} ...")
    df = pd.read_csv(src, usecols=usecols)
    print(f"[faf5] rows raw: {len(df):,}")

    df = df[df["dms_mode"] == TRUCK_MODE].drop(columns=["dms_mode"])
    print(f"[faf5] rows truck-only: {len(df):,}")

    # Aggregate over commodity / trade type / distance band
    grouped = (
        df.groupby(["dms_origst", "dms_destst"], as_index=False)[
            tons_cols + value_cols + tmiles_cols
        ].sum()
    )
    print(f"[faf5] OD pairs: {len(grouped):,}")

    # Wide -> long: one row per (origin, destination, year)
    long_frames = []
    for y in HISTORICAL_YEARS:
        sub = grouped[["dms_origst", "dms_destst", f"tons_{y}", f"value_{y}", f"tmiles_{y}"]].copy()
        sub.columns = ["origin_fips", "destination_fips", "tons", "value_musd", "ton_miles"]
        sub["year"] = y
        long_frames.append(sub)
    out = pd.concat(long_frames, ignore_index=True)

    # FIPS -> state abbreviation; FAF5 also has codes outside the 50 states + DC
    # (e.g. remainder-of-state foreign zones). Restrict to in-scope US states.
    valid = set(FIPS_TO_ABBR.keys())
    out = out[out["origin_fips"].isin(valid) & out["destination_fips"].isin(valid)].copy()
    out["origin_state"] = out["origin_fips"].map(FIPS_TO_ABBR)
    out["destination_state"] = out["destination_fips"].map(FIPS_TO_ABBR)

    out = out[
        [
            "year",
            "origin_state",
            "destination_state",
            "origin_fips",
            "destination_fips",
            "tons",
            "value_musd",
            "ton_miles",
        ]
    ]
    print(f"[faf5] long rows (in-scope, historical): {len(out):,}")
    return out


# --------------------------------------------------------------------------- #
# Step 2: Pull factors  (destination-side demand indicators)
# --------------------------------------------------------------------------- #
def _read_fred_annual(csv_path: Path, value_col: str) -> pd.DataFrame:
    """Read a FRED annual series and return year + value."""
    df = pd.read_csv(csv_path)
    df["year"] = pd.to_datetime(df["observation_date"]).dt.year
    df = df.rename(columns={value_col: "value"})
    return df[["year", "value"]]


def _read_fred_monthly_avg(csv_path: Path, value_col: str) -> pd.DataFrame:
    """Read a FRED monthly series and annualize via mean."""
    df = pd.read_csv(csv_path)
    df["year"] = pd.to_datetime(df["observation_date"]).dt.year
    df = df.rename(columns={value_col: "value"})
    return df.groupby("year", as_index=False)["value"].mean()


def build_state_gdp() -> pd.DataFrame:
    rows = []
    for abbr, *_ in STATES:
        path = RAW / "fred" / f"gdp_{abbr}.csv"
        if not path.exists():
            continue
        df = _read_fred_annual(path, f"{abbr}NGSP")
        df["state"] = abbr
        df = df.rename(columns={"value": "gdp_musd"})
        rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    return out[["state", "year", "gdp_musd"]]


def build_state_unemployment() -> pd.DataFrame:
    rows = []
    for abbr, *_ in STATES:
        path = RAW / "fred" / f"unemp_{abbr}.csv"
        if not path.exists():
            continue
        df = _read_fred_monthly_avg(path, f"{abbr}UR")
        df["state"] = abbr
        df = df.rename(columns={"value": "unemployment_rate"})
        rows.append(df)
    return pd.concat(rows, ignore_index=True)[["state", "year", "unemployment_rate"]]


def build_state_cpi() -> pd.DataFrame:
    """Regional CPI annualized, then broadcast to every state in that region."""
    regional = {}
    for region, sid in FRED_REGIONAL_CPI.items():
        path = RAW / "fred" / f"cpi_{region}.csv"
        regional[region] = _read_fred_monthly_avg(path, sid).rename(columns={"value": "cpi"})

    rows = []
    for abbr, *_ in STATES:
        region = ABBR_TO_REGION[abbr]
        df = regional[region].copy()
        df["state"] = abbr
        rows.append(df)
    return pd.concat(rows, ignore_index=True)[["state", "year", "cpi"]]


def _melt_pop(df: pd.DataFrame, years: range) -> pd.DataFrame:
    df = df[df["SUMLEV"] == 40].copy()
    df["state"] = df["NAME"].map(NAME_TO_ABBR)
    df = df.dropna(subset=["state"])
    pop_cols = {f"POPESTIMATE{y}": y for y in years}
    long = df.melt(
        id_vars=["state"],
        value_vars=list(pop_cols.keys()),
        var_name="pop_col",
        value_name="population",
    )
    long["year"] = long["pop_col"].map(pop_cols)
    return long[["state", "year", "population"]]


def build_state_population() -> pd.DataFrame:
    """Census state population. Combines the 2010-2020 vintage (for years
    2017-2019) with the 2020-2024 vintage (for 2020-2024). When both vintages
    cover a year (2020) we prefer the newer release."""
    legacy_src = RAW / "census" / "nst-est2020-alldata.csv"
    current_src = RAW / "census" / "NST-EST2024-ALLDATA.csv"

    legacy = _melt_pop(pd.read_csv(legacy_src, encoding="latin-1"), range(2017, 2020))
    current = _melt_pop(pd.read_csv(current_src), range(2020, 2025))

    combined = pd.concat([legacy, current], ignore_index=True)
    combined = combined.drop_duplicates(subset=["state", "year"], keep="last")
    return combined.sort_values(["state", "year"]).reset_index(drop=True)


def build_pull_factors() -> pd.DataFrame:
    """One row per (state, year) with all destination-side features."""
    gdp = build_state_gdp()
    ur = build_state_unemployment()
    cpi = build_state_cpi()
    pop = build_state_population()
    print(
        f"[pull] gdp={len(gdp):,} ur={len(ur):,} cpi={len(cpi):,} pop={len(pop):,}"
    )

    out = gdp.merge(ur, on=["state", "year"], how="outer")
    out = out.merge(cpi, on=["state", "year"], how="outer")
    out = out.merge(pop, on=["state", "year"], how="outer")
    out = out[out["year"].isin(HISTORICAL_YEARS)]
    return out


# --------------------------------------------------------------------------- #
# Step 3: Push factors (origin-side port throughput)
# --------------------------------------------------------------------------- #
def build_port_features() -> pd.DataFrame:
    src = RAW / "ports" / "us_port_teu.csv"
    df = pd.read_csv(src)
    agg = (
        df.groupby(["state", "year"], as_index=False)
        .agg(port_teu=("teu", "sum"), port_count=("port", "nunique"))
    )
    return agg


# --------------------------------------------------------------------------- #
# Step 4: Assemble and validate
# --------------------------------------------------------------------------- #
def build_dataset() -> pd.DataFrame:
    faf5 = load_faf5_truck_long()
    pull = build_pull_factors()
    push = build_port_features()

    # Pull factors -> destination
    dest = pull.rename(
        columns={
            "state": "destination_state",
            "gdp_musd": "dest_gdp_musd",
            "unemployment_rate": "dest_unemployment_rate",
            "cpi": "dest_cpi",
            "population": "dest_population",
        }
    )
    df = faf5.merge(dest, on=["destination_state", "year"], how="left")

    # Push factors -> origin
    orig = push.rename(
        columns={
            "state": "origin_state",
            "port_teu": "origin_port_teu",
            "port_count": "origin_port_count",
        }
    )
    df = df.merge(orig, on=["origin_state", "year"], how="left")

    # States without container ports have no row in the port table -> 0
    df["origin_port_teu"] = df["origin_port_teu"].fillna(0).astype("int64")
    df["origin_port_count"] = df["origin_port_count"].fillna(0).astype("int64")

    # Friendly metadata columns
    df["origin_state_name"] = df["origin_state"].map(ABBR_TO_NAME)
    df["destination_state_name"] = df["destination_state"].map(ABBR_TO_NAME)
    df["origin_has_port"] = (df["origin_port_teu"] > 0).astype("int8")
    df["is_intrastate"] = (df["origin_state"] == df["destination_state"]).astype("int8")

    # Final column order
    cols = [
        "year",
        "origin_state",
        "origin_state_name",
        "destination_state",
        "destination_state_name",
        # target & freight measures
        "tons",
        "value_musd",
        "ton_miles",
        # destination pull factors
        "dest_gdp_musd",
        "dest_unemployment_rate",
        "dest_cpi",
        "dest_population",
        # origin push factors
        "origin_port_teu",
        "origin_port_count",
        "origin_has_port",
        # helpers
        "is_intrastate",
        "origin_fips",
        "destination_fips",
    ]
    df = df[cols].sort_values(["year", "origin_state", "destination_state"]).reset_index(drop=True)
    return df


def validate(df: pd.DataFrame) -> None:
    print("\n=== validation ===")
    print(f"rows: {len(df):,}  cols: {df.shape[1]}")
    print(f"years: {sorted(df['year'].unique())}")
    print(f"origin states: {df['origin_state'].nunique()}")
    print(f"destination states: {df['destination_state'].nunique()}")
    print(f"OD pairs per year (median): "
          f"{df.groupby('year').size().median():.0f}")
    print(f"target (tons): mean={df['tons'].mean():.2f}  "
          f"min={df['tons'].min():.4f}  max={df['tons'].max():.2f}")
    print("\nmissing-value rate per column:")
    miss = df.isna().mean().sort_values(ascending=False)
    for c, v in miss.items():
        if v > 0:
            print(f"  {c:<28s} {v*100:>6.2f}%")
    if miss.max() == 0:
        print("  (none)")


def main() -> None:
    INTERIM.mkdir(parents=True, exist_ok=True)
    PROCESSED.mkdir(parents=True, exist_ok=True)

    df = build_dataset()
    validate(df)

    out_parquet = PROCESSED / "faf5_hub_dataset.parquet"
    out_csv_preview = PROCESSED / "faf5_hub_dataset_preview.csv"
    df.to_parquet(out_parquet, index=False)
    df.head(2000).to_csv(out_csv_preview, index=False)
    print(f"\n[ok] wrote {out_parquet} ({out_parquet.stat().st_size/1e6:.1f} MB)")
    print(f"[ok] wrote {out_csv_preview} (2000-row preview)")


if __name__ == "__main__":
    main()
