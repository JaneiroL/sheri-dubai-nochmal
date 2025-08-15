# fib_pairing.py
# -*- coding: utf-8 -*-

"""
61,8%-Entry-Pipeline je COT-Zeitfenster.
Benötigte Dateien im Arbeitsordner:
- cot_signals_output.csv
- fibreakout-fiborigin.csv (oder fibbreakout-fiborigin.csv)  ;  sep=';'
- SMA 1_10.xlsx, SMA 11_20.xlsx, SMA 21_28.xlsx (Sheets = 28 Paare, 4h)

Outputs:
- outputs/prep/overview_intervals.csv
- outputs/prep/<interval_id>/<interval_id>__TRADES.csv
- (optional) __SMA.csv, __FIB.csv
"""

from __future__ import annotations
import sys, re, unicodedata
from pathlib import Path
from typing import Optional, Tuple, List
import pandas as pd

# --------------------------- Konfig ---------------------------

BASE = Path(__file__).resolve().parent

COT_FILE = BASE / "cot_signals_output.csv"
FIB_FILES = [BASE / "fibreakout-fiborigin.csv", BASE / "fibbreakout-fiborigin.csv"]
SMA_FILES = [BASE / "SMA 1_10.xlsx", BASE / "SMA 11_20.xlsx", BASE / "SMA 21_28.xlsx"]

OUT_DIR = BASE / "outputs" / "prep"
DETAIL_SLICES = True
RETRACE = 0.618
MAX_ENTRY_DAYS = 20

# --------------------------- Helpers ---------------------------

def _norm_pair(s: str) -> str:
    return "".join(ch for ch in str(s).upper() if ch.isalpha())

def _norm_header(s: str) -> str:
    # Unicode-Normalisierung, NBSP -> Space, Kleinbuchstaben, nur a-z
    s = unicodedata.normalize("NFKC", str(s)).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return re.sub(r"[^a-z]", "", s)  # "Start Date" -> "startdate"

def _read_cot_intervals() -> pd.DataFrame:
    if not COT_FILE.exists():
        raise FileNotFoundError(f"{COT_FILE.name} fehlt.")

    # Auto-Delimiter (erkennt Tab, Komma, Semikolon, Pipe)
    df = pd.read_csv(COT_FILE, dtype=str, sep=None, engine="python")
    # Falls trotzdem alles in 1 Spalte gelandet ist: manuell splitten
    if df.shape[1] == 1:
        s = df.columns[0]
        parts = re.split(r"[,\t;|]", s)
        if len(parts) > 1:
            # Header aus der einen Spalte extrahieren und Daten wieder einlesen
            df = pd.read_csv(COT_FILE, dtype=str, sep=r"[,\t;|]", engine="python")

    # Header normalisieren und mappen
    norm_map = {c: _norm_header(c) for c in df.columns}
    inv = {v: k for k, v in norm_map.items()}

    def pick(*keys):
        for k in keys:
            if k in inv:
                return inv[k]
        return None

    c_start = pick("startdate", "start")
    c_end   = pick("enddate", "end")
    c_dir   = pick("direction", "dir")
    c_pair  = pick("pair", "symbol", "instrument")

    for need, col in [("Start Date", c_start), ("End Date", c_end), ("Direction", c_dir), ("Pair", c_pair)]:
        if col is None:
            raise ValueError(f"Spalte '{need}' nicht gefunden. Header erkannt: {list(df.columns)}")

    df = df.rename(columns={c_start:"start_date", c_end:"end_date", c_dir:"direction", c_pair:"pair"})
    df["start_date"] = pd.to_datetime(df["start_date"], utc=True, errors="coerce")
    df["end_date"]   = pd.to_datetime(df["end_date"],   utc=True, errors="coerce") + pd.Timedelta(hours=23, minutes=59, seconds=59)
    df["direction"]  = df["direction"].astype(str).str.strip().str.lower()
    df["pair"]       = df["pair"].astype(str).map(_norm_pair)

    df = df.dropna(subset=["start_date", "end_date"])
    df = df[df["end_date"] >= df["start_date"]].copy()
    df["interval_id"] = (
        df["pair"] + "_" + df["direction"] + "_" +
        df["start_date"].dt.strftime("%Y%m%d") + "_" +
        df["end_date"].dt.strftime("%Y%m%d")
    )
    return df.reset_index(drop=True)

def _read_fib_file() -> pd.DataFrame:
    fib_path = next((p for p in FIB_FILES if p.exists()), None)
    if fib_path is None:
        raise FileNotFoundError("Keine fibreakout-fiborigin.csv gefunden (beide Varianten geprüft).")

    df = pd.read_csv(fib_path, sep=";", dtype=str, engine="python")
    df.columns = [unicodedata.normalize("NFKC", c).strip() for c in df.columns]

    needed = {"signal_type","direction","start_time","end_time","extreme_time","extreme_price"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Fehlende Spalten in {fib_path.name}: {sorted(missing)}")

    for col in ("start_time","end_time","extreme_time"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    df["extreme_price"] = pd.to_numeric(df["extreme_price"], errors="coerce")
    df["direction"] = df["direction"].astype(str).str.strip().str.lower()
    df["signal_type"] = df["signal_type"].astype(str).str.strip()

    lsig = df["signal_type"].str.lower()
    df["is_origin"]   = lsig.str.contains("origin")
    df["is_breakout"] = lsig.str.contains("breakout")

    if "pair" in df.columns:
        df["pair"] = df["pair"].astype(str).map(_norm_pair)

    df = df.dropna(subset=["extreme_time","extreme_price"])
    return df.sort_values("extreme_time").reset_index(drop=True)

def _build_pair_to_file() -> dict[str, Path]:
    mapping = {}
    for f in SMA_FILES:
        if not f.exists():
            continue
        xls = pd.ExcelFile(f)
        for sheet in xls.sheet_names:
            mapping[_norm_pair(sheet)] = f
    if not mapping:
        raise FileNotFoundError("Keine SMA-Dateien oder Sheets gefunden.")
    return mapping

def _load_sma_sheet(file_path: Path, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, sheet_name=sheet)
    df.columns = [unicodedata.normalize("NFKC", c).strip() for c in df.columns]

    time_cols = [c for c in df.columns if c.lower() in ("time","datetime","date","timestamp")]
    if not time_cols:
        time_cols = [df.columns[0]]
    tcol = time_cols[0]

    df[tcol] = pd.to_datetime(df[tcol], utc=True, errors="coerce")
    df = df.dropna(subset=[tcol]).sort_values(tcol).reset_index(drop=True)
    df = df.rename(columns={tcol:"time"})

    def pick_col(names):
        for c in df.columns:
            if c.lower() in names:
                return c
        return None

    col_high = pick_col({"high","h","max"})
    col_low  = pick_col({"low","l","min"})
    col_close= pick_col({"close","c","last"})
    col_open = pick_col({"open","o"})

    if col_high is None: col_high = col_close or col_open
    if col_low  is None: col_low  = col_close or col_open
    if col_close is None: col_close = col_open or col_high or col_low

    for c in [col_high, col_low, col_close]:
        if c and df[c].dtype.kind not in "fi":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.rename(columns={col_high:"HIGH", col_low:"LOW", col_close:"CLOSE"})
    return df[["time","HIGH","LOW","CLOSE"]].copy()

def _slice_time(df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    return df.loc[(df["time"] >= start_ts) & (df["time"] <= end_ts)].copy()

# --------------------------- Kernlogik ---------------------------

def _first_breakout_after(breakouts: pd.DataFrame, t: pd.Timestamp) -> Optional[pd.Series]:
    after = breakouts[breakouts["extreme_time"] > t]
    return None if after.empty else after.iloc[0]

def _more_extreme_origin_between(origins: pd.DataFrame,
                                 t0: pd.Timestamp, t1: pd.Timestamp,
                                 want_high: bool) -> Optional[pd.Series]:
    between = origins[(origins["extreme_time"] > t0) & (origins["extreme_time"] < t1)]
    if between.empty:
        return None
    if want_high:  # SHORT: wir wollen das höchste Origin
        mx = between["extreme_price"].max()
        return between[between["extreme_price"] == mx].iloc[-1]
    else:         # LONG: wir wollen das tiefste Origin
        mn = between["extreme_price"].min()
        return between[between["extreme_price"] == mn].iloc[-1]

def _entry_time(price: pd.DataFrame,
                t_start: pd.Timestamp,
                t_end: pd.Timestamp,
                target: float,
                direction: str) -> Optional[Tuple[pd.Timestamp, float]]:
    window = price[(price["time"] > t_start) & (price["time"] <= t_end)]
    if window.empty:
        return None
    hit = window[window["HIGH"] >= target] if direction == "short" else window[window["LOW"] <= target]
    if hit.empty:
        return None
    t = pd.Timestamp(hit.iloc[0]["time"])
    return t, float(target)

def _find_trades_for_interval(
        fib_df: pd.DataFrame,
        price: pd.DataFrame,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        direction: str
    ) -> List[dict]:

    events = fib_df[(fib_df["extreme_time"] >= start_ts) & (fib_df["extreme_time"] <= end_ts)].copy()
    events = events.sort_values("extreme_time").reset_index(drop=True)

    origins = events[events["is_origin"]].copy()
    breakouts = events[events["is_breakout"]].copy()
    if origins.empty or breakouts.empty:
        return []

    want_high_origin = (direction == "short")
    def breakout_more_extreme(cur: float, nxt: float) -> bool:
        return (nxt < cur) if direction == "short" else (nxt > cur)

    results: List[dict] = []
    cursor = start_ts

    while True:
        o_cands = origins[origins["extreme_time"] >= cursor]
        if o_cands.empty:
            break
        O = o_cands.iloc[0]

        # Origin stabilisieren gegen "extremeres" Origin zwischen O und erstem Breakout
        while True:
            B1 = _first_breakout_after(breakouts, O["extreme_time"])
            if B1 is None:
                return results
            O2 = _more_extreme_origin_between(origins, O["extreme_time"], B1["extreme_time"], want_high_origin)
            if O2 is not None:
                O = O2
                continue
            break

        current_B = B1
        while True:
            O_price = float(O["extreme_price"])
            B_price = float(current_B["extreme_price"])
            target = (B_price + RETRACE * (O_price - B_price)) if direction == "short" else \
                     (B_price - RETRACE * (B_price - O_price))

            entry = _entry_time(price, current_B["extreme_time"], end_ts, target, direction)
            if entry is None:
                cursor = current_B["extreme_time"] + pd.Timedelta(seconds=1)
                break
            entry_time, entry_price = entry

            # Prüfen, ob vor dem Entry ein extremeres Breakout kam
            cand = breakouts[(breakouts["extreme_time"] > current_B["extreme_time"]) &
                             (breakouts["extreme_time"] < entry_time)]
            if not cand.empty:
                cand = cand[ breakout_more_extreme(current_B["extreme_price"], cand["extreme_price"]) ]
            if not cand.empty:
                current_B = cand.iloc[-1]
                continue

            # 20-Tage-Regel
            if (entry_time - pd.Timestamp(O["extreme_time"])) <= pd.Timedelta(days=MAX_ENTRY_DAYS):
                results.append({
                    "direction": direction,
                    "origin_time": pd.Timestamp(O["extreme_time"]),
                    "origin_price": float(O["extreme_price"]),
                    "breakout_time": pd.Timestamp(current_B["extreme_time"]),
                    "breakout_price": float(current_B["extreme_price"]),
                    "entry_time": pd.Timestamp(entry_time),
                    "entry_price": float(entry_price),
                })

            cursor = entry_time + pd.Timedelta(seconds=1)
            break

    return results

# --------------------------- Main ---------------------------

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cot = _read_cot_intervals()
    fib = _read_fib_file()
    pair_map = _build_pair_to_file()

    overview_rows = []

    for _, row in cot.iterrows():
        pair = _norm_pair(row["pair"])
        direction = row["direction"]
        start_ts = row["start_date"]
        end_ts = row["end_date"]
        iid = row["interval_id"]

        sma_file = pair_map.get(pair)
        if sma_file is None:
            print(f"[WARN] Kein Sheet für {pair} gefunden – überspringe {iid}.")
            continue

        sma_df = _load_sma_sheet(sma_file, pair)
        sma_slice = _slice_time(sma_df, start_ts, end_ts)
        sma_slice.insert(1, "pair", pair)
        sma_slice.insert(2, "direction", direction)
        sma_slice.insert(3, "interval_id", iid)

        fib_slice = fib[fib["direction"] == direction].copy()
        if "pair" in fib_slice.columns:
            fib_slice = fib_slice[fib_slice["pair"] == pair]

        if DETAIL_SLICES:
            (OUT_DIR / iid).mkdir(parents=True, exist_ok=True)
            sma_slice.to_csv(OUT_DIR / iid / f"{iid}__SMA.csv", index=False)
            fib_slice.to_csv(OUT_DIR / iid / f"{iid}__FIB.csv", index=False)

        trades = _find_trades_for_interval(fib_df=fib_slice, price=sma_slice,
                                           start_ts=start_ts, end_ts=end_ts,
                                           direction=direction)

        trades_df = pd.DataFrame(trades)
        trades_path = OUT_DIR / iid / f"{iid}__TRADES.csv"
        if not trades_df.empty:
            trades_df.insert(0, "interval_id", iid)
            trades_df.insert(1, "pair", pair)
            trades_df.to_csv(trades_path, index=False)
        else:
            pd.DataFrame(columns=[
                "interval_id","pair","direction",
                "origin_time","origin_price",
                "breakout_time","breakout_price",
                "entry_time","entry_price"
            ]).to_csv(trades_path, index=False)

        overview_rows.append({
            "interval_id": iid,
            "pair": pair,
            "direction": direction,
            "start": start_ts,
            "end": end_ts,
            "sma_file": sma_file.name,
            "sma_rows": int(len(sma_slice)),
            "fib_events": int(len(fib_slice)),
            "trades_found": int(len(trades))
        })

    overview = pd.DataFrame(overview_rows)
    overview.to_csv(OUT_DIR / "overview_intervals.csv", index=False)
    print(f"Fertig. Übersicht: {OUT_DIR / 'overview_intervals.csv'}")
    print(f"Details unter: {OUT_DIR}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
