import re
import glob
import csv
import pandas as pd
from pathlib import Path

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DATA_DIR = Path(r"C:\Users\dario\Downloads\csv")  # folder with ALL your Bluebook-*.csv

# map label substrings → our five output columns
LABEL_MAP = {
    'DISTRIBUTION':    'Distribution',
    'DISTRIBUTOR':     'Distribution',
    'TOTAL USA':       'USA_Canada',
    'USA/CANADA':      'USA_Canada',
    'AMERICAS':        'USA_Canada',
    'WESTERN EUROPE':  'West_Europe',
    'WEST EUROPE':     'West_Europe',
    'EUROPE':          'West_Europe',
    'JAPAN':           'Japan',
    'UNITS':           'Units'
}

# month name (full or common abbr) → month number
MONTH_MAP = {
    **{m.upper(): i+1 for i,m in enumerate([
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ])},
    **{"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
       "JUL":7,"AUG":8,"SEP":9,"SEPT":9,"OCT":10,"NOV":11,"DEC":12}
}

def classify_series(raw_label):
    L = re.sub(r'[^A-Za-z/ ]','', raw_label).upper()
    for key, outcol in LABEL_MAP.items():
        if key in L:
            return outcol
    return None

def extract_months(row):
    """Return list of ints if row contains ≥12 month names/abbr."""
    months = []
    for cell in row:
        key = re.sub(r'[^A-Za-z]','', cell).upper()
        if key in MONTH_MAP:
            months.append(MONTH_MAP[key])
    return months[:12] if len(months) >= 12 else None

# ─── PARSE & COLLECT ───────────────────────────────────────────────────────────
records = {}  # (product,year,month) → dict of all five series

for csv_path in sorted(DATA_DIR.glob("Bluebook-*.csv")):
    year = int(re.search(r"Bluebook-(\d{4})\.csv", csv_path.name).group(1))
    current_months = None
    current_product = None

    with open(csv_path, newline='', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        for row in reader:
            # 1) detect the month‐header row
            m = extract_months(row)
            if m:
                current_months = m
                continue

            # 2) detect a product header
            if ( row
                 and row[0].strip()
                 and len(row) > 1
                 and not row[1].strip()
                 and not re.search(r'\d', row[0])
                 and not any(tok in row[0].upper() for tok in ("STATISTICS","REPORT","PAGE")) ):
                current_product = row[0].strip()
                continue

            # 3) if we have a product + months, look for our five series
            if current_product and current_months:
                series = classify_series(row[0])
                if series:
                    # ─── NEW: collapse out any empty‐string columns ────────────
                    data_cells = [cell for cell in row[1:] if cell.strip() != '']
                    # now data_cells[0] → January, data_cells[1] → February, ... etc.
                    for idx, mon in enumerate(current_months):
                        raw = data_cells[idx] if idx < len(data_cells) else ''
                        raw = raw.replace(',','').replace('$','').strip()
                        try:
                            val = float(raw) if raw not in ('','-') else None
                        except ValueError:
                            val = None

                        key = (current_product, year, mon)
                        if key not in records:
                            records[key] = {
                                "Product":      current_product,
                                "Year":         year,
                                "Month":        mon,
                                "Distribution": None,
                                "USA_Canada":   None,
                                "West_Europe":  None,
                                "Japan":        None,
                                "Units":        None
                            }
                        records[key][series] = val

# ─── BUILD DATAFRAME & WRITE ──────────────────────────────────────────────────
df = pd.DataFrame(records.values())
df["Date"] = pd.to_datetime({
    "year":  df["Year"],
    "month": df["Month"],
    "day":   1
})
df = df[[
    "Product","Date",
    "Distribution","USA_Canada","West_Europe","Japan","Units"
]]
out = DATA_DIR / "all_bluebooks_1976_1999_combined.csv"
df.to_csv(out, index=False)
print(f"✅ Done — combined data in {out}")
