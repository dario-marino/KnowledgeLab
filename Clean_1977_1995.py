import re
import os
import glob
import pandas as pd
from pathlib import Path

# ─── CONFIG ───────────────────────────────────────────────────────────────────

# folder where your CSVs live
DATA_DIR = Path(r"C:\Users\dario\Downloads\csv")

# which text labels to pull, and how to name them in the final table
REGION_MAP = {
    'DISTRIBUTION':       'Distribution',
    '*TOTAL USA':         'USA_Canada',
    '*USA/CANADA':        'USA_Canada',
    'WEST EUROPE':        'West_Europe',
    'JAPAN':              'Japan',
    'UNITS':              'Units'
}

# month name → month number (handles both full and common abbrns like "SEPT")
MONTH_MAP = {
    'JANUARY':1,   'JAN':1,
    'FEBRUARY':2,  'FEB':2,
    'MARCH':3,     'MAR':3,
    'APRIL':4,     'APR':4,
    'MAY':5,
    'JUNE':6,      'JUN':6,
    'JULY':7,      'JUL':7,
    'AUGUST':8,    'AUG':8,
    'SEPTEMBER':9, 'SEP':9, 'SEPT':9,
    'OCTOBER':10,  'OCT':10,
    'NOVEMBER':11, 'NOV':11,
    'DECEMBER':12, 'DEC':12
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def is_product_header(fields):
    """
    A product header is a line where:
      - the first field is nonempty & contains no digits
      - the second field is empty
    """
    first = fields[0].strip()
    return bool(first) and len(fields) > 1 and not fields[1].strip() and not re.search(r'\d', first)

def extract_months(fields):
    """
    Given a CSV row split into fields, return a list of month numbers
    if this row contains >=12 month names (allowing abbrns like 'SEPT').
    """
    cleaned = []
    for f in fields:
        key = re.sub(r'[^A-Za-z]', '', f).upper()
        if key in MONTH_MAP:
            cleaned.append(MONTH_MAP[key])
    return cleaned if len(cleaned) >= 12 else None

# ─── MAIN LOOP ─────────────────────────────────────────────────────────────────

all_records = {}  # key = (product, year, month) → dict of our five series

for path in sorted(DATA_DIR.glob("Bluebook-*.csv")):
    year = int(re.search(r'Bluebook-(\d{4})\.csv', path.name).group(1))

    with open(path, encoding='utf-8', errors='ignore') as f:
        lines = [l.rstrip('\n') for l in f]

    current_product = None
    months = None

    for line in lines:
        fields = [c.strip() for c in line.split(',')]

        # 1) New product or sub-product classification?
        if is_product_header(fields):
            current_product = fields[0].strip()
            months = None
            continue

        # 2) If we know the product but haven't yet found the months row, try to
        #    extract the 12 month positions here:
        if current_product and months is None:
            m = extract_months(fields)
            if m:
                months = m[:12]
            continue

        # 3) Once we have both product and months, look for our five series
        if current_product and months:
            label = fields[0].strip().upper()
            if label in REGION_MAP:
                col = REGION_MAP[label]
                for idx, mon in enumerate(months):
                    raw = fields[idx+1] if idx+1 < len(fields) else ''
                    raw = raw.replace(',','').strip()
                    try:
                        val = float(raw) if raw else None
                    except ValueError:
                        val = None

                    key = (current_product, year, mon)
                    if key not in all_records:
                        all_records[key] = {
                            'Product': current_product,
                            'Year':    year,
                            'Month':   mon,
                            'Distribution': None,
                            'USA_Canada':   None,
                            'West_Europe':  None,
                            'Japan':        None,
                            'Units':        None
                        }
                    all_records[key][col] = val

# ─── BUILD FINAL DATAFRAME ─────────────────────────────────────────────────────

df = pd.DataFrame(all_records.values())

# create a true Date column (first day of each month)
df['Date'] = pd.to_datetime(dict(year=df.Year, month=df.Month, day=1))

# reorder to: Product, Date, then our five series
df = df[['Product','Date','Distribution','USA_Canada','West_Europe','Japan','Units']]

# (optional) save out a master CSV
df.to_csv(DATA_DIR / "all_bluebooks_combined.csv", index=False)

print("Finished — combined data in all_bluebooks_combined.csv")
