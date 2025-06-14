# Cleaning Bluebook
## We start with this code to get from 2000 to 2024, then we move to the next format

```
import os
import re
import glob
import pandas as pd

# ── CONFIG ─────────────────────────────────────────────────────────────────────
INPUT_DIR    = r"C:\Users\dario\Downloads\Shipment Data"
OUTPUT_CSV   = os.path.join(INPUT_DIR, "Shipment_Data_2000_2024.csv")
FILE_PATTERN = os.path.join(INPUT_DIR, "Bluebook-*.xls")

# ── DEPENDENCY CHECK ────────────────────────────────────────────────────────────
try:
    import xlrd
except ImportError:
    raise ImportError(
        "Missing `xlrd`. Install it with:\n"
        "    pip install xlrd\n"
        "  or\n"
        "    conda install -c anaconda xlrd"
    )

# ── HELPERS ─────────────────────────────────────────────────────────────────────
MONTH_ABBR = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"}

def is_month_label(x):
    if isinstance(x, str):
        s = x.strip()[:3].capitalize()
        return s in MONTH_ABBR
    try:
        i = int(x)
        return 1 <= i <= 12
    except:
        return False

# ── PROCESS ALL FILES ───────────────────────────────────────────────────────────
all_records = []

for path in glob.glob(FILE_PATTERN):
    year = int(re.search(r"(\d{4})", os.path.basename(path)).group(1))
    xls = pd.ExcelFile(path, engine="xlrd")
    
    # 1) Find the sheet & row‐index with the real 12‐month header
    best = {"score": 0}
    for sheet in xls.sheet_names:
        df0 = xls.parse(sheet, header=None)
        # look at first 30 rows
        for ridx in range(min(30, len(df0))):
            row = df0.iloc[ridx, 1:]  # skip col0
            cnt = sum(is_month_label(c) for c in row)
            if cnt > best["score"]:
                best = {
                    "sheet": sheet,
                    "header_idx": ridx,
                    "score": cnt,
                    "raw": df0
                }
    if best["score"] < 6:
        raise ValueError(f"Couldn't find a 12-month header in {path}")
    
    # 2) Slice down to header + data
    df = best["raw"].iloc[ best["header_idx"] : , : ].reset_index(drop=True)
    raw_labels = df.iloc[0, 1:].tolist()
    raw_cols   = df.columns[1:]
    
    # 3) Keep ONLY the 12 month columns
    month_pos = [i for i,lab in enumerate(raw_labels) if is_month_label(lab)]
    month_labels = [raw_labels[i] for i in month_pos]
    month_cols   = [raw_cols[i]    for i in month_pos]
    
    # 4) Walk the product/variable blocks
    current_code = None
    product_records = None
    
    for _, row in df.iloc[1:].iterrows():
        first = row.iloc[0]
        if isinstance(first, str) and re.match(r"^[A-Za-z]\d+", first.strip()):
            # flush previous
            if product_records:
                for col, rec in product_records.items():
                    rec["Product"] = current_code
                    label = month_labels[ month_cols.index(col) ]
                    # parse date
                    if isinstance(label, str) and label.strip()[:3].capitalize() in MONTH_ABBR:
                        dt = pd.to_datetime(f"{label.strip()} {year}", format="%b %Y")
                    else:
                        dt = pd.to_datetime(f"{year}-{int(label):02d}-01")
                    rec["Date"] = dt.strftime("%Y-%m-%d")
                    all_records.append(rec)
            # start new block
            current_code    = first.strip()
            product_records = {c: {} for c in month_cols}
        else:
            if product_records is None:
                continue
            var = str(first).strip()
            for c in month_cols:
                product_records[c][var] = row[c]
    
    # flush last block
    if product_records:
        for col, rec in product_records.items():
            rec["Product"] = current_code
            label = month_labels[ month_cols.index(col) ]
            if isinstance(label, str) and label.strip()[:3].capitalize() in MONTH_ABBR:
                dt = pd.to_datetime(f"{label.strip()} {year}", format="%b %Y")
            else:
                dt = pd.to_datetime(f"{year}-{int(label):02d}-01")
            rec["Date"] = dt.strftime("%Y-%m-%d")
            all_records.append(rec)

# ── BUILD & WRITE OUTPUT ────────────────────────────────────────────────────────
df_out = pd.DataFrame(all_records)
cols   = ["Product", "Date"] + [c for c in df_out.columns if c not in ("Product","Date")]
df_out = df_out[cols]
df_out.to_csv(OUTPUT_CSV, index=False)

print(f"✅  Written {len(df_out)} rows to {OUTPUT_CSV}")

```

### We did catch something that we shouldn't have let's cancel manually:
Row 2 to 13, column C and column G to column BD, and all the columns remaining to the left of WW ASP, except Americas Units, Europe Units, Japan Units, Asia Pacific Units.

Or you can use this code:

```
import pandas as pd
import string

# Load the CSV file
file_path = r"C:\Users\dario\Downloads\Shipment Data\Shipment_Data_2000_2024.csv"
df = pd.read_csv(file_path)

print(f"Original data shape: {df.shape}")
print(f"Original columns: {list(df.columns)}")

# Convert column positions to indices
def col_letter_to_index(letter):
    """Convert Excel-style column letter to 0-based index"""
    result = 0
    for char in letter:
        result = result * 26 + (ord(char.upper()) - ord('A')) + 1
    return result - 1

# Step 1: Remove rows 2-13 (0-based indexing: rows 1-12)
print("\nStep 1: Removing rows 2-13 (1997 data)")
df_cleaned = df.drop(df.index[1:13]).reset_index(drop=True)
print(f"After removing rows: {df_cleaned.shape}")

# Step 2: Identify columns to remove
columns_to_remove = []

# Get column indices for C, G through BD
c_index = col_letter_to_index('C')  # Column C
g_index = col_letter_to_index('G')  # Column G  
bd_index = col_letter_to_index('BD')  # Column BD
ww_index = col_letter_to_index('WW')  # Column WW

print(f"\nColumn indices - C: {c_index}, G: {g_index}, BD: {bd_index}, WW: {ww_index}")

# Add column C to removal list if it exists
if c_index < len(df_cleaned.columns):
    columns_to_remove.append(df_cleaned.columns[c_index])

# Add columns G through BD to removal list
for i in range(g_index, min(bd_index + 1, len(df_cleaned.columns))):
    if i < len(df_cleaned.columns):
        columns_to_remove.append(df_cleaned.columns[i])

# Add all columns to the left of WW (except Units columns)
for i in range(min(ww_index, len(df_cleaned.columns))):
    col_name = df_cleaned.columns[i]
    # Skip if it's already in removal list or contains "Units"
    if col_name not in columns_to_remove and "Units" not in col_name:
        # But add it to removal list if it's not a Units column
        columns_to_remove.append(col_name)

# Remove duplicate column names
columns_to_remove = list(set(columns_to_remove))

print(f"\nColumns to remove ({len(columns_to_remove)}):")
for col in sorted(columns_to_remove):
    print(f"  - {col}")

# Find Units columns that will be preserved
units_columns = [col for col in df_cleaned.columns if "Units" in col and col not in columns_to_remove]
print(f"\nUnits columns to preserve ({len(units_columns)}):")
for col in units_columns:
    print(f"  - {col}")

# Step 3: Remove the identified columns
df_final = df_cleaned.drop(columns=[col for col in columns_to_remove if col in df_cleaned.columns])

print(f"\nFinal data shape: {df_final.shape}")
print(f"Remaining columns ({len(df_final.columns)}):")
for i, col in enumerate(df_final.columns):
    print(f"  {i+1}. {col}")

# Save the cleaned data
output_path = r"C:\Users\dario\Downloads\Shipment Data\Shipment_Data_2000_2024_cleaned.csv"
df_final.to_csv(output_path, index=False)

print(f"\nCleaned data saved to: {output_path}")
print(f"Removed {len(columns_to_remove)} columns and 12 rows (1997 data)")
```

## From 1977 to 1995, there are some values missing now like average price, still we keep some information like quantities in different regions
### First we convert all the files from 1977 to 1995 to csv, then we use this code

```
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

```

## Now we are going to convert 1976, 1996, 1997, 1998, 1999 in csv and then apply the following code:


```
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

```
