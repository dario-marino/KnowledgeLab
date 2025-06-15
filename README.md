# Cleaning Bluebook
## We start with this code to get from 2000 to 2024, then we move to the next format
See the code [here](https://github.com/dario-marino/KnowledgeLab/blob/main/Clean_2000_2024.py)
First we convert all the files to csv. Then we can use the specific structure of the tables which is easier to extract. There are also more variable here, for example in the other csv there are no Average Selling Prices.

```
import os
import re
import glob
import pandas as pd

# Only these variables will be extracted
DESIRED_VARS = {
    'Americas', 'Europe', 'Japan', 'Asia Pacific',
    'WW Dollars', 'WW Units', 'WW ASP'
}

def _make_date(year, mon):
    """Convert a year and month/quarter string to a Timestamp for the first day."""
    q_map = {'Q1': '01', 'Q2': '04', 'Q3': '07', 'Q4': '10'}
    mon = mon.strip()
    if mon in q_map:
        month_num = int(q_map[mon])
    else:
        # Parse month abbreviations (Jan, Feb, …)
        month_num = pd.to_datetime(mon[:3], format='%b').month
    return pd.Timestamp(year=int(year), month=month_num, day=1)

def clean_and_restructure(data_dir):
    """
    Reads all Bluebook-*.csv files in `data_dir`, extracts product-level data by month/quarter,
    and returns a consolidated DataFrame with one row per product-date, variables as columns.
    """
    file_pattern = os.path.join(data_dir, "Bluebook-*.csv")
    files = sorted(glob.glob(file_pattern))
    
    if not files:
        raise FileNotFoundError(f"No files found in {data_dir} matching Bluebook-*.csv")
    
    all_records = []
    # Regex for valid period headers (months or quarters)
    period_regex = re.compile(r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Q[1-4])', re.IGNORECASE)
    
    for filepath in files:
        year_match = re.search(r'(\d{4})', os.path.basename(filepath))
        if not year_match:
            continue
        year = int(year_match.group(1))
        
        df = pd.read_csv(filepath)
        label_col = df.columns[0]
        # Filter to only month/quarter columns
        periods = [col for col in df.columns 
                   if col != label_col and period_regex.match(col)]
        
        current_product = None
        for _, row in df.iterrows():
            label = str(row[label_col]).strip()
            # Start of a new product
            if re.match(r'^[A-Za-z]\d+', label):
                current_product = label
                continue
            
            # Skip until we have a product, skip empty or classification headers
            if (
                not current_product 
                or not label 
                or 'world-wide detail by subproduct classification' in label.lower()
            ):
                continue
            
            # Only keep the seven requested variables
            if label not in DESIRED_VARS:
                continue
            
            # Record each period's value
            for p in periods:
                all_records.append({
                    'product': current_product,
                    'year': year,
                    'period': p,
                    'variable': label,
                    'value': row[p]
                })
    
    df_long = pd.DataFrame(all_records)
    
    # Pivot so that each variable becomes its own column
    df_wide = (
        df_long
        .pivot_table(
            index=['product', 'year', 'period'],
            columns='variable',
            values='value',
            aggfunc='first'
        )
        .reset_index()
    )
    
    # Build a proper date (first of each month/quarter)
    df_wide['date'] = df_wide.apply(
        lambda r: _make_date(r['year'], r['period']), axis=1
    )
    
    # Final column order
    final_cols = ['product', 'date'] + sorted(DESIRED_VARS)
    df_final = df_wide[final_cols].sort_values(['product', 'date'])
    
    return df_final

if __name__ == "__main__":
    # Update this to your actual folder; fallback for testing in /mnt/data
    data_directory = r"C:\Users\dario\Downloads\Shipment Data"
    if not os.path.isdir(data_directory):
        data_directory = "/mnt/data"

    cleaned_df = clean_and_restructure(data_directory)
    output_path = os.path.join(data_directory, "shipments_cleaned.csv")
    cleaned_df.to_csv(output_path, index=False)
    print(f"Cleaned data written to:\n  {output_path}")
    print(cleaned_df.head())

```

## From 1977 to 1995, there are some values missing now like average price, still we keep some information like quantities in different regions
First we convert all the files from 1977 to 1995 to csv, then we use [this code](https://github.com/dario-marino/KnowledgeLab/blob/main/Clean_1977_1995.py)

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

## 1976, 1996, 1997, 1998, 1999
Now we are going to convert 1976, 1996, 1997, 1998, 1999 in csv and then apply the following [code](https://github.com/dario-marino/KnowledgeLab/blob/main/Clean_1976-1996-1997-1998-1999.py):


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

## Now we are done, we are going to merge and then plot some graphs of average selling price to see that our data make sense:

See this code for [merging](https://github.com/dario-marino/KnowledgeLab/blob/main/merge.py), which will create this [file](https://github.com/dario-marino/KnowledgeLab/blob/main/Merged_Shipment_Data.csv), and then this code for [plotting](https://github.com/dario-marino/KnowledgeLab/blob/main/plot.py).
You can see for example this graph here for the A99 Diodes:


![a99 diodes](https://github.com/dario-marino/KnowledgeLab/blob/main/plots/Figure%202025-06-14%20225348%20(9).png)



## We expand the dictionary in the factsheet like Sinan did

We use this [code](https://github.com/dario-marino/KnowledgeLab/blob/main/Expand_Factsheet.py). The file is too big you will find it in: 



