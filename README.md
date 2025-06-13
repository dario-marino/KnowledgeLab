## Cleaning Bluebook
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

## We did catch something that we shouldn't have, 1997, let's cancel manually:
## Row 2 to 13, column C and column G to column BD, and all the columns remaining to the left of WW ASP
## Except Americas Units, Europe Units,	Japan Units, Asia Pacific Units
