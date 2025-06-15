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
