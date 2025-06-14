import pandas as pd
import json

# === CONFIGURE THESE ===
INPUT_CSV = r'C:\Users\dario\OneDrive\Documents\Università\Knowledge Lab\product_factsheets_raw.csv'
OUTPUT_CSV = r'C:\Users\dario\OneDrive\Documents\Università\Knowledge Lab\product_factsheets_expanded.csv'

def expand_attributes_column(df, attr_col='product_attributes'):
    """
    Takes a DataFrame and expands the JSON-dictionary-style column into separate columns.
    Missing keys become 'NA'.
    """
    # Parse each entry as a dict (empty dict if NaN)
    dicts = df[attr_col].apply(lambda x: json.loads(x) if pd.notna(x) else {})
    # Normalize into a DataFrame (one column per key)
    attrs_df = pd.json_normalize(dicts)
    # Fill missing values
    attrs_df = attrs_df.fillna('NA')
    # Join back onto original df (and drop the original attr column if you like)
    out = df.drop(columns=[attr_col]).join(attrs_df)
    return out

def main():
    # 1. Read your CSV (force everything to string so JSON stays intact)
    df = pd.read_csv(INPUT_CSV, dtype=str)
    
    # 2. Expand the product_attributes dict into columns
    df_expanded = expand_attributes_column(df, attr_col='product_attributes')
    
    # 3. Save out
    df_expanded.to_csv(OUTPUT_CSV, index=False)
    print(f"Done! Expanded file written to: {OUTPUT_CSV}")

if __name__ == '__main__':
    main()
