import pandas as pd
import matplotlib.pyplot as plt

# 1. Read the merged file, parse dates, avoid mixed-type warning
df = pd.read_csv(
    r"C:\Users\dario\Downloads\Merged_Shipment_Data.csv",
    parse_dates=['date'],
    low_memory=False
)

# 2. Make sure WW ASP is float
df['WW ASP'] = pd.to_numeric(df['WW ASP'], errors='coerce')

# 3. Drop any duplicate product–date rows, sort
df = (
    df
    .drop_duplicates(subset=['product', 'date'])
    .sort_values(['product', 'date'])
)

# 4. Loop through each product and plot
for product, grp in df.groupby('product'):
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(grp['date'], grp['WW ASP'], marker='o')
    
    # If the axis somehow ended up inverted, flip it back
    if ax.yaxis_inverted():
        ax.invert_yaxis()
    
    ax.set_title(f"WW ASP for {product}")
    ax.set_xlabel("Date")
    ax.set_ylabel("WW ASP")
    fig.tight_layout()
    plt.show()
