"""
Clean the Test_Data.xlsx file and export as a properly formatted CSV:
1. Fix column names (spaces and hyphens to underscores)
2. Remove embedded newlines and carriage returns
3. Strip whitespace from all text fields
4. Ensure proper CSV formatting
"""

import pandas as pd
from pathlib import Path

def clean_excel_to_csv(excel_path, output_path):
    """Read Excel file, clean data, and export as CSV"""

    print(f"Reading Excel from: {excel_path}")

    # Read the Excel file
    df = pd.read_excel(excel_path, engine='openpyxl')

    print(f"Loaded {len(df)} rows")
    print(f"Original columns: {list(df.columns)}")

    # Fix column names: replace spaces and hyphens with underscores
    df.columns = [
        col.strip()
           .replace(' ', '_')
           .replace('-', '_')
        for col in df.columns
    ]

    print(f"Cleaned columns: {list(df.columns)}")

    # Clean text fields - remove embedded newlines, carriage returns
    text_columns = [
        'Store_Name', 'Item_Code', 'Item_Barcode', 'Description',
        'Category', 'Department', 'Sub_Department', 'Section', 'Supplier'
    ]

    for col in text_columns:
        if col in df.columns:
            print(f"Cleaning column: {col}")
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'\r\n', ' ', regex=True)     # Remove Windows line breaks
                .str.replace(r'\r', ' ', regex=True)       # Remove Mac line breaks
                .str.replace(r'\n', ' ', regex=True)       # Remove Unix line breaks
                .str.replace(r'\s+', ' ', regex=True)      # Normalize whitespace
                .str.strip()                                # Trim leading/trailing whitespace
            )

    # Handle Date_Of_Sale column
    date_col = None
    for col in df.columns:
        if 'date' in col.lower():
            date_col = col
            break

    if date_col:
        print(f"Converting {date_col} to proper date format")
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        # Remove rows with invalid dates
        invalid_dates = df[date_col].isna().sum()
        if invalid_dates > 0:
            print(f"Warning: Removing {invalid_dates} rows with invalid dates")
            df = df.dropna(subset=[date_col])

    # Convert numeric columns
    numeric_columns = ['Quantity', 'Total_Sales', 'RRP']
    for col in numeric_columns:
        if col in df.columns:
            print(f"Converting {col} to numeric")
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove rows with missing critical data
    print("Checking for missing critical data")
    before_count = len(df)

    critical_cols = []
    for col in ['Store_Name', 'Description', 'Quantity', 'Total_Sales']:
        if col in df.columns:
            critical_cols.append(col)

    if critical_cols:
        df = df.dropna(subset=critical_cols)
        after_count = len(df)

        if before_count > after_count:
            print(f"Removed {before_count - after_count} rows with missing critical data")

    # Save cleaned CSV
    print(f"\nSaving cleaned CSV to: {output_path}")
    df.to_csv(
        output_path,
        index=False,
        encoding='utf-8',
        line_terminator='\n'  # Use Unix line endings
    )

    print(f"\n✓ Successfully cleaned and exported CSV!")
    print(f"  - Final row count: {len(df)}")
    print(f"  - Columns: {len(df.columns)}")
    print(f"  - Output file size: {Path(output_path).stat().st_size / 1024 / 1024:.2f} MB")

    return df


if __name__ == '__main__':
    # Define paths
    base_dir = Path(__file__).parent.parent
    input_file = base_dir / 'include' / 'datasets' / 'Test_Data.xlsx'
    output_file = base_dir / 'include' / 'datasets' / 'retail_sales.csv'

    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        exit(1)

    # Backup old CSV if exists
    if output_file.exists():
        backup_file = base_dir / 'include' / 'datasets' / 'retail_sales.csv.old'
        print(f"Backing up existing CSV to: {backup_file}")
        import shutil
        shutil.copy2(output_file, backup_file)

    # Clean the Excel and export to CSV
    cleaned_df = clean_excel_to_csv(input_file, output_file)

    # Show sample of cleaned data
    print("\n" + "="*80)
    print("Sample of cleaned data (first 5 rows):")
    print("="*80)
    print(cleaned_df.head())

    # Show data quality summary
    print("\n" + "="*80)
    print("Data Quality Summary:")
    print("="*80)
    print(f"Total rows: {len(cleaned_df)}")

    date_col = [col for col in cleaned_df.columns if 'date' in col.lower()]
    if date_col:
        print(f"Date range: {cleaned_df[date_col[0]].min()} to {cleaned_df[date_col[0]].max()}")

    if 'Store_Name' in cleaned_df.columns:
        print(f"Unique stores: {cleaned_df['Store_Name'].nunique()}")

    if 'Supplier' in cleaned_df.columns:
        print(f"Unique suppliers: {cleaned_df['Supplier'].nunique()}")

    if 'Total_Sales' in cleaned_df.columns:
        print(f"Total sales: ${cleaned_df['Total_Sales'].sum():,.2f}")
