"""
cleaning.py — Data Cleaning untuk Zomato Delivery Dataset
Diadaptasi dari Cekdata.ipynb

Input  : data/zomato.csv        (raw dataset dari Kaggle)
Output : data/zomato_clean.csv  (siap dipakai batch & streaming)

Cara menjalankan:
    pip install pandas numpy
    python data/cleaning.py
"""

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings('ignore')

# ── Path (relatif terhadap folder project) ──────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
# Try common raw filenames (keep backwards compatibility)
CANDIDATES = ["zomato.csv", "Zomato Dataset.csv", "zomato dataset.csv"]
RAW_PATH = None
for fn in CANDIDATES:
    p = os.path.join(BASE_DIR, fn)
    if os.path.exists(p):
        RAW_PATH = p
        break
if RAW_PATH is None:
    RAW_PATH = os.path.join(BASE_DIR, "zomato.csv")

CLEAN_PATH = os.path.join(BASE_DIR, "zomato_clean.csv")

# ── Load Dataset ────────────────────────────────────────────────────────────
print("Dataset berhasil dimuat!")
df = pd.read_csv(RAW_PATH)
print(f"Ukuran dataset: {df.shape}")
print(f"Total baris: {df.shape[0]}, Total kolom: {df.shape[1]}")

# ── 1. Explore Data Awal ────────────────────────────────────────────────────
print("\n" + "="*80)
print("5 BARIS PERTAMA DATA")
print("="*80)
print(df.head())

print("\n" + "="*80)
print("MISSING VALUES")
print("="*80)
missing = df.isnull().sum()
missing_percent = (missing / len(df)) * 100
missing_df = pd.DataFrame({'Missing Count': missing, 'Percentage': missing_percent})
print(missing_df[missing_df['Missing Count'] > 0])

print("\n" + "="*80)
print("STATISTIK DESKRIPTIF")
print("="*80)
print(df.describe())

# ── 2. Data Cleaning ────────────────────────────────────────────────────────
df_clean = df.copy()
print("\nMulai proses data cleaning...")
print(f"Data awal: {df_clean.shape}")

# -- 1. Handle Missing Values ------------------------------------------------
print("\n" + "="*80)
print("1. HANDLE MISSING VALUES")
print("="*80)

print(f"Sebelum drop: {df_clean.shape}")
df_clean = df_clean.dropna(subset=['multiple_deliveries'])
print(f"Sesudah drop multiple_deliveries NaN: {df_clean.shape}")

if df_clean['Vehicle_condition'].isnull().sum() > 0:
    df_clean['Vehicle_condition'].fillna(
        df_clean['Vehicle_condition'].mode()[0], inplace=True
    )

# -- 2. Konversi Data Type ---------------------------------------------------
print("\n" + "="*80)
print("2. KONVERSI DATA TYPE")
print("="*80)

df_clean['Delivery_person_Age']     = pd.to_numeric(df_clean['Delivery_person_Age'],     errors='coerce')
df_clean['Delivery_person_Ratings'] = pd.to_numeric(df_clean['Delivery_person_Ratings'], errors='coerce')
df_clean['Vehicle_condition']       = pd.to_numeric(df_clean['Vehicle_condition'],       errors='coerce')
df_clean['multiple_deliveries']     = pd.to_numeric(df_clean['multiple_deliveries'],     errors='coerce')
df_clean['Time_taken (min)']        = pd.to_numeric(df_clean['Time_taken (min)'],        errors='coerce')

df_clean['Order_Date'] = pd.to_datetime(
    df_clean['Order_Date'], format='%d-%m-%Y', errors='coerce'
)

print("Data types setelah konversi:")
print(df_clean.dtypes)

# -- 3. Handle Outliers (IQR) ------------------------------------------------
print("\n" + "="*80)
print("3. HANDLE OUTLIERS")
print("="*80)

def remove_outliers_iqr(data, column):
    """Hapus outliers menggunakan IQR method"""
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    before = len(data)
    data = data[(data[column] >= lower_bound) & (data[column] <= upper_bound)]
    after = len(data)
    print(f"{column}: Removed {before - after} outliers")
    return data

df_clean = remove_outliers_iqr(df_clean, 'Delivery_person_Age')
df_clean = remove_outliers_iqr(df_clean, 'Time_taken (min)')
print(f"Data setelah remove outliers: {df_clean.shape}")

# -- 4. Remove Duplicates ----------------------------------------------------
print("\n" + "="*80)
print("4. REMOVE DUPLICATES")
print("="*80)

print(f"Sebelum drop duplicates: {df_clean.shape}")
df_clean = df_clean.drop_duplicates()
print(f"Sesudah drop duplicates: {df_clean.shape}")
print(f"\nTotal unique Delivery_person_ID: {df_clean['Delivery_person_ID'].nunique()}")

# -- 5. Validasi Range Nilai -------------------------------------------------
print("\n" + "="*80)
print("5. VALIDASI RANGE NILAI")
print("="*80)

print(f"Age range: {df_clean['Delivery_person_Age'].min()} - {df_clean['Delivery_person_Age'].max()}")
print(f"Ratings range: {df_clean['Delivery_person_Ratings'].min()} - {df_clean['Delivery_person_Ratings'].max()}")
print(f"Time taken range: {df_clean['Time_taken (min)'].min()} - {df_clean['Time_taken (min)'].max()}")

df_clean = df_clean[df_clean['Delivery_person_Age'] > 0]
df_clean = df_clean[df_clean['Delivery_person_Age'] <= 100]
df_clean = df_clean[df_clean['Delivery_person_Ratings'] > 0]
df_clean = df_clean[df_clean['Time_taken (min)'] > 0]
print(f"\nData setelah validasi: {df_clean.shape}")

# ── 3. Ringkasan Cleaning ───────────────────────────────────────────────────
print("\n" + "="*80)
print("RINGKASAN CLEANING")
print("="*80)
print(f"Data Original : {df.shape}")
print(f"Data Cleaned  : {df_clean.shape}")
print(f"Total dihapus : {df.shape[0] - df_clean.shape[0]} baris")
print(f"Data tersisa  : {(df_clean.shape[0] / df.shape[0] * 100):.2f}%")

print("\n" + "="*80)
print("MISSING VALUES SETELAH CLEANING")
print("="*80)
print(df_clean.isnull().sum().sum(), "missing values")

# ── Export ──────────────────────────────────────────────────────────────────
df_clean.to_csv(CLEAN_PATH, index=False)
print(f"\n✓ Data bersih disimpan ke: {CLEAN_PATH}")
print("\nPreview data final:")
print(df_clean.head(10))
print("\nStatistik Data Final:")
print(df_clean.describe())