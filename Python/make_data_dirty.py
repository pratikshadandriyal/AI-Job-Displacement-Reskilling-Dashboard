import pandas as pd
import numpy as np
import random

# Load clean dataset
df = pd.read_csv('ai_job_replacement_2020_2026_v2.csv')

# 1. NULLs in specified columns (5-8%)
null_cols = ['automation_risk_category', 'salary_before_usd', 'country', 'education_requirement_level']
for col in null_cols:
    if col in df.columns:
        n_null = np.random.randint(int(0.05 * len(df)), int(0.08 * len(df)))
        null_indices = np.random.choice(df.index, n_null, replace=False)
        df.loc[null_indices, col] = np.nan

# 2. Duplicates (2-3%)
n_dup = np.random.randint(int(0.02 * len(df)), int(0.03 * len(df)))
dup_indices = np.random.choice(df.index, n_dup, replace=False)
df_dirty = pd.concat([df] + [df.loc[dup_indices]], ignore_index=True)

# 3. Inconsistent casing (country, industry)
cases = ['title', 'lower', 'upper']
if 'country' in df_dirty.columns:
    mask_c = df_dirty['country'].notna()
    for idx in df_dirty[mask_c].sample(frac=0.8).index:
        s = str(df_dirty.at[idx, 'country']).strip()
        case = np.random.choice(cases)
        df_dirty.at[idx, 'country'] = s.title() if case == 'title' else s.lower() if case == 'lower' else s.upper()

if 'industry' in df_dirty.columns:
    mask_i = df_dirty['industry'].notna()
    for idx in df_dirty[mask_i].sample(frac=0.8).index:
        s = str(df_dirty.at[idx, 'industry']).strip()
        case = np.random.choice(cases)
        df_dirty.at[idx, 'industry'] = s.title() if case == 'title' else s.lower() if case == 'lower' else s.upper()

# 4. Salary outliers (high/low)
salary_cols = ['salary_before_usd', 'salary_after_usd']
n_out = int(0.01 * len(df_dirty))
for col in salary_cols:
    if col in df_dirty.columns:
        mask = df_dirty[col].notna()
        out_idx = np.random.choice(df_dirty[mask].index, min(n_out, mask.sum()), replace=False)
        half = len(out_idx) // 2
        df_dirty.loc[out_idx[:half], col] = np.random.uniform(1000000, 5000000, half)
        df_dirty.loc[out_idx[half:], col] = np.random.uniform(-10000, 0, len(out_idx) - half)

# 5. Invalid automation_risk_category
if 'automation_risk_category' in df_dirty.columns:
    mask = df_dirty['automation_risk_category'].notna()
    n_inv = int(0.02 * mask.sum())
    inv_idx = np.random.choice(df_dirty[mask].index, min(n_inv, mask.sum()), replace=False)
    inv_vals = ['MED', 'low ', 'HIGH!', 'medIum', 'hIGH', 'Lowww']
    df_dirty.loc[inv_idx, 'automation_risk_category'] = np.random.choice(inv_vals, len(inv_idx))

# 6. Year string errors
if 'year' in df_dirty.columns:
    mask_y = df_dirty['year'].notna()
    n_err = int(0.01 * mask_y.sum())
    err_idx = np.random.choice(df_dirty[mask_y].index, min(n_err, mask_y.sum()), replace=False)
    err_vals = ['2O2O', '2O21', '2O22', '2O23', '2O24', '2O25', '2O26']
    df_dirty.loc[err_idx, 'year'] = np.random.choice(err_vals, len(err_idx))

# Save dirty CSV
df_dirty.to_csv('ai_jobs_dirty.csv', index=False)

# Verify issues
print('Shape:', df_dirty.shape)
print('NULLs:', df_dirty[null_cols].isna().sum().to_dict())
print('Duplicates:', df_dirty.duplicated().sum())
print('Year sample:', df_dirty['year'].dropna().unique()[:10])
print('Salary before describe:\n', df_dirty['salary_before_usd'].describe())
print('Risk cat sample:', df_dirty['automation_risk_category'].dropna().unique()[:10])
