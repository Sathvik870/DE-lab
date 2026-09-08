"""
==============================================================================
ETL Pipeline: Data Validation & Quality Gates
==============================================================================
Module: etl.validator
Description:
    Enforces data contracts and business constraints on transformed e-commerce
    records before loading into production staging (`earbuds_staging`).

Class Comments:
    In enterprise Data Engineering, bad data should NEVER be allowed to silently
    poison analytical tables or cause full pipeline crashes. Instead, pipelines
    implement a Quarantine Pattern (also known as a Dead Letter Queue or DLQ):
    - Clean, valid records proceed forward to the primary destination.
    - Violating records are isolated into a rejected dataset for audit,
      alerting, and root-cause analysis.

    Validation Rules Enforced:
    1. Mandatory Title: Products must have a non-empty string title.
    2. Non-Negative Pricing: Price and MRP must be >= 0 (and not NaN).
    3. Rating Boundedness: Ratings must fall within the range [0.0, 5.0].
    4. Discount Validity: Percentage discount must be within [0.0, 100.0].
==============================================================================
"""

from typing import Tuple
import pandas as pd


def validate_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate transformed records against schema constraints and business rules.

    Splits the input DataFrame into two disjoint subsets:
    - valid_df: Records passing 100% of data quality checks.
    - rejected_df: Records failing one or more validation constraints.

    Args:
        df (pd.DataFrame): Transformed DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (valid_df, rejected_df)
    """
    print('[VALIDATE] Validating records against data quality rules...')

    if df.empty:
        print('[VALIDATE] Empty dataset provided to validator.')
        return df.copy(), df.copy()

    df_eval = df.copy()

    # RULE 1: MANDATORY TITLE
    if 'title' in df_eval.columns:
        invalid_title = (
            df_eval['title'].isna()
            | (df_eval['title'].astype(str).str.strip() == '')
            | (df_eval['title'].astype(str).str.lower() == 'nan')
        )
    else:
        invalid_title = pd.Series(True, index=df_eval.index)

    # RULE 2: PRICE INTEGRITY
    if 'price' in df_eval.columns:
        invalid_price = (
            df_eval['price'].isna()
            | (df_eval['price'] < 0)
        )
    else:
        invalid_price = pd.Series(False, index=df_eval.index)

    # RULE 3: MRP INTEGRITY
    if 'mrp' in df_eval.columns:
        invalid_mrp = (
            df_eval['mrp'].isna()
            | (df_eval['mrp'] < 0)
        )
    else:
        invalid_mrp = pd.Series(False, index=df_eval.index)

    # RULE 4: RATING BOUNDARY [0.0 - 5.0]
    if 'rating' in df_eval.columns:
        invalid_rating = (
            df_eval['rating'].notna()
            & (
                (df_eval['rating'] < 0.0)
                | (df_eval['rating'] > 5.0)
            )
        )
    else:
        invalid_rating = pd.Series(False, index=df_eval.index)

    # RULE 5: DISCOUNT BOUNDARY [0.0% - 100.0%]
    if 'discount' in df_eval.columns:
        invalid_discount = (
            df_eval['discount'].notna()
            & (
                (df_eval['discount'] < 0.0)
                | (df_eval['discount'] > 100.0)
            )
        )
    else:
        invalid_discount = pd.Series(False, index=df_eval.index)

    invalid_mask = (
        invalid_title
        | invalid_price
        | invalid_mrp
        | invalid_rating
        | invalid_discount
    )

    rejected_df = df_eval[invalid_mask].copy()
    valid_df = df_eval[~invalid_mask].copy()

    print(f'[VALIDATE] Total evaluated: {len(df_eval)}')
    print(f'[VALIDATE] Valid records  : {len(valid_df)}')
    print(f'[VALIDATE] Rejected records: {len(rejected_df)}')

    return valid_df, rejected_df
