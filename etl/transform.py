"""
==============================================================================
ETL Pipeline: Transformation Logic
==============================================================================
Module: etl.transform
Description:
    Implements deterministic, functional data cleaning and transformation for
    raw e-commerce product records extracted from PostgreSQL `earbuds_rough`.

Class Comments:
    In real-world data pipelines (especially scraping and external ingestion),
    incoming data suffers from high entropy: currency symbols (₹, $), commas
    in integer strings, percentage characters, varying representations of null
    ('None', 'null', 'N/A', 'invalid'), and unexpected whitespaces.

    Key Architectural Principles Applied:
    1. Immutability: The source DataFrame is never modified in-place; df.copy()
       guarantees pure transformations without downstream side effects.
    2. Defensive Parsing: All numeric fields are sanitized using regex extraction
       and float casting, safeguarding downstream aggregations and ML models.
    3. Idempotency: Running transform_data multiple times on identical input
       produces identical outputs.
==============================================================================
"""

import math
import re
from typing import Any
import pandas as pd


def clean_numeric(value: Any) -> float:
    """
    Sanitize and cast raw dirty input into a standard float.

    Handles:
    - NaN, None, and pd.NA values -> 0.0
    - Currency symbols (e.g. ₹, $) and formatting commas (e.g. 1,499) -> 1499.0
    - Percentage signs (e.g. 65%) -> 65.0
    - Textual representations of null ('nan', 'none', 'null', 'na', 'n/a', 'invalid') -> 0.0
    - Whitespaces and non-numeric garbage characters -> stripped
    - Infinities and NaN floats -> 0.0

    Args:
        value (Any): Raw cell value from rough ingestion.

    Returns:
        float: Clean numeric value, or 0.0 if unparseable/null.
    """
    if pd.isna(value):
        return 0.0

    str_val = str(value).strip()

    if not str_val:
        return 0.0

    # Strip currency and formatting symbols
    str_val = str_val.replace('₹', '').replace('$', '').replace(',', '').replace('%', '')

    # Check for text representations of missing data
    if str_val.lower() in {
        'nan',
        'none',
        'null',
        'na',
        'n/a',
        'invalid',
        'undefined',
        '-',
    }:
        return 0.0

    # Extract valid numeric pattern: optional sign, digits, optional decimal
    sanitized = re.sub(r'[^0-9.\-]', '', str_val)

    if not sanitized or sanitized in {'-', '.', '-.'}:
        return 0.0

    try:
        number = float(sanitized)
        if math.isnan(number) or math.isinf(number):
            return 0.0
        return number
    except (ValueError, TypeError):
        return 0.0


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Execute stage-2 ETL transformations across all numeric and text columns.

    Args:
        df (pd.DataFrame): Raw DataFrame extracted from rough staging.

    Returns:
        pd.DataFrame: Transformed DataFrame ready for schema validation.
    """
    print('[TRANSFORM] Starting transformation...')

    # Preserve immutability by operating on a defensive copy
    df_transformed = df.copy()

    # Numeric Field Sanitization
    numeric_columns = [
        'price',
        'mrp',
        'discount',
        'rating',
        'rating_count',
    ]

    for column in numeric_columns:
        if column in df_transformed.columns:
            df_transformed[column] = df_transformed[column].apply(clean_numeric)

    # Text Field Normalization (trim whitespace, handle string casting)
    text_columns = [
        'title',
        'description',
        'highlights',
    ]

    for column in text_columns:
        if column in df_transformed.columns:
            df_transformed[column] = (
                df_transformed[column]
                .astype('string')
                .str.strip()
            )

    print('[TRANSFORM] Transformation completed.')
    return df_transformed
