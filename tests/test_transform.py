"""
==============================================================================
Unit Tests: Transformation Logic (etl.transform)
==============================================================================
Module: tests.test_transform
Description:
    Validates numeric sanitization and text normalization functions against
    a diverse battery of clean, noisy, edge-case, and adversarial inputs.

Class Comments:
    Unit testing data transformations is vital in Data Engineering because
    dirty data variations are endless. By exhaustively testing edge cases
    (currency formatting, non-breaking whitespaces, textual representations
    of null, and non-numeric characters), we prevent downstream data corruption.
==============================================================================
"""

import numpy as np
import pandas as pd
import pytest

from etl.transform import clean_numeric, transform_data


class TestCleanNumericUnit:
    """Unit tests for the clean_numeric scalar sanitizer."""

    def test_clean_numeric_valid_integers_and_floats(self):
        """Verify standard integers, floats, and numeric strings convert accurately."""
        assert clean_numeric(100) == 100.0
        assert clean_numeric(49.95) == 49.95
        assert clean_numeric("299") == 299.0
        assert clean_numeric("149.50") == 149.50

    def test_clean_numeric_currency_symbols(self):
        """Verify rupee and dollar currency prefixes and suffixes are stripped."""
        assert clean_numeric("₹1,499") == 1499.0
        assert clean_numeric("₹ 2,999") == 2999.0
        assert clean_numeric("$49.99") == 49.99
        assert clean_numeric("1500 ₹") == 1500.0

    def test_clean_numeric_comma_formatting(self):
        """Verify thousands separators and commas are removed."""
        assert clean_numeric("10,000") == 10000.0
        assert clean_numeric("1,234,567.89") == 1234567.89

    def test_clean_numeric_percentage_signs(self):
        """Verify discount percentage signs are stripped without losing numeric value."""
        assert clean_numeric("50%") == 50.0
        assert clean_numeric("15.5%") == 15.5
        assert clean_numeric("0%") == 0.0

    @pytest.mark.parametrize(
        "null_val",
        [
            None,
            np.nan,
            pd.NA,
            "",
            "   ",
            "nan",
            "NaN",
            "NAN",
            "none",
            "None",
            "null",
            "NULL",
            "na",
            "N/A",
            "n/a",
            "invalid",
            "undefined",
            "-",
        ],
    )
    def test_clean_numeric_null_representations(self, null_val):
        """Verify all variations of null, missing, or blank cells return 0.0."""
        assert clean_numeric(null_val) == 0.0

    def test_clean_numeric_negative_values(self):
        """Verify negative values are parsed with their negative sign preserved."""
        assert clean_numeric("-500") == -500.0
        assert clean_numeric("₹ -150.25") == -150.25

    def test_clean_numeric_infinities_and_unparseable(self):
        """Verify mathematical infinities and garbage strings default to 0.0."""
        assert clean_numeric(float("inf")) == 0.0
        assert clean_numeric(float("-inf")) == 0.0
        assert clean_numeric(float("nan")) == 0.0
        assert clean_numeric("abcdef") == 0.0
        assert clean_numeric("!@#$%^&*()") == 0.0


class TestTransformDataUnit:
    """Unit tests for the dataframe-level transform_data function."""

    def test_transform_data_cleans_all_numeric_columns(self, sample_raw_dirty_df):
        """Verify transform_data casts and sanitizes all designated numeric columns."""
        transformed = transform_data(sample_raw_dirty_df)

        for col in ["price", "mrp", "discount", "rating", "rating_count"]:
            assert col in transformed.columns
            # All values should be float or int
            assert all(isinstance(v, (float, int)) for v in transformed[col])

        # Verify specific expected conversions from sample_raw_dirty_df
        assert transformed.loc[0, "price"] == 1499.0
        assert transformed.loc[0, "mrp"] == 2999.0
        assert transformed.loc[0, "discount"] == 50.0
        assert transformed.loc[0, "rating"] == 4.5
        assert transformed.loc[0, "rating_count"] == 1250.0

    def test_transform_data_strips_text_fields(self, sample_raw_dirty_df):
        """Verify string fields are stripped of leading and trailing whitespaces."""
        transformed = transform_data(sample_raw_dirty_df)

        assert transformed.loc[0, "title"] == "Wireless Earbuds Pro"
        assert transformed.loc[0, "description"] == "High quality sound with active noise cancellation."
        assert transformed.loc[0, "highlights"] == "[ANC, 30hr battery]"

    def test_transform_data_immutability(self, sample_raw_dirty_df):
        """Verify transform_data does not alter the original input DataFrame."""
        original_price = sample_raw_dirty_df.loc[0, "price"]
        _ = transform_data(sample_raw_dirty_df)

        assert sample_raw_dirty_df.loc[0, "price"] == original_price
        assert sample_raw_dirty_df.loc[0, "title"] == "  Wireless Earbuds Pro  "

    def test_transform_data_missing_optional_columns(self):
        """Verify transform_data does not crash when columns are missing."""
        minimal_df = pd.DataFrame({
            "title": ["  Basic Earbuds  "],
            "price": ["₹599"],
        })

        result = transform_data(minimal_df)
        assert result.loc[0, "title"] == "Basic Earbuds"
        assert result.loc[0, "price"] == 599.0

    def test_transform_data_empty_dataframe(self):
        """Verify empty DataFrame returns empty transformed DataFrame without error."""
        empty_df = pd.DataFrame(columns=["title", "price", "mrp"])
        result = transform_data(empty_df)
        assert result.empty
        assert list(result.columns) == ["title", "price", "mrp"]
