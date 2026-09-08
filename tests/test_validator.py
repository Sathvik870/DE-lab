"""
==============================================================================
Unit Tests: Data Validation Quality Gates (etl.validator)
==============================================================================
Module: tests.test_validator
Description:
    Validates business rules and data quality constraints enforced by
    `validate_data` on transformed records.

Class Comments:
    Data Validation in DataOps acts as an automated firewall preventing corrupt
    records from reaching the analytical warehouse. Testing the validator
    ensures:
    1. Zero False Positives: Good records are never discarded.
    2. Zero False Negatives: Corrupted records are quarantined into rejected_df.
    3. Conservation of Records: Every single input record must be accounted for
       either in valid_df or rejected_df (len(input) == len(valid) + len(rejected)).
==============================================================================
"""

import pandas as pd

from etl.validator import validate_data


class TestValidatorUnit:
    """Unit tests for the validate_data quality gate function."""

    def test_validate_all_valid_records(self, sample_clean_df):
        """Verify clean, conformant records all pass into valid_df with 0 rejections."""
        valid_df, rejected_df = validate_data(sample_clean_df)

        assert len(valid_df) == len(sample_clean_df)
        assert len(rejected_df) == 0

    def test_validate_rejects_missing_or_blank_title(self):
        """Verify records with missing, empty, or whitespace-only titles are rejected."""
        df = pd.DataFrame({
            "title": [None, "", "   ", "nan", "Valid Earbuds Model X"],
            "price": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
            "mrp": [2000.0, 2000.0, 2000.0, 2000.0, 2000.0],
            "rating": [4.0, 4.0, 4.0, 4.0, 4.0],
            "discount": [50.0, 50.0, 50.0, 50.0, 50.0],
        })

        valid_df, rejected_df = validate_data(df)

        assert len(valid_df) == 1
        assert len(rejected_df) == 4
        assert valid_df.iloc[0]["title"] == "Valid Earbuds Model X"

    def test_validate_rejects_negative_price(self):
        """Verify negative price or null price records are rejected."""
        df = pd.DataFrame({
            "title": ["Earbuds A", "Earbuds B", "Earbuds C"],
            "price": [-1.0, None, 500.0],
            "mrp": [1000.0, 1000.0, 1000.0],
            "rating": [4.0, 4.0, 4.0],
            "discount": [50.0, 50.0, 50.0],
        })

        valid_df, rejected_df = validate_data(df)

        assert len(valid_df) == 1
        assert len(rejected_df) == 2
        assert valid_df.iloc[0]["title"] == "Earbuds C"

    def test_validate_rejects_negative_mrp(self):
        """Verify negative MRP or null MRP records are rejected."""
        df = pd.DataFrame({
            "title": ["Earbuds A", "Earbuds B"],
            "price": [500.0, 500.0],
            "mrp": [-100.0, 1000.0],
            "rating": [4.0, 4.0],
            "discount": [50.0, 50.0],
        })

        valid_df, rejected_df = validate_data(df)

        assert len(valid_df) == 1
        assert len(rejected_df) == 1
        assert valid_df.iloc[0]["title"] == "Earbuds B"

    def test_validate_rating_bounds(self):
        """Verify ratings are strictly enforced within [0.0, 5.0]."""
        df = pd.DataFrame({
            "title": ["Earbuds 1", "Earbuds 2", "Earbuds 3", "Earbuds 4", "Earbuds 5"],
            "price": [500.0] * 5,
            "mrp": [1000.0] * 5,
            "discount": [50.0] * 5,
            "rating": [0.0, 5.0, 4.2, -0.1, 5.1],
        })

        valid_df, rejected_df = validate_data(df)

        # 0.0, 5.0, and 4.2 are valid; -0.1 and 5.1 are invalid
        assert len(valid_df) == 3
        assert len(rejected_df) == 2
        assert set(valid_df["title"]) == {"Earbuds 1", "Earbuds 2", "Earbuds 3"}
        assert set(rejected_df["title"]) == {"Earbuds 4", "Earbuds 5"}

    def test_validate_discount_bounds(self):
        """Verify discount percentage is strictly enforced within [0.0, 100.0]."""
        df = pd.DataFrame({
            "title": ["Earbuds 1", "Earbuds 2", "Earbuds 3", "Earbuds 4"],
            "price": [500.0] * 4,
            "mrp": [1000.0] * 4,
            "rating": [4.0] * 4,
            "discount": [0.0, 100.0, -5.0, 105.0],
        })

        valid_df, rejected_df = validate_data(df)

        # 0.0 and 100.0 are valid; -5.0 and 105.0 are invalid
        assert len(valid_df) == 2
        assert len(rejected_df) == 2
        assert set(valid_df["title"]) == {"Earbuds 1", "Earbuds 2"}
        assert set(rejected_df["title"]) == {"Earbuds 3", "Earbuds 4"}

    def test_validate_records_conservation(self, sample_raw_dirty_df):
        """
        Conservation of Records:
        Verify no records vanish during validation (len(valid) + len(rejected) == len(input)).
        """
        from etl.transform import transform_data

        transformed_df = transform_data(sample_raw_dirty_df)
        valid_df, rejected_df = validate_data(transformed_df)

        assert len(valid_df) + len(rejected_df) == len(sample_raw_dirty_df)

    def test_validate_empty_dataset(self):
        """Verify empty DataFrame returns two empty DataFrames without error."""
        empty_df = pd.DataFrame(columns=["title", "price", "mrp"])
        valid_df, rejected_df = validate_data(empty_df)

        assert valid_df.empty
        assert rejected_df.empty
