"""
==============================================================================
Pytest Fixtures: Data Pipeline Testing Configuration
==============================================================================
Module: tests.conftest
Description:
    Provides reusable pytest fixtures representing real-world dirty e-commerce
    payloads, expected cleaned outputs, and mock database fixtures.
==============================================================================
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock


@pytest.fixture
def sample_raw_dirty_df():
    """
    Returns a DataFrame representing typical raw, unstructured data extracted
    from the rough staging table. Contains formatting noise, currency symbols,
    percentages, text nulls, missing fields, and edge cases.
    """
    data = {
        "id": [1, 2, 3, 4, 5, 6, 7],
        "url": [
            "https://example.com/earbuds-1",
            "https://example.com/earbuds-2",
            "https://example.com/earbuds-3",
            "https://example.com/earbuds-4",
            "https://example.com/earbuds-5",
            "https://example.com/earbuds-6",
            "https://example.com/earbuds-1",  # duplicate URL within batch
        ],
        "title": [
            "  Wireless Earbuds Pro  ",
            "Bass Booster In-Ear Headphones",
            "   ",  # invalid: blank title
            "Studio Sound Buds",
            "Budget Pods",
            None,  # invalid: missing title
            "Wireless Earbuds Pro Duplicate",
        ],
        "price": [
            "₹1,499",
            "2,999.00",
            "Invalid",
            "-500",  # invalid: negative price
            "₹ 999",
            "1,200",
            "₹1,499",
        ],
        "mrp": [
            "₹2,999",
            "4,999",
            "null",
            "2,000",
            "1,999",
            "3,000",
            "₹2,999",
        ],
        "discount": [
            "50%",
            "40",
            "N/A",
            "150%",  # invalid: discount > 100%
            "50%",
            "60%",
            "50%",
        ],
        "rating": [
            "4.5",
            "3.8",
            "none",
            "5.5",  # invalid: rating > 5.0
            "4.0",
            "4.2",
            "4.5",
        ],
        "rating_count": [
            "1,250",
            "8,432",
            "0",
            "120",
            "500",
            "300",
            "1,250",
        ],
        "description": [
            "  High quality sound with active noise cancellation.  ",
            "Deep bass and long battery life.",
            "N/A",
            "Premium studio audio.",
            "Affordable daily drivers.",
            "",
            "High quality sound.",
        ],
        "highlights": [
            " [ANC, 30hr battery] ",
            "[Fast charging]",
            "[]",
            "[Bluetooth 5.3]",
            "[IPX4 water resistant]",
            "[]",
            "[ANC, 30hr battery]",
        ],
        "specifications": ["{}", "{}", "{}", "{}", "{}", "{}", "{}"],
        "image_urls": ["[]", "[]", "[]", "[]", "[]", "[]", "[]"],
        "video_urls": ["[]", "[]", "[]", "[]", "[]", "[]", "[]"],
        "batch_id": ["batch-uuid-001"] * 7,
        "uploaded_at": [pd.Timestamp("2026-09-08 12:00:00")] * 7,
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_clean_df():
    """
    Returns a clean, post-transformation DataFrame ready for validation testing.
    """
    data = {
        "url": [
            "https://example.com/earbuds-1",
            "https://example.com/earbuds-2",
        ],
        "title": [
            "Wireless Earbuds Pro",
            "Bass Booster In-Ear Headphones",
        ],
        "price": [1499.0, 2999.0],
        "mrp": [2999.0, 4999.0],
        "discount": [50.0, 40.0],
        "rating": [4.5, 3.8],
        "rating_count": [1250.0, 8432.0],
        "description": [
            "High quality sound with active noise cancellation.",
            "Deep bass and long battery life.",
        ],
        "highlights": ["[ANC, 30hr battery]", "[Fast charging]"],
        "batch_id": ["batch-uuid-001", "batch-uuid-001"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_db_connection():
    """
    Pytest fixture yielding a mocked PostgreSQL connection and cursor.
    """
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur
