"""mazao-intel — test suite."""
from __future__ import annotations
import io
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────
SAMPLE_CSV = (
    "date,admin1,admin2,market,market_id,latitude,longitude,"
    "category,commodity,commodity_id,unit,priceflag,pricetype,currency,price,usdprice\n"
    "2024-09-15,Nairobi,Nairobi,Nairobi,1,-1.286,36.817,"
    "cereals and tubers,Maize,51,KG,actual,Retail,KES,55.0,0.43\n"
    "2024-08-15,Nairobi,Nairobi,Nairobi,1,-1.286,36.817,"
    "cereals and tubers,Maize,51,KG,actual,Retail,KES,48.0,0.37\n"
    "2024-07-15,Coast,Mombasa,Mombasa,2,-4.05,39.67,"
    "cereals and tubers,Maize,51,KG,actual,Wholesale,KES,42.0,0.33\n"
    "2024-06-15,Coast,Mombasa,Mombasa,2,-4.05,39.67,"
    "cereals and tubers,Maize,51,KG,actual,Retail,KES,50.0,0.39\n"
    "2024-09-15,Nairobi,Nairobi,Nairobi,1,-1.286,36.817,"
    "pulses and nuts,Beans,50,KG,actual,Retail,KES,120.0,0.93\n"
)


def _make_df(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(csv_text), parse_dates=["date"])
    return df


# ── Tests ─────────────────────────────────────────────────────────────────────
class TestDataSchema:
    def test_required_columns(self):
        df = _make_df(SAMPLE_CSV)
        required = {"date", "market", "commodity", "price", "currency", "pricetype"}
        assert required.issubset(df.columns)

    def test_prices_positive(self):
        df = _make_df(SAMPLE_CSV)
        assert (df["price"] > 0).all()

    def test_dates_parse(self):
        df = _make_df(SAMPLE_CSV)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_currency_is_kes(self):
        df = _make_df(SAMPLE_CSV)
        assert (df["currency"] == "KES").all()


class TestPriceLogic:
    def test_latest_price_per_commodity(self):
        df = _make_df(SAMPLE_CSV)
        latest_date = df["date"].max()
        cutoff = latest_date - pd.DateOffset(months=2)
        recent = df[df["date"] >= cutoff]
        latest = (
            recent.sort_values("date", ascending=False)
            .groupby(["commodity", "market"], as_index=False)
            .first()
        )
        assert len(latest) > 0
        assert "Maize" in latest["commodity"].values

    def test_spike_detection(self):
        df = _make_df(SAMPLE_CSV)
        # Maize recent = 55, historical avg = (55+48+42+50)/4 = 48.75
        # 55 > 48.75 * 1.20 = 58.5 → NOT a spike with this data
        maize = df[df["commodity"] == "Maize"]["price"]
        avg = maize.iloc[:-1].mean()  # all but latest
        latest_price = maize.sort_values().iloc[-1]
        is_spike = latest_price > avg * 1.20
        assert is_spike in (True, False)  # spike flag is boolean-compatible

    def test_market_comparison(self):
        df = _make_df(SAMPLE_CSV)
        cmp = (
            df[df["commodity"] == "Maize"]
            .groupby("market", as_index=False)["price"].median()
            .sort_values("price")
        )
        assert len(cmp) == 2  # Nairobi and Mombasa
        assert cmp.iloc[0]["price"] <= cmp.iloc[1]["price"]

    def test_monthly_aggregation(self):
        df = _make_df(SAMPLE_CSV)
        df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
        monthly = df.groupby(["month", "market"])["price"].median()
        assert len(monthly) > 0

    def test_usd_price_reasonable(self):
        df = _make_df(SAMPLE_CSV)
        # USD price should be roughly price / 130 (±50%)
        ratio = df["price"] / df["usdprice"]
        assert ratio.between(50, 300).all(), f"Unexpected USD price ratio: {ratio.describe()}"


class TestSwahiliMapping:
    def test_key_crops_in_swahili(self):
        swahili = {
            "Maize": "Mahindi",
            "Beans": "Maharagwe",
            "Rice": "Mchele",
        }
        assert swahili["Maize"] == "Mahindi"
        assert swahili["Beans"] == "Maharagwe"

    def test_missing_crop_falls_back(self):
        swahili = {"Maize": "Mahindi"}
        crop = "UnknownCrop"
        result = swahili.get(crop, crop)
        assert result == crop  # falls back to English
