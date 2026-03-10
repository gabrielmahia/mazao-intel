"""
mazao-intel — Agricultural market price intelligence for Kenyan smallholders.

Data source: WFP Kenya Food Prices (HDX / Humanitarian Data Exchange)
  URL: https://data.humdata.org/dataset/wfp-food-prices-for-kenya
  License: Creative Commons Attribution for Intergovernmental Organisations
  Coverage: 2006–present, 20+ markets, 30+ commodities across Kenya

This tool:
  1. Fetches the latest WFP price data on load (cached 6h)
  2. Shows current prices by commodity and market
  3. Plots price trends — detects spikes (>20% above 12-month average)
  4. Compares prices across markets to highlight where to buy or sell
  5. Provides plain-language alerts in English and Kiswahili

TRUST RULE: All figures come directly from WFP source data.
  No synthetic values, no interpolation without labelling.
  When data is unavailable, the app says so clearly.
"""
from __future__ import annotations

import io
import urllib.request
from datetime import datetime, date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Mazao Intel — Kenya Crop Prices",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Mobile CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}

.mz-header{
  background:linear-gradient(135deg,#386641 0%,#6a994e 60%,#a7c957 100%);
  color:white;padding:1.6rem 2rem;border-radius:10px;margin-bottom:1.2rem;
}
.mz-header h1{font-size:1.8rem;margin:0 0 .2rem;letter-spacing:-1px;}
.mz-header p{font-size:.9rem;opacity:.85;margin:0;}

.price-card{
  background:#f8f9fa;border-radius:8px;padding:.9rem 1.1rem;
  border-left:4px solid #6a994e;margin-bottom:.5rem;
}
.price-card .crop{font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;color:#555;}
.price-card .price{font-size:1.5rem;font-weight:700;color:#386641;}
.price-card .sub{font-size:.78rem;color:#6c757d;}

.spike{border-left-color:#e63946!important;}
.spike .price{color:#e63946!important;}
.normal{border-left-color:#6a994e!important;}

.alert-spike{background:#fdf3f3;border:1px solid #f5c6cb;padding:.7rem 1rem;border-radius:6px;margin-bottom:.5rem;}
.alert-ok   {background:#f0f7f0;border:1px solid #c3e6cb;padding:.7rem 1rem;border-radius:6px;margin-bottom:.5rem;}

@media(max-width:768px){
  [data-testid="column"]{width:100%!important;flex:1 1 100%!important;min-width:100%!important;}
  [data-testid="stMetricValue"]{font-size:1.4rem!important;}
  [data-testid="stDataFrame"]{overflow-x:auto!important;}
  .stButton>button{width:100%!important;min-height:48px!important;}
  .mz-header h1{font-size:1.3rem!important;}
}
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
WFP_CSV_URL = (
    "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff"
    "/resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"
)
WFP_MARKETS_URL = (
    "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff"
    "/resource/fde6f6c7-9776-4b52-a826-d77f9d3f6688/download/wfp_markets_ken.csv"
)

# Kiswahili crop names
SWAHILI = {
    "Maize": "Mahindi", "Maize (white)": "Mahindi (nyeupe)",
    "Beans": "Maharagwe", "Beans (dry)": "Maharagwe (kavu)",
    "Rice": "Mchele", "Rice (imported)": "Mchele (kutoka nje)",
    "Wheat": "Ngano", "Sorghum": "Mtama",
    "Potatoes (Irish)": "Viazi (Irish)", "Sweet potatoes": "Viazi vitamu",
    "Cassava": "Muhogo", "Sugar": "Sukari",
    "Milk": "Maziwa", "Eggs": "Mayai",
    "Tomatoes": "Nyanya", "Onions": "Vitunguu",
    "Cooking oil": "Mafuta ya kupikia",
    "Fuel (diesel)": "Mafuta (dizeli)",
}

KEY_CROPS = [
    "Maize", "Maize (white)", "Beans", "Beans (dry)",
    "Rice", "Sorghum", "Potatoes (Irish)", "Sugar", "Milk",
]

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=21600, show_spinner=False)  # 6-hour cache
def fetch_prices() -> tuple[pd.DataFrame, str, bool]:
    """Fetch WFP price data from HDX. Returns (df, timestamp, is_live)."""
    try:
        req = urllib.request.Request(
            WFP_CSV_URL, headers={"User-Agent": "mazao-intel/0.1"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read()
        df = pd.read_csv(io.BytesIO(raw), parse_dates=["date"])
        return df, datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), True
    except Exception as e:
        # Return empty df with schema — never silently return wrong data
        empty = pd.DataFrame(columns=[
            "date", "admin1", "admin2", "market", "category",
            "commodity", "unit", "pricetype", "currency", "price", "usdprice"
        ])
        return empty, f"UNAVAILABLE — {e}", False


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_markets() -> pd.DataFrame:
    try:
        req = urllib.request.Request(
            WFP_MARKETS_URL, headers={"User-Agent": "mazao-intel/0.1"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
        return pd.read_csv(io.BytesIO(raw))
    except Exception:
        return pd.DataFrame()


def compute_spike_threshold(df: pd.DataFrame, commodity: str, market: str) -> float | None:
    """12-month rolling average for a commodity+market pair. None if insufficient data."""
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=12)
    hist = df[
        (df["commodity"] == commodity) &
        (df["market"] == market) &
        (df["date"] >= cutoff)
    ]["price"]
    return round(hist.mean(), 2) if len(hist) >= 3 else None


# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("🌾 Loading WFP Kenya food price data…"):
    prices, fetched_at, is_live = fetch_prices()
    markets_meta = fetch_markets()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="mz-header">
  <h1>🌾 Mazao Intel</h1>
  <p>Kenya crop market prices — powered by WFP/HDX · Nguvu ya Bei kwa Wakulima</p>
</div>
""", unsafe_allow_html=True)

# Data source banner
if is_live:
    st.success(
        f"🟢 **LIVE DATA** — WFP Kenya Food Prices (HDX). "
        f"Fetched: {fetched_at}. "
        f"Coverage: {prices['market'].nunique()} markets · "
        f"{prices['commodity'].nunique()} commodities · "
        f"{prices['date'].min().strftime('%b %Y')} – {prices['date'].max().strftime('%b %Y')}.",
        icon=None
    )
else:
    st.error(
        f"⛔ **LIVE DATA UNAVAILABLE** — {fetched_at}. "
        "Price figures below are not shown to avoid misleading farmers. "
        "Check your network connection or visit "
        "[data.humdata.org](https://data.humdata.org/dataset/wfp-food-prices-for-kenya) directly.",
        icon=None
    )
    st.stop()

if prices.empty:
    st.warning("No data loaded. Please reload.")
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.markdown("### 🇰🇪 Filters · Vichujio")
lang = st.sidebar.radio("Language / Lugha", ["English", "Kiswahili"])

all_markets   = sorted(prices["market"].dropna().unique())
all_commodities = sorted(prices["commodity"].dropna().unique())
key_crops_avail = [c for c in KEY_CROPS if c in all_commodities]

market_sel = st.sidebar.selectbox(
    "Market / Soko", ["All markets"] + all_markets
)
price_type = st.sidebar.selectbox(
    "Price type", ["Retail", "Wholesale", "All"]
)

st.sidebar.divider()
st.sidebar.caption(
    "Source: WFP Kenya Food Prices\n"
    "via Humanitarian Data Exchange (HDX)\n"
    "License: CC BY IGO 3.0"
)

# ── Filter prices ─────────────────────────────────────────────────────────────
df = prices.copy()
if market_sel != "All markets":
    df = df[df["market"] == market_sel]
if price_type != "All":
    df = df[df["pricetype"].str.lower() == price_type.lower()]

# Latest observation per commodity × market
latest_date = df["date"].max()
recent_cutoff = latest_date - pd.DateOffset(months=2)
recent = df[df["date"] >= recent_cutoff]
latest = (
    recent.sort_values("date", ascending=False)
    .groupby(["commodity", "market"], as_index=False)
    .first()
)

# ═══════════════════════════════════════════════════════════
# SECTION 1: CURRENT PRICE CARDS — Key staples
# ═══════════════════════════════════════════════════════════
st.subheader("📌 Current Prices — Staple Crops" if lang == "English"
             else "📌 Bei za Sasa — Mazao ya Msingi")

key_latest = latest[latest["commodity"].isin(key_crops_avail)]
if key_latest.empty:
    st.info("No recent data for key crops in this market.")
else:
    # Group by commodity, take median if multiple markets
    crop_summary = (
        key_latest.groupby("commodity")
        .agg(price=("price", "median"), unit=("unit", "first"),
             currency=("currency", "first"), n_markets=("market", "nunique"))
        .reset_index()
    )

    # Compute 12-month average for spike detection
    cutoff12 = latest_date - pd.DateOffset(months=12)
    hist12 = prices[prices["date"] >= cutoff12]
    avg12 = (
        hist12[hist12["commodity"].isin(key_crops_avail)]
        .groupby("commodity")["price"].mean()
        .rename("avg12")
    )
    crop_summary = crop_summary.join(avg12, on="commodity")
    crop_summary["spike"] = crop_summary.apply(
        lambda r: r["price"] > r["avg12"] * 1.20 if pd.notna(r.get("avg12")) else False,
        axis=1
    )

    cols = st.columns(min(4, len(crop_summary)))
    for i, (_, row) in enumerate(crop_summary.iterrows()):
        crop_name = (SWAHILI.get(row["commodity"], row["commodity"])
                     if lang == "Kiswahili" else row["commodity"])
        spike_class = "spike" if row["spike"] else "normal"
        spike_tag = " ⚠️ HIGH" if row["spike"] else ""
        avg_note = f"12-mo avg: KES {row['avg12']:.0f}" if pd.notna(row.get("avg12")) else ""
        mkt_note = f"{int(row['n_markets'])} market{'s' if row['n_markets']>1 else ''}"

        with cols[i % 4]:
            st.markdown(f"""
<div class="price-card {spike_class}">
  <div class="crop">{crop_name}{spike_tag}</div>
  <div class="price">KES {row['price']:.0f}</div>
  <div class="sub">per {row['unit']} · {mkt_note}<br>{avg_note}</div>
</div>""", unsafe_allow_html=True)

    if crop_summary["spike"].any():
        spiked = crop_summary[crop_summary["spike"]]["commodity"].tolist()
        swnames = [SWAHILI.get(c, c) for c in spiked]
        if lang == "English":
            st.warning(
                f"⚠️ **Price alert:** {', '.join(spiked)} are >20% above their 12-month average. "
                "Consider delaying purchase or sourcing from alternative markets.",
                icon=None
            )
        else:
            st.warning(
                f"⚠️ **Tahadhari ya Bei:** {', '.join(swnames)} ni zaidi ya 20% ya wastani wa miezi 12. "
                "Fikiria kununua baadaye au kutafuta soko lingine.",
                icon=None
            )

# ═══════════════════════════════════════════════════════════
# SECTION 2: PRICE TREND
# ═══════════════════════════════════════════════════════════
st.divider()
st.subheader("📈 Price Trend" if lang == "English" else "📈 Mwelekeo wa Bei")

t_col1, t_col2 = st.columns([2, 1])
with t_col1:
    trend_crop = st.selectbox(
        "Select crop" if lang == "English" else "Chagua zao",
        options=key_crops_avail + [c for c in all_commodities if c not in key_crops_avail],
    )
with t_col2:
    trend_months = st.selectbox("Period", [6, 12, 24, 60], index=1,
                                format_func=lambda m: f"{m} months")

trend_cutoff = latest_date - pd.DateOffset(months=trend_months)
trend_df = df[
    (df["commodity"] == trend_crop) &
    (df["date"] >= trend_cutoff)
].copy()

if trend_df.empty:
    st.info(f"No data for {trend_crop} in this market/period.")
else:
    # Monthly median per market
    trend_df["month"] = trend_df["date"].dt.to_period("M").dt.to_timestamp()
    trend_monthly = (
        trend_df.groupby(["month", "market"], as_index=False)["price"].median()
    )

    crop_label = SWAHILI.get(trend_crop, trend_crop) if lang == "Kiswahili" else trend_crop
    fig = px.line(
        trend_monthly, x="month", y="price", color="market",
        labels={"month": "Month", "price": "Price (KES)", "market": "Market"},
        title=f"{crop_label} — Price trend ({trend_months} months)",
    )
    # Add 12-month average line
    avg_price = trend_df["price"].mean()
    fig.add_hline(
        y=avg_price, line_dash="dot", line_color="gray",
        annotation_text=f"Avg KES {avg_price:.0f}",
        annotation_position="top left",
    )
    fig.update_layout(height=380, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Source: WFP Kenya Food Prices (HDX) · {len(trend_df)} observations · {fetched_at}")

# ═══════════════════════════════════════════════════════════
# SECTION 3: MARKET PRICE COMPARISON
# ═══════════════════════════════════════════════════════════
st.divider()
st.subheader("🗺️ Market Comparison" if lang == "English" else "🗺️ Ulinganisho wa Masoko")
st.caption(
    "Compare the same commodity across markets to find where to sell at best price"
    if lang == "English" else
    "Linganisha bei ya zao moja katika masoko mbalimbali"
)

cmp_crop = st.selectbox(
    "Commodity to compare" if lang == "English" else "Zao la kulinganisha",
    options=key_crops_avail,
    key="cmp_crop",
)

cmp_cutoff = latest_date - pd.DateOffset(months=3)
cmp_df = prices[
    (prices["commodity"] == cmp_crop) &
    (prices["date"] >= cmp_cutoff)
].groupby("market", as_index=False)["price"].median().sort_values("price", ascending=False)

if cmp_df.empty:
    st.info("No recent data for comparison.")
else:
    fig2 = px.bar(
        cmp_df, x="price", y="market", orientation="h",
        color="price",
        color_continuous_scale=["#6a994e", "#f4a261", "#e63946"],
        labels={"price": "Median Price (KES)", "market": "Market"},
        title=f"{SWAHILI.get(cmp_crop, cmp_crop) if lang=='Kiswahili' else cmp_crop} — "
              f"Price by market (last 3 months)",
    )
    fig2.update_layout(height=min(60 * len(cmp_df) + 80, 500), coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)

    best   = cmp_df.iloc[-1]
    worst  = cmp_df.iloc[0]
    spread = worst["price"] - best["price"]
    if lang == "English":
        st.info(
            f"💡 **Best price to sell:** {worst['market']} — KES {worst['price']:.0f} · "
            f"**Lowest to buy:** {best['market']} — KES {best['price']:.0f} · "
            f"Spread: KES {spread:.0f}",
            icon=None
        )
    else:
        st.info(
            f"💡 **Bei nzuri ya kuuza:** {worst['market']} — KES {worst['price']:.0f} · "
            f"**Bei chini ya kununua:** {best['market']} — KES {best['price']:.0f} · "
            f"Tofauti: KES {spread:.0f}",
            icon=None
        )

# ═══════════════════════════════════════════════════════════
# SECTION 4: FULL DATA TABLE
# ═══════════════════════════════════════════════════════════
with st.expander("📋 Full recent price data (last 60 days)"):
    table = df[df["date"] >= (latest_date - pd.DateOffset(days=60))].copy()
    table = table.sort_values(["date", "market", "commodity"], ascending=[False, True, True])
    st.dataframe(
        table[["date", "admin1", "market", "commodity", "unit",
               "pricetype", "currency", "price", "usdprice"]].rename(columns={
            "date": "Date", "admin1": "County", "market": "Market",
            "commodity": "Commodity", "unit": "Unit",
            "pricetype": "Type", "currency": "CCY",
            "price": "Price", "usdprice": "USD equiv."
        }),
        use_container_width=True, hide_index=True,
    )
    st.caption(
        "Source: WFP Kenya Food Prices · Humanitarian Data Exchange · "
        "CC BY IGO 3.0 · data.humdata.org/dataset/wfp-food-prices-for-kenya"
    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Mazao Intel · Data: WFP Kenya Food Prices via HDX (CC BY IGO 3.0) · "
    "App: CC BY-NC-ND 4.0 · contact@aikungfu.dev · Not affiliated with WFP or HDX"
)
