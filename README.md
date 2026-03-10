# 🌾 Mazao Intel

Agricultural market price intelligence for Kenyan smallholders.

[![CI](https://github.com/gabrielmahia/mazao-intel/actions/workflows/ci.yml/badge.svg)](https://github.com/gabrielmahia/mazao-intel/actions)

**Live:** [mazao-intel.streamlit.app](https://mazao-intel.streamlit.app)

## What it does

Live WFP Kenya food price data for 20+ markets, 30+ commodities — surfaced in plain language.

| Feature | Description |
|---------|-------------|
| 📌 Current prices | Median price per staple crop across markets, flagged if >20% above 12-month average |
| 📈 Trend chart | Price over time by market — monthly median with average baseline |
| 🗺️ Market comparison | Same crop across markets — shows where to sell high and buy low |
| 🇰🇪 Kiswahili | Full bilingual interface |

## Data source

**WFP Kenya Food Prices** via Humanitarian Data Exchange (HDX)  
License: CC BY IGO 3.0  
URL: https://data.humdata.org/dataset/wfp-food-prices-for-kenya  
Fetched live every 6 hours. When unavailable, the app stops and says so — no fabricated fallback.

## IP & Collaboration

© 2026 Gabriel Mahia · CC BY-NC-ND 4.0  
Data: WFP/HDX CC BY IGO 3.0  
Not affiliated with WFP, HDX, or the Government of Kenya.
