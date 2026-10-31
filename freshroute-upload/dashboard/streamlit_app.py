"""FreshRoute dashboard - reads GOLD only, with the read-only analyst role.

Local:      streamlit run dashboard/streamlit_app.py     (same .env variables, SNOWFLAKE_ROLE=FRESHROUTE_ANALYST)
In Snowflake: Snowsight -> Projects -> Streamlit -> paste this file (then use get_active_session()).
"""
import os

import pandas as pd
import snowflake.connector
import streamlit as st

st.set_page_config(page_title="FreshRoute - delivery performance", layout="wide")


@st.cache_resource
def conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "FRESHROUTE_ANALYST"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "FRESHROUTE_WH"), database="FRESHROUTE", schema="GOLD")


@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    df = pd.read_sql(sql, conn())
    df.columns = [c.lower() for c in df.columns]
    return df


perf = q("SELECT * FROM GOLD_DELIVERY_PERFORMANCE_DAILY ORDER BY business_date")
weather = q("SELECT * FROM GOLD_WEATHER_IMPACT")
revenue = q("SELECT * FROM GOLD_REVENUE_WEEKLY ORDER BY week_start")

st.title("FreshRoute Logistics - are we keeping our delivery promise?")
st.caption("Source: FRESHROUTE.GOLD (dbt). Business day = promised day, London time. Target OTIF 85 %.")

c1, c2, c3 = st.columns(3)
c1.metric("OTIF (all depots)", f"{perf.delivered_on_time.sum() / perf.deliveries_due.sum():.1%}")
c2.metric("Billed fees (GBP)", f"£{revenue.delivery_fee_gbp.sum():,.0f}")
c3.metric("Lost on failed attempts (EUR)", f"€{revenue.lost_fee_eur.sum():,.0f}")

st.subheader("UC1 - Which depot misses its promise? (OTIF by depot)")
by_depot = perf.groupby(["depot_id", "city"], as_index=False)[["deliveries_due", "delivered_on_time"]].sum()
by_depot["otif"] = by_depot.delivered_on_time / by_depot.deliveries_due
by_depot["depot"] = by_depot.city + " #" + by_depot.depot_id.astype(str)
st.bar_chart(by_depot.set_index("depot")["otif"])

st.subheader("UC2 - Does rain hurt delivery success?")
st.bar_chart(weather.pivot_table(index="rain_category", values="success_rate", aggfunc="mean"))

st.subheader("UC3 - Weekly billed vs lost fees (EUR)")
st.line_chart(revenue.groupby("week_start")[["delivery_fee_eur", "lost_fee_eur"]].sum())
