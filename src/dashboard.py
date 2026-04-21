import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import psycopg2

# ── Connection — works both locally and on Streamlit Cloud ────────────────────
def get_connection():
    try:
        # Streamlit Cloud — reads from secrets manager
        creds = st.secrets["database"]
        return psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            sslmode=creds["sslmode"]
        )
    except (KeyError, FileNotFoundError):
        # Local — reads from .env
        from dotenv import load_dotenv
        load_dotenv()
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            sslmode=os.getenv("DB_SSLMODE", "require")
        )
# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kenya Water Quality Dashboard",
    page_icon="💧",
    layout="wide"
)

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    conn = get_connection()

    df = pd.read_sql("""
        SELECT
            f.year,
            f.value,
            f.value_rural,
            f.value_urban,
            f.is_estimated,
            i.code        AS indicator_code,
            i.name        AS indicator_name,
            i.category,
            i.unit,
            s.name        AS source
        FROM fact_indicator_value f
        JOIN dim_indicator   i ON f.indicator_id = i.indicator_id
        JOIN dim_data_source s ON f.source_id    = s.source_id
        ORDER BY i.code, f.year
    """, conn)

    conn.close()
    return df

@st.cache_data(ttl=3600)
def load_thresholds():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            i.code AS indicator_code,
            t.authority,
            t.min_value,
            t.max_value,
            t.severity,
            t.notes
        FROM dim_threshold t
        JOIN dim_indicator i ON t.indicator_id = i.indicator_id
    """, conn)
    conn.close()
    return df

# ── Load ──────────────────────────────────────────────────────────────────────
df       = load_data()
df_thresh = load_thresholds()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("💧 Kenya Water Quality Dashboard")
st.markdown(
    "Tracking water access and sanitation trends across Kenya (2000–2024). "
    "Data sourced from WHO/UNICEF JMP and World Bank Open Data."
)
st.divider()

# ── Summary metrics ───────────────────────────────────────────────────────────
st.subheader("Current snapshot")

latest_year = df[~df["is_estimated"]]["year"].max()
latest      = df[(df["year"] == latest_year) & (~df["is_estimated"])]

def get_latest(code):
    row = latest[latest["indicator_code"] == code]
    return round(row["value"].values[0], 1) if len(row) else None

basic_water  = get_latest("pct_at_least_basic_water_total")
surface_water = get_latest("pct_surface_water_total")
sanitation   = get_latest("SH.STA.BASS.ZS")
water_stress = get_latest("ER.H2O.FWST.ZS")

col1, col2, col3, col4 = st.columns(4)

with col1:
    val = basic_water or 0
    st.metric(
        label="Basic water access",
        value=f"{val}%",
        delta=None,
        help="% population using at least basic drinking water services"
    )
    if val < 50:
        st.error("Critical threshold")
    elif val < 80:
        st.warning("Below WHO target")
    else:
        st.success("Within target")

with col2:
    val = surface_water or 0
    st.metric(
        label="Surface water use",
        value=f"{val}%",
        help="% population relying on surface water — highest contamination risk"
    )
    if val > 15:
        st.error("Critical — high contamination risk")
    elif val > 5:
        st.warning("Above safe threshold")
    else:
        st.success("Within safe range")

with col3:
    val = sanitation or 0
    st.metric(
        label="Basic sanitation access",
        value=f"{val}%",
        help="% population using at least basic sanitation services"
    )
    if val < 50:
        st.warning("Significant gap")
    else:
        st.success("Acceptable level")

with col4:
    val = water_stress or 0
    st.metric(
        label="Water stress level",
        value=f"{val}%",
        help="Freshwater withdrawal as % of available resources"
    )
    if val > 70:
        st.error("Severely stressed")
    elif val > 25:
        st.warning("Water stressed")
    else:
        st.success("Low stress")

st.caption(f"Latest observed data year: {latest_year}")
st.divider()

# ── Chart 1: Water access trend ───────────────────────────────────────────────
st.subheader("Water access trends over time")

WATER_INDICATORS = [
    "pct_at_least_basic_water_total",
    "pct_limited_water_total",
    "pct_unimproved_water_total",
    "pct_surface_water_total",
]

INDICATOR_LABELS = {
    "pct_at_least_basic_water_total": "At least basic",
    "pct_limited_water_total":        "Limited",
    "pct_unimproved_water_total":     "Unimproved",
    "pct_surface_water_total":        "Surface water",
}

df_trend = df[df["indicator_code"].isin(WATER_INDICATORS)].copy()
df_trend["label"] = df_trend["indicator_code"].map(INDICATOR_LABELS)

# Split observed vs projected
df_observed  = df_trend[~df_trend["is_estimated"]]
df_projected = df_trend[df_trend["is_estimated"]]

fig1 = px.line(
    df_observed,
    x="year", y="value",
    color="label",
    markers=True,
    labels={"value": "% population", "year": "Year", "label": "Service level"},
    color_discrete_sequence=px.colors.qualitative.Set2
)

# Add projected lines as dashed
for label in df_projected["label"].unique():
    proj = df_projected[df_projected["label"] == label]
    fig1.add_scatter(
        x=proj["year"], y=proj["value"],
        mode="lines",
        line=dict(dash="dash"),
        name=f"{label} (projected)",
        showlegend=True
    )

fig1.update_layout(
    xaxis_title="Year",
    yaxis_title="% of population",
    legend_title="Service level",
    hovermode="x unified",
    height=420
)

st.plotly_chart(fig1, use_container_width=True)
st.caption(
    "Dashed lines are JMP regression projections beyond the current year. "
    "Surface water use carries the highest contamination risk — unprotected "
    "sources are vulnerable to faecal, agricultural, and industrial pollution."
)
st.divider()

# ── Chart 2: Rural vs Urban gap ───────────────────────────────────────────────
st.subheader("Rural vs urban access gap")

df_gap = df[
    (df["indicator_code"] == "pct_at_least_basic_water_total") &
    (~df["is_estimated"])
].copy()

df_gap = df_gap.dropna(subset=["value_rural", "value_urban"])

fig2 = go.Figure()

fig2.add_trace(go.Scatter(
    x=df_gap["year"], y=df_gap["value_urban"],
    mode="lines+markers",
    name="Urban",
    line=dict(color="#1D9E75", width=2)
))

fig2.add_trace(go.Scatter(
    x=df_gap["year"], y=df_gap["value_rural"],
    mode="lines+markers",
    name="Rural",
    line=dict(color="#D85A30", width=2)
))

fig2.add_trace(go.Scatter(
    x=pd.concat([df_gap["year"], df_gap["year"].iloc[::-1]]),
    y=pd.concat([df_gap["value_urban"], df_gap["value_rural"].iloc[::-1]]),
    fill="toself",
    fillcolor="rgba(200,200,200,0.2)",
    line=dict(color="rgba(255,255,255,0)"),
    name="Gap",
    showlegend=True
))

fig2.update_layout(
    xaxis_title="Year",
    yaxis_title="% of population",
    legend_title="Setting",
    hovermode="x unified",
    height=420
)

st.plotly_chart(fig2, use_container_width=True)

# Calculate current gap
if len(df_gap):
    latest_gap = df_gap[df_gap["year"] == df_gap["year"].max()].iloc[0]
    gap_size   = round(latest_gap["value_urban"] - latest_gap["value_rural"], 1)
    st.info(
        f"The rural-urban gap in basic water access is currently "
        f"**{gap_size} percentage points**. Rural communities are "
        f"disproportionately reliant on unimproved and surface water sources, "
        f"which carry significantly higher contamination risk."
    )

st.divider()

# ── Chart 3: Sanitation and water stress ─────────────────────────────────────
st.subheader("Sanitation coverage and water stress")

col_a, col_b = st.columns(2)

with col_a:
    df_san = df[
        (df["indicator_code"] == "SH.STA.BASS.ZS") &
        (~df["is_estimated"])
    ].sort_values("year")

    fig3 = px.area(
        df_san, x="year", y="value",
        labels={"value": "% population", "year": "Year"},
        color_discrete_sequence=["#1D9E75"]
    )
    fig3.update_layout(
        yaxis_title="% of population",
        height=340,
        showlegend=False
    )
    st.plotly_chart(fig3, use_container_width=True)
    st.caption("Basic sanitation coverage (World Bank, 2000–2024)")

with col_b:
    df_stress = df[
        (df["indicator_code"] == "ER.H2O.FWST.ZS") &
        (~df["is_estimated"])
    ].sort_values("year")

    fig4 = px.line(
        df_stress, x="year", y="value",
        markers=True,
        labels={"value": "% of available freshwater", "year": "Year"},
        color_discrete_sequence=["#D85A30"]
    )
    fig4.add_hline(
        y=25, line_dash="dash", line_color="orange",
        annotation_text="Water stress threshold (25%)"
    )
    fig4.add_hline(
        y=70, line_dash="dash", line_color="red",
        annotation_text="Severe stress threshold (70%)"
    )
    fig4.update_layout(height=340, showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)
    st.caption(
        "Water stress level (World Bank, 2000–2022). "
        "Kenya has been above the 25% stress threshold throughout this period."
    )

st.divider()

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    "**Data sources:** WHO/UNICEF JMP · World Bank Open Data  \n"
    "**Thresholds:** WHO Guidelines for Drinking-water Quality · "
    "Kenya Bureau of Standards (KEBS)  \n"
    "Built By Boniface Kibet Data Engineering Portfolio"
)