import streamlit as st
import os
import subprocess
import sys
import pandas as pd
from pathlib import Path
from src.ingestion.data_generator import generate_synthetic_transactions

# --- THEME COLORS ---
PRIMARY = "#1e3a5c"  # Deep blue
ACCENT = "#00b4d8"   # Cyan accent
BG_DARK = "#181c24"  # Dark background
CARD_BG = "#23293a"  # Card background
SUCCESS = "#2ecc71"
ERROR = "#e74c3c"

st.set_page_config(page_title="Fraud Detection Data Platform", layout="wide", initial_sidebar_state="expanded")

if "analysis_ready" not in st.session_state:
    st.session_state["analysis_ready"] = False
if "last_pipeline_status" not in st.session_state:
    st.session_state["last_pipeline_status"] = "NOT_RUN"

# --- SIDEBAR ---

# --- SIDEBAR ---
with st.sidebar:
    st.markdown(f"""
        <div style='text-align:center;padding:1rem 0;'>
            <img src='https://img.icons8.com/fluency/96/000000/fraud.png' width='64'/><br/>
            <span style='font-size:1.5rem;font-weight:bold;color:{ACCENT}'>Fraud Control Center</span>
        </div>
        <hr style='border:1px solid {ACCENT};margin:0.5rem 0;'>
    """, unsafe_allow_html=True)
    if st.button("🧬 Generate New Data", width="stretch"):
        with st.spinner("Generating synthetic data..."):
            import random
            import time
            row_count = random.randint(46000, 60000)
            df = generate_synthetic_transactions(row_count)
            # Store dataset in session state
            st.session_state["generated_df"] = df
            # Save dataset with timestamp to avoid overwrite
            uploaded_dir = Path("data/uploaded")
            uploaded_dir.mkdir(parents=True, exist_ok=True)
            filename = f"transactions_{int(time.time())}.csv"
            output_path = uploaded_dir / filename
            df.to_csv(output_path, index=False)
            st.session_state["analysis_ready"] = False
            st.session_state["last_pipeline_status"] = "NOT_RUN"
            st.success(f"Generated {row_count} rows of synthetic data.")
    run_pipeline_clicked = st.button("⚡ Run Pipeline", width='stretch')
    if run_pipeline_clicked:
        st.session_state["last_pipeline_status"] = "RUNNING"
        with st.spinner("Running PySpark fraud pipeline..."):
            result = subprocess.run(
                [sys.executable, "pipeline_runner.py", "--mode", "dev"],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                st.session_state["last_pipeline_status"] = "SUCCESS"
                st.session_state["analysis_ready"] = True
                st.success("Pipeline completed successfully.")
            else:
                st.session_state["last_pipeline_status"] = "FAILED"
                st.session_state["analysis_ready"] = False
                st.error("Pipeline execution failed. Please check terminal output.")
    st.markdown(f"<hr style='border:1px solid {ACCENT};margin:0.5rem 0;'>", unsafe_allow_html=True)
    st.header("Navigation")
    nav = st.radio("Go to:", ["Overview", "Data Summary", "Fraud Alerts", "Analytics", "System Health"], label_visibility="collapsed")
    st.markdown(f"<hr style='border:1px solid {ACCENT};margin:0.5rem 0;'>", unsafe_allow_html=True)
    st.caption("Built by Himanshu Shukla (Data Engineer)")

# --- CUSTOM CSS ---
st.markdown(f"""
    <style>
    .block-container {{background: {BG_DARK};}}
    .stApp {{background: {BG_DARK};}}
    .stMetric {{background: {CARD_BG}; border-radius: 12px; padding: 1.2rem; color: #fff;}}
    .stButton>button {{width: 100%; background: {ACCENT}; color: #fff; font-weight: bold; border-radius: 8px;}}
    .stRadio>div>label {{font-size: 1.1rem;}}
    .stDataFrame {{background: {CARD_BG}; color: #fff;}}
    .st-cb {{color: {ACCENT};}}
    .st-bb {{color: {ACCENT};}}
    </style>
    """, unsafe_allow_html=True)

# --- HEADER ---
st.markdown(f"""
    <div style='background:{PRIMARY};padding:2rem 1rem 1rem 1rem;border-radius:18px;margin-bottom:2rem;'>
        <h1 style='color:{ACCENT};margin-bottom:0;'>💳 Fraud Detection Data Platform</h1>
        <span style='color:#fff;font-size:1.2rem;'>Enterprise Data Engineering & Business Intelligence</span>
    </div>
    """, unsafe_allow_html=True)

# --- TOP HIGHLIGHTS (AT THE TOP) ---
st.markdown("<h3 style='color:#fff;margin-top:0;'>Highlights</h3>", unsafe_allow_html=True)
st.markdown(
    """
    <div style='color:#fff;font-size:1rem;'>
    <ul>
        <li>Production-style Medallion pipeline with Bronze, Silver, and Gold layers.</li>
        <li>Realistic fraud scoring using transaction velocity, device changes, and location shifts.</li>
        <li>Interactive fraud monitoring dashboard for KPIs, trends, alerts, and risk analytics.</li>
    </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

# --- PROJECT SUMMARY (BELOW HIGHLIGHTS) ---
st.markdown("<h3 style='color:#fff;margin-top:1rem;'>Project Summary</h3>", unsafe_allow_html=True)
st.markdown(
    """
    <div style='color:#fff;font-size:1rem;'>
    This project simulates an internal fintech fraud monitoring system: data ingestion, cleaning,
    feature engineering, risk scoring, and alert generation are orchestrated through PySpark ETL,
    while Streamlit provides business-facing fraud intelligence and operational visibility.
    </div>
    """,
    unsafe_allow_html=True,
)

# --- CENTERED DATA UPLOAD (FOR USER DATA ANALYSIS) ---
st.markdown("<h3 style='color:#fff;margin-top:1rem;'>Upload Dataset for Analysis</h3>", unsafe_allow_html=True)
upload_left, upload_center, upload_right = st.columns([1, 2, 1])
with upload_center:
    uploaded_file = st.file_uploader(
        "Upload CSV or JSON",
        type=["csv", "json"],
        width="stretch",
        key="top_upload",
    )
    if uploaded_file is not None:
        import time

        uploaded_dir = Path("data/uploaded")
        uploaded_dir.mkdir(parents=True, exist_ok=True)
        timestamp = int(time.time())

        try:
            if uploaded_file.name.lower().endswith(".csv"):
                uploaded_df = pd.read_csv(uploaded_file)
            else:
                uploaded_df = pd.read_json(uploaded_file)

            csv_name = f"uploaded_{timestamp}.csv"
            output_path = uploaded_dir / csv_name
            uploaded_df.to_csv(output_path, index=False)
            st.session_state["uploaded_df"] = uploaded_df
            st.session_state["analysis_ready"] = False
            st.session_state["last_pipeline_status"] = "NOT_RUN"
            st.success(f"Dataset uploaded and saved as {csv_name}")
        except Exception as exc:
            st.error(f"Unable to read uploaded dataset: {exc}")

# --- DISPLAY UPLOADED DATASET ---
if "uploaded_df" in st.session_state:
    st.subheader("Uploaded Dataset Preview")
    st.dataframe(st.session_state["uploaded_df"].head(500), width="stretch")

# --- DISPLAY GENERATED DATASET ---
if "generated_df" in st.session_state:
    st.subheader("Generated Synthetic Dataset")
    df_preview = st.session_state["generated_df"].head(500)
    st.dataframe(df_preview, width="stretch")

# --- LOAD KPIS ---
def load_kpis(gold_dir):
    features_path = os.path.join(gold_dir, "features_gold.parquet")
    alerts_path = os.path.join(gold_dir, "fraud_alerts.parquet")
    metadata_path = os.path.join(gold_dir, "pipeline_metadata.parquet")
    try:
        features = pd.read_parquet(features_path)
        alerts = pd.read_parquet(alerts_path)
        meta = pd.read_parquet(metadata_path)
        kpis = {
            "transactions_processed": len(features),
            "fraud_alerts": len(alerts),
            "fraud_rate": round(len(alerts) / len(features), 4) if len(features) > 0 else 0.0,
            "pipeline_runtime": meta.iloc[-1]["end_time"] if not meta.empty else "-"
        }
        return kpis, alerts, features
    except Exception:
        return None, None, None



# --- PIPELINE STATUS ---
gold_dir = "data/gold"
kpis, alerts, features = (None, None, None)
if st.session_state.get("analysis_ready"):
    kpis, alerts, features = load_kpis(gold_dir)

pipeline_status = st.session_state.get("last_pipeline_status")
status_color = ERROR
if pipeline_status == "SUCCESS":
    status_color = SUCCESS
elif pipeline_status == "RUNNING":
    status_color = ACCENT
elif pipeline_status == "NOT_RUN":
    status_color = ACCENT
elif pipeline_status == "FAILED":
    status_color = ERROR

st.markdown(f"""
<div style='margin-bottom:1rem;'>
    <span style='font-size:1.2rem;font-weight:bold;'>
        <span style='color:{status_color};font-size:1.5rem;'>
            {'🟢' if pipeline_status=='SUCCESS' else '🟡' if pipeline_status in ['RUNNING', 'NOT_RUN'] else '🔴'}
        </span>
        Pipeline Status: <span style='color:{status_color};'>{pipeline_status}</span>
    </span>
</div>
""", unsafe_allow_html=True)

# --- KPI METRIC BAR ---
st.markdown("<h2 style='color:#fff;margin-top:0;'>Fraud Risk Overview</h2>", unsafe_allow_html=True)
if kpis and alerts is not None and features is not None:
    avg_risk_score = alerts["risk_score"].mean() if "risk_score" in alerts.columns else 0
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Transactions", kpis["transactions_processed"])
    kpi2.metric("Fraud Alerts", kpis["fraud_alerts"])
    kpi3.metric("Fraud Rate", f"{round(100*kpis['fraud_rate'],2)}%")
    kpi4.metric("Avg Risk Score", f"{round(avg_risk_score,2)}")

    # --- RISK DISTRIBUTION CHART ---
    st.markdown("<h3 style='color:#fff;margin-top:2rem;'>Fraud Risk Distribution</h3>", unsafe_allow_html=True)
    risk_counts = alerts["risk_level"].value_counts() if "risk_level" in alerts.columns else pd.Series()
    import plotly.express as px
    fig_risk = px.pie(
        values=risk_counts.values,
        names=risk_counts.index,
        color=risk_counts.index,
        color_discrete_map={"LOW":SUCCESS, "MEDIUM":ACCENT, "HIGH":ERROR},
        title="Fraud Risk Distribution"
    )

    # --- FRAUD ALERTS BY LOCATION CHART ---
    st.markdown("<h3 style='color:#fff;margin-top:2rem;'>Fraud Alerts by Location</h3>", unsafe_allow_html=True)
    loc_counts = alerts["location"].value_counts().head(10) if "location" in alerts.columns else pd.Series()
    fig_loc = px.bar(
        x=loc_counts.index,
        y=loc_counts.values,
        labels={"x":"Location", "y":"Fraud Alerts"},
        color=loc_counts.values,
        color_continuous_scale=[SUCCESS, ACCENT, ERROR],
        title="Fraud Alerts by Location"
    )

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.plotly_chart(fig_risk, width='stretch')
    with chart_col2:
        st.plotly_chart(fig_loc, width='stretch')

    # --- FRAUD TREND OVER TIME ---
    st.markdown("<h3 style='color:#fff;margin-top:2rem;'>Fraud Trend Over Time</h3>", unsafe_allow_html=True)
    if "timestamp" in alerts.columns and not alerts.empty:
        alerts_trend = alerts.copy()
        alerts_trend["timestamp"] = pd.to_datetime(alerts_trend["timestamp"], errors="coerce")
        alerts_trend = alerts_trend.dropna(subset=["timestamp"])
        alerts_trend["alert_date"] = alerts_trend["timestamp"].apply(
            lambda x: x.date() if pd.notna(x) else None
        )
        trend_series = alerts_trend.groupby("alert_date")["transaction_id"].count()
        st.line_chart(trend_series, width='stretch')
    else:
        st.info("No timestamp data available to render fraud trend.")

    # --- FRAUD ALERTS TABLE ---
    st.markdown("<h3 style='color:#fff;margin-top:2rem;'>Fraud Alerts Table</h3>", unsafe_allow_html=True)
    # Sort by risk_score descending
    if "risk_score" in alerts.columns:
        alerts_sorted = alerts.sort_values("risk_score", ascending=False)
    else:
        alerts_sorted = alerts
    # Color-code risk levels
    def highlight_risk(row):
        color = SUCCESS if row["risk_level"]=="LOW" else ACCENT if row["risk_level"]=="MEDIUM" else ERROR
        return ["" if col!="risk_level" else f"background-color: {color}; color: #fff;" for col in display_cols]
    display_cols = ["transaction_id", "user_id", "merchant_id", "amount", "location", "device_id", "risk_score", "risk_level"]
    if all(col in alerts_sorted.columns for col in display_cols):
        styled_alerts = alerts_sorted[display_cols].style.apply(highlight_risk, axis=1)
        st.dataframe(styled_alerts, width='stretch')
    else:
        st.dataframe(alerts_sorted, width='stretch')
else:
    st.info("Generate or upload data, then run the pipeline to view KPIs and fraud analytics.")

# --- NAVIGATION ---
if nav == "Overview":
    st.header("Platform Overview")
    st.markdown(f"""
    <div style='color:#fff;font-size:1.1rem;'>
    <b>Fraud Detection Data Platform</b> is a production-grade, enterprise-ready solution designed by a professional Data Engineer for real-world financial and fintech environments.<br><br>
    <ul>
        <li>Robust ingestion, cleaning, and transformation of transaction data using PySpark and scalable medallion architecture (Bronze, Silver, Gold layers).</li>
        <li>Advanced feature engineering, risk scoring, and rule-based fraud detection tailored for Indian banking and payment systems.</li>
        <li>Interactive Streamlit dashboard with recruiter-friendly KPIs, fraud analytics, and alert visualizations.</li>
        <li>Supports CSV, JSON, and other formats; validates uploaded files for schema compliance.</li>
        <li>Modern UI, dark theme, and professional design for real-world data engineering workflows.</li>
        <li>Built for extensibility, auditability, and rapid deployment in enterprise settings.</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)

if nav == "Data Summary" and st.session_state.get("analysis_ready") and kpis:
    st.header("Data Summary")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Transactions Processed", kpis["transactions_processed"])
    kpi2.metric("Fraud Alerts", kpis["fraud_alerts"])
    kpi3.metric("Fraud Rate", kpis["fraud_rate"])
    kpi4.metric("Pipeline Runtime", kpis["pipeline_runtime"])

if nav == "Fraud Alerts" and st.session_state.get("analysis_ready") and alerts is not None:
    st.header("Fraud Alerts Table")
    st.dataframe(alerts, width='stretch')

if nav == "Analytics" and st.session_state.get("analysis_ready") and alerts is not None and features is not None:
    st.header("Fraud Distribution Analytics")
    import plotly.express as px
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.bar_chart(alerts["location"].value_counts().sort_values(ascending=False).head(20), width='stretch')
        st.caption("Fraud Alerts by Location (Top 20)")
        # Pie chart: Payment method distribution
        st.subheader("Payment Method Distribution (Pie Chart)")
        pm_counts = features["payment_method"].value_counts()
        fig_pm = px.pie(values=pm_counts.values, names=pm_counts.index, title="Payment Method Distribution")
        st.plotly_chart(fig_pm, width='stretch')
    with chart_col2:
        st.bar_chart(alerts["merchant_id"].value_counts().sort_values(ascending=False).head(20), width='stretch')
        st.caption("Fraud Alerts by Merchant (Top 20)")
        # Histogram: Transaction amount
        st.subheader("Transaction Amount Histogram")
        fig_hist = px.histogram(features, x="amount", nbins=30, title="Transaction Amount Histogram")
        st.plotly_chart(fig_hist, width='stretch')
    # Line chart: Daily transaction volume
    st.subheader("Daily Transaction Volume (Line Chart)")
    feature_ts = pd.Series(pd.to_datetime(features["timestamp"], errors="coerce"))
    features["date"] = [x.date() if pd.notna(x) else None for x in feature_ts]
    daily_counts = features.groupby("date")["transaction_id"].count()
    st.line_chart(daily_counts, width='stretch')
    # Pie chart: Fraud alert type breakdown
    if "alert_type" in alerts.columns:
        st.subheader("Fraud Alert Type Breakdown (Pie Chart)")
        at_counts = alerts["alert_type"].value_counts()
        fig_at = px.pie(values=at_counts.values, names=at_counts.index, title="Fraud Alert Type Breakdown")
        st.plotly_chart(fig_at, width='stretch')
    # Bar chart: Top users by transaction count
    st.subheader("Top Users by Transaction Count")
    st.bar_chart(features["user_id"].value_counts().head(10), width='stretch')
    # Bar chart: Device usage
    st.subheader("Top Devices by Usage")
    st.bar_chart(features["device_id"].value_counts().head(10), width='stretch')

if nav == "System Health":
    st.header("System Health & Status")
    st.markdown(f"<span style='color:{SUCCESS};font-weight:bold;'>All systems operational.</span>", unsafe_allow_html=True)
    st.markdown(f"<span style='color:{ACCENT};'>Last pipeline run: {kpis['pipeline_runtime'] if kpis else '-'} </span>", unsafe_allow_html=True)

if not st.session_state.get("analysis_ready"):
    st.info("Dashboard analytics are intentionally hidden until you run the pipeline for the current dataset.")
