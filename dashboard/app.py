"""
Streamlit Analytics Dashboard (Phase 6)
Real-time and Historical Stock Market Analytics Engine

Architecture:
- LIVE View: Reads Delta Lake tables directly via delta-rs (no Spark session).
  Auto-refreshes every 5 seconds using st.fragment(run_every=5).
- HISTORICAL View: Reads Warehouse (Amazon Redshift or local DuckDB) via parameterized queries.
- PIPELINE HEALTH: Monitors throughput, latency, consumer lag, and watermark freshness with stalled pipeline detection.
"""

from datetime import datetime, timezone, timedelta
import os
import sys
from pathlib import Path
from typing import Optional

# Ensure project root is in sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

import dashboard.data as data_layer
from config import WAREHOUSE_TARGET, DELTA_BASE_PATH

# ---------------------------------------------------------------------------
# Page Configuration & Aesthetics
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Real-Time Stock Market Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Apple-inspired dark mode styling
CUSTOM_CSS = """
<style>
    /* Dark glassmorphism & minimal typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background-color: #0d1117;
        color: #e6edf3;
    }

    /* Metric Cards */
    .metric-card {
        background: rgba(22, 27, 34, 0.7);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(48, 54, 61, 0.8);
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 12px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.25);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: rgba(88, 166, 255, 0.4);
    }
    .metric-label {
        font-size: 0.78rem;
        font-weight: 500;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: #f0f6fc;
        line-height: 1.1;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: #58a6ff;
        margin-top: 4px;
    }

    /* Live Badge */
    .badge-live {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: rgba(46, 160, 67, 0.15);
        color: #3fb950;
        border: 1px solid rgba(46, 160, 67, 0.35);
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.04em;
    }
    .badge-pulse {
        width: 8px;
        height: 8px;
        background-color: #3fb950;
        border-radius: 50%;
        box-shadow: 0 0 0 0 rgba(63, 185, 80, 0.7);
        animation: pulse 1.8s infinite;
    }
    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(63, 185, 80, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(63, 185, 80, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(63, 185, 80, 0); }
    }

    /* Stalled Alert Banner */
    .stalled-banner {
        background: rgba(248, 81, 73, 0.12);
        border: 1px solid rgba(248, 81, 73, 0.4);
        color: #f85149;
        border-radius: 10px;
        padding: 12px 18px;
        margin-bottom: 20px;
        display: flex;
        align-items: center;
        gap: 12px;
        font-size: 0.88rem;
    }
    .healthy-banner {
        background: rgba(46, 160, 67, 0.12);
        border: 1px solid rgba(46, 160, 67, 0.4);
        color: #3fb950;
        border-radius: 10px;
        padding: 12px 18px;
        margin-bottom: 20px;
        display: flex;
        align-items: center;
        gap: 12px;
        font-size: 0.88rem;
    }

    /* Clean table adjustments */
    .dataframe {
        border-radius: 8px !important;
        overflow: hidden;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Plotly Helpers
# ---------------------------------------------------------------------------

def create_candlestick_chart(df: pd.DataFrame, ticker: str, granularity: str) -> go.Figure:
    """Build high-contrast candlestick chart with volume bars on a synchronized subplot."""
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=[0.75, 0.25],
    )

    # 1. Candlestick Trace
    fig.add_trace(
        go.Candlestick(
            x=df["window_start"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="OHLC",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
            increasing_fillcolor="#26a69a",
            decreasing_fillcolor="#ef5350",
        ),
        row=1,
        col=1,
    )

    # 2. Volume Bars Trace
    colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df["close"], df["open"])]
    fig.add_trace(
        go.Bar(
            x=df["window_start"],
            y=df["volume"],
            name="Volume",
            marker_color=colors,
            opacity=0.6,
        ),
        row=2,
        col=1,
    )

    # Styling & Layout
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(13, 17, 23, 0.6)",
        paper_bgcolor="rgba(13, 17, 23, 0)",
        margin=dict(l=20, r=20, t=30, b=20),
        height=520,
        showlegend=False,
        xaxis_rangeslider_visible=False,
        xaxis=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        yaxis=dict(
            title="Price ($)",
            showgrid=True,
            gridcolor="rgba(48, 54, 61, 0.4)",
            side="right",
        ),
        xaxis2=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        yaxis2=dict(
            title="Vol",
            showgrid=False,
            side="right",
        ),
    )
    return fig


def create_historical_chart(df: pd.DataFrame, tickers: list) -> go.Figure:
    """Build multi-ticker close-price lines with combined volume bars."""
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=[0.75, 0.25],
    )

    color_palette = [
        "#58a6ff", "#f0883e", "#a371f7", "#3fb950", "#f85149",
        "#db61a2", "#388bfd", "#e3b341", "#79c0ff", "#7ee787"
    ]

    for i, t in enumerate(tickers):
        tdf = df[df["ticker"] == t].sort_values(by="window_start")
        if tdf.empty:
            continue
        c = color_palette[i % len(color_palette)]
        fig.add_trace(
            go.Scatter(
                x=tdf["window_start"],
                y=tdf["close"],
                mode="lines",
                name=t,
                line=dict(color=c, width=2),
            ),
            row=1,
            col=1,
        )

    # Combined volume
    vol_df = df.groupby("window_start")["volume"].sum().reset_index()
    fig.add_trace(
        go.Bar(
            x=vol_df["window_start"],
            y=vol_df["volume"],
            name="Aggregate Volume",
            marker_color="rgba(88, 166, 255, 0.4)",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(13, 17, 23, 0.6)",
        paper_bgcolor="rgba(13, 17, 23, 0)",
        margin=dict(l=20, r=20, t=30, b=20),
        height=500,
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        yaxis=dict(title="Close Price ($)", showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)", side="right"),
        xaxis2=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        yaxis2=dict(title="Volume", showgrid=False, side="right"),
    )
    return fig


# ---------------------------------------------------------------------------
# Cached Data Loaders (TTL = 5 seconds)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=5)
def get_cached_live_summary_metrics() -> dict:
    return data_layer.get_live_summary_metrics()


@st.cache_data(ttl=5)
def get_cached_live_candles(table_name: str, ticker: Optional[str] = None) -> pd.DataFrame:
    return data_layer.get_live_candles(table_name=table_name, ticker=ticker)


@st.cache_data(ttl=5)
def get_cached_live_top_movers(table_name: str) -> pd.DataFrame:
    return data_layer.get_live_top_movers(table_name=table_name)


# ---------------------------------------------------------------------------
# Live Fragment Component (Auto-refreshes every 5 seconds)
# ---------------------------------------------------------------------------

@st.fragment(run_every=5)
def render_live_tab():
    """Live tab fragment: auto-refreshes every 5s without reloading the whole page."""
    # Top Status Bar
    col_head_left, col_head_right = st.columns([3, 1])
    with col_head_left:
        st.markdown(
            """
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
                <h2 style="margin: 0; font-weight: 700; font-size: 1.5rem;">Live Market Stream</h2>
                <div class="badge-live"><div class="badge-pulse"></div> LIVE (Delta Direct)</div>
            </div>
            <p style="color: #8b949e; font-size: 0.85rem; margin-bottom: 16px;">
                Direct streaming insights from Delta Lake storage • Auto-refreshing every 5 seconds
            </p>
            """,
            unsafe_allow_html=True,
        )
    with col_head_right:
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        st.markdown(
            f"""
            <div style="text-align: right; color: #8b949e; font-size: 0.8rem; margin-top: 6px;">
                Last Refreshed: <span style="color: #e6edf3; font-weight: 600;">{now_str}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 1. Metric Cards Row
    metrics = get_cached_live_summary_metrics()


    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Events / Second</div>
                <div class="metric-value">{metrics['events_per_sec']:,.1f}</div>
                <div class="metric-sub">⚡ Processing Throughput</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m2:
        lag_color = "#3fb950" if metrics["consumer_lag"] == 0 else "#f85149"
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Consumer Lag</div>
                <div class="metric-value" style="color: {lag_color};">{metrics['consumer_lag']}</div>
                <div class="metric-sub">Kafka Offsets Behind</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Active Tickers</div>
                <div class="metric-value">{metrics['active_tickers']}</div>
                <div class="metric-sub">Streaming Windows</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m4:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Last Batch Duration</div>
                <div class="metric-value">{metrics['last_batch_duration_ms']:,.0f} <span style="font-size: 1rem; color: #8b949e;">ms</span></div>
                <div class="metric-sub">Trigger: 5,000 ms</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 2. Controls: Granularity & Ticker Selector
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 2])
    with ctrl_col1:
        granularity = st.radio(
            "Window Granularity",
            ["5-Minute (agg_5min)", "1-Hour (agg_1hour)"],
            horizontal=True,
            key="live_granularity",
        )
        table_name = "agg_5min" if "5-Minute" in granularity else "agg_1hour"

    # Fetch data safely
    try:
        candles_df = get_cached_live_candles(table_name=table_name)
    except FileNotFoundError:
        st.warning(f"Delta table '{table_name}' has not been created yet. Ensure PySpark Streaming job is running.")
        return
    except Exception as e:
        st.error(f"Error reading live Delta table: {str(e)}")
        return

    if candles_df.empty:
        st.info("No candle records found in the Delta table yet. Producer and streaming pipeline may be starting up.")
        return

    available_tickers = sorted(candles_df["ticker"].unique().tolist())
    with ctrl_col2:
        selected_ticker = st.selectbox(
            "Select Ticker",
            available_tickers,
            index=0 if "AAPL" not in available_tickers else available_tickers.index("AAPL"),
            key="live_ticker",
        )

    # 3. Candlestick Chart for Selected Ticker
    ticker_df = candles_df[candles_df["ticker"] == selected_ticker].sort_values(by="window_start")
    if not ticker_df.empty:
        latest_candle = ticker_df.iloc[-1]
        st.markdown(
            f"""
            <div style="display: flex; align-items: baseline; gap: 16px; margin: 12px 0 6px 0;">
                <span style="font-size: 1.25rem; font-weight: 700; color: #f0f6fc;">{selected_ticker}</span>
                <span style="color: #8b949e; font-size: 0.9rem;">{latest_candle.get('company_name', '')} • {latest_candle.get('sector', '')}</span>
                <span style="font-size: 1.25rem; font-weight: 700; color: #58a6ff;">${latest_candle['close']:.2f}</span>
                <span style="color: #8b949e; font-size: 0.8rem;">Vol: {int(latest_candle['volume']):,}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        fig = create_candlestick_chart(ticker_df, selected_ticker, granularity)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No candle data available for {selected_ticker}.")

    # 4. Top Movers Table
    st.markdown("### Top Movers & Performance")
    try:
        movers_df = get_cached_live_top_movers(table_name=table_name)
        if not movers_df.empty:
            change_label = movers_df["change_type"].iloc[0]

            def style_pct(val):
                color = "#3fb950" if val >= 0 else "#f85149"
                return f"color: {color}; font-weight: 600;"

            display_df = movers_df[["ticker", "company_name", "sector", "last_price", "pct_change", "volume"]].copy()
            display_df.columns = ["Ticker", "Company", "Sector", "Last Price ($)", f"{change_label} (%)", "Volume"]

            st.dataframe(
                display_df.style.format({
                    "Last Price ($)": "${:.2f}",
                    f"{change_label} (%)": "{:+.2f}%",
                    "Volume": "{:,}",
                }).map(style_pct, subset=[f"{change_label} (%)"]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Awaiting window completion to calculate top movers.")
    except Exception as e:
        st.warning(f"Could not compute top movers: {str(e)}")


# ---------------------------------------------------------------------------
# Historical Tab Component (Warehouse Ingestion - Parameterized Queries)
# ---------------------------------------------------------------------------

def render_historical_tab():
    """Historical tab: queries Redshift or DuckDB with parameterized filters."""
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
            <h2 style="margin: 0; font-weight: 700; font-size: 1.5rem;">Warehouse Historical Analytics</h2>
            <span style="background: rgba(88, 166, 255, 0.15); color: #58a6ff; border: 1px solid rgba(88, 166, 255, 0.3); padding: 4px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600;">
                Target: {WAREHOUSE_TARGET.upper()} (Batch Staged)
            </span>
        </div>
        <p style="color: #8b949e; font-size: 0.85rem; margin-bottom: 16px;">
            Audited, finalized window data loaded into warehouse via Lambda batch orchestration.
        </p>
        """,
        unsafe_allow_html=True,
    )

    # Test warehouse connection
    healthy, msg = data_layer.test_warehouse_connection()
    if not healthy:
        st.error(f"Warehouse Unreachable: {msg}. Please ensure your warehouse is running and exported batches have loaded.")
        return

    meta = data_layer.get_warehouse_metadata()
    if meta["total_rows"] == 0:
        st.info("Warehouse tables are currently empty. Run `python -m batch.export_finalized --once` to stage and load exported batches.")
        return

    # Filter Sidebar / Controls
    f_col1, f_col2, f_col3, f_col4 = st.columns([2, 2, 2, 2])
    with f_col1:
        granularity = st.selectbox("Granularity", ["agg_5min", "agg_1hour"], index=0, key="hist_granularity")
    with f_col2:
        sectors_selected = st.multiselect("Filter Sector", meta["sectors"], default=None, key="hist_sectors")
    with f_col3:
        tickers_selected = st.multiselect("Filter Tickers", meta["tickers"], default=meta["tickers"][:4], key="hist_tickers")
    with f_col4:
        min_d = meta["min_date"].date() if meta["min_date"] else datetime.now().date()
        max_d = meta["max_date"].date() if meta["max_date"] else datetime.now().date()
        date_range = st.date_input(
            "Date Range",
            value=(min_d, max_d),
            min_value=min_d,
            max_value=max_d,
            key="hist_dates",
        )

    # Parse dates
    start_d_str = None
    end_d_str = None
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d_str = f"{date_range[0]} 00:00:00"
        end_d_str = f"{date_range[1]} 23:59:59"

    # Query warehouse
    try:
        hist_df = data_layer.query_warehouse_candles(
            table_name=granularity,
            tickers=tickers_selected if tickers_selected else None,
            start_date=start_d_str,
            end_date=end_d_str,
            sectors=sectors_selected if sectors_selected else None,
        )
    except Exception as e:
        st.error(f"Failed to query warehouse: {str(e)}")
        return

    if hist_df.empty:
        st.warning("No warehouse records match the selected filter criteria.")
        return

    # Historical Multi-line chart
    st.markdown("#### Price Trend & Cumulative Volume")
    active_display_tickers = tickers_selected if tickers_selected else hist_df["ticker"].unique().tolist()[:5]
    fig = create_historical_chart(hist_df, active_display_tickers)
    st.plotly_chart(fig, use_container_width=True)

    # Data Table with CSV Export
    st.markdown("#### Audited Warehouse Records")
    col_tbl_left, col_tbl_right = st.columns([3, 1])
    with col_tbl_left:
        st.caption(f"Displaying {len(hist_df):,} finalized window records from {WAREHOUSE_TARGET.upper()}.")
    with col_tbl_right:
        csv_data = hist_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name=f"warehouse_{granularity}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    st.dataframe(
        hist_df[[
            "ticker", "company_name", "sector", "window_start", "window_end",
            "open", "high", "low", "close", "volume", "trade_count"
        ]].style.format({
            "open": "${:.2f}",
            "high": "${:.2f}",
            "low": "${:.2f}",
            "close": "${:.2f}",
            "volume": "{:,}",
            "trade_count": "{:,}",
        }),
        use_container_width=True,
        hide_index=True,
        height=320,
    )


# ---------------------------------------------------------------------------
# Pipeline Health Tab Component
# ---------------------------------------------------------------------------

def render_pipeline_health_tab():
    """Pipeline Health tab: throughput, consumer lag, batch duration, and freshness monitoring."""
    st.markdown(
        """
        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
            <h2 style="margin: 0; font-weight: 700; font-size: 1.5rem;">Pipeline Health & Observability</h2>
            <span style="background: rgba(163, 113, 247, 0.15); color: #a371f7; border: 1px solid rgba(163, 113, 247, 0.3); padding: 4px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600;">
                CloudWatch & Listener Metrics
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    metrics = data_layer.get_live_summary_metrics()

    # Stalled Pipeline Banner (< 2 minutes threshold)
    if metrics["is_stalled"]:
        fresh_mins = metrics["data_freshness_seconds"] / 60.0
        st.markdown(
            f"""
            <div class="stalled-banner">
                <span style="font-size: 1.4rem;">⚠️</span>
                <div>
                    <strong>PIPELINE STALLED:</strong> No new finalized windows emitted in 
                    <strong>{fresh_mins:.1f} minutes</strong> ({metrics['data_freshness_seconds']:.0f}s). 
                    Threshold is 120s (2 minutes). Last recorded window end: <strong>{metrics['max_window_end']}</strong>.
                    <br><span style="font-size: 0.8rem; opacity: 0.9;">Ensure Kafka broker, Trade Producer, and PySpark Streaming jobs are running.</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""
            <div class="healthy-banner">
                <span style="font-size: 1.4rem;">✅</span>
                <div>
                    <strong>PIPELINE HEALTHY:</strong> Data freshness is 
                    <strong>{metrics['data_freshness_seconds']:.1f}s</strong> (&le; 120s limit). 
                    Watermarks are actively advancing. Latest window end: <strong>{metrics['max_window_end']}</strong>.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Fetch Metrics Timeseries
    metrics_df = data_layer.get_pipeline_metrics_history(limit=200)

    if metrics_df.empty:
        st.info("No pipeline metrics logged yet. Start the streaming pipeline to generate metrics logs.")
        return

    # Charts: Throughput & Consumer Lag
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Processing Throughput (Rows / Second)")
        fig_tp = go.Figure()
        for q in metrics_df["query_name"].unique():
            q_df = metrics_df[metrics_df["query_name"] == q]
            fig_tp.add_trace(
                go.Scatter(
                    x=q_df["timestamp"],
                    y=q_df["processed_rows_per_second"],
                    mode="lines+markers",
                    name=q,
                )
            )
        fig_tp.update_layout(
            template="plotly_dark",
            plot_bgcolor="rgba(13, 17, 23, 0.6)",
            paper_bgcolor="rgba(13, 17, 23, 0)",
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            yaxis=dict(title="Rows/Sec", showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
            xaxis=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        )
        st.plotly_chart(fig_tp, use_container_width=True)

    with c2:
        st.markdown("#### Kafka Consumer Lag (Offsets Behind)")
        fig_lag = go.Figure()
        for q in metrics_df["query_name"].unique():
            q_df = metrics_df[metrics_df["query_name"] == q]
            fig_lag.add_trace(
                go.Scatter(
                    x=q_df["timestamp"],
                    y=q_df["kafka_consumer_lag"],
                    mode="lines",
                    name=f"{q} Lag",
                )
            )
        fig_lag.update_layout(
            template="plotly_dark",
            plot_bgcolor="rgba(13, 17, 23, 0.6)",
            paper_bgcolor="rgba(13, 17, 23, 0)",
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            yaxis=dict(title="Offset Lag", showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
            xaxis=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        )
        st.plotly_chart(fig_lag, use_container_width=True)

    # Chart: Batch Duration vs Trigger Interval
    st.markdown("#### Micro-Batch Execution Duration (ms)")
    fig_dur = go.Figure()
    for q in metrics_df["query_name"].unique():
        q_df = metrics_df[metrics_df["query_name"] == q]
        fig_dur.add_trace(
            go.Scatter(
                x=q_df["timestamp"],
                y=q_df["batch_duration_ms"],
                mode="lines",
                name=q,
            )
        )
    # Add 5,000 ms trigger threshold reference
    fig_dur.add_hline(
        y=5000,
        line_dash="dash",
        line_color="#f85149",
        annotation_text="5,000ms Trigger Interval (Falling Behind Threshold)",
        annotation_position="top right",
    )
    fig_dur.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(13, 17, 23, 0.6)",
        paper_bgcolor="rgba(13, 17, 23, 0)",
        height=320,
        margin=dict(l=20, r=20, t=20, b=20),
        yaxis=dict(title="Duration (ms)", showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
        xaxis=dict(showgrid=True, gridcolor="rgba(48, 54, 61, 0.4)"),
    )
    st.plotly_chart(fig_dur, use_container_width=True)

    # Recent Audit & Micro-batch Events Table
    st.markdown("#### Recent Micro-batch Logs")
    disp_cols = [c for c in ["timestamp", "query_name", "batch_id", "num_input_rows", "processed_rows_per_second", "batch_duration_ms", "kafka_consumer_lag", "falling_behind"] if c in metrics_df.columns]
    st.dataframe(
        metrics_df.tail(20).sort_values(by="timestamp", ascending=False)[disp_cols],
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# Main Application Shell
# ---------------------------------------------------------------------------

def main():
    # Sidebar
    with st.sidebar:
        st.markdown(
            """
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px;">
                <span style="font-size: 1.8rem;">📊</span>
                <div>
                    <h3 style="margin: 0; font-size: 1.1rem; font-weight: 700;">Stock Analytics</h3>
                    <span style="font-size: 0.75rem; color: #8b949e;">Phase 6 Dashboard</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown("**System Architecture**")
        st.caption(
            "• **Live**: Delta Lake (delta-rs direct)<br>"
            f"• **Warehouse**: {WAREHOUSE_TARGET.upper()}<br>"
            f"• **Storage**: `{DELTA_BASE_PATH}`",
            unsafe_allow_html=True,
        )
        st.markdown("---")
        st.markdown("**Navigation Guide**")
        st.caption(
            "• **Live Stream**: 5s auto-refresh, candlestick + volume, top-movers.<br>"
            "• **Historical**: Warehouse multi-select query, CSV export.<br>"
            "• **Pipeline Health**: Consumer lag, throughput, stalled alerts.",
            unsafe_allow_html=True,
        )

    # Top Tabs
    tab_live, tab_hist, tab_health = st.tabs([
        "🟢 Live Market Stream",
        "🏛️ Warehouse Historical",
        "⚡ Pipeline Health",
    ])

    with tab_live:
        render_live_tab()

    with tab_hist:
        render_historical_tab()

    with tab_health:
        render_pipeline_health_tab()


if __name__ == "__main__":
    main()
