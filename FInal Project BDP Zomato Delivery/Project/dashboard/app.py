import streamlit as st
import json, os, time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

DASHBOARD_DIR = os.getenv("DASHBOARD_DIR", "/app/dashboard_data")
SNAPSHOT_PATH = f"{DASHBOARD_DIR}/latest_snapshot.json"
HISTORY_PATH  = f"{DASHBOARD_DIR}/history.jsonl"
BATCH_PATH    = f"{DASHBOARD_DIR}/batch_results.json"

st.set_page_config(
    page_title="Zomato Delivery Dashboard",
    page_icon="🛵",
    layout="wide"
)

st.title("🛵 Zomato Delivery — Big Data Pipeline Dashboard")
st.caption("Final Project Big Data Processing")

tab_realtime, tab_batch = st.tabs(["📡 Real-time Metrics", "📊 Batch Insights"])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — REAL-TIME
# ════════════════════════════════════════════════════════════════════════════
with tab_realtime:

    if not os.path.exists(SNAPSHOT_PATH):
        st.warning("⏳ Menunggu data dari Spark Streaming... Pastikan streaming_job.py sudah berjalan.")
    else:
        with open(SNAPSHOT_PATH) as f:
            snap = json.load(f)

        # ── KPI Cards ───────────────────────────────────────────────────────
        st.subheader("📈 Live KPI")
        c1, c2, c3 = st.columns(3)
        c1.metric(
            label="🔢 Total Orders (batch ini)",
            value=snap.get("total_orders", 0)
        )
        c2.metric(
            label="⏱️ Avg Delivery Time",
            value=f"{snap.get('avg_delivery_time', 0)} menit"
        )
        c3.metric(
            label="🔄 Batch ID",
            value=snap.get("batch_id", "-")
        )

        st.divider()

        # ── Row 2: Distribusi Kota + Traffic ────────────────────────────────
        col_city, col_traffic = st.columns(2)

        with col_city:
            st.subheader("🏙️ Distribusi Order per Kota")
            city_data = snap.get("by_city", [])
            if city_data:
                df_city = pd.DataFrame(city_data)
                fig_city = px.bar(
                    df_city,
                    x="City",
                    y="order_count",
                    color="City",
                    text="order_count",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    labels={"order_count": "Jumlah Order", "City": "Kota"}
                )
                fig_city.update_traces(textposition="outside")
                fig_city.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig_city, use_container_width=True)
                st.dataframe(df_city, use_container_width=True, hide_index=True)
            else:
                st.info("Belum ada data kota.")

        with col_traffic:
            st.subheader("🚦 Traffic Condition Live")
            traffic_data = snap.get("by_traffic", [])
            if traffic_data:
                df_traffic = pd.DataFrame(traffic_data)

                # Warna per traffic density
                color_map = {
                    "Low":    "#2ecc71",
                    "Medium": "#f39c12",
                    "High":   "#e74c3c",
                    "Jam":    "#8e44ad",
                }
                df_traffic["color"] = df_traffic["Road_traffic_density"].map(
                    lambda x: color_map.get(x, "#95a5a6")
                )

                fig_traffic = px.bar(
                    df_traffic,
                    x="Road_traffic_density",
                    y="order_count",
                    color="Road_traffic_density",
                    text="order_count",
                    color_discrete_map=color_map,
                    labels={
                        "order_count": "Jumlah Order",
                        "Road_traffic_density": "Traffic Density"
                    }
                )
                fig_traffic.update_traces(textposition="outside")
                fig_traffic.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig_traffic, use_container_width=True)
                st.dataframe(
                    df_traffic[["Road_traffic_density", "order_count", "avg_time"]],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Belum ada data traffic.")

        st.divider()

        # ── Tren Historis ────────────────────────────────────────────────────
        st.subheader("📉 Tren Order & Avg Delivery Time per Batch")
        if os.path.exists(HISTORY_PATH):
            records = []
            with open(HISTORY_PATH) as f:
                for line in f:
                    try:
                        records.append(json.loads(line.strip()))
                    except Exception:
                        pass
            if records:
                df_hist = pd.DataFrame(records)
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Scatter(
                    x=df_hist["batch_id"],
                    y=df_hist["total_orders"],
                    name="Total Orders",
                    mode="lines+markers",
                    line=dict(color="#3498db", width=2)
                ))
                fig_hist.add_trace(go.Scatter(
                    x=df_hist["batch_id"],
                    y=df_hist["avg_delivery_time"],
                    name="Avg Delivery Time (min)",
                    mode="lines+markers",
                    line=dict(color="#e74c3c", width=2),
                    yaxis="y2"
                ))
                fig_hist.update_layout(
                    xaxis_title="Batch ID",
                    yaxis=dict(title="Total Orders", side="left"),
                    yaxis2=dict(title="Avg Time (min)", overlaying="y", side="right"),
                    legend=dict(x=0, y=1),
                    height=350
                )
                st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("History belum tersedia. Streaming job baru saja dimulai.")

# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — BATCH
# ════════════════════════════════════════════════════════════════════════════
with tab_batch:
    st.subheader("📊 Batch Insight — Faktor yang Mempengaruhi Waktu Delivery")
    st.write(f"🔍 DEBUG: BATCH_PATH = {BATCH_PATH}")
    st.write(f"🔍 DEBUG: File exists = {os.path.exists(BATCH_PATH)}")

    if not os.path.exists(BATCH_PATH):
        st.warning(
            "⏳ File `batch_results.json` belum ada. "
            "Jalankan `batch_analysis.py` via spark-submit terlebih dahulu."
        )
        st.code(
            "docker exec -it spark-master /opt/spark/bin/spark-submit \\\n"
            "  --master spark://spark-master:7077 \\\n"
            "  /opt/jobs/batch_analysis.py",
            language="bash"
        )
    else:
        st.write("✅ File found! Loading...")
        with open(BATCH_PATH) as f:
            batch = json.load(f)

        st.write(f"✅ Loaded! Keys: {list(batch.keys())}")
        st.metric("📦 Total Orders dalam Dataset", batch.get("total_orders", 0))
        st.divider()

        # ── 1. Bar chart: Traffic vs Avg Time ───────────────────────────────
        st.markdown("### 🚦 Avg Delivery Time per Traffic Density")
        df_t = pd.DataFrame(batch.get("by_traffic", []))
        if not df_t.empty:
            color_map = {"Low": "#2ecc71", "Medium": "#f39c12",
                         "High": "#e74c3c", "Jam": "#8e44ad"}
            fig_t = px.bar(
                df_t.sort_values("avg_delivery_time_min", ascending=True),
                x="avg_delivery_time_min",
                y="Road_traffic_density",
                orientation="h",
                color="Road_traffic_density",
                color_discrete_map=color_map,
                text="avg_delivery_time_min",
                labels={
                    "avg_delivery_time_min": "Avg Delivery Time (menit)",
                    "Road_traffic_density": "Traffic Density"
                }
            )
            fig_t.update_traces(textposition="outside")
            fig_t.update_layout(showlegend=False, height=300)
            col1, col2 = st.columns([2, 1])
            with col1:
                st.plotly_chart(fig_t, use_container_width=True)
            with col2:
                st.dataframe(df_t, use_container_width=True, hide_index=True)

        # ── 2. Bar chart: Cuaca vs Avg Time ─────────────────────────────────
        st.markdown("### ☁️ Avg Delivery Time per Kondisi Cuaca")
        df_w = pd.DataFrame(batch.get("by_weather", []))
        if not df_w.empty:
            fig_w = px.bar(
                df_w.sort_values("avg_delivery_time_min", ascending=True),
                x="avg_delivery_time_min",
                y="Weather_conditions",
                orientation="h",
                color="avg_delivery_time_min",
                color_continuous_scale="Blues",
                text="avg_delivery_time_min",
                labels={
                    "avg_delivery_time_min": "Avg Delivery Time (menit)",
                    "Weather_conditions": "Cuaca"
                }
            )
            fig_w.update_traces(textposition="outside")
            fig_w.update_layout(height=320, coloraxis_showscale=False)
            col1, col2 = st.columns([2, 1])
            with col1:
                st.plotly_chart(fig_w, use_container_width=True)
            with col2:
                st.dataframe(df_w, use_container_width=True, hide_index=True)

        # ── 3. Bar chart: Kota vs Avg Time ───────────────────────────────────
        st.markdown("### 🏙️ Avg Delivery Time per Kota")
        df_c = pd.DataFrame(batch.get("by_city", []))
        if not df_c.empty:
            fig_c = px.bar(
                df_c,
                x="City",
                y="avg_delivery_time_min",
                color="City",
                text="avg_delivery_time_min",
                color_discrete_sequence=px.colors.qualitative.Set2,
                labels={
                    "avg_delivery_time_min": "Avg Delivery Time (menit)",
                    "City": "Kota"
                }
            )
            fig_c.update_traces(textposition="outside")
            fig_c.update_layout(showlegend=False, height=320)
            col1, col2 = st.columns([2, 1])
            with col1:
                st.plotly_chart(fig_c, use_container_width=True)
            with col2:
                st.dataframe(df_c, use_container_width=True, hide_index=True)

        # ── 4. Heatmap: Traffic x Cuaca ──────────────────────────────────────
        st.markdown("### 🔥 Heatmap: Traffic × Cuaca vs Avg Delivery Time")
        heatmap_data = batch.get("heatmap", [])
        if heatmap_data:
            df_h = pd.DataFrame(heatmap_data)
            df_pivot = df_h.pivot(
                index="Weather_conditions",
                columns="Road_traffic_density",
                values="avg_delivery_time_min"
            )
            fig_h = px.imshow(
                df_pivot,
                color_continuous_scale="RdYlGn_r",
                aspect="auto",
                text_auto=True,
                labels=dict(
                    x="Traffic Density",
                    y="Kondisi Cuaca",
                    color="Avg Time (min)"
                )
            )
            fig_h.update_layout(height=380)
            st.plotly_chart(fig_h, use_container_width=True)
            st.caption("Warna merah = waktu delivery lebih lama. Hijau = lebih cepat.")

        # ── 5. Tabel: Top Delivery Person ────────────────────────────────────
        st.markdown("### 👤 Top 20 Delivery Person by Jumlah Delivery")
        df_p = pd.DataFrame(batch.get("by_person", []))
        if not df_p.empty:
            st.dataframe(
                df_p.rename(columns={
                    "Delivery_person_ID":    "Delivery Person ID",
                    "total_deliveries":      "Total Deliveries",
                    "avg_delivery_time_min": "Avg Time (min)",
                    "avg_rating":            "Avg Rating"
                }),
                use_container_width=True,
                hide_index=True
            )