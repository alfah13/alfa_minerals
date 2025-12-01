import os
from pathlib import Path

import streamlit as st
import pandas as pd
import sqlalchemy as sa
import folium
from streamlit_folium import st_folium
import numpy as np

from pipelines import ingest_crone_pem_flow
from models import Base


# ---------- DB helpers ----------

def get_engine():
    """Create database engine"""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER", "alfauser")
    db_password = os.getenv("DB_PASSWORD", "alfapass123")
    db_name = os.getenv("DB_NAME", "alfa_minerals")
    url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return sa.create_engine(url)


def query_to_df(query, engine, params=None):
    """Execute raw SQL query and return DataFrame"""
    if params is None:
        params = {}
    try:
        with engine.connect() as conn:
            result = conn.execute(sa.text(query), params)
            rows = result.fetchall()
            cols = result.keys()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# ---------- Streamlit UI ----------

st.set_page_config(page_title="ALFA Minerals EM Dashboard", layout="wide")
st.title("⛏️ ALFA Minerals – EM Survey Dashboard")

tab_upload, tab_surveys, tab_responses, tab_channels, tab_map = st.tabs(
    ["📤 Upload File", "📊 Survey Overview", "📈 EM Responses", "📉 Decay Curves", "🗺️ Survey Map"]
)

engine = get_engine()


# ---------- Tab 0: Upload ----------

with tab_upload:
    st.subheader("Upload new Crone .STP/.PEM file")

    uploaded_file = st.file_uploader(
        "Choose a .STP or .PEM file", 
        type=["stp", "STP", "pem", "PEM"]
    )

    default_survey_dir = "./data/data_archive"
    st.caption(f"Files saved to: {default_survey_dir}")

    if uploaded_file is not None:
        st.write(f"**File:** {uploaded_file.name}")

        col1, col2 = st.columns(2)
        with col1:
            survey_folder = st.text_input(
                "Survey folder (e.g., P-141)",
                value="P-141",
            )
        with col2:
            run_ingest = st.checkbox("Run ingestion after upload", value=True)

        if st.button("💾 Save & Ingest", use_container_width=True):
            try:
                target_dir = Path(default_survey_dir) / survey_folder
                target_dir.mkdir(parents=True, exist_ok=True)

                target_path = target_dir / uploaded_file.name

                with open(target_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())

                st.success(f"✅ Saved to `{target_path}`")

                if run_ingest:
                    with st.spinner("🔄 Running ingestion pipeline..."):
                        result = ingest_crone_pem_flow(str(target_path))
                        st.success("✅ Ingestion complete!")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Survey ID", result.get('survey_id'))
                        with col2:
                            st.metric("Loops", result.get('loops_stored', 0))
                        with col3:
                            st.metric("Stations", result.get('stations_stored', 0))
                        with col4:
                            st.metric("Responses", result.get('responses_stored', 0))
                        
                        st.json(result)
            except Exception as e:
                st.error(f"❌ Error: {e}")


# ---------- Tab 1: Survey Overview ----------

with tab_surveys:
    st.subheader("Survey Summary")

    survey_query = """
    SELECT 
        s.id,
        s.survey_id,
        s.survey_date::date as survey_date,
        s.client_name,
        COUNT(DISTINCT l.id) as loop_count,
        COUNT(DISTINCT rs.id) as station_count,
        COUNT(DISTINCT er.id) as response_count,
        COUNT(DISTINCT oc.id) as channel_count
    FROM surveys s
    LEFT JOIN loops l ON s.id = l.survey_id
    LEFT JOIN receiver_stations rs ON s.id = rs.survey_id
    LEFT JOIN em_responses er ON s.id = er.survey_id
    LEFT JOIN offtime_channels oc ON er.id = oc.response_id
    GROUP BY s.id, s.survey_id, s.survey_date, s.client_name
    ORDER BY s.survey_date DESC
    """
    
    survey_df = query_to_df(survey_query, engine)
    
    if survey_df.empty:
        st.info("ℹ️ No surveys found yet. Upload and ingest a file first.")
    else:
        st.dataframe(survey_df, use_container_width=True, height=300)
        
        st.markdown("---")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Surveys", len(survey_df))
        with col2:
            st.metric("Total Loops", int(survey_df['loop_count'].sum()))
        with col3:
            st.metric("Total Stations", int(survey_df['station_count'].sum()))
        with col4:
            st.metric("Total Responses", int(survey_df['response_count'].sum()))
        with col5:
            st.metric("Total Channels", int(survey_df['channel_count'].sum()))


# ---------- Tab 2: EM Responses ----------

with tab_responses:
    st.subheader("EM Responses by Survey & Component")

    # Survey filter
    survey_query = """
    SELECT id, survey_id, survey_date 
    FROM surveys 
    WHERE id IN (SELECT DISTINCT survey_id FROM em_responses)
    ORDER BY survey_date DESC
    """
    survey_df = query_to_df(survey_query, engine)
    
    if survey_df.empty:
        st.info("ℹ️ No surveys with responses found.")
    else:
        survey_map = {
            f"{row.survey_id} ({row.survey_date.date()})" : row.id 
            for row in survey_df.itertuples()
        }
        selected_survey_label = st.selectbox("Select survey", list(survey_map.keys()))
        selected_survey_id = survey_map[selected_survey_label]

        # Component filter
        component = st.selectbox("Component", ["Z", "X", "Y"])

        # Load responses for this survey/component
        response_query = """
        SELECT
            er.id AS response_id,
            rs.station_label,
            rs.distance_along_profile_m,
            er.component,
            er.source_file,
            er.primary_pulse_nt_per_sec,
            er.apparent_resistance,
            COUNT(oc.id) as channel_count
        FROM em_responses er
        JOIN receiver_stations rs ON er.receiver_station_id = rs.id
        LEFT JOIN offtime_channels oc ON er.id = oc.response_id
        WHERE er.survey_id = :survey_id
          AND er.component = :component
        GROUP BY 
            er.id, rs.station_label, rs.distance_along_profile_m, 
            er.component, er.source_file, er.primary_pulse_nt_per_sec, 
            er.apparent_resistance
        ORDER BY rs.distance_along_profile_m
        """
        
        df = query_to_df(response_query, engine, {
            "survey_id": selected_survey_id,
            "component": component
        })
        
        if df.empty:
            st.warning("⚠️ No EM responses found for this survey/component.")
        else:
            st.write(f"**{len(df)} responses found**")
            st.dataframe(df, use_container_width=True, height=300)

            st.markdown("### Primary Pulse vs Distance")
            if not df['primary_pulse_nt_per_sec'].isna().all():
                chart_data = df.set_index("distance_along_profile_m")[["primary_pulse_nt_per_sec"]].sort_index()
                st.line_chart(chart_data)
            else:
                st.info("ℹ️ No primary pulse data available")

            st.markdown("### Apparent Resistance vs Distance")
            if not df['apparent_resistance'].isna().all():
                chart_data = df.set_index("distance_along_profile_m")[["apparent_resistance"]].sort_index()
                st.line_chart(chart_data)
            else:
                st.info("ℹ️ No apparent resistance data available")


# ---------- Tab 3: Channels / Decay Curves ----------

with tab_channels:
    st.subheader("Channel Decay Curves")

    # Survey selection
    survey_query = """
    SELECT id, survey_id, survey_date 
    FROM surveys 
    WHERE id IN (SELECT DISTINCT survey_id FROM em_responses)
    ORDER BY survey_date DESC
    """
    survey_df = query_to_df(survey_query, engine)
    
    if survey_df.empty:
        st.info("ℹ️ No surveys with responses found.")
    else:
        survey_map = {
            f"{row.survey_id} ({row.survey_date.date()})": row.id 
            for row in survey_df.itertuples()
        }
        selected_survey_label = st.selectbox(
            "Select survey", list(survey_map.keys()), key="channel_survey"
        )
        selected_survey_id = survey_map[selected_survey_label]

        # Component selection
        component = st.selectbox("Component", ["Z", "X", "Y"], key="channel_component")

        # Get stations with responses for this survey/component
        station_query = """
        SELECT DISTINCT rs.station_label, rs.distance_along_profile_m
        FROM em_responses er
        JOIN receiver_stations rs ON er.receiver_station_id = rs.id
        WHERE er.survey_id = :survey_id
          AND er.component = :component
        ORDER BY rs.distance_along_profile_m
        """
        
        station_df = query_to_df(station_query, engine, {
            "survey_id": selected_survey_id,
            "component": component
        })
        
        if station_df.empty:
            st.warning("⚠️ No stations with responses for this survey/component.")
        else:
            station_options = [
                f"{row.station_label} ({row.distance_along_profile_m} m)"
                for row in station_df.itertuples()
            ]
            station_choice = st.selectbox(
                "Select station", station_options, key="station_choice"
            )
            station_label = station_choice.split(" ")[0]

            # Get response for this station/component
            response_query = """
            SELECT er.id, er.source_file, er.receiver_code, er.primary_pulse_nt_per_sec
            FROM em_responses er
            JOIN receiver_stations rs ON er.receiver_station_id = rs.id
            WHERE er.survey_id = :survey_id
              AND er.component = :component
              AND rs.station_label = :station_label
            ORDER BY er.created_at DESC
            LIMIT 1
            """
            
            resp_df = query_to_df(response_query, engine, {
                "survey_id": selected_survey_id,
                "component": component,
                "station_label": station_label
            })
            
            if resp_df.empty:
                st.warning("⚠️ No response found for this station.")
            else:
                response_id = resp_df.iloc[0]["id"]
                source_file = resp_df.iloc[0]["source_file"]
                receiver_code = resp_df.iloc[0]["receiver_code"]
                primary_pulse = resp_df.iloc[0]["primary_pulse_nt_per_sec"]
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Source File", source_file)
                with col2:
                    st.metric("Receiver Code", receiver_code)
                with col3:
                    st.metric("Primary Pulse", f"{primary_pulse:.2f}")
                with col4:
                    st.metric("Component", component)

                # Load channels
                channel_query = """
                SELECT 
                    channel_number, 
                    time_ms, 
                    amplitude_nt_per_sec
                FROM offtime_channels
                WHERE response_id = :response_id
                ORDER BY channel_number
                """
                
                ch_df = query_to_df(channel_query, engine, {
                    "response_id": response_id
                })

                if ch_df.empty:
                    st.warning("⚠️ No channel data for this response.")
                else:
                    st.markdown(f"### Channel Data ({len(ch_df)} channels)")
                    st.dataframe(ch_df, use_container_width=True, height=250)

                    st.markdown("### Decay Curve (Amplitude vs Time)")
                    ch_df_sorted = ch_df.sort_values("time_ms").copy()
                    ch_df_sorted['time_ms'] = ch_df_sorted['time_ms'] / 1000  # Convert to seconds
                    ch_df_sorted = ch_df_sorted.set_index("time_ms")

                    st.line_chart(ch_df_sorted["amplitude_nt_per_sec"])


# ---------- Tab 4: Survey Map ----------
# ---------- Tab 4: Survey Map ----------

with tab_map:
    st.subheader("All Survey Locations & Transmitter Loops")

    # Get all surveys with data
    survey_query = """
    SELECT id, survey_id, survey_date 
    FROM surveys 
    WHERE id IN (SELECT DISTINCT survey_id FROM loops)
       OR id IN (SELECT DISTINCT survey_id FROM receiver_stations)
    ORDER BY survey_date DESC
    """
    survey_df = query_to_df(survey_query, engine)
    
    if survey_df.empty:
        st.info("ℹ️ No surveys with loop/station data found.")
    else:
        # Get all loops with WGS84 geometry
        loops_query = """
        SELECT survey_id, loop_point_number, 
               ST_X(geometry_wgs84::geometry) as lon,
               ST_Y(geometry_wgs84::geometry) as lat,
               elevation
        FROM loops
        ORDER BY survey_id, loop_point_number
        """
        
        loops_df = query_to_df(loops_query, engine)
        
        # Get all receiver stations with WGS84 geometry
        station_query = """
        SELECT survey_id, station_label, 
               ST_X(geometry_wgs84::geometry) as lon,
               ST_Y(geometry_wgs84::geometry) as lat,
               elevation, distance_along_profile_m
        FROM receiver_stations
        ORDER BY survey_id, distance_along_profile_m
        """
        
        stations_df = query_to_df(station_query, engine)
        
        # Get all EM responses with WGS84 geometry
        response_query = """
        SELECT rs.survey_id, rs.station_label,
               ST_X(rs.geometry_wgs84::geometry) as lon,
               ST_Y(rs.geometry_wgs84::geometry) as lat,
               er.component,
               er.primary_pulse_nt_per_sec,
               COUNT(oc.id) as channel_count
        FROM receiver_stations rs
        LEFT JOIN em_responses er ON rs.id = er.receiver_station_id
        LEFT JOIN offtime_channels oc ON er.id = oc.response_id
        GROUP BY rs.survey_id, rs.station_label, rs.geometry_wgs84, er.component, er.primary_pulse_nt_per_sec
        ORDER BY rs.survey_id
        """
        
        responses_df = query_to_df(response_query, engine)
        
        if loops_df.empty and stations_df.empty:
            st.warning("⚠️ No loop or station data for any survey.")
        else:
            # Calculate global map center
            all_coords = []
            if not loops_df.empty and not loops_df['lat'].isna().all():
                all_coords.extend(zip(loops_df['lat'], loops_df['lon']))
            if not stations_df.empty and not stations_df['lat'].isna().all():
                all_coords.extend(zip(stations_df['lat'], stations_df['lon']))
            
            if all_coords:
                center_lat = sum(c[0] for c in all_coords) / len(all_coords)
                center_lon = sum(c[1] for c in all_coords) / len(all_coords)
            else:
                center_lat, center_lon = 43.0, -81.0
            
            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=12,
                tiles="OpenStreetMap"
            )
            
            # Add all transmitter loops (color by survey)
            if not loops_df.empty and not loops_df['lat'].isna().all():
                surveys_list = loops_df['survey_id'].unique()
                colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'darkblue', 'darkgreen']
                survey_colors = {sid: colors[i % len(colors)] for i, sid in enumerate(surveys_list)}
                
                for survey_id in surveys_list:
                    survey_loops = loops_df[loops_df['survey_id'] == survey_id]
                    loop_coords = []
                    
                    for row in survey_loops.itertuples():
                        if not pd.isna(row.lat) and not pd.isna(row.lon):
                            loop_coords.append([row.lat, row.lon])
                    
                    if loop_coords:
                        loop_coords.append(loop_coords[0])  # Close the loop
                        
                        survey_info = survey_df[survey_df['id'] == survey_id]
                        survey_name = survey_info.iloc[0]['survey_id'] if not survey_info.empty else f"Survey {survey_id}"
                        
                        color = survey_colors[survey_id]
                        folium.PolyLine(
                            loop_coords,
                            color=color,
                            weight=3,
                            opacity=0.8,
                            popup=f'TX Loop: {survey_name}',
                            tooltip=f'Transmitter Loop - {survey_name}'
                        ).add_to(m)
                        
                        # Add loop points
                        for row in survey_loops.itertuples():
                            if not pd.isna(row.lat) and not pd.isna(row.lon):
                                folium.CircleMarker(
                                    location=[row.lat, row.lon],
                                    radius=4,
                                    popup=f"{survey_name}<br>Loop Point {row.loop_point_number}<br>Elev: {row.elevation:.1f}m",
                                    color=color,
                                    fill=True,
                                    fillColor=color,
                                    fillOpacity=0.6,
                                    weight=1
                                ).add_to(m)
            
            # Add all receiver stations (color by component)
            if not stations_df.empty and not stations_df['lat'].isna().all():
                for row in stations_df.itertuples():
                    if not pd.isna(row.lat) and not pd.isna(row.lon):
                        # Get response data for this station
                        station_responses = responses_df[responses_df['station_label'] == row.station_label]
                        
                        survey_info = survey_df[survey_df['id'] == row.survey_id]
                        survey_name = survey_info.iloc[0]['survey_id'] if not survey_info.empty else f"Survey {row.survey_id}"
                        
                        popup_text = f"<b>{survey_name}</b><br><b>Station:</b> {row.station_label}<br><b>Distance:</b> {row.distance_along_profile_m:.0f}m<br><b>Elev:</b> {row.elevation:.1f}m"
                        
                        if not station_responses.empty:
                            popup_text += "<br><b>Components:</b><br>"
                            for sr_row in station_responses.itertuples():
                                if sr_row.component:
                                    popup_text += f"  {sr_row.component}: {sr_row.primary_pulse_nt_per_sec:.0f} nT/sec<br>"
                        
                        # Color by component
                        component = station_responses['component'].iloc[0] if not station_responses.empty else None
                        color_map = {'X': 'blue', 'Y': 'green', 'Z': 'purple', 'UNK': 'gray'}
                        color = color_map.get(component, 'gray')
                        
                        folium.CircleMarker(
                            location=[row.lat, row.lon],
                            radius=6,
                            popup=folium.Popup(popup_text, max_width=300),
                            tooltip=f"{survey_name} - {row.station_label}",
                            color=color,
                            fill=True,
                            fillColor=color,
                            fillOpacity=0.7,
                            weight=2
                        ).add_to(m)
            
            # Add anomalies (larger markers for strong signals)
            if not responses_df.empty and not responses_df['primary_pulse_nt_per_sec'].isna().all():
                # Filter out NoneType values first
                responses_clean = responses_df[responses_df['primary_pulse_nt_per_sec'].notna()].copy()
                
                if not responses_clean.empty:
                    responses_clean['abs_pulse'] = responses_clean['primary_pulse_nt_per_sec'].abs()
                    max_pulse = responses_clean['abs_pulse'].max()
                    
                    for row in responses_clean.itertuples():
                        if row.primary_pulse_nt_per_sec == 0 or pd.isna(row.lat) or pd.isna(row.lon):
                            continue
                        
                        size = 8 + (row.abs_pulse / max_pulse) * 20
                        
                        survey_info = survey_df[survey_df['id'] == row.survey_id]
                        survey_name = survey_info.iloc[0]['survey_id'] if not survey_info.empty else f"Survey {row.survey_id}"
                        
                        popup_text = f"<b>{survey_name}</b><br>{row.station_label}<br>{row.component} Component<br>{row.primary_pulse_nt_per_sec:.0f} nT/sec"
                        
                        folium.CircleMarker(
                            location=[row.lat, row.lon],
                            radius=size,
                            popup=popup_text,
                            color='orange',
                            fill=True,
                            fillColor='orange',
                            fillOpacity=0.4,
                            weight=2
                        ).add_to(m)

            
            # Add legend
            legend_html = '''
            <div style="position: fixed; 
                     bottom: 50px; right: 50px; width: 220px; height: auto;
                     background-color: white; border:2px solid grey; z-index:9999; 
                     font-size:12px; padding: 10px">
                <p style="margin: 0; font-weight: bold;">Legend</p>
                <p><span style="background:red; width:12px; height:12px; display:inline-block;"></span> TX Loop (Survey)</p>
                <p><span style="background:blue; width:12px; height:12px; display:inline-block;"></span> RX Station (X)</p>
                <p><span style="background:green; width:12px; height:12px; display:inline-block;"></span> RX Station (Y)</p>
                <p><span style="background:purple; width:12px; height:12px; display:inline-block;"></span> RX Station (Z)</p>
                <p><span style="background:gray; width:12px; height:12px; display:inline-block;"></span> RX Station (UNK)</p>
                <p><span style="background:orange; width:12px; height:12px; display:inline-block;"></span> Anomaly (size ∝ signal)</p>
            </div>
            '''
            m.get_root().html.add_child(folium.Element(legend_html))
            
            st_folium(m, width=1400, height=700)
            
            st.caption("🗺️ All surveys displayed. Red = TX Loop | Colored dots = RX Stations | Orange = Anomalies")
            
            # Summary stats
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Surveys", len(survey_df))
            with col2:
                st.metric("Loops", len(loops_df))
            with col3:
                st.metric("Stations", len(stations_df))
            with col4:
                st.metric("Responses", len(responses_df.drop_duplicates(subset=['station_label'])))
            with col5:
                st.metric("Total Signal Points", len(responses_df[~responses_df['primary_pulse_nt_per_sec'].isna()]))

