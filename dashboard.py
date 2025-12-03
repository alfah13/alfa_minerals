import os
from pathlib import Path

import streamlit as st
import pandas as pd
import sqlalchemy as sa
import folium
from streamlit_folium import st_folium

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

engine = get_engine()

# ---------- Tab 1: Summary Stats ----------

st.subheader("📊 Ingestion Summary")

summary_query = """
SELECT 
    COUNT(DISTINCT s.id) as surveys,
    COUNT(DISTINCT l.id) as loops,
    COUNT(DISTINCT rs.id) as stations,
    COUNT(DISTINCT er.id) as responses,
    COUNT(DISTINCT oc.id) as channels,
    COUNT(DISTINCT CONCAT(s.id, er.source_file)) as files_ingested
FROM surveys s
LEFT JOIN loops l ON s.id = l.survey_id
LEFT JOIN receiver_stations rs ON s.id = rs.survey_id
LEFT JOIN em_responses er ON s.id = er.survey_id
LEFT JOIN offtime_channels oc ON er.id = oc.response_id
"""

summary_df = query_to_df(summary_query, engine)

if not summary_df.empty:
    row = summary_df.iloc[0]
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("📂 Files", int(row['files_ingested']))
    with col2:
        st.metric("📊 Surveys", int(row['surveys']))
    with col3:
        st.metric("🔵 Loops", int(row['loops']))
    with col4:
        st.metric("🟢 Stations", int(row['stations']))
    with col5:
        st.metric("📈 Responses", int(row['responses']))
    with col6:
        st.metric("📉 Channels", int(row['channels']))
    
    st.markdown("---")
    
    # Table summary
    table_data = {
        'Table': ['Surveys', 'Loops', 'Receiver Stations', 'EM Responses', 'Off-Time Channels'],
        'Rows': [int(row['surveys']), int(row['loops']), int(row['stations']), int(row['responses']), int(row['channels'])]
    }
    st.table(pd.DataFrame(table_data))
else:
    st.warning("⚠️ No data ingested yet")


# ---------- Tab 2: Map ----------

st.subheader("🗺️ Survey Locations")

loops_query = """
SELECT DISTINCT
    ST_X(geometry_wgs84::geometry) as lon,
    ST_Y(geometry_wgs84::geometry) as lat,
    s.survey_id
FROM loops l
JOIN surveys s ON l.survey_id = s.id
WHERE geometry_wgs84 IS NOT NULL
"""

stations_query = """
SELECT DISTINCT
    ST_X(geometry_wgs84::geometry) as lon,
    ST_Y(geometry_wgs84::geometry) as lat,
    s.survey_id,
    rs.station_label
FROM receiver_stations rs
JOIN surveys s ON rs.survey_id = s.id
WHERE geometry_wgs84 IS NOT NULL
"""

loops_df = query_to_df(loops_query, engine)
stations_df = query_to_df(stations_query, engine)

if loops_df.empty and stations_df.empty:
    st.warning("⚠️ No location data found")
else:
    # Calculate center
    all_coords = []
    if not loops_df.empty:
        all_coords.extend(zip(loops_df['lat'], loops_df['lon']))
    if not stations_df.empty:
        all_coords.extend(zip(stations_df['lat'], stations_df['lon']))
    
    if all_coords:
        center_lat = sum(c[0] for c in all_coords) / len(all_coords)
        center_lon = sum(c[1] for c in all_coords) / len(all_coords)
    else:
        center_lat, center_lon = 43.0, -81.0
    
    # Create map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles="OpenStreetMap"
    )
    
    # Add loops (red)
    if not loops_df.empty:
        for row in loops_df.itertuples():
            folium.CircleMarker(
                location=[row.lat, row.lon],
                radius=5,
                popup=f"TX Loop: {row.survey_id}",
                tooltip=f"Transmitter - {row.survey_id}",
                color='red',
                fill=True,
                fillColor='red',
                fillOpacity=0.7,
                weight=2
            ).add_to(m)
    
    # Add stations (blue)
    if not stations_df.empty:
        for row in stations_df.itertuples():
            folium.CircleMarker(
                location=[row.lat, row.lon],
                radius=6,
                popup=f"{row.survey_id} - {row.station_label}",
                tooltip=f"RX Station - {row.survey_id}",
                color='blue',
                fill=True,
                fillColor='blue',
                fillOpacity=0.7,
                weight=2
            ).add_to(m)
    
    # Legend
    legend_html = '''
    <div style="position: fixed; bottom: 50px; right: 50px; width: 150px;
         background-color: white; border:2px solid grey; z-index:9999; 
         font-size:12px; padding: 10px">
        <p style="margin: 0; font-weight: bold;">Legend</p>
        <p><span style="background:red; width:12px; height:12px; display:inline-block;"></span> Transmitter Loop</p>
        <p><span style="background:blue; width:12px; height:12px; display:inline-block;"></span> Receiver Station</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    st_folium(m, width=1400, height=500)


# ---------- Tab 3: EM Response Profile ----------

st.subheader("📈 EM Response Profile by Distance")

# Survey selector
survey_query = """
SELECT DISTINCT survey_id FROM surveys
WHERE id IN (SELECT survey_id FROM em_responses)
ORDER BY survey_id
"""
survey_list = query_to_df(survey_query, engine)

if survey_list.empty:
    st.warning("⚠️ No surveys with EM responses found")
else:
    selected_survey = st.selectbox(
        "Select Survey",
        survey_list['survey_id'].tolist()
    )
    
    # Component selector
    selected_component = st.selectbox("Component", ["X", "Y", "Z"])
    
    # Get profile data
    profile_query = """
    SELECT
        rs.distance_along_profile_m as distance,
        rs.station_label,
        er.primary_pulse_nt_per_sec as amplitude,
        er.source_file
    FROM em_responses er
    JOIN receiver_stations rs ON er.receiver_station_id = rs.id
    JOIN surveys s ON er.survey_id = s.id
    WHERE s.survey_id = :survey_id
      AND er.component = :component
    ORDER BY rs.distance_along_profile_m
    """
    
    profile_df = query_to_df(profile_query, engine, {
        "survey_id": selected_survey,
        "component": selected_component
    })
    
    if profile_df.empty:
        st.warning(f"⚠️ No data for {selected_survey} - Component {selected_component}")
    else:
        # Plot
        st.line_chart(profile_df.set_index('distance')[['amplitude']])
        
        # Table
        st.dataframe(profile_df, use_container_width=True)

