"""
Crone PEM/STP data ingestion pipeline with PostGIS geometry
"""


import os
import re
from typing import Dict, List
from pathlib import Path
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact


from models import (
    Base, Survey, Loop, ReceiverStation, EMResponse, OffTimeChannel, 
    ComponentEnum
)



def get_db_session():
    """Create database session"""
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "alfauser")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "alfapass123")
    DB_NAME = os.getenv("DB_NAME", "alfa_minerals")
    
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = sa.create_engine(connection_string)
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    return Session()



# ============= PARSING TASKS =============


@task()
def load_crone_file(file_path: str) -> tuple:
    """Load file and detect format (STP vs PEM)"""
    logger = get_run_logger()
    logger.info(f"Loading file: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    logger.info(f"Loaded {len(content)} bytes")
    
    # Detect format
    if "<FMT> 230" in content:
        file_format = "STP"
    elif "<FMT> 210" in content:
        file_format = "PEM"
    else:
        file_format = "UNKNOWN"
    
    logger.info(f"Detected format: {file_format}")
    return content, file_format



@task()
def parse_header(file_content: str) -> Dict:
    """Parse header metadata"""
    logger = get_run_logger()
    
    header = {}
    
    patterns = {
        'FMT': r'<FMT>\s+(\S+)',
        'UNI': r'<UNI>\s+(.+?)(?=\n|$)',
        'OPR': r'<OPR>\s+(.+?)(?=\n|$)',
        'CUR': r'<CUR>\s+([\d.]+)',
        'TXS': r'<TXS>\s+([\d.]+)\s+([\d.]+)',
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, file_content)
        if match:
            header[key] = match.group(1).strip() if key != 'TXS' else (float(match.group(1)), float(match.group(2)))
    
    lines = file_content.split('\n')
    for i, line in enumerate(lines):
        if 'North American Nickel' in line:
            header['CLIENT'] = line.strip()
            if i + 1 < len(lines):
                survey_id = lines[i + 1].strip()
                if survey_id:
                    header['SURVEY_ID'] = survey_id
    
    # Set CRS info for NAN Maniitsoq project
    header['DATUM'] = 'WGS 1984'
    header['PROJECTION'] = 'UTM'
    header['UTM_ZONE'] = '22N'
    
    logger.info(f"Parsed header: {header}")
    return header



@task()
def parse_loop_coordinates(file_content: str) -> List[Dict]:
    """Parse transmitter loop coordinates"""
    logger = get_run_logger()
    
    loops = []
    pattern = r'<L(\d{2})>\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\d)'
    
    for match in re.finditer(pattern, file_content):
        loops.append({
            'loop_number': int(match.group(1)),
            'easting': float(match.group(2)),
            'northing': float(match.group(3)),
            'elevation': float(match.group(4)),
            'units': int(match.group(5))
        })
    
    logger.info(f"Parsed {len(loops)} loop coordinates")
    return loops



@task()
def parse_receiver_stations(file_content: str, line_name: str) -> List[Dict]:
    """Parse receiver stations with line context"""
    logger = get_run_logger()
    
    stations = []
    pattern = r'<P(\d{2})>\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\d)\s+([-\d.]+)'
    
    for match in re.finditer(pattern, file_content):
        station_num = int(match.group(1))
        distance_m = float(match.group(6))
        
        # Create composite label: "100E_00", "100E_01", etc.
        # Or simpler: encode distance: "0m", "25m", "50m"
        station_label = f"{distance_m:.0f}m"  # "0m", "25m", "50m"...
        
        stations.append({
            'station_number': station_num,
            'station_label': station_label,
            'line_name': line_name,
            'easting': float(match.group(2)),
            'northing': float(match.group(3)),
            'elevation': float(match.group(4)),
            'units': int(match.group(5)),
            'distance_m': distance_m
        })
    
    logger.info(f"Parsed {len(stations)} stations on line {line_name}")
    return stations



@task()
def parse_measurements(file_content: str, time_gates: List[float], line_name: str) -> List[Dict]:
    """Parse measurements with line context"""
    logger = get_run_logger()
    
    measurements = []
    dollar_idx = file_content.find('$')
    if dollar_idx == -1:
        logger.warning("No $ marker found")
        return measurements
    
    data_section = file_content[dollar_idx + 1:]
    lines = data_section.split('\n')
    
    # Station pattern: "100N", "125N", etc. (distances along profile)
    station_pattern = r'(\d+[NSEWnsew])\s+([XYZxyz]R\d+R?)\s+(\d+)\s+([A-Z])\s+([\d.]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)'
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        match = re.match(station_pattern, line)
        if match:
            # Extract just the numeric part: "100N" → "100"
            station_distance_str = re.match(r'(\d+)', match.group(1)).group(1)
            station_label = f"{float(station_distance_str):.0f}m"  # "100m", "125m", etc.
            
            receiver_code = match.group(2)
            component = 'Z' if 'Z' in receiver_code.upper() else ('X' if 'X' in receiver_code.upper() else 'Y')
            
            current_record = {
                'station_label': station_label,
                'line_name': line_name,
                'receiver_code': receiver_code,
                'receiver_number': int(match.group(3)),
                'angle': float(match.group(5)),
                'component': component,
            }
            
            # Parse D4/D7 data...
            i += 1
            if i < len(lines):
                d4_match = re.match(r'D4\s+([-\d.e+]+)\s+([-\d.e+]+)\s+([-\d.e+]+)\s+([-\d.e+]+)', lines[i].strip())
                if d4_match:
                    current_record['current_on_time'] = float(d4_match.group(1))
                    current_record['apparent_resistance'] = float(d4_match.group(2))
            
            i += 1
            channel_data = []
            while i < len(lines):
                data_line = lines[i].strip()
                if not data_line or re.match(r'^\d{3}[NSEWnsew]|^[A-Z]\d{3}|^D[47]', data_line):
                    break
                try:
                    vals = [float(v) for v in data_line.split()]
                    channel_data.extend(vals)
                    i += 1
                except ValueError:
                    break
            
            if channel_data:
                current_record['primary_pulse'] = channel_data[0]
                current_record['channels'] = channel_data[1:]
                measurements.append(current_record)
                continue
        
        i += 1
    
    logger.info(f"Parsed {len(measurements)} measurements on line {line_name}")
    return measurements




@task()
def parse_time_gates(file_content: str) -> List[float]:
    """Extract time gate values"""
    logger = get_run_logger()
    
    time_gates = []
    dollar_idx = file_content.find('$')
    if dollar_idx == -1:
        logger.warning("No $ marker found")
        return time_gates
    
    header_section = file_content[:dollar_idx]
    lines = header_section.split('\n')
    
    time_gate_lines = []
    for line in reversed(lines):
        line = line.strip()
        if not line or line.startswith('~') or line.startswith('<') or 'North American' in line:
            continue
        
        try:
            vals = [float(v) for v in line.split()]
            if vals:
                time_gate_lines.insert(0, vals)
                if len(vals) < 5:
                    break
        except ValueError:
            continue
    
    for vals in time_gate_lines:
        time_gates.extend(vals)
    
    logger.info(f"Parsed {len(time_gates)} time gates: {time_gates[:5]}... (showing first 5)")
    return time_gates




# ============= DATABASE STORAGE TASKS =============


@task()
def store_survey(header: Dict, file_path: str) -> Dict:
    """Create or get Survey record"""
    logger = get_run_logger()
    
    session = get_db_session()
    
    survey_id = header.get('SURVEY_ID', Path(file_path).stem)
    
    existing = session.query(Survey).filter_by(survey_id=survey_id).first()
    if existing:
        logger.info(f"Survey '{survey_id}' already exists (ID: {existing.id})")
        session.close()
        return {'survey_id': existing.id, 'survey_record': 0}
    
    survey = Survey(
        survey_id=survey_id,
        survey_date=datetime.now(),
        data_format=header.get('FMT', '230'),
        data_units=header.get('UNI', 'nanoTesla/sec'),
        operator_name=header.get('OPR'),
        peak_current_amps=float(header.get('CUR', 0)) if header.get('CUR') else 0,
        client_name='North American Nickel',
        acquisition_company='Crone Geophysics & Exploration Ltd.',
        datum=header.get('DATUM', 'WGS 1984'),
        projection=header.get('PROJECTION', 'UTM'),
        utm_zone=header.get('UTM_ZONE', '22N'),
        header_data=header,
    )
    session.add(survey)
    session.commit()
    logger.info(f"Created survey: {survey.id} (UTM {survey.utm_zone}, {survey.datum})")
    session.close()
    return {'survey_id': survey.id, 'survey_record': 1}



@task()
def store_loops(survey_id: int, loops: List[Dict]) -> Dict:
    """Store transmitter loop coordinates"""
    logger = get_run_logger()
    
    session = get_db_session()
    
    existing_count = session.query(Loop).filter(Loop.survey_id == survey_id).count()
    if existing_count > 0:
        logger.info(f"Loops already exist ({existing_count}); skipping")
        session.close()
        return {'loops_stored': existing_count, 'new_loops': 0}
    
    for loop_data in loops:
        # UTM Zone 22N = SRID 32622
        utm_geom = f"SRID=32622;POINT({loop_data['easting']} {loop_data['northing']})"
        
        loop = Loop(
            survey_id=survey_id,
            loop_point_number=loop_data["loop_number"],
            easting=loop_data["easting"],
            northing=loop_data["northing"],
            elevation=loop_data["elevation"],
            loop_size_x_units=loop_data.get("loop_size_x", 600.0),
            loop_size_y_units=loop_data.get("loop_size_y", 700.0),
            geometry=utm_geom,
        )
        session.add(loop)
    
    session.commit()
    
    # Transform to WGS84
    session.execute(sa.text("""
        UPDATE loops 
        SET geometry_wgs84 = ST_Transform(geometry, 4326)
        WHERE survey_id = :survey_id AND geometry_wgs84 IS NULL
    """), {"survey_id": survey_id})
    
    session.commit()
    logger.info(f"Stored {len(loops)} loop coordinates with geometry")
    session.close()
    return {'loops_stored': existing_count + len(loops), 'new_loops': len(loops)}



@task()
def store_receiver_stations(survey_id: int, stations: List[Dict]) -> Dict:
    """Store receiver stations with line tracking"""
    logger = get_run_logger()
    
    session = get_db_session()
    
    existing_count = session.query(ReceiverStation).filter(ReceiverStation.survey_id == survey_id).count()
    if existing_count > 0:
        logger.info(f"Receiver stations already exist ({existing_count}); skipping")
        session.close()
        return {'stations_stored': existing_count, 'new_stations': 0}
    
    for station_data in stations:
        line_name = station_data.get('line_name', 'UNK')
        station_label = station_data.get('station_label', f"{station_data['station_number']:02d}N")
        
        # Parse direction from label or line_name
        line_direction = line_name[-1] if line_name else 'N'  # Last char of "100E" is 'E'
        
        utm_geom = f"SRID=32622;POINT({station_data['easting']} {station_data['northing']})"
        
        station = ReceiverStation(
            survey_id=survey_id,
            station_number=station_data["station_number"],
            station_label=station_label,  # "0m", "25m", "50m"...
            line_direction=line_direction,
            line_distance=station_data["distance_m"],
            easting=station_data["easting"],
            northing=station_data["northing"],
            elevation=station_data["elevation"],
            distance_along_profile_m=station_data["distance_m"],
            geometry=utm_geom,
        )
        session.add(station)
    
    session.commit()
    session.execute(sa.text("""
        UPDATE receiver_stations 
        SET geometry_wgs84 = ST_Transform(geometry, 4326)
        WHERE survey_id = :survey_id AND geometry_wgs84 IS NULL
    """), {"survey_id": survey_id})
    session.commit()
    logger.info(f"Stored {len(stations)} receiver stations")
    session.close()
    return {'stations_stored': len(stations), 'new_stations': len(stations)}




@task()
def store_em_responses(survey_id: int, measurements: List[Dict], time_gates: List[float], source_file: str) -> Dict:
    """Store EM responses matching by station label"""
    logger = get_run_logger()
    
    session = get_db_session()
    
    all_stations = session.query(ReceiverStation).filter_by(survey_id=survey_id).all()
    # Map by station_label (e.g., "100N", "125N")
    station_label_map = {s.station_label: s for s in all_stations}
    
    logger.info(f"Available stations: {sorted(list(station_label_map.keys()))}")
    logger.info(f"Processing {len(measurements)} measurements from {source_file}")
    
    response_count = 0
    channel_count = 0
    skipped = 0
    duplicates = 0
    
    for meas in measurements:
        meas_station_label = meas['station_label']  # e.g., "100N"
        component = meas['component']
        
        station = station_label_map.get(meas_station_label)
        
        if not station:
            logger.debug(f"Station '{meas_station_label}' not found")
            skipped += 1
            continue
        
        # Check for duplicate
        existing = session.query(EMResponse).filter_by(
            survey_id=survey_id,
            receiver_station_id=station.id,
            component=ComponentEnum[component],
            source_file=source_file
        ).first()
        
        if existing:
            logger.debug(f"Skipping duplicate: {meas_station_label}/{component}")
            duplicates += 1
            continue
        
        response = EMResponse(
            survey_id=survey_id,
            receiver_station_id=station.id,
            component=ComponentEnum[component],
            source_file=source_file,
            station_label=meas['station_label'],
            receiver_code=meas['receiver_code'],
            primary_pulse_nt_per_sec=meas.get('primary_pulse', 0),
        )
        session.add(response)
        session.flush()
        
        channels = meas.get('channels', [])
        for ch_num in range(min(len(time_gates), len(channels))):
            time_ms = time_gates[ch_num]
            amplitude = channels[ch_num]
            
            channel = OffTimeChannel(
                response_id=response.id,
                channel_number=ch_num + 1,
                time_ms=time_ms * 1000 if time_ms < 1 else time_ms,
                amplitude_nt_per_sec=amplitude,
                is_valid=True,
            )
            session.add(channel)
            channel_count += 1
        
        response_count += 1
    
    session.commit()
    logger.info(f"✅ Stored {response_count} responses, {channel_count} channels ({duplicates} dups, {skipped} not found)")
    session.close()
    return {
        'responses_stored': response_count,
        'channels_stored': channel_count,
        'duplicates': duplicates,
        'skipped': skipped
    }



# ============= MAIN FLOWS =============


@flow()  # Change from @task() to @flow()
def ingest_crone_pem_flow(file_path: str):
    """Complete Crone PEM data ingestion with line name from filename"""
    logger = get_run_logger()
    
    file_name = Path(file_path).name
    
    # Extract line name from filename: "100EAV.STP" → "100E"
    line_name_match = re.match(r'([A-Z0-9]+[NSEWnsew])', file_name)
    line_name = line_name_match.group(1).upper() if line_name_match else file_name.split('.')[0]
    
    logger.info(f"🚀 Starting ingestion: {file_path} (Line: {line_name})\n")
    
    file_content, file_format = load_crone_file(file_path)
    
    header = parse_header(file_content)
    loops = parse_loop_coordinates(file_content)
    stations = parse_receiver_stations(file_content, line_name)
    time_gates = parse_time_gates(file_content)
    measurements = parse_measurements(file_content, time_gates, line_name)
    
    survey_result = store_survey(header, file_path)
    survey_id = survey_result['survey_id']
    
    loops_result = store_loops(survey_id, loops)
    stations_result = store_receiver_stations(survey_id, stations)
    responses_result = store_em_responses(survey_id, measurements, time_gates, file_name)
    
    result = {
        'file': file_path,
        'file_name': file_name,
        'format': file_format,
        'survey_id': survey_id,
        'survey_rows': survey_result['survey_record'],
        'loops_rows': loops_result['new_loops'],
        'stations_rows': stations_result['new_stations'],
        'responses_rows': responses_result['responses_stored'],
        'channels_rows': responses_result['channels_stored'],
        'duplicates_skipped': responses_result['duplicates'],
        'stations_not_found': responses_result['skipped'],
        'status': 'success'
    }
    
    logger.info(f"""
    ✅ File ingestion complete:
    📊 Survey: {result['survey_rows']} new record(s)
    🔵 Loops: {result['loops_rows']} new record(s)
    🟢 Stations: {result['stations_rows']} new record(s)
    📈 Responses: {result['responses_rows']} new record(s)
    📉 Channels: {result['channels_rows']} new record(s)
    ⚠️  Duplicates skipped: {result['duplicates_skipped']}
    ⚠️  Stations not found: {result['stations_not_found']}
    """)
    
    return result



@flow()
def ingest_crone_dir_flow(dir_path: str) -> Dict:
    """Ingest all .STP / .PEM files in directory"""
    logger = get_run_logger()
    
    p = Path(dir_path)
    if not p.exists() or not p.is_dir():
        raise ValueError(f"{dir_path} is not valid")
    
    files = sorted(
        list(p.glob("*.STP")) + list(p.glob("*.stp")) +
        list(p.glob("*.PEM")) + list(p.glob("*.pem"))
    )
    
    if not files:
        raise ValueError(f"No .STP or .PEM files in {dir_path}")
    
    logger.info(f"Found {len(files)} files to ingest")
    
    results = []
    for f in files:
        result = ingest_crone_pem_flow(str(f))
        results.append(result)
    
    total_surveys = sum(r['survey_rows'] for r in results)
    total_loops = sum(r['loops_rows'] for r in results)
    total_stations = sum(r['stations_rows'] for r in results)
    total_responses = sum(r['responses_rows'] for r in results)
    total_channels = sum(r['channels_rows'] for r in results)
    total_duplicates = sum(r['duplicates_skipped'] for r in results)
    
    summary = {
        'directory': dir_path,
        'files_processed': len(results),
        'survey_rows': total_surveys,
        'loops_rows': total_loops,
        'stations_rows': total_stations,
        'responses_rows': total_responses,
        'channels_rows': total_channels,
        'duplicates_skipped': total_duplicates,
        'file_results': results
    }
    
    logger.info(f"""
    🎉 Directory ingestion complete:
    📂 Files: {len(results)}
    📊 Surveys: {total_surveys} new
    🔵 Loops: {total_loops} new
    🟢 Stations: {total_stations} new
    📈 Responses: {total_responses} new
    📉 Channels: {total_channels} new
    """)
    
    return summary



@flow()
def ingest_all_surveys_flow(base_dir: str = "./data/data_archive") -> Dict:
    """Ingest all survey folders - processes ALL folders, not just first"""
    logger = get_run_logger()
    
    base_path = Path(base_dir)
    if not base_path.exists():
        raise ValueError(f"Base directory {base_dir} does not exist")
    
    # Find all P-* folders using iterdir and filtering (more reliable than glob)
    survey_folders = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name.startswith("P-")])
    
    if not survey_folders:
        raise ValueError(f"No survey folders found in {base_dir}")
    
    logger.info(f"Found {len(survey_folders)} survey folders")
    for folder in survey_folders:
        logger.info(f"  - {folder.name}")
    
    all_results = []
    grand_totals = {
        'survey_rows': 0,
        'loops_rows': 0,
        'stations_rows': 0,
        'responses_rows': 0,
        'channels_rows': 0,
    }
    
    for folder in survey_folders:
        logger.info(f"\n🚀 Ingesting survey folder: {folder.name}")
        try:
            result = ingest_crone_dir_flow(str(folder))
            all_results.append(result)
            
            grand_totals['survey_rows'] += result.get('survey_rows', 0)
            grand_totals['loops_rows'] += result.get('loops_rows', 0)
            grand_totals['stations_rows'] += result.get('stations_rows', 0)
            grand_totals['responses_rows'] += result.get('responses_rows', 0)
            grand_totals['channels_rows'] += result.get('channels_rows', 0)
            
            logger.info(f"✅ Completed {folder.name}: {result.get('files_processed', 0)} files")
        except Exception as e:
            logger.error(f"❌ Failed {folder.name}: {e}")
            continue
    
    final_result = {
        'total_surveys': len(survey_folders),
        'completed': len(all_results),
        'survey_rows': grand_totals['survey_rows'],
        'loops_rows': grand_totals['loops_rows'],
        'stations_rows': grand_totals['stations_rows'],
        'responses_rows': grand_totals['responses_rows'],
        'channels_rows': grand_totals['channels_rows'],
        'survey_results': all_results
    }
    
    summary_md = f"""
# Data Ingestion Results


| Table | Rows Added |
|-------|-----------|
| Surveys | {grand_totals['survey_rows']} |
| Loops | {grand_totals['loops_rows']} |
| Stations | {grand_totals['stations_rows']} |
| Responses | {grand_totals['responses_rows']} |
| Channels | {grand_totals['channels_rows']} |
| **TOTAL** | **{sum(grand_totals.values())}** |


## Coordinate System
- **Datum**: WGS 1984
- **Projection**: UTM Zone 22N
- **SRID**: 32622 (UTM 22N), 4326 (WGS84)


## Summary
- **Survey Folders**: {len(survey_folders)}
- **Completed**: {len(all_results)}
- **Total Rows Ingested**: {sum(grand_totals.values())}
"""
    
    create_markdown_artifact(
        key="ingestion-summary",
        markdown=summary_md,
        description="Data ingestion summary with CRS info"
    )
    
    logger.info(f"""
    🎉🎉🎉 ALL SURVEYS COMPLETE 🎉🎉🎉
    📊 Surveys table: {grand_totals['survey_rows']} rows
    🔵 Loops table: {grand_totals['loops_rows']} rows (UTM 22N, WGS84)
    🟢 Stations table: {grand_totals['stations_rows']} rows
    📈 Responses table: {grand_totals['responses_rows']} rows
    📉 Channels table: {grand_totals['channels_rows']} rows
    """)
    
    return final_result



if __name__ == "__main__":
    import sys


    if len(sys.argv) > 1 and sys.argv[1] == "--all":
        print("Ingesting all survey folders...")
        ingest_all_surveys_flow()
    else:
        target = sys.argv[1] if len(sys.argv) > 1 else "./data/data_archive/P-141"
        p = Path(target)
        
        if p.is_dir():
            files = list(p.glob("*.STP")) + list(p.glob("*.stp")) + list(p.glob("*.PEM")) + list(p.glob("*.pem"))
            if files:
                print(f"Ingesting all Crone files in directory: {p}")
                ingest_crone_dir_flow(str(p))
            else:
                print(f"No files found in {p}")
        else:
            print(f"Ingesting single file: {p}")
            ingest_crone_pem_flow(str(p))
