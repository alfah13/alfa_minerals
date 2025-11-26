"""
Pipeline to ingest Crone PEM survey data files with Prefect orchestration.
"""

import os
import re
from typing import Dict, List
from pathlib import Path
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from prefect import flow, task, get_run_logger

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

@task(name="Load Crone File")
def load_crone_file(file_path: str) -> str:
    """Load Crone PEM file"""
    logger = get_run_logger()
    logger.info(f"Loading file: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    logger.info(f"Loaded {len(content)} bytes")
    return content


@task(name="Parse Header")
def parse_header(file_content: str) -> Dict:
    """Parse header metadata"""
    logger = get_run_logger()
    
    header = {}
    
    patterns = {
        'FMT': r'<FMT>\s+(\S+)',
        'UNI': r'<UNI>\s+(.+)',
        'OPR': r'<OPR>\s+(.+)',
        'CUR': r'<CUR>\s+([\d.]+)',
        'TXS': r'<TXS>\s+([\d.]+)\s+([\d.]+)',
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, file_content)
        if match:
            if key == 'TXS':
                header[key] = (float(match.group(1)), float(match.group(2)))
            else:
                header[key] = match.group(1).strip()
    
    lines = file_content.split('\n')
    for i, line in enumerate(lines):
        if 'North American Nickel' in line:
            header['CLIENT'] = line.strip()
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line:
                    header['SURVEY_ID'] = next_line
    
    logger.info(f"Parsed header: {header}")
    return header


@task(name="Parse Loop Coordinates")
def parse_loop_coordinates(file_content: str) -> List[Dict]:
    """Parse transmitter loop coordinates"""
    logger = get_run_logger()
    
    loops = []
    pattern = r'<L(\d+)>\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\d)'
    
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


@task(name="Parse Receiver Stations")
def parse_receiver_stations(file_content: str) -> List[Dict]:
    """Parse receiver stations"""
    logger = get_run_logger()
    
    stations = []
    pattern = r'<P(\d+)>\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+(\d)\s+([-\d.]+)'
    
    for match in re.finditer(pattern, file_content):
        station_num = int(match.group(1))
        stations.append({
            'station_number': station_num,
            'easting': float(match.group(2)),
            'northing': float(match.group(3)),
            'elevation': float(match.group(4)),
            'units': int(match.group(5)),
            'distance_m': float(match.group(6))
        })
    
    logger.info(f"Parsed {len(stations)} receiver stations")
    return stations


@task(name="Parse Time Gates")
def parse_time_gates(file_content: str) -> List[float]:
    """Extract time gate values"""
    logger = get_run_logger()
    
    time_gates = []
    lines = file_content.split('\n')
    
    for i, line in enumerate(lines):
        if '$' in line:
            prev_line = lines[i-1].strip()
            values = prev_line.split()
            time_gates = [float(v) for v in values if v]
            break
    
    logger.info(f"Parsed {len(time_gates)} time gates: {time_gates}")
    return time_gates


@task(name="Parse Measurements")
def parse_measurements(file_content: str, time_gates: List[float]) -> List[Dict]:
    """Parse EM measurement records"""
    logger = get_run_logger()
    
    measurements = []
    
    comp_pattern = r'(\d+N|ON|0N)\s+([XYZ]R\d+R?)\s+(\d+)\s+([A-Z])\s+([\d.]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)'
    d4_pattern = r'D4\s+([-\d.e+]+)\s+([-\d.e+]+)\s+([-\d.e+]+)\s+([-\d.e+]+)'
    
    lines = file_content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        comp_match = re.match(comp_pattern, line)
        if comp_match:
            station_label = comp_match.group(1)
            receiver_code = comp_match.group(2)
            
            if 'Z' in receiver_code:
                component = 'Z'
            elif 'X' in receiver_code:
                component = 'X'
            else:
                component = 'Y'
            
            current_record = {
                'station_label': station_label,
                'receiver_code': receiver_code,
                'receiver_number': int(comp_match.group(3)),
                'angle': float(comp_match.group(5)),
                'num_samples': int(comp_match.group(7)),
                'component': component,
            }
            
            i += 1
            if i < len(lines):
                d4_match = re.match(d4_pattern, lines[i].strip())
                if d4_match:
                    current_record['current_on_time'] = float(d4_match.group(1))
                    current_record['apparent_resistance'] = float(d4_match.group(2))
                    current_record['phase_component'] = float(d4_match.group(3))
                    current_record['phase_magnitude'] = float(d4_match.group(4))
            
            i += 1
            if i < len(lines):
                data_line = lines[i].strip()
                values = []
                for v in data_line.split():
                    try:
                        values.append(float(v))
                    except ValueError:
                        pass
                
                if values:
                    current_record['primary_pulse'] = values[0]
                    current_record['secondary_1'] = values[1] if len(values) > 1 else None
                    current_record['secondary_2'] = values[2] if len(values) > 2 else None
                    current_record['channels'] = values[1:] if len(values) > 1 else []
                    
                    measurements.append(current_record)
        
        i += 1
    
    logger.info(f"Parsed {len(measurements)} measurement records")
    return measurements


# ============= DATABASE STORAGE TASKS =============

@task(name="Store Survey")
def store_survey(header: Dict, file_path: str) -> int:
    """Create or get Survey record"""
    logger = get_run_logger()
    session = get_db_session()
    
    survey_id = header.get('SURVEY_ID', Path(file_path).stem)
    
    existing_survey = session.query(Survey).filter_by(survey_id=survey_id).first()
    
    if existing_survey:
        logger.info(f"Survey '{survey_id}' already exists (ID: {existing_survey.id})")
        session.close()
        return existing_survey.id
    
    survey = Survey(
        survey_id=survey_id,
        survey_date=datetime.now(),
        data_format=header.get('FMT', '230'),
        data_units=header.get('UNI', 'nanoTesla/sec'),
        operator_name=header.get('OPR'),
        peak_current_amps=float(header.get('CUR', 0)),
        client_name='North American Nickel',
        acquisition_company='Crone Geophysics & Exploration Ltd.',
        header_data=header,
    )
    session.add(survey)
    session.commit()
    
    logger.info(f"Created survey: {survey.id}")
    session.close()
    return survey.id


@task(name="Store Loops")
def store_loops(survey_id: int, loops: List[Dict]) -> int:
    """Store transmitter loop coordinates"""
    logger = get_run_logger()
    session = get_db_session()
    
    for loop_data in loops:
        loop = Loop(
            survey_id=survey_id,
            loop_point_number=loop_data['loop_number'],
            easting=loop_data['easting'],
            northing=loop_data['northing'],
            elevation=loop_data['elevation'],
            coordinate_units='metres' if loop_data['units'] == 0 else 'feet',
        )
        session.add(loop)
    
    session.commit()
    logger.info(f"Stored {len(loops)} loop coordinates")
    session.close()
    return len(loops)


@task(name="Store Receiver Stations")
def store_receiver_stations(survey_id: int, stations: List[Dict]) -> int:
    """Store receiver profile stations"""
    logger = get_run_logger()
    session = get_db_session()
    
    for station_data in stations:
        station_label = f"{station_data['station_number']:02d}N"
        
        station = ReceiverStation(
            survey_id=survey_id,
            station_number=station_data['station_number'],
            station_label=station_label,
            easting=station_data['easting'],
            northing=station_data['northing'],
            elevation=station_data['elevation'],
            coordinate_units='metres' if station_data['units'] == 0 else 'feet',
            distance_along_profile_m=station_data['distance_m'],
        )
        session.add(station)
    
    session.commit()
    logger.info(f"Stored {len(stations)} receiver stations")
    session.close()
    return len(stations)


@task(name="Store EM Responses")
def store_em_responses(survey_id: int, measurements: List[Dict], time_gates: List[float]) -> int:
    """Store EM responses and off-time channels"""
    logger = get_run_logger()
    session = get_db_session()
    
    all_stations = session.query(ReceiverStation).filter_by(survey_id=survey_id).all()
    station_map = {s.station_label: s for s in all_stations}
    
    logger.info(f"Available stations: {sorted(list(station_map.keys()))}")
    
    count = 0
    skipped = 0
    
    for meas in measurements:
        station_label = meas['station_label']
        normalized_label = station_label.lstrip('0').upper() if station_label else None
        if not normalized_label or normalized_label == 'N':
            normalized_label = '00N'
        
        station = None
        
        if station_label in station_map:
            station = station_map[station_label]
        
        if not station:
            for db_label in station_map.keys():
                if db_label.lstrip('0').upper() == normalized_label.upper():
                    station = station_map[db_label]
                    break
        
        if not station:
            logger.warning(f"Station '{station_label}' not found")
            skipped += 1
            continue
        
        response = EMResponse(
            survey_id=survey_id,
            receiver_station_id=station.id,
            component=ComponentEnum[meas['component']],
            station_label=meas['station_label'],
            receiver_code=meas['receiver_code'],
            primary_pulse_nt_per_sec=meas.get('primary_pulse', 0),
            secondary_pulse_1=meas.get('secondary_1'),
            secondary_pulse_2=meas.get('secondary_2'),
            current_on_time_value=meas.get('current_on_time'),
            apparent_resistance=meas.get('apparent_resistance'),
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
                time_ms=time_ms * 1000,
                amplitude_nt_per_sec=amplitude,
                is_valid=True,
            )
            session.add(channel)
        
        count += 1
    
    session.commit()
    logger.info(f"Stored {count} EM responses with channels")
    if skipped > 0:
        logger.warning(f"Skipped {skipped} measurements")
    session.close()
    return count


# ============= MAIN FLOW =============

@flow(name="Ingest Crone PEM Survey Data", description="Complete pipeline for Crone EM file ingestion")
def ingest_crone_pem_flow(file_path: str):
    """
    Complete Crone PEM data ingestion with Prefect orchestration.
    
    Args:
        file_path: Path to Crone PEM file
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting Crone PEM ingestion: {file_path}")
    
    # Parse file
    file_content = load_crone_file(file_path)
    header = parse_header(file_content)
    loops = parse_loop_coordinates(file_content)
    stations = parse_receiver_stations(file_content)
    time_gates = parse_time_gates(file_content)
    measurements = parse_measurements(file_content, time_gates)
    
    # Store to database
    survey_id = store_survey(header, file_path)
    loops_count = store_loops(survey_id, loops)
    stations_count = store_receiver_stations(survey_id, stations)
    responses_count = store_em_responses(survey_id, measurements, time_gates)
    
    result = {
        'survey_id': survey_id,
        'file': file_path,
        'loops_stored': loops_count,
        'stations_stored': stations_count,
        'responses_stored': responses_count,
        'total_channels': responses_count * len(time_gates) if len(time_gates) > 0 else 0,
        'status': 'success'
    }
    
    logger.info(f"✅ Ingestion complete: {result}")
    return result


if __name__ == "__main__":
    import sys
    file_path = sys.argv[1] if len(sys.argv) > 1 else "./data/data_archive/P-141/100EAV.STP"
    ingest_crone_pem_flow(file_path)
