from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, Enum, ForeignKey, UniqueConstraint, Index, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from datetime import datetime
import enum


Base = declarative_base()


class Survey(Base):
    """Top-level survey from Crone PEM file"""
    __tablename__ = "surveys"
    
    id = Column(Integer, primary_key=True)
    survey_id = Column(String(50), unique=True, nullable=False)  # e.g., "P-008"
    line_id = Column(String(50))  # e.g., "50N"
    survey_date = Column(DateTime, nullable=False)
    
    # File metadata
    data_format = Column(String(50))  # "230"
    data_units = Column(String(50))  # "nanoTesla/sec"
    operator_name = Column(String(255))
    
    # Equipment
    equipment_model = Column(String(255))  # "S-COIL Metric Crystal-Master 16.66"
    coil_area = Column(Float)  # Coil area in m² (from HE3 tag)
    coil_number = Column(String(50))  # "#117"
    
    # Survey parameters
    peak_current_amps = Column(Float)  # <CUR>
    num_channels = Column(Integer)  # 16
    
    # Client/Company
    client_name = Column(String(255))  # "North American Nickel"
    acquisition_company = Column(String(255))  # "Crone Geophysics & Exploration Ltd."
    
    # Raw header data (for reference)
    header_data = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    loops = relationship("Loop", back_populates="survey", cascade="all, delete-orphan")
    receiver_stations = relationship("ReceiverStation", back_populates="survey", cascade="all, delete-orphan")
    measurements = relationship("EMResponse", back_populates="survey", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index("idx_survey_date_line", "survey_date", "line_id"),
    )


class Loop(Base):
    """Transmitter loop from <L00> to <L82> tags"""
    __tablename__ = "loops"
    
    id = Column(Integer, primary_key=True)
    survey_id = Column(Integer, ForeignKey("surveys.id"), nullable=False)
    
    # Loop point identification
    loop_point_number = Column(Integer)  # 0-82 from L00, L01, etc.
    
    # Coordinates (UTM or projected)
    easting = Column(Float, nullable=False)
    northing = Column(Float, nullable=False)
    elevation = Column(Float)
    coordinate_units = Column(String(10), default="metres")  # "metres" or "feet"
    
    # PostGIS geometry: Point with SRID 26918 (UTM Zone 18N)
    geometry = Column(Geometry("POINT", srid=26918), index=True, nullable=True)
    # WGS84 geometry for web mapping
    geometry_wgs84 = Column(Geometry("POINT", srid=4326), index=True, nullable=True)
    
    # Loop geometry (from <TXS> tag)
    loop_size_x_units = Column(Float)  # 600.0
    loop_size_y_units = Column(Float)  # 700.0
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    survey = relationship("Survey", back_populates="loops")
    
    __table_args__ = (
        UniqueConstraint("survey_id", "loop_point_number", name="uq_loop_point"),
        Index("idx_loop_location", "easting", "northing"),
    )


class ReceiverStation(Base):
    """Receiver/Profile station from <P00> to <P20> tags"""
    __tablename__ = "receiver_stations"
    
    id = Column(Integer, primary_key=True)
    survey_id = Column(Integer, ForeignKey("surveys.id"), nullable=False)
    
    # Station identification
    station_number = Column(Integer, nullable=False)  # 0-20 from P00, P01, etc.
    station_label = Column(String(50))  # e.g., "0N", "25N", "50N"
    
    # Coordinates
    easting = Column(Float, nullable=False)
    northing = Column(Float, nullable=False)
    elevation = Column(Float)
    coordinate_units = Column(String(10), default="metres")
    
    # PostGIS geometry: Point with SRID 26918 (UTM Zone 18N)
    geometry = Column(Geometry("POINT", srid=26918), index=True, nullable=True)
    # WGS84 geometry for web mapping
    geometry_wgs84 = Column(Geometry("POINT", srid=4326), index=True, nullable=True)
    
    # Survey distance along profile
    distance_along_profile_m = Column(Float)  # "stn" field: 0.0, 25.0, 50.0, etc.
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    survey = relationship("Survey", back_populates="receiver_stations")
    measurements = relationship("EMResponse", back_populates="receiver_station", cascade="all, delete-orphan")
    
    __table_args__ = (
        UniqueConstraint("survey_id", "station_number", name="uq_receiver_station"),
        Index("idx_receiver_location", "easting", "northing"),
    )


class ComponentEnum(enum.Enum):
    """EM response component"""
    X = "X"
    Y = "Y"
    Z = "Z"
    UNK = "UNK"  # Unknown/unclassified


class EMResponse(Base):
    """Complete EM response at a receiver station (one record = one line in data section)"""
    __tablename__ = "em_responses"
    
    id = Column(Integer, primary_key=True)
    survey_id = Column(Integer, ForeignKey("surveys.id"), nullable=False)
    receiver_station_id = Column(Integer, ForeignKey("receiver_stations.id"), nullable=False)
    
    # Component
    component = Column(Enum(ComponentEnum), nullable=False)  # X, Y, Z, UNK
    
    # Source file (allows same response from different files)
    source_file = Column(String(255), nullable=False)  # e.g., "100EAV.STP", "200EAV.STP"
    
    # Station identifiers from file
    station_label = Column(String(50))  # "0N", "25N", etc.
    receiver_code = Column(String(20))  # "XR8", "ZR10R", etc.
    receiver_number = Column(Integer)  # Receiver # from probe
    
    # Measurement parameters
    angle_degrees = Column(Float)  # 90 degrees
    num_samples = Column(Integer)  # 256, 192, 288, etc.
    instrument_id = Column(String(50))  # Instrument identifier
    
    # Primary pulse response
    primary_pulse_nt_per_sec = Column(Float, nullable=False)
    
    # Secondary values
    secondary_pulse_1 = Column(Float)
    secondary_pulse_2 = Column(Float)
    
    # Decay info
    current_on_time_value = Column(Float)
    apparent_resistance = Column(Float)
    phase_component = Column(Float)
    phase_magnitude = Column(Float)
    
    # Raw data quality
    is_valid = Column(Boolean, default=True)
    measurement_time = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    survey = relationship("Survey", back_populates="measurements")
    receiver_station = relationship("ReceiverStation", back_populates="measurements")
    channels = relationship("OffTimeChannel", back_populates="response", cascade="all, delete-orphan")
    anomalies = relationship("Anomaly", back_populates="response", cascade="all, delete-orphan")
    
    __table_args__ = (
        # Allow same (survey, station, component) but different files
        UniqueConstraint("survey_id", "receiver_station_id", "component", "source_file", name="uq_response_per_file"),
        Index("idx_response_component", "receiver_station_id", "component"),
    )


class OffTimeChannel(Base):
    """Off-time electromagnetic channel (13 channels per response)"""
    __tablename__ = "offtime_channels"
    
    id = Column(Integer, primary_key=True)
    response_id = Column(Integer, ForeignKey("em_responses.id"), nullable=False)
    
    # Channel identification
    channel_number = Column(Integer, nullable=False)  # 1-13
    
    # Time gate (milliseconds from pulse)
    time_ms = Column(Float, nullable=False)  # Predefined: 1.597e-05, 3.193e-05, etc.
    
    # Measured amplitude
    amplitude_nt_per_sec = Column(Float, nullable=False)  # Response value
    
    # Derived fields
    log_amplitude = Column(Float)  # log10(abs(amplitude))
    decay_rate = Column(Float)  # Rate of decay
    normalized_amplitude = Column(Float)  # Amplitude / primary pulse
    
    # Data quality
    is_valid = Column(Boolean, default=True)
    snr_estimate = Column(Float)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    response = relationship("EMResponse", back_populates="channels")
    
    __table_args__ = (
        UniqueConstraint("response_id", "channel_number", name="uq_channel"),
        Index("idx_channel_time", "response_id", "time_ms"),
    )


class Anomaly(Base):
    """Detected anomaly in EM response"""
    __tablename__ = "anomalies"
    
    id = Column(Integer, primary_key=True)
    response_id = Column(Integer, ForeignKey("em_responses.id"), nullable=False)
    
    # Anomaly classification
    response_type = Column(String(10))  # "A", "B", "C", "D" from Crone classification
    anomaly_strength = Column(String(50))  # "Strong", "Moderate", "Weak"
    
    # Anomaly characteristics
    peak_amplitude_nt_per_sec = Column(Float)
    peak_channel = Column(Integer)
    decay_exponent = Column(Float)
    
    # Interpretive results
    conductivity_estimate = Column(Float)  # S/m
    conductivity_method = Column(String(50))
    depth_estimate_m = Column(Float)
    
    # Confidence
    confidence = Column(Float)  # 0.0-1.0
    notes = Column(String(500))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    response = relationship("EMResponse", back_populates="anomalies")

