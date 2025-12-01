#!/usr/bin/env python
"""Reset database schema"""

from models import Base
import sqlalchemy as sa
import os

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'alfauser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'alfapass123')
DB_NAME = os.getenv('DB_NAME', 'alfa_minerals')

engine = sa.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

print("🔄 Enabling PostGIS extension...")
with engine.connect() as conn:
    conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
    conn.commit()

print("🔄 Dropping all tables...")
Base.metadata.drop_all(engine)

print("📝 Creating all tables...")
Base.metadata.create_all(engine)

print("✅ Database reset complete!")
