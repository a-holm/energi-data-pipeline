import dlt
import duckdb
from pathlib import Path
from datetime import datetime

def run_silver_transformation():
    """
    Transformerer bronze data til silver layer med fact og dimension tabeller.
    Bruker incremental upserts for å kun prosessere nye data.
    """
    # Koble til bronze DuckDB database
    bronze_db = dlt.pipeline(
        pipeline_name="energy_bronze",
        destination=dlt.destinations.duckdb("energy_bronze.duckdb"),
        dataset_name="bronze_energy_data"
    ).sql_client()
    
    with bronze_db as client:
        # Opprett schema hvis det ikke finnes
        client.execute_sql("CREATE SCHEMA IF NOT EXISTS silver_energy_data")
        # Lag tabeller hvis de ikke finnes
        # Dim Time
        client.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS silver_energy_data.dim_time (
                time_id TIMESTAMP PRIMARY KEY,
                date DATE,
                hour INTEGER,
                minute INTEGER,
                day_of_week INTEGER,
                is_weekend BOOLEAN
            )
        """)
        # Fact Power System
        client.execute_sql("""
            CREATE TABLE IF NOT EXISTS silver_energy_data.fact_power_system (
                time_id TIMESTAMP,
                co2_emission DOUBLE,
                production_large_plants DOUBLE,
                production_small_plants DOUBLE,
                solar_production DOUBLE,
                offshore_wind_production DOUBLE,
                onshore_wind_production DOUBLE,
                exchange_sum DOUBLE,
                exchange_germany DOUBLE,
                exchange_netherlands DOUBLE,
                exchange_great_brt DOUBLE,
                exchange_norway DOUBLE,
                exchange_sweden DOUBLE,
                exchange_dk1_dk2 DOUBLE,
                PRIMARY KEY (time_id)
            )
        """)
        # Finn siste prosesserte timestamp i silver
        last_processed = client.execute_sql("""
            SELECT COALESCE(MAX(time_id), '1970-01-01'::TIMESTAMP) as last_time
            FROM silver_energy_data.fact_power_system
        """)
        last_timestamp = last_processed[0][0] if last_processed else '1970-01-01'
        print(f"Last processed timestamp: {last_timestamp}")
        # === DIMENSION TABELLER (UPSERT) ===
        client.execute_sql(f"""
            INSERT INTO silver_energy_data.dim_time
            SELECT DISTINCT
                minutes1_utc as time_id,
                CAST(minutes1_utc AS DATE) as date,
                EXTRACT(HOUR FROM minutes1_utc) as hour,
                EXTRACT(MINUTE FROM minutes1_utc) as minute,
                EXTRACT(DOW FROM minutes1_utc) as day_of_week,
                CASE 
                    WHEN EXTRACT(DOW FROM minutes1_utc) IN (0, 6) THEN TRUE 
                    ELSE FALSE 
                END as is_weekend
            FROM bronze_energy_data.power_system_raw
            WHERE minutes1_utc > '{last_timestamp}'
            ON CONFLICT (time_id) DO NOTHING
        """)
        # === FACT TABELL (APPEND ONLY) ===
        # Insert kun nye facts
        rows_inserted = client.execute_sql(f"""
            INSERT INTO silver_energy_data.fact_power_system
            SELECT
                r.minutes1_utc as time_id,
                r.co2_emission,
                r.production_ge100_mw as production_large_plants,
                r.production_lt100_mw as production_small_plants,
                r.solar_power as solar_production,
                r.offshore_wind_power as offshore_wind_production,
                r.onshore_wind_power as onshore_wind_production,
                r.exchange_sum,
                r.exchange_dk1_de + r.exchange_dk2_de as exchange_germany,
                r.exchange_dk1_nl as exchange_netherlands,
                r.exchange_dk1_gb as exchange_great_brt,
                r.exchange_dk1_no as exchange_norway,
                r.exchange_dk1_se + r.exchange_dk2_se exchange_sweden,
                r.exchange_dk1_dk2
            FROM bronze_energy_data.power_system_raw r
            WHERE minutes1_utc > '{last_timestamp}'
                AND minutes1_utc IS NOT NULL
            ON CONFLICT (time_id) DO NOTHING
        """)
        print(f"Silver transformations completed")
        # Vis statistikk
        stats = client.execute_sql("""
            SELECT 
                COUNT(*) as total_facts,
                MIN(time_id) as earliest,
                MAX(time_id) as latest
            FROM silver_energy_data.fact_power_system
        """)
        if stats:
            stats = stats[0]
            print(f"Total facts: {stats[0]}, Range: {stats[1]} to {stats[2]}")
        else:
            print("No facts found in silver layer.")


if __name__ == "__main__":
    run_silver_transformation()