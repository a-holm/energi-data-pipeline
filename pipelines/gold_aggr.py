import dlt
import duckdb
from pathlib import Path
from datetime import datetime,timedelta

def run_gold_aggregation():
    """
    Transformerer sølvdatasett til gull lag med aggregerte metrics.
    Bruker incremental upserts for å kun prosessere nye data.
    """
    # Koble til silver DuckDB database
    silver_db = dlt.pipeline(
        pipeline_name="energy",
        destination=dlt.destinations.duckdb("energy.duckdb"),
        dataset_name="silver_energy_data"
    ).sql_client()
    with silver_db as client:
        # Opprett schema hvis det ikke finnes
        client.execute_sql("CREATE SCHEMA IF NOT EXISTS gold_energy_data")
        # Opprett gull-tabell for 5-minutters glidende gjennomsnitt
        client.execute_sql("""
            CREATE TABLE IF NOT EXISTS gold_energy_data.power_system_5min_avg (
                time_id TIMESTAMP PRIMARY KEY,
                -- Produksjonsmetrics
                avg_co2_emission DOUBLE,
                avg_total_production DOUBLE,
                avg_renewable_ratio DOUBLE,
                avg_solar_production DOUBLE,
                avg_wind_production DOUBLE,
                avg_offshore_wind DOUBLE,
                avg_onshore_wind DOUBLE,
                
                -- Last og forbruk
                avg_production_large_plants DOUBLE,
                avg_production_small_plants DOUBLE,
                
                -- Internasjonal handel
                avg_exchange_sum DOUBLE,
                avg_exchange_germany DOUBLE,
                avg_exchange_netherlands DOUBLE,
                avg_exchange_great_brt DOUBLE,
                avg_exchange_norway DOUBLE,
                avg_exchange_sweden DOUBLE,
                avg_exchange_dk1_dk2 DOUBLE,
                
                -- Volatilitetsmetrics for ML
                production_volatility DOUBLE,
                co2_volatility DOUBLE,
                wind_solar_ratio DOUBLE,
                
                -- Tidsbaserte features for ML siden vi lagde en dim_time tabell
                day_of_week INTEGER,
                hour_of_day INTEGER,
                is_weekend BOOLEAN,
                season INTEGER
            )
        """)
        # Finn siste kalkulerte timestamp i gull
        last_processed = client.execute_sql("""
            SELECT COALESCE(MAX(time_id), '1970-01-01'::TIMESTAMP) as last_time
            FROM gold_energy_data.power_system_5min_avg
        """)
        last_timestamp = last_processed[0][0] if last_processed else '1970-01-01'
        print(f"Last processed timestamp in gold: {last_timestamp}")
        # Kalkuler 5-minutters glidende gjennomsnitt og andre metrics
        client.execute_sql(f"""
            INSERT INTO gold_energy_data.power_system_5min_avg
            WITH time_series AS (
                SELECT 
                    fs.time_id,
                    fs.co2_emission,
                    (fs.production_large_plants + fs.production_small_plants) as total_production,
                    CASE 
                        WHEN (fs.production_large_plants + fs.production_small_plants) > 0 
                        THEN (fs.solar_production + fs.offshore_wind_production + fs.onshore_wind_production) 
                             / (fs.production_large_plants + fs.production_small_plants)
                        ELSE 0 
                    END as renewable_ratio,
                    fs.solar_production,
                    (fs.offshore_wind_production + fs.onshore_wind_production) as wind_production,
                    fs.offshore_wind_production,
                    fs.onshore_wind_production,
                    fs.production_large_plants,
                    fs.production_small_plants,
                    fs.exchange_sum,
                    fs.exchange_germany,
                    fs.exchange_netherlands,
                    fs.exchange_great_brt,
                    fs.exchange_norway,
                    fs.exchange_sweden,
                    fs.exchange_dk1_dk2,
                    dt.day_of_week,
                    dt.hour as hour_of_day,
                    dt.is_weekend,
                    dt.season
                FROM silver_energy_data.fact_power_system fs
                JOIN silver_energy_data.dim_time dt ON fs.time_id = dt.time_id
                WHERE fs.time_id > '{last_timestamp - timedelta(minutes=4)}'
            ),
            aggregated AS (
                SELECT 
                    time_id,
                    -- 5-minutters glidende gjennomsnitt
                    AVG(co2_emission) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_co2_emission,
                    AVG(total_production) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_total_production,
                    AVG(renewable_ratio) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_renewable_ratio,
                    AVG(solar_production) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_solar_production,
                    AVG(wind_production) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_wind_production,
                    AVG(offshore_wind_production) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_offshore_wind,
                    AVG(onshore_wind_production) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_onshore_wind,
                    AVG(production_large_plants) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_production_large_plants,
                    AVG(production_small_plants) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_production_small_plants,
                    AVG(exchange_sum) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_sum,
                    AVG(exchange_germany) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_germany,
                    AVG(exchange_netherlands) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_netherlands,
                    AVG(exchange_great_brt) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_great_brt,
                    AVG(exchange_norway) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_norway,
                    AVG(exchange_sweden) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_sweden,
                    AVG(exchange_dk1_dk2) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as avg_exchange_dk1_dk2,
                    
                    -- Volatilitet (standardavvik for siste 5 minutter)
                    STDDEV(total_production) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as production_volatility,
                    STDDEV(co2_emission) OVER (
                        ORDER BY time_id 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as co2_volatility,
                    
                    -- Forhold mellom vind og sol
                    CASE 
                        WHEN solar_production > 0 
                        THEN wind_production / solar_production
                        ELSE 1
                    END as wind_solar_ratio,
                    
                    -- Tidsfeatures
                    day_of_week,
                    hour_of_day,
                    is_weekend,
                    season
                FROM time_series
            )
            SELECT 
                time_id,
                avg_co2_emission,
                avg_total_production,
                avg_renewable_ratio,
                avg_solar_production,
                avg_wind_production,
                avg_offshore_wind,
                avg_onshore_wind,
                avg_production_large_plants,
                avg_production_small_plants,
                avg_exchange_sum,
                avg_exchange_germany,
                avg_exchange_netherlands,
                avg_exchange_great_brt,
                avg_exchange_norway,
                avg_exchange_sweden,
                avg_exchange_dk1_dk2,
                production_volatility,
                co2_volatility,
                wind_solar_ratio,
                day_of_week,
                hour_of_day,
                is_weekend,
                season
            FROM aggregated
            WHERE time_id > '{last_timestamp}'
            ON CONFLICT (time_id)  DO NOTHING
        """)
        print(f"Gold aggregation completed - 5-minute moving averages updated")

if __name__ == "__main__":
    run_gold_aggregation()
    # lagre gull-laget som csv
    file_path = "ml_features.csv"
    gold_db = dlt.pipeline(
        pipeline_name="energy",
        destination=dlt.destinations.duckdb("energy.duckdb"),
        dataset_name="gold_energy_data"
    ).sql_client()
    with gold_db as client:
        client.execute_sql(f"""
            COPY (
                SELECT 
                    time_id,
                    avg_co2_emission,
                    avg_total_production,
                    avg_renewable_ratio,
                    avg_solar_production,
                    avg_wind_production,
                    avg_offshore_wind,
                    avg_onshore_wind,
                    production_volatility,
                    co2_volatility,
                    wind_solar_ratio,
                    hour_of_day,
                    is_weekend,
                    season
                FROM gold_energy_data.power_system_5min_avg
                ORDER BY time_id
            ) TO '{file_path}' (HEADER, DELIMITER ',')
        """)
        print(f"Gold layer data exported to {file_path}")