import dlt
from dlt.sources.helpers import requests
from typing import Iterator, Dict, Any
from datetime import datetime
from pathlib import Path

@dlt.resource(
    name = "power_system_raw",
    write_disposition="append",
    primary_key="Minutes1UTC"
)
def hent_power_system_data(
    updated_at = dlt.sources.incremental("Minutes1UTC", initial_value="2025-10-01T00:00")
    ) -> Iterator[Dict[str, Any]]:
    """
    Henter realtime rådata fra Energy Data Service API.
    Bruker inkrementell last basert på "Minutes1UTC" feltet.
    Første gang den kjøres hentes alle data siden 1970-01-01T00:00.
    Alle data etterpå hentes ved påfølgende kjøringer.
    """
    base_url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow"
    # Hent siste oppdateringstidspunkt fra dlt-hub state
    last_updated = updated_at.start_value

    # Konverter til riktig format (yyyy-MM-ddTHH:mm) - fjern sekunder
    if isinstance(last_updated, str):
        dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
    else:
        dt = last_updated
    last_updated = dt.strftime("%Y-%m-%dT%H:%M")

    params = {
        "offset": 0,
        "start": last_updated, # inkrementell last verdi
        "sort": "Minutes1UTC"
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status() # gir feilkode hvis HTTP-koder ikke er 200-299
    data = response.json()
    records = data.get("records", [])
    print(f"Fetched {len(records)} new records")
    for record in records:
        yield record

@dlt.source
def energy_data_source():
    """
    DLT source som definerer bronse lag ingest med inkrementell last.
    """
    return hent_power_system_data()

def run_bronze_pipeline():
    """
    Kjører bronze pipeline - laster kun nye data basert på siste Minutes1UTC.
    """
    
    # Definer hvor dlt skal lagre sin metadata
    # Jeg gjør dette for å holde prosjektfiler samlet og ha kontroll
    project_root = Path(__file__).parent.parent
    pipelines_dir = project_root / ".dlt" / "pipeline_metadata"

    pipeline = dlt.pipeline(
        pipeline_name="energy_bronze",
        destination="duckdb", #lagrer til en fil som heter energy_bronze.duckdb
        dataset_name="bronze_energy_data",
        pipelines_dir=str(pipelines_dir)
    )
    
    # Kjør pipeline - dlt håndterer inkrementell last og annet automatisk
    import time
    start_time = time.time()
    load_info = pipeline.run(
        energy_data_source(),
        table_name="power_system_raw"
    ) # kan gi permission error i Windows innimellom, prøv igjen hvis det skjer
    end_time = time.time()
    duration = end_time - start_time
    print(f"\n{'='*60}")
    print(f"✓ Pipeline completed successfully!")
    print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    print(f"{'='*60}")
    return load_info


if __name__ == "__main__":
    # NB: Første gang du kjører dette, vil det hente alle data 
    # siden 1970-01-01 (som er default selv om jeg skriver det eksplisitt)
    # På følgende kjøringer vil det kun hente nye data siden siste Minutes1UTC
    # Så lenge du ikke sletter dlt-hub state, vil dette fungere som inkrementell last
    run_bronze_pipeline()
