import yaml
import pandas as pd
from scripts.extract import get_coordinates, get_onecall
from scripts.transform import flatten_city_payload
from scripts.load import make_engine, write_weather


def main():
    with open("config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    api_key = cfg["openweather"]["api_key"]
    cities = cfg["cities"]
    units = cfg["api"]["units"]
    exclude = cfg["api"]["exclude"]

    # Extract
    coords = [c for c in (get_coordinates(city, api_key) for city in cities) if c]
    frames = []
    for c in coords:
        payload = get_onecall(c["lat"], c["lon"], api_key, units, exclude)
        if payload:
            frames.append(flatten_city_payload(c["city"], payload))
    df = pd.concat(frames, ignore_index=True)
    print(f"Extracted rows: {len(df)}")

    # Load
    az = cfg["azure_sql"]
    engine = make_engine(az["server"], az["database"], az["username"], az["password"])
    write_weather(df, engine, table="WeatherDataClean", if_exists="replace")
    print("Loaded into Azure SQL ✓")


if __name__ == "__main__":
    main()
