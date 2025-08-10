# main.py
import yaml
import pandas as pd

from scripts.extract import get_coordinates, get_onecall
from scripts.transform import flatten_city_payload
from scripts.load import make_engine, write_weather


def main():
    # ---- Read config ----
    with open("config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    api_key = cfg["openweather"]["api_key"]
    cities = cfg["cities"]
    units = cfg.get("api", {}).get("units", "metric")
    exclude = cfg.get("api", {}).get("exclude", "minutely,alerts")

    # ---- Extract & transform ----
    frames = []
    for city in cities:
        c = get_coordinates(city, api_key)
        if not c:
            continue
        payload = get_onecall(c["lat"], c["lon"], api_key, units, exclude)
        if not payload:
            continue
        frames.append(flatten_city_payload(c["city"], payload))

    if not frames:
        raise RuntimeError("No data extracted from OpenWeather.")

    df = pd.concat(frames, ignore_index=True)
    print(f"Extracted rows: {len(df)}")

    # ---- Load ----
    az = cfg["azure_sql"]  # or cfg["sql"] if you switch naming later
    engine = make_engine(
        server=az["server"],
        database=az["database"],
        username=az["username"],
        password=az["password"],
    )
    write_weather(df, engine, table="WeatherDataClean", if_exists="replace")
    print("Loaded into Azure SQL OK")


if __name__ == "__main__":
    main()
