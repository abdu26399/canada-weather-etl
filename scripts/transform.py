import pandas as pd
from datetime import datetime


def flatten_city_payload(city: str, payload: dict) -> pd.DataFrame:
    now = datetime.utcnow()
    rows = []

    cur = payload.get("current") or {}
    if cur:
        rows.append({
            "city": city, "type": "current",
            "timestamp": datetime.utcfromtimestamp(cur["dt"]),
            "temp": cur.get("temp"),
            "humidity": cur.get("humidity"),
            "weather": (cur.get("weather") or [{}])[0].get("description"),
            "wind_speed": cur.get("wind_speed"),
            "wind_deg": cur.get("wind_deg"),
            "wind_gust": cur.get("wind_gust"),
            "source_time": now
        })

    for h in payload.get("hourly") or []:
        rows.append({
            "city": city, "type": "hourly",
            "timestamp": datetime.utcfromtimestamp(h["dt"]),
            "temp": h.get("temp"),
            "humidity": h.get("humidity"),
            "weather": (h.get("weather") or [{}])[0].get("description"),
            "wind_speed": h.get("wind_speed"),
            "wind_deg": h.get("wind_deg"),
            "wind_gust": h.get("wind_gust"),
            "source_time": now
        })

    for d in payload.get("daily") or []:
        rows.append({
            "city": city, "type": "daily",
            "timestamp": datetime.utcfromtimestamp(d["dt"]),
            "temp": d.get("temp"),
            "humidity": d.get("humidity"),
            "weather": (d.get("weather") or [{}])[0].get("description"),
            "wind_speed": d.get("wind_speed"),
            "wind_deg": d.get("wind_deg"),
            "wind_gust": d.get("wind_gust"),
            "source_time": now
        })

    df = pd.DataFrame(rows)
    if "wind_gust" in df.columns:
        df["wind_gust"] = df["wind_gust"].fillna(0)

    mask_daily = df["type"] == "daily"
    df.loc[mask_daily, "temp_day"] = df.loc[mask_daily, "temp"].apply(
        lambda x: x.get("day") if isinstance(x, dict) else None)
    df.loc[mask_daily, "temp_min"] = df.loc[mask_daily, "temp"].apply(
        lambda x: x.get("min") if isinstance(x, dict) else None)
    df.loc[mask_daily, "temp_max"] = df.loc[mask_daily, "temp"].apply(
        lambda x: x.get("max") if isinstance(x, dict) else None)
    df.loc[mask_daily, "temp_night"] = df.loc[mask_daily, "temp"].apply(
        lambda x: x.get("night") if isinstance(x, dict) else None)
    df.loc[mask_daily, "temp_eve"] = df.loc[mask_daily, "temp"].apply(
        lambda x: x.get("eve") if isinstance(x, dict) else None)
    df.loc[mask_daily, "temp_morn"] = df.loc[mask_daily, "temp"].apply(
        lambda x: x.get("morn") if isinstance(x, dict) else None)

    mask_simple = df["type"].isin(["current", "hourly"])
    df.loc[mask_simple, "temp_actual"] = df.loc[mask_simple, "temp"].astype(float)

    return df.drop(columns=["temp"])
