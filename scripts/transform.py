import pandas as pd
from datetime import datetime


def flatten_city_payload(city: str, payload: dict) -> pd.DataFrame:
    now = datetime.utcnow()
    rows = []

    # current
    cur = payload.get("current") or {}
    if cur:
        rows.append({
            "city": city, "type": "current",
            "timestamp": datetime.utcfromtimestamp(cur.get("dt", now.timestamp())),
            "temp": cur.get("temp"),  # number
            "humidity": cur.get("humidity"),
            "weather": ((cur.get("weather") or [{}])[0] or {}).get("description"),
            "wind_speed": cur.get("wind_speed"),
            "wind_deg": cur.get("wind_deg"),
            "wind_gust": cur.get("wind_gust"),
            "source_time": now,
        })

    # hourly
    for h in (payload.get("hourly") or []):
        rows.append({
            "city": city, "type": "hourly",
            "timestamp": datetime.utcfromtimestamp(h.get("dt", now.timestamp())),
            "temp": h.get("temp"),  # number
            "humidity": h.get("humidity"),
            "weather": ((h.get("weather") or [{}])[0] or {}).get("description"),
            "wind_speed": h.get("wind_speed"),
            "wind_deg": h.get("wind_deg"),
            "wind_gust": h.get("wind_gust"),
            "source_time": now,
        })

    # daily (temp is a dict: day/min/max/night/eve/morn)
    for d in (payload.get("daily") or []):
        rows.append({
            "city": city, "type": "daily",
            "timestamp": datetime.utcfromtimestamp(d.get("dt", now.timestamp())),
            "temp": d.get("temp"),  # dict
            "humidity": d.get("humidity"),
            "weather": ((d.get("weather") or [{}])[0] or {}).get("description"),
            "wind_speed": d.get("wind_speed"),
            "wind_deg": d.get("wind_deg"),
            "wind_gust": d.get("wind_gust"),
            "source_time": now,
        })

    df = pd.DataFrame(rows)

    # wind_gust is optional; fill 0 to simplify visuals
    if "wind_gust" in df.columns:
        df["wind_gust"] = df["wind_gust"].fillna(0)

    # split daily 'temp' dict safely
    def part(x, k):
        return x.get(k) if isinstance(x, dict) else None

    mask_daily = df["type"] == "daily"
    for key, col in [
        ("day",   "temp_day"),
        ("min",   "temp_min"),
        ("max",   "temp_max"),
        ("night", "temp_night"),
        ("eve",   "temp_eve"),
        ("morn",  "temp_morn"),
    ]:
        df.loc[mask_daily, col] = df.loc[mask_daily, "temp"].apply(lambda x, k=key: part(x, k))

    # numeric temp for current/hourly
    mask_simple = df["type"].isin(["current", "hourly"])
    df.loc[mask_simple, "temp_actual"] = pd.to_numeric(df.loc[mask_simple, "temp"], errors="coerce")

    # daily average of available parts (ignore nulls)
    if mask_daily.any():
        part_cols = ["temp_morn", "temp_eve", "temp_night"]
        have_cols = [c for c in part_cols if c in df.columns]
        if have_cols:
            df.loc[mask_daily, "daily_avg"] = df.loc[mask_daily, have_cols].apply(
                lambda r: (r.dropna().mean() if r.dropna().size > 0 else None), axis=1
            )
        else:
            df["daily_avg"] = None

    # one temperature to use everywhere
    df["temp_unified"] = None
    df.loc[mask_daily,  "temp_unified"] = df.loc[mask_daily, "daily_avg"].where(
        df["daily_avg"].notna(), df.get("temp_max")
    )
    df.loc[mask_simple, "temp_unified"] = df.loc[mask_simple, "temp_actual"]

    # drop the mixed 'temp' source column
    if "temp" in df.columns:
        df = df.drop(columns=["temp"])

    # enforce numeric types
    num_cols = [
        "humidity","wind_speed","wind_deg","wind_gust",
        "temp_day","temp_min","temp_max","temp_night","temp_eve","temp_morn",
        "temp_actual","daily_avg","temp_unified"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # drop rows with no usable temperature (keeps PBI clean)
    df = df[df["temp_unified"].notna()].copy()

    # tidy column order
    ordered = [
        "city","type","timestamp","temp_unified",
        "temp_actual","temp_day","temp_min","temp_max","temp_morn","temp_eve","temp_night",
        "humidity","weather","wind_speed","wind_deg","wind_gust","source_time","daily_avg"
    ]
    df = df[[c for c in ordered if c in df.columns]]

    return df
