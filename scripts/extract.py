import requests
from typing import Optional, Dict


def get_coordinates(city: str, api_key: str) -> Optional[Dict]:
    url = f"https://api.openweathermap.org/geo/1.0/direct?q={city},CA&limit=1&appid={api_key}"
    r = requests.get(url, timeout=20)
    if r.status_code == 200 and r.json():
        j = r.json()[0]
        return {"city": city, "lat": j["lat"], "lon": j["lon"]}
    print(f"[WARN] Geocode failed for {city}: {r.status_code}")
    return None


def get_onecall(lat: float, lon: float, api_key: str, units="metric", exclude="minutely,alerts"):
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={exclude}&units={units}&appid={api_key}"
    r = requests.get(url, timeout=30)
    return r.json() if r.status_code == 200 else None
