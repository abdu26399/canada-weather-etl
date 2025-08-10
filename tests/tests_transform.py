from scripts.transform import flatten_city_payload

def test_flatten_shapes():
    sample = {
        "current":{"dt": 1700000000, "temp": 20, "humidity": 50, "weather":[{"description":"ok"}]},
        "hourly":[{"dt":1700003600,"temp":21,"humidity":49,"weather":[{"description":"ok"}]}],
        "daily":[{"dt":1700086400,"temp":{"day":25,"min":18,"max":27,"night":20,"eve":23,"morn":19},"humidity":55,"weather":[{"description":"ok"}]}]
    }
    df = flatten_city_payload("Toronto", sample)
    assert set(["temp_day","temp_min","temp_max","temp_actual"]).issubset(df.columns)
