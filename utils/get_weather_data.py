import requests
import pandas as pd
import time

def fetch_weather_info(lat, lon, trip_time):
    """
    Fetches real-time/historical weather for exact coordinates and time.
    
    Args:
        lat (float): Latitude of the trip
        lon (float): Longitude of the trip
        trip_time (str or datetime): Timestamp of the trip
        
    Returns:
        dict: Weather features (temperature, precipitation, wind, pressure)
    """
    # 1. Format the time
    ts = pd.to_datetime(trip_time)
    date_str = ts.strftime('%Y-%m-%d')
    target_hour = ts.floor('h')

    # 2. Construct API URL (Archive API for historical/recent data)
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={date_str}&end_date={date_str}"
        "&hourly=temperature_2m,precipitation,windspeed_10m,pressure_msl"
        "&timezone=UTC"
    )

    try:
        response = requests.get(url, timeout=10)
        
        # Handle Rate Limiting (429)
        if response.status_code == 429:
            time.sleep(2) # Short sleep for real-time API needs
            response = requests.get(url, timeout=10)
            
        response.raise_for_status()
        data = response.json()

        # 3. Filter for the specific hour
        df = pd.DataFrame({
            "time": pd.to_datetime(data["hourly"]["time"]),
            "temperature_2m": data["hourly"]["temperature_2m"],
            "precipitation": data["hourly"]["precipitation"],
            "windspeed_10m": data["hourly"]["windspeed_10m"],
            "pressure_msl": data["hourly"]["pressure_msl"],
        })

        result = df[df['time'] == target_hour]

        if result.empty:
            return {"error": "Time not found in API response"}

        return result.iloc[0].to_dict()

    except Exception as e:
        return {"error": f"Failed to fetch weather: {str(e)}"}

# --- Example Call ---
if __name__ == "__main__":
    from datetime import datetime
    weather = fetch_weather_info(40.712850, -74.0060, datetime.now())
    print(weather)