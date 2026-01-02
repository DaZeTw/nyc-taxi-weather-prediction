# Taxi Dataset
## NYC TLC Yellow & Green Taxi Trip Record Datasets

### Overview

The NYC Taxi & Limousine Commission (TLC) Trip Record Data contains detailed information about individual taxi trips in New York City.  
This project focuses on **Yellow Taxi** and **Green Taxi** trip records, which are widely used for urban mobility analysis and machine learning tasks such as **trip duration prediction**.

This document describes the columns available in the **Yellow Taxi** and **Green Taxi** trip record datasets provided by the NYC Taxi & Limousine Commission (TLC).

### Yellow Taxi Dataset Columns

- `VendorID`: Identifier of the technology provider that recorded the trip.
- `tpep_pickup_datetime`: Date and time when the taxi meter was engaged (trip start time).
- `tpep_dropoff_datetime`: Date and time when the taxi meter was disengaged (trip end time).
- `passenger_count`: Number of passengers reported by the driver.
- `trip_distance`: Distance of the trip in miles.
- `RatecodeID`: Final rate code applied to the trip (e.g., standard rate, airport flat fare).
- `store_and_fwd_flag`: Indicates whether the trip record was stored in the vehicle memory before being sent to the server.
- `PULocationID`: Taxi zone ID where the passenger was picked up.
- `DOLocationID`: Taxi zone ID where the passenger was dropped off.
- `payment_type`: Method of payment used for the trip (cash, credit card, etc.).
- `fare_amount`: Time-and-distance fare calculated by the meter.
- `extra`: Miscellaneous extras and surcharges (e.g., night or rush hour charges).
- `mta_tax`: Mandatory Metropolitan Transportation Authority tax.
- `tip_amount`: Tip amount paid by the passenger (credit card tips only).
- `tolls_amount`: Total tolls paid during the trip.
- `improvement_surcharge`: Surcharge applied to fund transportation improvements.
- `total_amount`: Total amount charged to the passenger.
- `congestion_surcharge`: Congestion surcharge applied to trips in congested areas.
- `Airport_fee`: Flat fee applied for trips involving certain airports.
- `cbd_congestion_fee`: Central Business District congestion pricing fee (added from 2025 onward).


### Green Taxi Dataset Columns

- `VendorID`: Identifier of the technology provider that recorded the trip.
- `lpep_pickup_datetime`: Date and time when the trip began (meter engaged).
- `lpep_dropoff_datetime`: Date and time when the trip ended (meter disengaged).
- `store_and_fwd_flag`: Indicates whether the trip data was temporarily stored before transmission.
- `RatecodeID`: Final rate code applied to the trip.
- `PULocationID`: Taxi zone ID where the passenger was picked up.
- `DOLocationID`: Taxi zone ID where the passenger was dropped off.
- `passenger_count`: Number of passengers reported by the driver.
- `trip_distance`: Distance of the trip in miles.
- `fare_amount`: Base fare calculated by the meter.
- `extra`: Additional charges such as peak-hour surcharges.
- `mta_tax`: Mandatory MTA tax applied to the trip.
- `tip_amount`: Tip amount paid by the passenger (credit card tips only).
- `tolls_amount`: Total toll charges incurred during the trip.
- `ehail_fee`: Fee applied for trips booked through electronic hailing.
- `improvement_surcharge`: Surcharge applied to fund transportation improvements.
- `total_amount`: Total fare charged to the passenger.
- `payment_type`: Method of payment used for the trip.
- `trip_type`: Indicates whether the trip was a street-hail or a prearranged trip.
- `congestion_surcharge`: Congestion surcharge applied in designated areas.
- `cbd_congestion_fee`: Central Business District congestion pricing fee (added from 2025 onward).

### Taxi Zone Geo Table Columns (taxi_zones.shp)

- `LocationID`: Unique identifier of the taxi zone used in TLC taxi trip datasets.

- `zone`: Official name of the taxi zone.

- `borough`: Borough in which the taxi zone is located
  (Manhattan, Brooklyn, Queens, Bronx, Staten Island, or EWR).

- `latitude`: Latitude of the taxi zone centroid in WGS84 coordinate system.

- `longitude`: Longitude of the taxi zone centroid in WGS84 coordinate system.

### Data Source

NYC Taxi & Limousine Commission (TLC)  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Core Taxi Trip Dataset for Trip Duration Prediction

This table defines the core taxi trip dataset used in the
**nyc-taxi-weather-prediction** project.

It is created by merging **Yellow Taxi** and **Green Taxi** trip records
from the NYC Taxi & Limousine Commission (TLC) and retaining only columns
that represent real-world conditions affecting **trip duration**.


### Core Taxi Trip Table Columns
- `taxi_type`: Type of taxi service that recorded the trip (`yellow` or `green`).

- `pickup_time`: Timestamp when the taxi meter was engaged and the trip started.

- `dropoff_time`: Timestamp when the taxi meter was disengaged and the trip ended.

- `midpoint_latitude`: Latitude of the geographic mid-point between pickup and
  drop-off taxi zone centroids.

- `midpoint_longitude`: Longitude of the geographic mid-point between pickup and
  drop-off taxi zone centroids.

- `passenger_count`: Number of passengers reported by the driver.

- `trip_distance_miles`: Distance of the trip in miles.

- `rate_code_id`: Rate code applied at the start of the trip.

- `store_and_forward_flag`: Indicates whether the trip record was temporarily
  stored in the vehicle before being transmitted.

- `trip_duration`: Target variable representing the duration of the trip,
  calculated as `dropoff_time - pickup_time`.


# Weather Dataset
## NOAA Weather.gov API Datasets

This document describes the columns retrieved from the **Weather.gov API**,
provided by the National Oceanic and Atmospheric Administration (NOAA).
The data represents **observed weather conditions in New York City (NYC)** and
is collected from NOAA weather stations based on geographic location and time.

### Weather.gov Stations API (`/stations`)

This API provides metadata for NOAA weather stations, including their fixed
geographic locations. Weather stations serve as spatial reference points for
all observed weather measurements.

- `stationId`: Unique identifier of the NOAA weather station.
- `name`: Official name of the weather station.
- `latitude`: Latitude of the weather station in decimal degrees (WGS84).
- `longitude`: Longitude of the weather station in decimal degrees (WGS84).
- `elevation`: Elevation of the weather station above sea level.
- `timeZone`: Time zone in which the weather station is located.


### Weather.gov Observations API (`/stations/{stationId}/observations`)

This API provides time-series **observed weather data** recorded at a specific
NOAA weather station. These measurements reflect actual atmospheric conditions
at the time of observation.

- `time`: Timestamp indicating when the weather observation was recorded.
- `temperature`: Observed air temperature.
- `dewpoint`: Observed dew point temperature.
- `relativeHumidity`: Observed relative humidity percentage.
- `windSpeed`: Observed wind speed.
- `windDirection`: Observed wind direction in degrees.
- `windGust`: Observed maximum wind gust speed.
- `precipitation`: Observed precipitation amount during the previous hour.
- `snowfall`: Observed snowfall amount during the previous hour.
- `visibility`: Observed horizontal visibility distance.
- `pressure`: Observed atmospheric pressure.
- `stationId`: Identifier of the weather station that recorded the observation.


### Notes

- All weather data represents **measured observations**, not forecasts.
- Weather stations are treated as fixed geographic points.
- Observations are typically available at hourly or sub-hourly intervals.
- Missing values may occur depending on station reporting frequency.

### Data Source

National Oceanic and Atmospheric Administration (NOAA)  
National Weather Service (NWS)  
Weather.gov API  
https://api.weather.gov


## Core Weather Dataset for Trip Duration Prediction

- `time`: Timestamp indicating when the weather observation was recorded.

- `station_id`: Unique identifier of the NOAA weather station that recorded
  the observation.

- `latitude`: Latitude of the weather station in decimal degrees (WGS84).

- `longitude`: Longitude of the weather station in decimal degrees (WGS84).

- `temperature`: Observed air temperature at the station.

- `dewpoint`: Observed dew point temperature.

- `relative_humidity`: Observed relative humidity expressed as a percentage.

- `wind_speed`: Observed wind speed.

- `wind_direction`: Observed wind direction in degrees.

- `wind_gust`: Observed maximum wind gust speed.

- `precipitation`: Observed precipitation amount during the previous hour.

- `snowfall`: Observed snowfall amount during the previous hour.

- `visibility`: Observed horizontal visibility distance.

- `pressure`: Observed atmospheric pressure.
