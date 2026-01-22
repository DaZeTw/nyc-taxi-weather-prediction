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
## Open-Meteo Archive API Datasets

This document describes the columns retrieved from the Open-Meteo Archive API,
provided by Open‑Meteo, a global provider of historical, grid-based weather data.

For the purpose of this project, a spatially and temporally filtered subset of the
Open-Meteo dataset was extracted to represent historical hourly weather conditions within
New York City (NYC). Weather data was collected for a predefined set of geographic points
covering the NYC area and aligned with taxi trip records for downstream analysis.

Unlike station-based weather systems, Open-Meteo provides gridded reanalysis-based weather data,
ensuring consistent spatial coverage across the study area without reliance on individual
physical weather stations.

## Open-Meteo Archive API (`/v1/archive`)

This API provides **historical, hourly weather data** for a given geographic coordinate
(latitude and longitude) over a specified time range.

Each coordinate represents a **spatial weather point**, which implicitly covers a surrounding
geographic area.

Weather data is retrieved for multiple spatial points arranged in a regular grid
with approximately **10 km spacing**, ensuring full coverage of New York City while
minimizing the number of external API requests.


## Spatial Reference (Weather Points - NYC)

Each weather observation is associated with a fixed spatial point.

- `point_id`: Unique identifier of the spatial weather point.
- `latitude`: Latitude of the weather point in decimal degrees (WGS84).
- `longitude`: Longitude of the weather point in decimal degrees (WGS84).


## Open-Meteo Hourly Weather Variables

The Open-Meteo Archive API returns time-series weather data at **hourly resolution**.
All values correspond to the specified geographic point and timestamp.

- `time`: Timestamp indicating the hour of the weather observation (UTC).
- `temperature_2m`: Air temperature measured at 2 meters above ground level (°C).
- `precipitation`: Total precipitation during the hour (mm).
- `windspeed_10m`: Wind speed measured at 10 meters above ground level (km/h).
- `pressure_msl`: Mean sea-level atmospheric pressure (hPa).


## Notes

- All weather data represents **historical reanalysis-based observations**, not real-time forecasts.
- Data is provided at **hourly temporal resolution**, suitable for time-aligned integration
  with taxi trip pickup times.
- Spatial coverage is achieved using a **regular grid of weather points** rather than
  individual weather stations.
- Each taxi trip location is mapped to the **nearest weather point**, implicitly defining
  the spatial coverage area of that point.
- Weather variables vary smoothly across space, making this representation appropriate
  for city-scale transportation analysis.
- Missing values are rare due to the gridded nature of the dataset.


## Core Weather Dataset for Trip Duration Prediction

The final weather dataset used for enriching taxi trip records contains the following columns:

- `time`: Timestamp indicating the hour of the weather observation (UTC).

- `point_id`: Identifier of the spatial weather point associated with the observation.

- `latitude`: Latitude of the weather point in decimal degrees (WGS84).

- `longitude`: Longitude of the weather point in decimal degrees (WGS84).

- `temperature_2m`: Air temperature at 2 meters above ground level.

- `precipitation`: Hourly precipitation amount.

- `windspeed_10m`: Wind speed at 10 meters above ground level.

- `pressure_msl`: Mean sea-level atmospheric pressure.


## Data Source

Open-Meteo  
Open-Meteo Archive API  
https://archive-api.open-meteo.com
