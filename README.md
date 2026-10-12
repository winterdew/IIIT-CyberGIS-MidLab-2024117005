# CyberGIS Mid-lab

### Submitted by: Tatva Agarwal
### Roll Number: 2024117005
### Email: tatva.agarwal@research.iiit.ac.in

### Github Link: [https://github.com/winterdew/IIIT-CyberGIS-MidLab-2024117005](https://github.com/winterdew/IIIT-CyberGIS-MidLab-2024117005)


## Identified gap
While the Bhoonidhi platform has data access via direct download and Bhoonidhi API which is a subset of the OGC API standards, It does not provide a SensorThings Compliant API. Sensor things allows a unified access to Time Series data from both MQTT and HTTP. This is essential for collating multiple datasets together including ground based measurements like Tsunami Buoys. This helps end users be able to perform various temporal analyses without having to download the entire dataset. 

### Justification
A Sensor Things API provides the following benefits:
1. Eliminates requirement to parse different data formats like HDF5 and GeoTIFF and GeoJSON as some of the file formats in which such data can be provided for each end user. 
2. It allows a unified API where implementing a SensorThings compliant request mechanism is all you need to be able to fetch the data.
3. Allows for sensors supporting MQTT to attach and publish data.
4. The message bus allows for Publisher Subscriber architechture, where multiple consumers can subscribe to a topic and get real time information. 

Putting the scatterometer data provides a fast and interoperable access mechanism for processing it and to be used by various consumers concurrently. 

## Implementation

I have used the FROST-Server [https://github.com/FraunhoferIOSB/FROST-Server](https://github.com/FraunhoferIOSB/FROST-Server) software. Which fully implements the SensorThings API Specification for v1 and v1.1. 

The script takes in the data downloaded by manually from Bhoonidhi Platform and creates `Things` for all the grid cells. Each cell (thing) has Datastreams, which are posted as actual observations.

I have also created a small visualisation html page which can be used to test the functionality and showcase the usecases of such a system.

### Internal Data Model

The FROST-Server internally uses Post-GIS to store data in relational tables. The data is available to an external consumer in the form of SensorThing Things. Each `Thing` stores multiple `Datastreams`, which are collections of observations. The script also defines a `Sensor` to record the measurements. Each measurement is added to the Datastream as an `Observation`.

## Sample Queries

All queries are relative to the API root `http://localhost:8080/FROST-Server/v1.1` (called `$BASE` below). Spaces and quotes must be URL-encoded, so `curl -G --data-urlencode` is the easiest way to try them:

`curl -G "$BASE/Things" --data-urlencode "\$filter=properties/cellId eq 'r400_c0300'"`

**1. Find the cell nearest a point.**

`/Things?$filter=properties/cellId eq 'r400_c0300'&$expand=Locations`

**2. Full ascending wind-speed time series for that cell:**

`/Things?$filter=properties/cellId eq 'r400_c0300'
  &$expand=Datastreams($filter=properties/pass eq 'Ascending' and properties/variable eq 'wind_speed';
  $expand=Observations($orderby=phenomenonTime asc;$select=phenomenonTime,result))`

**3. Latest observation of a Datastream**:

`/Datastreams(1)/Observations?$orderby=phenomenonTime desc&$top=1`

**4. All Things inside a polygon**:

`/Things?$filter=geo.intersects(Locations/location, geography'POLYGON((70 5, 80 5, 80 15, 70 15, 70 5))')`

**5. Strong winds in a time window, with their locations:**

`/Observations?$filter=Datastream/ObservedProperty/name eq 'Wind Speed'
  and result gt 15 and phenomenonTime ge 2026-09-25T00:00:00Z
  &$expand=Datastream/Thing/Locations&$top=100
`

## Usage Instructions

**Prerequisites:** Docker with Compose, Python 3.9+.

1. **Start FROST-Server and PostGIS**

   `$ docker compose up -d`

   The API is then available at `http://localhost:8080/FROST-Server/v1.1` and MQTT at port `1883`.
   Check with: `curl http://localhost:8080/FROST-Server/v1.1`

2. **Install the Python dependencies**

   `$ pip install -r requirements.txt`

3. **Add the data.** Download the EOS-06 SCAT L3W `.h5` products from Bhoonidhi and place them as `dataset/<folder>/<product>.h5` next to `data_import.py`.

4. **Ingest**

   `$ python data_import.py`
   
   The script ingests the North Indian Ocean (0-30N, 40-100E) region at full 25 km resolution


**Note**: Wind direction is oceanographic (direction the wind blows *towards*, clockwise from North).

## Limitations: 

The limitations are: 
1. The data ingestion is still human mediated.
2. Generating `Thing` for each grid cell is not efficient. SensorThings is not ideal for Gridded Rasters. 
3. There is not security implementation in the current implementation. 


