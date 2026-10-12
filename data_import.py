"""Ingest EOS-06 Scatterometer L3W wind products (dataset/*/*.h5) into FROST-Server.

Each product is a daily global 0.25 deg grid (720 x 1440) holding wind speed,
wind direction and a quality flag for the ascending and descending passes.

SensorThings data model
-----------------------
- Sensor            : one, the EOS-06 Scatterometer.
- ObservedProperty  : two, Wind Speed and Wind Direction.
- Thing + Location  : one per 25 km grid cell (point at the cell centre), created
                      the first time that cell holds a valid wind vector.
- Datastream        : four per Thing - {Ascending, Descending} x {speed, direction}.
- Observation       : one per Datastream per product. phenomenonTime is the
                      acquisition interval of the daily product, resultTime is its
                      production date, parameters carry the product id and the raw
                      quality flag of the wind vector cell.

Grid layout (verified against the product quicklook): row 0 is the southernmost
row (90S) and column 0 starts at 0E, so the centre of cell (r, c) is
lat = -90 + (r + 0.5) * 0.25, lon = (c + 0.5) * 0.25.

Wind direction is in oceanographic convention: the direction the wind blows
towards, clockwise from North.

Usage:
    pip install h5py numpy requests
    python data_import.py                                  # North Indian Ocean, 25 km
    python data_import.py --bbox -90 -180 90 180 --stride 4  # global, every 4th cell (1 deg)
"""

import argparse
import glob
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import h5py
import numpy as np
import requests

SOURCE = "EOS-06 SCAT L3W 25km"
GRID_ROWS, GRID_COLS, CELL_DEG = 720, 1440, 0.25
PASSES = ("Ascending", "Descending")
SPEED_FILL, DIRECTION_FILL = 32767, 65535

OBSERVED_PROPERTIES = {
    "wind_speed": {
        "name": "Wind Speed",
        "definition": "http://mmisw.org/ont/cf/parameter/wind_speed",
        "description": "Speed of the 10 m equivalent neutral wind over the ocean surface.",
    },
    "wind_direction": {
        "name": "Wind Direction",
        "definition": "http://mmisw.org/ont/cf/parameter/wind_to_direction",
        "description": "Direction the 10 m wind is blowing towards, clockwise from North.",
    },
}

UNITS = {
    "wind_speed": {
        "name": "metre per second",
        "symbol": "m/s",
        "definition": "http://unitsofmeasure.org/ucum.html#m/s",
    },
    "wind_direction": {
        "name": "degree",
        "symbol": "deg",
        "definition": "http://unitsofmeasure.org/ucum.html#deg",
    },
}

MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"

_local = threading.local()


def session():
    """One requests.Session per worker thread."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers["Content-Type"] = "application/json"
    return _local.session


class Frost:
    def __init__(self, base_url):
        self.base = base_url.rstrip("/")

    def url(self, path):
        return path if path.startswith("http") else f"{self.base}/{path}"

    def get_all(self, path, params=None):
        """GET a collection, following @iot.nextLink.

        Pages are fetched with $skip, so a stable $orderby is required, otherwise
        the database may return overlapping pages and skip entities.
        """
        params = {"$orderby": "id asc", **(params or {})}
        url, items = self.url(path), []
        while url:
            r = session().get(url, params=params)
            r.raise_for_status()
            body = r.json()
            items.extend(body["value"])
            url, params = body.get("@iot.nextLink"), None
        return items

    def post(self, path, entity):
        r = session().post(self.url(path), json=entity)
        if not r.ok:
            raise RuntimeError(f"POST {path} failed ({r.status_code}): {r.text}")
        return r

    def create(self, path, entity):
        """POST an entity and return the id FROST assigned to it."""
        location = self.post(path, entity).headers["Location"]
        raw = location.rsplit("(", 1)[1].rstrip(")")
        return int(raw) if raw.isdigit() else raw.strip("'")

    def find_or_create(self, path, name, entity):
        found = self.get_all(path, {"$filter": f"name eq '{name}'", "$select": "id"})
        return found[0]["@iot.id"] if found else self.create(path, entity)


def odata_time(t):
    """'2026-268T00:45:16.400' (year-day of year) -> ISO 8601 UTC."""
    t = t.decode().strip() if isinstance(t, bytes) else t.strip()
    dt = datetime.strptime(t, "%Y-%jT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def cell_centres():
    lats = -90 + (np.arange(GRID_ROWS) + 0.5) * CELL_DEG
    lons = (np.arange(GRID_COLS) + 0.5) * CELL_DEG
    return lats, np.where(lons > 180, lons - 360, lons)


def region_mask(bbox, stride):
    """Boolean (rows, cols) mask of grid cells inside bbox, keeping every `stride`-th cell."""
    min_lat, min_lon, max_lat, max_lon = bbox
    lats, lons = cell_centres()
    lat_ok = (lats >= min_lat) & (lats <= max_lat)
    if min_lon <= max_lon:
        lon_ok = (lons >= min_lon) & (lons <= max_lon)
    else:  # bbox crosses the antimeridian
        lon_ok = (lons >= min_lon) | (lons <= max_lon)
    lat_ok[np.arange(GRID_ROWS) % stride != 0] = False
    lon_ok[np.arange(GRID_COLS) % stride != 0] = False
    return lat_ok[:, None] & lon_ok[None, :]


def read_product(path, mask):
    """Read one .h5 product, returning its metadata and per-pass valid wind vectors inside mask."""
    with h5py.File(path, "r") as f:
        sd = f["science_data"]
        attrs = sd.attrs
        speed_scale = float(attrs["Wind Speed Scale"])
        direction_scale = float(attrs["Wind Direction Scale"])
        product = {
            "id": os.path.splitext(os.path.basename(path))[0],
            "phenomenonTime": f"{odata_time(attrs['Start Revolution Time'])}/{odata_time(attrs['End Revolution Time'])}",
            "resultTime": odata_time(attrs["Production Date"]),
            "passes": {},
        }
        for p in PASSES:
            speed = sd[f"{p}_wind_speed"][()]
            direction = sd[f"{p}_wind_direction"][()]
            quality = sd[f"{p}_wind_quality_flag"][()]
            valid = mask & (speed != SPEED_FILL) & (direction != DIRECTION_FILL)
            rows, cols = np.nonzero(valid)
            product["passes"][p] = {
                "cells": list(zip(rows.tolist(), cols.tolist())),
                "wind_speed": np.round(speed[rows, cols] * speed_scale, 2).tolist(),
                "wind_direction": np.round(direction[rows, cols] * direction_scale, 2).tolist(),
                "quality": quality[rows, cols].tolist(),
            }
    return product


def cell_id(row, col):
    return f"r{row:03d}_c{col:04d}"


def thing_entity(row, col, sensor_id, op_ids):
    lats, lons = cell_centres()
    lat, lon = round(float(lats[row]), 3), round(float(lons[col]), 3)
    label = f"{abs(lat):.3f}{'N' if lat >= 0 else 'S'} {abs(lon):.3f}{'E' if lon >= 0 else 'W'}"
    datastreams = []
    for p in PASSES:
        for variable, op in OBSERVED_PROPERTIES.items():
            datastreams.append({
                "name": f"{p} {op['name']} @ {label}",
                "description": f"EOS-06 Scatterometer L3W {p.lower()} pass {op['name'].lower()} for the 25 km cell at {label}.",
                "observationType": MEASUREMENT,
                "unitOfMeasurement": UNITS[variable],
                "Sensor": {"@iot.id": sensor_id},
                "ObservedProperty": {"@iot.id": op_ids[variable]},
                "properties": {"pass": p, "variable": variable},
            })
    return {
        "name": f"SCAT L3W cell {label}",
        "description": f"EOS-06 Scatterometer 25 km wind vector cell centred at {label}.",
        "properties": {
            "source": SOURCE,
            "cellId": cell_id(row, col),
            "row": row,
            "col": col,
            "cellSizeDeg": CELL_DEG,
        },
        "Locations": [{
            "name": f"Cell centre {label}",
            "description": f"Centre of the {CELL_DEG} deg grid cell {cell_id(row, col)}.",
            "encodingType": "application/geo+json",
            "location": {"type": "Point", "coordinates": [lon, lat]},
        }],
        "Datastreams": datastreams,
    }


def load_datastreams(frost):
    """Map cellId -> {(pass, variable): datastream id} for every Thing this script created."""
    things = frost.get_all("Things", {
        "$filter": f"properties/source eq '{SOURCE}'",
        "$select": "id,properties",
        "$expand": "Datastreams($select=id,properties)",
        "$top": 1000,
    })
    return {
        t["properties"]["cellId"]: {
            (d["properties"]["pass"], d["properties"]["variable"]): d["@iot.id"] for d in t["Datastreams"]
        }
        for t in things
    }


def ingested_datastreams(frost, product_id):
    """Ids of Datastreams that already hold an Observation of this product."""
    observations = frost.get_all("Observations", {
        "$filter": f"parameters/productId eq '{product_id}'",
        "$select": "id",
        "$expand": "Datastream($select=id)",
        "$top": 10000,
    })
    return {o["Datastream"]["@iot.id"] for o in observations}


def observation_batches(product, datastreams, skip, batch_size):
    """Yield batches of (datastream id, dataArray row) for a product's wind vectors not yet in FROST.

    All rows of one grid cell go into the same batch: FROST generates a cell's FeatureOfInterest
    from its Location on first use, and two parallel requests doing that for the same Location
    deadlock in the database.
    """
    by_cell = {}
    for p, data in product["passes"].items():
        for i, (row, col) in enumerate(data["cells"]):
            ds = datastreams[cell_id(row, col)]
            params = {"productId": product["id"], "qualityFlag": data["quality"][i]}
            for variable in OBSERVED_PROPERTIES:
                if ds[(p, variable)] not in skip:
                    by_cell.setdefault((row, col), []).append(
                        (ds[(p, variable)], [product["phenomenonTime"], product["resultTime"], data[variable][i], params]))
    batch = []
    for rows in by_cell.values():
        batch.extend(rows)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def post_observations(frost, batch):
    """POST a batch of (datastream id, row) using the dataArray extension."""
    grouped = {}
    for ds_id, row in batch:
        grouped.setdefault(ds_id, []).append(row)
    body = [{
        "Datastream": {"@iot.id": ds_id},
        "components": ["phenomenonTime", "resultTime", "result", "parameters"],
        "dataArray": rows,
    } for ds_id, rows in grouped.items()]
    # FROST answers with one entry per row: the new Observation's link, or an error message.
    errors = [x for x in frost.post("CreateObservations", body).json() if "Observations(" not in str(x)]
    if errors:
        raise RuntimeError(f"{len(errors)} of {len(batch)} observations were rejected by FROST")
    return len(batch)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8080/FROST-Server/v1.1", help="FROST SensorThings API root")
    parser.add_argument("--dataset", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset"))
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("MIN_LAT", "MIN_LON", "MAX_LAT", "MAX_LON"),
                        default=[0, 40, 30, 100], help="region to ingest (default: North Indian Ocean)")
    parser.add_argument("--stride", type=int, default=1, help="keep every N-th grid row/column (1 = full 25 km)")
    parser.add_argument("--batch-size", type=int, default=5000, help="observations per CreateObservations request")
    parser.add_argument("--workers", type=int, default=4, help="parallel HTTP requests")
    args = parser.parse_args()

    frost = Frost(args.url)
    files = sorted(glob.glob(os.path.join(args.dataset, "*", "*.h5")))
    if not files:
        raise SystemExit(f"No .h5 files found under {args.dataset}")
    mask = region_mask(args.bbox, args.stride)
    print(f"{len(files)} products, {int(mask.sum())} grid cells in the selected region")

    # Shared Sensor and ObservedProperties.
    sensor_id = frost.find_or_create("Sensors", "EOS-06 Scatterometer", {
        "name": "EOS-06 Scatterometer",
        "description": "Ku-band pencil-beam scatterometer (SCAT) on ISRO's EOS-06 (Oceansat-3), Level-3 25 km global wind vector product.",
        "encodingType": "text/html",
        "metadata": "https://bhoonidhi.nrsc.gov.in",
    })
    op_ids = {v: frost.find_or_create("ObservedProperties", op["name"], op) for v, op in OBSERVED_PROPERTIES.items()}

    print("Reading products ...")
    products = [read_product(path, mask) for path in files]

    # Create a Thing (with Location and Datastreams) for every cell that holds data and is not in FROST yet.
    datastreams = load_datastreams(frost)
    needed = {cell for prod in products for data in prod["passes"].values() for cell in data["cells"]}
    missing = sorted(c for c in needed if cell_id(*c) not in datastreams)
    if missing:
        print(f"Creating {len(missing)} Things with Locations and Datastreams ...")
        with ThreadPoolExecutor(args.workers) as pool:
            for i, _ in enumerate(pool.map(lambda c: frost.create("Things", thing_entity(*c, sensor_id, op_ids)), missing), 1):
                if i % 500 == 0 or i == len(missing):
                    print(f"  {i}/{len(missing)}")
        datastreams = load_datastreams(frost)

    # Push Observations product by product, skipping Datastreams that already have this product
    # (so re-runs and interrupted runs only add what is missing).
    total = 0
    for product in products:
        skip = ingested_datastreams(frost, product["id"])
        with ThreadPoolExecutor(args.workers) as pool:
            done = sum(pool.map(lambda b: post_observations(frost, b),
                                observation_batches(product, datastreams, skip, args.batch_size)))
        total += done
        print(f"  {product['id']}: {done} new observations ({len(skip)} already present)")
    print(f"Done, {total} observations ingested.")


if __name__ == "__main__":
    main()
