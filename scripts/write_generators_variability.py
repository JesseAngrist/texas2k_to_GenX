"""
write_generators_variability.py
================================
Compute hourly wind and solar capacity factors from ERA5 reanalysis data (via
atlite) and write Generators_variability.csv for GenX.

Inputs (relative to scripts/ directory):
  ../case/inputs/resources/VRE.csv   – resource names produced by write_generators.jl
  ../TAMU_data/buses.csv             – bus coordinates
  ../ERCOT_Load/texas_2025.nc        – atlite ERA5 cutout cache (created if absent)
  ../ERCOT_Load/master.csv           – defines which UTC hours appear in the model
                                       (may cover fewer than 8760 hours if some months
                                        of load data are missing)

Output:
  ../case/inputs/system/Generators_variability.csv  – hourly CF time series, one row
                                                       per row in master.csv, aligned
                                                       with Demand_data.csv Time_Index

Requirements:
  pip install atlite pandas
  A Copernicus CDS API key configured at ~/.cdsapirc (for ERA5 download).
  Accept the ERA5 license at https://cds.climate.copernicus.eu before first run.
"""

import os
import sys
import numpy as np
import pandas as pd
import atlite

# ---------------------------------------------------------------------------
# Paths (all relative to this script's directory)
# ---------------------------------------------------------------------------
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
VRE_CSV      = os.path.join(SCRIPT_DIR, "../case/inputs/resources/VRE.csv")
BUSES_CSV    = os.path.join(SCRIPT_DIR, "../TAMU_data/buses.csv")
CUTOUT_PATH  = os.path.join(SCRIPT_DIR, "../ERCOT_Load/texas_2025.nc")
MASTER_CSV   = os.path.join(SCRIPT_DIR, "../ERCOT_Load/master.csv")
OUT_CSV      = os.path.join(SCRIPT_DIR, "../case/inputs/system/Generators_variability.csv")
YEAR         = 2025

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cutout_is_prepared(c, min_hours=6000):
    """
    Return True only if the cutout has actual ERA5 time-series data.

    atlite writes the .nc skeleton (with metadata but no data variables) on
    Cutout() construction, before prepare() is called.  Checking isfile() alone
    is therefore insufficient — a prior interrupted run may have left an empty
    skeleton.  We verify that at least one data variable exists AND that the
    time dimension has at least min_hours entries.
    """
    try:
        data_vars = list(c.data.data_vars)
        if len(data_vars) == 0:
            return False
        n_times = int(c.data.dims.get("time", 0))
        return n_times >= min_hours
    except Exception:
        return False


def get_ts(da, lon, lat):
    """
    Extract a 1-D time series from an xarray DataArray for the grid cell
    nearest to (lon, lat).

    Identifies dimension roles by name rather than position, so it is robust
    to the dim ordering used by different atlite / xarray versions.
    """
    dims = list(da.dims)

    # Classify each dim as time, latitude, or longitude by name
    time_dims = [d for d in dims if "time" in d.lower()]
    # common spatial dim names used by atlite/ERA5
    y_dims    = [d for d in dims if d in ("y", "lat", "latitude")]
    x_dims    = [d for d in dims if d in ("x", "lon", "longitude")]

    # Fallback for unrecognised names: spatial dims are everything that isn't time
    if not y_dims or not x_dims:
        spatial = [d for d in dims if d not in time_dims]
        if len(spatial) >= 2:
            y_dims = [spatial[-2]]
            x_dims = [spatial[-1]]

    if not x_dims or not y_dims:
        # 1-D or fully unrecognised layout – just flatten
        return np.array(da).flatten()

    x_dim = x_dims[0]
    y_dim = y_dims[0]

    x_arr = np.array(da.coords[x_dim])
    y_arr = np.array(da.coords[y_dim])
    xi    = int(np.argmin(np.abs(x_arr - float(lon))))
    yi    = int(np.argmin(np.abs(y_arr - float(lat))))
    ts    = da.isel(**{x_dim: xi, y_dim: yi})
    return np.array(ts).flatten()


# ---------------------------------------------------------------------------
# Load VRE resource list
# ---------------------------------------------------------------------------
vre_df = pd.read_csv(VRE_CSV)

# Resource name format: "{FuelCode}_{bus_number}_{generator_id}"
# e.g. "WND_13246_1" or "SUN_3017_2"
def parse_resource(name):
    parts    = name.split("_")
    fuel     = parts[0]           # "WND" or "SUN"
    bus_num  = int(parts[1])
    return fuel, bus_num

# ---------------------------------------------------------------------------
# Load bus coordinates
# ---------------------------------------------------------------------------
buses_df = pd.read_csv(BUSES_CSV)
# Column names: "# of AC Lines", "PU Volt", "Number", ..., "Longitude", "Latitude", ...
bus_coord = buses_df.set_index("Number")[["Longitude", "Latitude"]]

# ---------------------------------------------------------------------------
# Collect unique bus locations needed
# ---------------------------------------------------------------------------
bus_set = set()
for name in vre_df["Resource"]:
    _, bus_num = parse_resource(name)
    bus_set.add(bus_num)

# Build bounding box with 0.5° padding
lons = [bus_coord.loc[b, "Longitude"] for b in bus_set if b in bus_coord.index]
lats = [bus_coord.loc[b, "Latitude"]  for b in bus_set if b in bus_coord.index]

if not lons:
    print("ERROR: No bus coordinates found for any VRE resource. Check VRE.csv and buses.csv.")
    sys.exit(1)

x_min, x_max = min(lons) - 0.5, max(lons) + 0.5
y_min, y_max = min(lats) - 0.5, max(lats) + 0.5

# ---------------------------------------------------------------------------
# Build or load atlite cutout
# ---------------------------------------------------------------------------
os.makedirs(os.path.dirname(CUTOUT_PATH), exist_ok=True)

print(f"Opening atlite cutout: {CUTOUT_PATH}")
cutout = atlite.Cutout(
    path   = CUTOUT_PATH,
    module = "era5",
    x      = slice(x_min, x_max),
    y      = slice(y_min, y_max),
    time   = str(YEAR),
)

if not cutout_is_prepared(cutout):
    print(f"Preparing ERA5 cutout for Texas {YEAR}. Downloading weather data (may take ~1-2 hours)...")
    # Only download variables needed for wind and solar PV:
    #   "wind"        -> 100 m u/v wind components
    #   "influx"      -> surface solar radiation downwards
    #   "temperature" -> 2 m air temperature (PV panel efficiency correction)
    cutout.prepare(features=["wind", "influx", "temperature"])
else:
    print(f"Using existing ERA5 cutout ({int(cutout.data.dims.get('time', 0))} timesteps).")

# ---------------------------------------------------------------------------
# Compute capacity factors
# ---------------------------------------------------------------------------
print("Computing solar capacity factors...")
solar_xr = cutout.pv(
    panel                     = "CSi",
    orientation               = "latitude_optimal",
    capacity_factor_timeseries = True,   # return hourly CF(time,y,x), not time-mean
)

print("Computing wind capacity factors...")
wind_xr = cutout.wind(
    turbine                   = "Vestas_V112_3MW",
    capacity_factor_timeseries = True,   # return hourly CF(time,y,x), not time-mean
)

print(f"atlite solar dims: {dict(solar_xr.sizes)}")

if "time" not in solar_xr.dims or solar_xr.sizes["time"] < 6000:
    print(
        "ERROR: Cutout has fewer than 6000 timesteps — it may be an empty skeleton from a "
        "previous interrupted run. Delete the .nc file and re-run:\n"
        f"  rm {CUTOUT_PATH}"
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Align to the demand time series
#
# master.csv defines which UTC hours appear in the model (some months may be
# absent if load data was unavailable).  We select exactly those hours from
# the ERA5 DataArrays so that every row in Generators_variability.csv
# corresponds to the matching row in Demand_data.csv.
# ---------------------------------------------------------------------------
master_df  = pd.read_csv(MASTER_CSV)
# Parse UTC timestamps and strip timezone info to match xarray's naive UTC index
utc_times  = (pd.to_datetime(master_df["interval_start_utc"], utc=True)
                .dt.tz_localize(None)
                .values)  # numpy datetime64[ns], timezone-naive UTC

solar_xr = solar_xr.sel(time=utc_times, method="nearest")
wind_xr  = wind_xr.sel(time=utc_times, method="nearest")

n_times  = len(utc_times)
print(f"Aligned to {n_times} demand timesteps "
      f"({master_df['interval_start_local'].iloc[0]} – "
      f"{master_df['interval_start_local'].iloc[-1]}).")

# ---------------------------------------------------------------------------
# Build Generators_variability DataFrame
# ---------------------------------------------------------------------------
variability = {"Time_Index": list(range(1, n_times + 1))}

missing_buses = set()

for _, row in vre_df.iterrows():
    name            = row["Resource"]
    fuel, bus_num   = parse_resource(name)

    if bus_num not in bus_coord.index:
        print(f"WARNING: Bus {bus_num} not in buses.csv; filling {name} with zeros.")
        variability[name] = [0.0] * n_times  # n_times = demand timesteps
        missing_buses.add(bus_num)
        continue

    lon = bus_coord.loc[bus_num, "Longitude"]
    lat = bus_coord.loc[bus_num, "Latitude"]

    if fuel == "WND":
        ts = get_ts(wind_xr,  lon, lat)
    else:  # SUN
        ts = get_ts(solar_xr, lon, lat)

    variability[name] = np.round(np.clip(ts, 0.0, 1.0), 4).tolist()

# ---------------------------------------------------------------------------
# Write output
# ---------------------------------------------------------------------------
out_dir = os.path.dirname(OUT_CSV)
os.makedirs(out_dir, exist_ok=True)

variability_df = pd.DataFrame(variability)
variability_df.to_csv(OUT_CSV, index=False)

n_vre = len(variability_df.columns) - 1  # exclude Time_Index
print(f"Generators_variability.csv written: {n_vre} VRE resources, {n_times} timesteps -> {OUT_CSV}")
