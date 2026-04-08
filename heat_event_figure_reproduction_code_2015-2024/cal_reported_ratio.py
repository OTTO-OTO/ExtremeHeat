# Script:calculate_reported_ratio.py
# Purpose:Calculate the share of physical heat-wave events reported in news using collision detection

import xarray as xr
import pandas as pd
import numpy as np
import argparse
from scipy.ndimage import label as ndimage_label
from tqdm import tqdm
import cartopy.feature as cfeature
import shapely.vectorized
import shapely.geometry

# --- 1. Helper: create a land mask (reuse earlier logic) ---
def create_land_mask(ds):
    print("Generating/loading land mask...")
    lats = ds.coords['lat'].values
    lons = ds.coords['lon'].values
    lon2d, lat2d = np.meshgrid(lons, lats)
    land_feature = cfeature.NaturalEarthFeature('physical', 'land', '110m')
    mask = np.zeros(lon2d.shape, dtype=bool)
    for poly in land_feature.geometries():
        if isinstance(poly, shapely.geometry.MultiPolygon):
            for sub in poly.geoms: mask |= shapely.vectorized.contains(sub, lon2d, lat2d)
        else:
            mask |= shapely.vectorized.contains(poly, lon2d, lat2d)
    return mask

def calculate_intersection_ratio(
    nc_filename='temperature_025degree_2015_2024_mhw_daily.nc',
    news_csv='raw_news_events.csv',
    output_filename='final_event_reporting_ratio.csv',
):
    # --- Configure paths ---
    
    # 1. Load news data
    print(f"Loading news data: {news_csv}")
    df_news = pd.read_csv(news_csv, usecols=['year', 'month', 'day', 'X', 'Y'])
    df_news.dropna(inplace=True)
    
    # 2. Initialize NetCDF reading
    print(f"Opening meteorological data: {nc_filename}")
    ds_full = xr.open_dataset(nc_filename, chunks={'time': 365})
    
    # Get coordinate metadata for converting news latitude/longitude to array indices
    lats = ds_full.coords['lat'].values
    lons = ds_full.coords['lon'].values
    # Assume evenly spaced coordinates and compute resolution plus origin
    lat_res = lats[1] - lats[0]
    lon_res = lons[1] - lons[0]
    lat_start = lats[0]
    lon_start = lons[0]
    
    print(f"Detected grid resolution: Lat={lat_res:.4f}, Lon={lon_res:.4f}")

    # Create the land mask
    land_mask = create_land_mask(ds_full.isel(time=0))
    
    results = []
    
    # --- Process by year ---
    for year in range(2015, 2025):
        print(f"\n>>> Processing year {year}...")
        
        # A. Prepare the physical layer (Physical Layer)
        ds_year = ds_full.sel(time=str(year)).load()
        # Logic: heat wave occurs and the grid cell is on land
        is_event = (ds_year['t2m_mhw'].values > 0) & land_mask
        
        # Label connected components (Labeling)
        # labeled_array uses 0 as background and 1,2,3... as event IDs
        labeled_array, num_physical_events = ndimage_label(is_event)
        print(f"  - Total physical events: {num_physical_events}")
        
        if num_physical_events == 0:
            results.append({'year': year, 'physical_events': 0, 'reported_events': 0, 'ratio': 0})
            continue

        # B. Prepare the news layer (News Layer)
        # Filter news for the current year
        df_curr = df_news[df_news['year'] == year].copy()
        
        if len(df_curr) == 0:
            print("  - No news data for this year")
            results.append({'year': year, 'physical_events': num_physical_events, 'reported_events': 0, 'ratio': 0})
            continue
            
        # C. Coordinate mapping: map news (Date, Lat, Lon) to labeled_array (t, y, x) indices.
        print("  - Running spatiotemporal collision detection...")
        
        # Convert time indices: NetCDF time is day-of-year (0-364/365).
        # First convert news dates to day-of-year
        # Assumes ds_year starts on January 1 and covers the full year
        dates = pd.to_datetime(df_curr[['year', 'month', 'day']])
        day_of_year = dates.dt.dayofyear.values - 1 # indiceszero-based
        
        # Convert latitude/longitude indices.
        # index = (value - start) / resolution
        # Use round to snap to the nearest grid cell
        lat_idx = np.round((df_curr['Y'].values - lat_start) / lat_res).astype(int)
        lon_idx = np.round((df_curr['X'].values - lon_start) / lon_res).astype(int)
        
        # Filter indices outside the map bounds, such as malformed news coordinates.
        n_lat = len(lats)
        n_lon = len(lons)
        n_time = labeled_array.shape[0] # number of days in the current year
        
        valid_mask = (
            (day_of_year >= 0) & (day_of_year < n_time) &
            (lat_idx >= 0) & (lat_idx < n_lat) &
            (lon_idx >= 0) & (lon_idx < n_lon)
        )
        
        # Extract valid indices.
        t_valid = day_of_year[valid_mask]
        y_valid = lat_idx[valid_mask]
        x_valid = lon_idx[valid_mask]
        
        # D. Collision detection (The Intersection)
        # Read the values at news locations directly from labeled_array
        # hit_ids is an array containing all event IDs hit by news reports
        hit_ids = labeled_array[t_valid, y_valid, x_valid]
        
        # Remove 0 (background) and duplicates, keeping unique event IDs only
        unique_hit_ids = np.unique(hit_ids)
        unique_hit_ids = unique_hit_ids[unique_hit_ids > 0] # exclude 0
        
        num_reported_events = len(unique_hit_ids)
        ratio = num_reported_events / num_physical_events
        
        print(f"  - Reported independent events: {num_reported_events}")
        print(f"  - Reporting ratio: {ratio:.2%}")
        
        results.append({
            'year': year,
            'physical_events': num_physical_events,
            'reported_events': num_reported_events,
            'ratio': ratio
        })
        
        # Free memory
        del ds_year, labeled_array, is_event

    # --- Save results ---
    df_res = pd.DataFrame(results)
    df_res.to_csv(output_filename, index=False)
    print(f"\nCalculation complete. Results saved to {output_filename}")
    print(df_res)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate annual reporting ratios by intersecting raw news records with gridded heat-wave events."
    )
    parser.add_argument("--netcdf", default="temperature_025degree_2015_2024_mhw_daily.nc")
    parser.add_argument("--news-csv", default="raw_news_events.csv")
    parser.add_argument("--output", default="final_event_reporting_ratio.csv")
    args = parser.parse_args()

    calculate_intersection_ratio(args.netcdf, args.news_csv, args.output)
