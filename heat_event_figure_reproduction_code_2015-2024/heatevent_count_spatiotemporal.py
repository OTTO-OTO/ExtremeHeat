# Script:process_mhw_spatiotemporal.py
# Purpose:Calculate independent spatiotemporal marine heat-wave event counts

import xarray as xr
import pandas as pd
import numpy as np
from scipy.ndimage import label as ndimage_label  # Core 3D labeling function
from tqdm import tqdm  # Progress bar for the outer year loop
import time as pytime

def process_spatiotemporal_events():
    """
    Load yearly data and use 3D connected-component labeling to calculate independent spatiotemporal heat-wave events,
    then save annual totals to a CSV file.
    """
    # --- 1. Initialize ---
    filename = 'temperature_025degree_2015_2024_mhw_daily.nc'
    output_csv_filename = 'annual_mhw_counts_spatiotemporal.csv'  # Distinct output file name
    
    years_to_process = range(2015, 2025) # define the years to process
    annual_results = {} # create an empty dictionary for results

    print("--- Starting spatiotemporal event analysis script ---")
    start_time = pytime.time()
    
    print(f"Opening large file: {filename}")
    # Open with dask chunks, then load one year at a time into memory
    try:
        ds_full = xr.open_dataset(filename, chunks={'time': 365})
    except FileNotFoundError:
        print(f"Error: file not found '{filename}'.")
        return

    # --- 2. Loop by year ---
    # Wrap the year loop with tqdm to show progress
    for year in tqdm(years_to_process, desc="Processing yearly data"):
        
        # a. Select and load one year of data into memory
        # This strategy avoids out-of-memory errors by loading only one year at a time
        print(f"\nLoading {year} data into memory...")
        ds_year = ds_full.sel(time=str(year)).load()
        
        # b. Create a boolean mask
        print(f"Creating 3D event mask for {year}...")
        # .values converts the DataArray to a plain NumPy array for scipy.
        is_event_year = (ds_year['t2m_mhw'].values > 0)
        
        # c. Run 3D connected-component labeling
        print(f"Running {year} 3D connected-component labeling for year...")
        # ndimage_label finds regions connected across time and latitude/longitude
        # The structure parameter defines connectivity; the default works for this spatiotemporal case
        # labeled_array is the labeled array, and num_events is the target event count
        labeled_array, num_events = ndimage_label(is_event_year)
        
        # d. Record results
        print(f"--- {year} year has {num_events} independent spatiotemporal heat events ---")
        annual_results[year] = num_events
        
        # Free memory; useful inside the loop.
        del ds_year, is_event_year, labeled_array
        
    ds_full.close()

    # --- 3. Save results ---
    print(f"\nAll years processed; saving results to {output_csv_filename}...")
    
    # Convert the results dictionary to a pandas DataFrame.
    df_to_save = pd.DataFrame(list(annual_results.items()), columns=['year', 'event_count'])
    
    # Save as a CSV file.
    df_to_save.to_csv(output_csv_filename, index=False)

    end_time = pytime.time()
    print(f"--- Spatiotemporal event analysis script complete ---")
    print(f"Results saved to '{output_csv_filename}'.")
    print(f"Total runtime: {((end_time - start_time) / 60):.2f} minutes.")

if __name__ == "__main__":
    process_spatiotemporal_events()