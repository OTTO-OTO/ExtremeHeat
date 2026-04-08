# File name: process_netcdf_to_frequency.py

import xarray as xr
import pandas as pd
import numpy as np

def calculate_frequency_from_netcdf(netcdf_file_path, output_csv_path):
    """
    Calculate total heat-wave frequency for each grid cell from a NetCDF file and save it as CSV.
    """
    print(f"Opening NetCDF file: {netcdf_file_path}")
    
    try:
        with xr.open_dataset(netcdf_file_path) as ds:
            # --- Key step: update these variable and coordinate names for your file ---
            VAR_NAME = 't2m_mhw'  # Replace with your heat-wave intensity variable name.
            LAT_NAME = 'lat'      # Replace with your latitude coordinate name.
            LON_NAME = 'lon'      # Replace with your longitude coordinate name.
            TIME_NAME = 'time'    # Replace with your time coordinate name.

            # Print dataset information for inspection; uncomment on the first run if needed
            # print("--- NetCDF file information ---")
            # print(ds)
            # print("--------------------")

            # Check whether variables and coordinates exist
            for name in [VAR_NAME, LAT_NAME, LON_NAME, TIME_NAME]:
                if name not in ds.variables and name not in ds.coords:
                     raise KeyError(f"Variable or coordinate '{name}' is not in the file. Please check and update it.")

            print("Calculating total heat-wave frequency for each grid cell...")
            # Core steps:
            # 1. (ds[VAR_NAME] > 0) creates a boolean DataArray; heat-wave cells are True and all others are False.
            # 2. .sum(dim=TIME_NAME) sums along time. True counts as 1 and False counts as 0.
            # The result is a 2D (latitude, longitude) DataArray whose values are total heat-wave days for each grid cell.
            frequency_grid = (ds[VAR_NAME] > 0).sum(dim=TIME_NAME)

            # Convert the result to a pandas DataFrame for saving.
            df_freq = frequency_grid.to_dataframe(name='frequency').reset_index()

            # Keep grid cells with frequency greater than zero
            df_freq = df_freq[df_freq['frequency'] > 0]

            print(f"Calculation complete. {len(df_freq)} grid cells had heat waves.")
            
            # Keep only the latitude, longitude, and frequency columns
            df_to_save = df_freq[[LAT_NAME, LON_NAME, 'frequency']]

            # Save as CSV.
            print(f"Saving to CSV file: {output_csv_path}")
            df_to_save.to_csv(output_csv_path, index=False)
            
            print("Processing complete. Frequency CSV saved successfully.")

    except FileNotFoundError:
        print(f"Error: file not found '{netcdf_file_path}'. Please check whether the path is correct.")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Input NetCDF file
    netcdf_input_path = 'temperature_025degree_2015_2024_mhw_daily.nc'
    # Output CSV file containing frequencies
    csv_output_path = 'calculated_heat_events_frequency.csv'
    
    calculate_frequency_from_netcdf(netcdf_input_path, csv_output_path)
