# Heat-Event Detection and Calculation

This directory contains MATLAB code and processed outputs used to calculate **Heat Events** under different threshold definitions. 

---

## Environment

- MATLAB (R2018 or newer recommended)

---

## Calculation Heat-Event Code Structure

Core scripts are located in:

### `calculate_mhw_method/`

- `making2_mhw_res.m`  
  Main script for running the heat-event detection workflow.

- `mhw_clim.m`  
  Computes climatology and percentile thresholds (M90, M95, M97.5).

- `mhw_detect.m`  
  Detects heatwave events based on threshold exceedance and duration.

- `ncall_create.m`  
  Writes results into NetCDF files.

- `read_ncall.m`  
  Reads NetCDF files for validation and analysis.

---

## Processed Outputs Data

Processed outputs data are stored in:

### `mhw_data_res/`

---

### CMIP Future Projections (2050-2100)

- `CMIP_Heat_event_duration_days_per_year_in_2050-2100_def_m90_5.nc`
- `CMIP_Heat_event_duration_days_per_year_in_2050-2100_def_m95_2.nc`
- `CMIP_Heat_event_duration_days_per_year_in_2050-2100_def_m975_4.nc`

These files represent:

> Annual duration (days per year) of heat events in 2050-2100 under different thresholds and durations:
- M90 (90th percentile), at least 5 days
- M95 (95th percentile), at least 2 days
- M97.5 (97.5th percentile), at least 4 days

---

### ERA5 Data (2015-2024)

- `ERA5_Heat_event_duration_days_per_year_in_2015-2024_def_m90_5.nc`
- `ERA5_Heat_event_duration_days_per_year_in_2015-2024_def_m95_2.nc`
- `ERA5_Heat_event_duration_days_per_year_in_2015-2024_def_m975_4.nc`

These represent:

> Annual duration of observed heat events in 2015-2024 derived from ERA5 reanalysis under different thresholds and durations.

---

### Seasonal Event Counts (ERA5)

- `ERA5_Heat_event_event_count_per_season_2015_2024_m90_5`

This file contains:

> Number of heat events per season derived from ERA5 reanalysis.

---

## Notes

- Filename suffix:
  - `m90 / m95 / m975`: threshold level
  - `_5 / _2 / _4`: minimum duration (days)

---
