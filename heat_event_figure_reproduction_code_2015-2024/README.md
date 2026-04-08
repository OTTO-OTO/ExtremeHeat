# Heat-Event Figure Reproduction Code

This directory contains the code and tabular inputs used to reproduce the heat-event analysis figures submitted with the manuscript. The scripts focus on generating publication figure panels from gridded physical heat-wave events, geocoded news reports, and UN-reported heat-event records for 2015-2024.

## Environment

Use Python 3.10 or newer. Install the Python dependencies with:

```bash
pip install -r requirements.txt
```

Cartopy may require system geospatial libraries, depending on the operating system and Python distribution.

## Included Data

- `calculated_heat_events_frequency.csv`: gridded physical heat-event frequency.
- `combined_news_events_locations.csv`: geocoded news-event locations.
- `annual_mhw_counts_land_only.csv`: annual physical heat-event counts over land.
- `annual_mhw_counts_spatiotemporal.csv`: annual connected-component heat-event counts from the gridded physical data.
- `news_events_yearly_deduplicated.csv`: annual deduplicated news-event counts.
- `un_events_bounding_box.csv`: UN event country bounding boxes and event counts.
- `un_events_yearly_counts.csv`: annual UN heat-event counts.
- `final_event_reporting_ratio.csv`: annual physical-event, reported-event, and reporting-ratio summary.
- `01UN_*.xlsx`: source UN summary tables retained for traceability.

The NetCDF file `temperature_025degree_2015_2024_mhw_daily.nc` is not included in this directory. It is required only for regenerating `calculated_heat_events_frequency.csv`, `annual_mhw_counts_spatiotemporal.csv`, or `final_event_reporting_ratio.csv` from the original gridded physical-event data.

## Main Figure Panels

Run the following commands from this directory:

```bash
python panel_a_final.py
python panel_b_final.py
python panel_c_final.py
python 'panel_de(ef)_final.py'
python panel_f_final.py
```

These scripts write PDF and JPG files to `final_output/`.

## Optional Processing Scripts

The included CSV files are sufficient for the main plotting scripts above. To regenerate intermediate data from raw inputs, use:

```bash
python nc_preprocess_v2.py
python heatevent_count_spatiotemporal.py
python news_yearly_count_spatiotemp.py --input raw_news_events.csv
python cal_reported_ratio.py --news-csv raw_news_events.csv
```

The raw news file should contain `year`, `month`, `day`, `X`, and `Y` columns, where `X` is longitude and `Y` is latitude.

## Alternate Figures

- `line_graph.py`: labeled annual comparison line chart; writes to `output_line_graph/`.
- `news_gaussian_coord.py`: alternate smoothed news-event map; writes to `output_refined_maps/`.
- `un_bubble_nocoord.py`: alternate UN bubble map; writes to `output_un_heat_events_map/`.
