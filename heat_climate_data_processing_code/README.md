# Heat and Climate Data Processing Code

This repository contains Python scripts and processed tabular inputs used to build heat-event analysis tables. The workflow joins event coordinates with climate, vulnerability, governance, language, ICT, GDP/population, and media-source variables.

## Contents

- `merge_new.py` and `create_new_master_table.py`: build master tables from coordinate attributes and processed yearly result files.
- `DICL_join.py`, `CCPI_join.py`, `SIDSEAD_join.py`, `NDGAIN_join.py`, `WGI_join.py`, `ICT_join.py`, `LDI_join.py`, `newGDP_join.py`: join external indicators to the master table.
- `cmip5_query.py`, `era5/era5_query.py`, `CCVI/ccvi_query.py`: extract yearly gridded climate or vulnerability outputs.
- `mainstream/`: clean and match media-source information.
- `Climate_CCPI_2025/`, `Climate_NDGAIN_2015-2023/`, `CCVI/`, `era5/`, `NegEvt_Ratios_joined/`: processed inputs and outputs used by the scripts.

## Included Data

The repository keeps the smaller processed CSV/XLSX files needed for table construction, including:

- coordinate and attribute tables such as `coordinates_combined_with_type.csv`, `Ratio_country_joined_result.xlsx`, and `master_coordinates_1015.csv`
- joined indicator inputs such as `DICL_joined.xlsx`, `WGI_joined.xlsx`, `ICT_joined.xlsx`, `Climate_CCPI_2025/CCPI_joined.xlsx`, and the joined ND-GAIN files
- processed yearly outputs such as `results_cmip5_prAdjust_yearly_wide.csv`, `era5/results_era5_t2m_yearly_wide.csv`, and `CCVI/results_ccvi_yearly_wide.csv`

Large raw datasets and duplicate export formats are omitted from this code package. They should be stored separately if full raw-data reconstruction is required.

## Environment

The scripts require Python with:

- `pandas`
- `numpy`
- `scipy`
- `openpyxl`
- `xarray`
- `rasterio`
- `tldextract`

## Notes

Some extraction scripts reference external raw climate or raster data that are not included in this lightweight package. The included processed CSV/XLSX files are intended for reproducing the table-building steps used in the analysis.
