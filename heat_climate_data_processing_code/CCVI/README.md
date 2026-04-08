# CCVI Data Release

## License

The CCVI data is licensed under the [Creative Commons Attribution-NonCommercial 4.0 International license](https://creativecommons.org/licenses/by-nc/4.0/).

## Background

Find more information at [climate-conflict.org](https://climate-conflict.org).

---

### [base_grid_prio.parquet](geo/base_grid_prio.parquet)

| column | data type | description          |
| ------ | --------- | -------------------- |
| lat    | DOUBLE    | Latitude             |
| lon    | DOUBLE    | Longitude            |
| iso3   | String    | ISO3 code of country |
| pgid   | INT64     | Cell ID              |

---

## [/exposure](/exposure)

### [population_worldpop.parquet](exposure/population_worldpop.parquet)

| column         | data type | description          |
| -------------- | --------- | -------------------- |
| lat            | DOUBLE    | Latitude             |
| lon            | DOUBLE    | Longitude            |
| iso3           | String    | ISO3 code of country |
| wp_land_area   | DOUBLE    | Land area            |
| wp_pop_count   | DOUBLE    | Population Count     |
| wp_pop_density | DOUBLE    | Population density   |
| pgid           | INT64     | Cell ID              |
| year           | INT64     | Year                 |
| quarter        | INT64     | Quarter (1-4)        |

Updated as part of the index pipeline.

### [exposure_layers.parquet](exposure/exposure_layers.parquet)

| column                | data type | description               |
| --------------------- | --------- | ------------------------- |
| pgid                  | INT64     | Cell ID                   |
| lat                   | DOUBLE    | Latitude                  |
| lon                   | DOUBLE    | Longitude                 |
| year                  | INT64     | Year                      |
| quarter               | INT64     | Quarter (1-4)             |
| <exposure_layer_1_id> | DOUBLE    | Value of exposure layer 1 |
| <exposure_layer_2_id> | DOUBLE    | Value of exposure layer 2 |
| ...                   | DOUBLE    | ...                       |

Updated as part of the index pipeline.

---

## [/index_structure](/index_structure)

### [index_structure.csv](index_structure/index_structure.csv)

| column        | description                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------- |
| level         | Depth in hierarchy, 0-3                                                                                             |
| ready         | Ready to be used? (1/0)                                                                                             |
| id            | ID of index node. Per convention, IDs are constructed as a hierarchy path using underscores as separators           |
| pillarId      | ID of corresponding pillar                                                                                          |
| dimensionId   | ID of corresponding dimension                                                                                       |
| label         | display label (35 characters max.)                                                                                  |
| rawUnit       | Unit label for the raw value (60 characters max.)                                                                   |
| definition    | Technical definition (800 characters max.). Can use Markdown for formatting.                                        |
| description   | Description for a wider audience (400 characters max.) Can use Markdown for formatting.                             |
| dataSourceIds | List of data sources for this indicator, separated by semicolons. Need to correspond to ID used in data-sources.csv |

### [data_recency.csv](index_structure/data_recency.csv)

| column      | description                                                   |
| ----------- | ------------------------------------------------------------- |
| id          | index node id                                                 |
| lastUpdated | Data of last available (non-imputed) quarter in format YYYY-Q |

### [data_sources.csv](index_structure/data_sources.csv)

| column     | description                                                                |
| ---------- | -------------------------------------------------------------------------- |
| id         | index node id                                                              |
| shortLabel | short label (10 characters max.) to be used in compact source descriptions |
| label      | Full label (60 characters max.)                                            |
| url        | URL of data set or data provider                                           |

---

## [/index_data](index_data)

### [aggregate_scores.parquet](index_data/aggregate_scores.parquet)

Aggregate index scores (total CCVI, risk scores, pillar hazard scores, dimensions, i.e. all index elements except indicator scores) for all grid cells and quarters.

| column          | description                     |
| --------------- | ------------------------------- |
| pgid            | Cell ID                         |
| year            | Year                            |
| quarter         | Quarter                         |
| CCVI            | Value of CCVI score             |
| CON_risk        | Value of CON_risk score         |
| <index_node_id> | Further values identified by ID |

### [climate_indicators.parquet](index-data/climate_indicators.parquet)

Index scores and corresponding raw values in the climate pillar for all grid cells and quarters.

| column                   | description                               |
| ------------------------ | ----------------------------------------- |
| pgid                     | Cell ID                                   |
| year                     | Year                                      |
| quarter                  | Quarter                                   |
| <indicator_id>           | Value of indicator                        |
| <indicator_id>\_raw      | Corresponding raw value                   |
| <indicator_id>\_exposure | Indicator value with exposure factored in |
| …                        | Further values identified by ID           |

### [conflict_indicators.parquet](index-data/conflict_indicators.parquet)

Index scores and corresponding raw values in the conflict pillar for all grid cells and quarters.

| column              | description                     |
| ------------------- | ------------------------------- |
| pgid                | Cell ID                         |
| year                | Year                            |
| quarter             | Quarter                         |
| <indicator_id>      | Value of indicator              |
| <indicator_id>\_raw | Corresponding raw value         |
| …                   | Further values identified by ID |

### [vulnerability_indicators.parquet](index-data/vulnerability_indicators.parquet)

Index scores and corresponding raw values in the vulnerability pillar for all grid cells and quarters.

| column              | description                     |
| ------------------- | ------------------------------- |
| pgid                | Cell ID                         |
| year                | Year                            |
| quarter             | Quarter                         |
| <indicator_id>      | Value of indicator              |
| <indicator_id>\_raw | Corresponding raw value         |
| …                   | Further values identified by ID |
