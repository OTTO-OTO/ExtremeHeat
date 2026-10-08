---
title: "README: Analysis Code for Figure 5"
output: html_document
---

# README: Analysis Code for Figure 5

This repository contains the R code used to produce Figure 5 and the associated statistical analyses in the manuscript. The code covers the XGBoost + SHAP analysis, group comparisons for ICT and CCPI, and the fractional logit regressions of language proximity in Africa.

## 1. Project Overview

- **Objective**: To examine the structural correlates of the extreme-heat reporting ratio (`Ratio`) using machine learning and regression analyses.
- **Methods**:
  - XGBoost regression with SHAP (SHapley Additive exPlanations) for feature importance.
  - Group comparisons (ICT and CCPI) using t‑tests or Mann–Whitney U tests depending on normality.
  - Fractional logit regressions for the Common Language Proximity Index (LPN2CommonLanguage) in Africa.
- **Target**: The code is intended to be fully reproducible and accompanies the manuscript submitted to *Nature Climate Change*.

## 2. File Structure & Environment

### Required Data
- `Figure5_RawData.xlsx` – the single input file for all analyses.
  - The file has already had Antarctica removed.
  - Column 1: `Ratio` (reporting ratio).
  - Columns 6–13: eight predictors used in the machine-learning model.
  - Column 14: `CCPI` – used for the group comparison.
  - Other columns: `country1`, `country2`, `Alpha_3_Co`, `continent`, `income_grp`, `lon`, `lat`.

### R Environment
- R version ≥ 4.0.0.
- Required packages (install all before running):

```{r, eval=FALSE}
install.packages(c(
  "readxl", "dplyr", "tidyr", "ggplot2", "caret", "xgboost",
  "SHAPforxgboost", "car", "rstatix", "boot", "purrr"
))
```

> **Note**: The script uses a hard‑coded Windows path (`C:/Users/HP/Desktop/Figure5_RawData.xlsx`). Change this path to match your local environment before running.

## 3. Data Preparation

All analyses read `Figure5_RawData.xlsx` directly.

- **Global imputation** (`imputed_data`): For the eight predictors in columns 6–13, missing values are imputed using the median within each World Bank income group. This imputed dataset is used for the group comparison and the language‑proximity regressions.
- **Within‑fold imputation** (`impute_fold`): For the XGBoost model, imputation is performed separately inside the training fold and then applied to the test fold, to avoid information leakage.
- **Missing outcome**: Rows with missing `Ratio` are removed before model training.

## 4. Modules & Execution Order

Run the code from top to bottom. The script is organised into the following sections.

| Module | Description | Output |
|--------|-------------|--------|
| 1. XGBoost + SHAP bar plot | Trains an XGBoost model with 5‑fold cross‑validated hyperparameter tuning, evaluates on a held‑out test set, and produces a SHAP importance bar plot with error bars. | Console: RMSE, MAE, R². Graphics: SHAP bar plot (positive/negative effects). |
| 2. Group‑level SHAP importance | Aggregates the SHAP values into three conceptual domains (Socioeconomic Status, Climate Vulnerability, Monitoring Ability) and produces a horizontal bar chart and a Nightingale rose plot. | Graphics: bar chart and rose plot. |
| 3. Module 2 (REMOVABLE) | A second XGBoost model using complete cases only, with a SHAP summary scatter plot. This section is optional and can be removed without affecting the main results. | Graphics: SHAP summary scatter plot. |
| 4. Group comparison (ICT & CCPI) | Divides countries within Africa, Asia, and Europe into High/Low groups using the 1/3 quantile rule. Performs Shapiro–Wilk normality tests, Levene tests, and either t‑tests or Mann–Whitney U tests. Reports sample sizes, effect sizes, bootstrap 95% CIs, and FDR‑adjusted p‑values. | Console: summary tables. Graphics: boxplots for ICT and CCPI. |
| 5. Language proximity regression | Fits fractional logit models of `Ratio` on `LPN2CommonLanguage` for Africa overall and the Arabic subgroup. Reports coefficients, standard errors, p‑values, 95% CIs, and average marginal effects per 1‑SD. | Console: model statistics. Graphics: scatter plots with fitted curves. |

### 4.1 Geographic cross-validation (Supplementary Section S9)

The 5-fold geographic cross-validation reported in Supplementary Section S9 is integrated into the 
Figure 5 workflow in the same R script. It uses k-means clustering on grid-centroid coordinates to 
construct five spatially separated folds, applies imputation and model training only within each 
training fold, and reports out-of-fold \(R^2\), RMSE, MAE, and comparisons against four baseline 
models (global mean, regional mean, geographic OLS, and socioeconomic OLS). No separate script is 
required.

## 5. Important Notes

- **Random seed**: `SEED_MAIN <- 42` is used for all random operations (train/test split, cross‑validation, bootstrap).
- **Hyperparameter tuning**: The grid search covers `eta`, `max_depth`, `subsample`, `colsample_bytree`, `reg_lambda`, and `min_child_weight`. The best combination is selected by minimum mean cross‑validated RMSE.
- **Group definitions**:
  - Within each continent, the 1/3 and 2/3 quantiles of the variable define the Low and High groups. Countries between the two thresholds are excluded.
  - Ties at the thresholds are assigned to the lower group.
- **Missing data**:
  - For the XGBoost model, imputation is fitted on the training fold only.
  - For the group comparison and language regressions, the globally imputed dataset (`imputed_data`) is used.

## 6. Input/Output Details

### Input
- `Figure5_RawData.xlsx` – must be placed in the working directory or the path must be updated.

### Output
- **Console**: All model metrics, statistical test results, and regression summaries are printed.
- **Graphics**: Displayed in the active graphics device. No automatic saving is implemented; users can add `ggsave()` if they wish to save the figures.

## 7. Reproduction Steps

1. Install R (≥ 4.0) and RStudio (recommended).
2. Install the required packages (see Section 2).
3. Place `Figure5_RawData.xlsx` in a known folder.
4. Open the R script and update the `IN_PATH` variable to point to your data file.
5. Run the entire script line by line or source it.
6. Check the console for model metrics and statistical results.
7. View the generated plots in the R graphics window.

## 8. Citation & License

- **License**: MIT – you are free to use, modify, and distribute this code with proper attribution.
- **Citation**: If you use this code in a publication, please cite the original manuscript (DOI to be added upon publication) and this repository.