# Analysis Code for Nature Submissions

This repository contains the complete R analysis code for the manuscript submitted to a Nature Portfolio journal. The code covers five main analytical modules: machine learning with XGBoost and SHAP, group comparisons (ICT and CCPI), boxplot visualizations, and linear regression analyses of language proximity in Africa.

## 1. Project Overview

- **Objective**: To investigate the relationship between a reporting ratio (`Ratio`) and multiple predictors including ICT usage, climate change performance (CCPI), and language proximity, using both traditional statistics and machine learning.
- **Methods**:
  - XGBoost regression with SHAP (SHapley Additive exPlanations) for feature importance.
  - Independent t‑tests or Mann‑Whitney U tests (chosen based on normality) for group comparisons.
  - Boxplots and scatter plots with regression lines.
- **Target Journal**: Nature Communications / Scientific Reports (or any Nature Portfolio journal requiring reproducible code).

## 2. File Structure & Environment

### Required Files
- `MLdata_selected.xlsx` – used in Modules 1 and 2 (contains features and target variable).
- `RMdata_selected.xlsx` – used in Modules 3, 4, and 5 (contains Ratio, continent, ICT_NETUSER_2023, CCPI_Score, LPN2CommonLanguage_2024, country1, etc.).

> **Note**: The code currently uses hard‑coded Windows paths (e.g., `C:\Users\HP\Desktop\...`). You **must** change these paths to match your local file locations.

### R Environment
- R version ≥ 4.0.0
- Required packages (install all before running):

```r
install.packages(c(
  "tidyverse", "shapr", "caret", "rpart", "ggplot2", "vip", "pdp",
  "kernelshap", "SHAPforxgboost", "xgboost", "data.table", "readxl",
  "writexl", "shapviz", "car", "rstatix", "coin"
))
```r

## 3. Data Preparation
All modules read Excel files directly. Key variables:

## Variable	Description	Used in modules
Ratio	Target variable (reporting ratio)	1,2,3,4,5
ICT_NETUSER_2023	Internet users rate	3,4
CCPI_Score	Climate Change Performance Index score	3,4
LPN2CommonLanguage_2024	Language proximity index	5
continent	Continent name	3,4,5
country1	Country name (for language grouping in Africa)	5
Missing data handling:

Module 2: na.omit(data) before model training.

Module 3: filter(!is.na(...)) for each analysis variable.

Module 4: same as Module 3.

Module 5: no explicit na.omit, but lm() and geom_smooth() handle NAs by default.

Categorical variable encoding (Modules 1 & 2):
Factors and characters are converted to numeric using as.numeric(factor(x)) - 1. This is necessary for XGBoost.

## 4. Modules & Execution Order
Run the code from top to bottom exactly as written. Each module is separated by # =========...= comments.

Module	Description	Output
1	XGBoost regression + SHAP bar plot	Console: RMSE, MAE, R². Graphics: actual vs predicted plot, SHAP importance bar plot.
2	XGBoost + SHAP scatter plot (with na.omit)	Graphics: SHAP summary scatter plot (feature value vs SHAP value).
3	Group comparisons (ICT & CCPI)	Console: summary tables with sample sizes, means, medians, normality test results, test method, p‑value, effect size (Cohen's d or r), significance stars.
4	Boxplots for ICT and CCPI groups	Graphics: two boxplots (one for CCPI, one for ICT) with dashed vertical lines separating continents.
5	Linear regression of Ratio on language proximity (Africa only)	Graphics: scatter plots with regression lines (overall Africa, and Arabic subgroup).
Important notes about Module 3 (statistical tests)
A strict rule is applied to choose between t‑test and Mann‑Whitney U test:

If both groups are normally distributed (Kolmogorov‑Smirnov test on scaled data, p > 0.05) → t‑test (Welch’s if variances unequal, otherwise pooled).

If at least one group is non‑normal → Mann‑Whitney U test.

Effect sizes:

Cohen’s d for t‑tests.

r (from wilcox_effsize) for Mann‑Whitney U tests.

The analysis is performed per continent and also for all continents combined.

## 5. Input/Output Details
Input file paths (must be edited by user)
MLdata_selected.xlsx → used in Modules 1 & 2 (sheet = 1, guess_max = 10000)

RMdata_selected.xlsx → used in Modules 3, 4, 5 (sheet = 2 in Modules 4 & 5, no sheet specified in Module 3 – default sheet 1)

Recommended change: Replace absolute paths with relative paths, e.g.,
read_excel("./data/MLdata_selected.xlsx")

Output
Console: All numerical results (model metrics, test summaries) are printed.

Graphics: Displayed in the active graphics device. No automatic saving is implemented; users can manually add ggsave() if needed.

## 6. Important Notes for Reproduction
Random seed: set.seed(1314) is used in Modules 1 and 2 for reproducible train/test split (90% training).

Group definitions:

ICT high/low: within each continent, countries with ICT_NETUSER_2023 above the 1/3 quantile are “High ICT”, others “Low ICT”.

CCPI high/low: within each continent, countries with CCPI_Score above the median (probs = 1/2) in Module 3, but 1/3 quantile in Module 4 (boxplots). This inconsistency should be noted; the boxplot definition (probs = 1/3) is used for visualisation only.

Language groups in Africa: Based on a manually defined list of English, French, Arabic, and Portuguese‑speaking countries (see vectors ENG, FRA, ARB, POR). Country names must exactly match the country1 column in the Excel file.

Excluded continents:

ICT analysis: “North America”, “Seven seas (open ocean)”

CCPI analysis: “North America”, “South America”, “Oceania”, “NA”

Boxplots: further exclude some continents depending on the plot.

## 7. Reproduction Steps (Minimal Example)
Install R (≥4.0) and RStudio (recommended).

Install required packages (see Section 2).

Place the two Excel files in a known folder, e.g., C:/MyProject/data/.

Open the R script (copy the code into a new .R file).

Modify file paths in all read_excel() calls to point to your data files.

Run the entire script line by line or source it.

Check console output for model metrics and statistical test results.

View generated plots in the R graphics window.

## 8. Citation & License
License: MIT – you are free to use, modify, and distribute this code with proper attribution.

Citation: If you use this code in a publication, please cite the original manuscript (DOI to be added upon publication) and this repository.