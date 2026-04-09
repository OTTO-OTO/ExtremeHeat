# ============================================================================
# R code for Fig.5
# ============================================================================

# ============================================================================
# Module 1: Machine Learning with XGBoost and SHAP (Bar Plot)
# ============================================================================

library(tidyverse)
library(shapr)
library(caret)
library(rpart)
library(ggplot2)
library(vip)
library(pdp)
library(kernelshap)
library(SHAPforxgboost)
library(xgboost)
library(data.table)
library(readxl)
library(writexl)
library(shapviz)

backup <- read_excel("C:\\Users\\HP\\Desktop\\MLdata_selected.xlsx", sheet=1, guess_max = 10000)
data <- backup

# Separate features and target variable
X <- data[, -1]  # all feature columns (excluding first column)
y <- data[[1]]   # target variable Ratio

# Convert categorical variables to numeric (required by XGBoost)
convert_categorical <- function(df) {
  for (col in names(df)) {
    if (is.factor(df[[col]]) || is.character(df[[col]])) {
      df[[col]] <- as.numeric(factor(df[[col]])) - 1
      cat("Converting variable:", col, "-> numeric\n")
    }
  }
  return(df)
}

X_processed <- convert_categorical(X)

# Split into training and test sets (90% training)
set.seed(1314)
train_index <- sample(1:nrow(X_processed), size = floor(0.9 * nrow(X_processed)))
X_train <- X_processed[train_index, ]
y_train <- y[train_index]
X_test <- X_processed[-train_index, ]
y_test <- y[-train_index]

# Convert to DMatrix format for XGBoost
dtrain <- xgb.DMatrix(data = as.matrix(X_train), label = y_train)
dtest <- xgb.DMatrix(data = as.matrix(X_test), label = y_test)

# Set model parameters
params <- list(
  objective = "reg:squarederror",
  eta = 0.1,
  max_depth = 6,
  subsample = 0.8,
  colsample_bytree = 0.8
)

# Train model
xgb_model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 500,
  early_stopping_rounds = 20,
  watchlist = list(train = dtrain, test = dtest),
  print_every_n = 50
)

cat("Best iteration:", xgb_model$best_iteration, "\n")

# Predict on test set
preds <- predict(xgb_model, dtest)

# Evaluation metrics
eval_metrics <- function(actual, predicted) {
  rmse <- sqrt(mean((actual - predicted)^2))
  mae <- mean(abs(actual - predicted))
  r2 <- cor(actual, predicted)^2
  return(list(RMSE = rmse, MAE = mae, R_squared = r2))
}

metrics <- eval_metrics(y_test, preds)
print(metrics)

# Visualization: Actual vs Predicted
ggplot(data.frame(Actual = y_test, Predicted = preds), 
       aes(x = Actual, y = Predicted)) +
  geom_point(alpha = 0.6) +
  geom_abline(slope = 1, intercept = 0, color = "red") +
  labs(title = "Actual vs Predicted Values", 
       subtitle = paste("R² =", round(metrics$R_squared, 4))) +
  theme_minimal()

# Compute SHAP values
shap_values <- shap.values(
  xgb_model = xgb_model,
  X_train = as.matrix(X_train)
)

# Create SHAP summary data
shap_data <- shap.prep(
  xgb_model = xgb_model,
  X_train = as.matrix(X_train),
  top_n = 40
)

shap_importance_abs <- colMeans(abs(shap_values$shap_score))

# Enhanced importance data frame
enhanced_importance <- data.frame(
  Feature = names(shap_importance_abs),
  Abs_Direction = shap_importance_abs,
  Direction = colMeans(shap_values$shap_score)
) %>%
  arrange(desc(Abs_Direction)) %>%
  head(40) %>%
  mutate(
    Effect = ifelse(Direction > 0, "Positive", "Negative"),
    Shap_Value = ifelse(Direction > 0, Abs_Direction, -Abs_Direction)
  )

# Enhanced bar plot (left-right split)
ggplot(enhanced_importance, 
       aes(x = Shap_Value, y = reorder(Feature, Abs_Direction), 
           fill = Effect)) +
  geom_col(width = 0.4) +
  scale_fill_manual(values = c("Positive" = "#FF0D57", "Negative" = "#1E90FF")) +
  labs(fill = "Overall Effect") +
  theme(
    text = element_text(size = 28),
    axis.title.x = element_text(size = 28, hjust = -0.5),
    axis.title.y = element_text(size = 28),
    axis.text.y = element_text(size = 24),
    axis.ticks.x = element_blank(),
    legend.position = "bottom",
    panel.grid.major.y = element_blank(),
    panel.grid.major.x = element_line(color = "grey80"),
    panel.grid.minor.x = element_blank()
  ) +
  expand_limits(x = c(-max(enhanced_importance$Abs_Direction) * 1, 
                      max(enhanced_importance$Abs_Direction) * 1.1)) +
  geom_vline(xintercept = 0, color = "grey40", linetype = "solid", size = 0.5) +
  theme_classic()


# ============================================================================
# Module 2: Machine Learning with XGBoost and SHAP (Scatter Plot)
# ============================================================================

data <- na.omit(data)

X <- data[, -1]
y <- data[[1]]

convert_categorical <- function(df) {
  for (col in names(df)) {
    if (is.factor(df[[col]]) || is.character(df[[col]])) {
      df[[col]] <- as.numeric(factor(df[[col]])) - 1
      cat("Converting variable:", col, "-> numeric\n")
    }
  }
  return(df)
}

X_processed <- convert_categorical(X)

set.seed(1314)
train_index <- sample(1:nrow(X_processed), size = floor(0.9 * nrow(X_processed)))
X_train <- X_processed[train_index, ]
y_train <- y[train_index]
X_test <- X_processed[-train_index, ]
y_test <- y[-train_index]

dtrain <- xgb.DMatrix(data = as.matrix(X_train), label = y_train)
dtest <- xgb.DMatrix(data = as.matrix(X_test), label = y_test)

params <- list(
  objective = "reg:squarederror",
  eta = 0.1,
  max_depth = 6,
  subsample = 0.8,
  colsample_bytree = 0.8
)

xgb_model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 500,
  early_stopping_rounds = 20,
  watchlist = list(train = dtrain, test = dtest),
  print_every_n = 50
)

shap_values <- shap.values(
  xgb_model = xgb_model,
  X_train = as.matrix(X_train)
)

shap_data <- shap.prep(
  xgb_model = xgb_model,
  X_train = as.matrix(X_train),
  top_n = 40
)

shap_importance_abs <- colMeans(abs(shap_values$shap_score))

shap.plot.summary(shap_data) +
  coord_cartesian(ylim = c(-1, 1)) +
  theme(
    text = element_text(size = 28),
    axis.title.x = element_text(size = 28),
    axis.title.y = element_text(size = 28),
    axis.text.x = element_text(size = 24),
    axis.text.y = element_text(size = 24),
    legend.position = "bottom",
    legend.direction = "horizontal",
    legend.key.width = unit(2, "cm"),
    legend.key.height = unit(0.4, "cm"),
    legend.text = element_text(size = 15),
    legend.title = element_text(size = 0, vjust = 1.3)
  ) +
  scale_color_gradient2(
    low = "#1E90FF", high = "#FF0D57",
    mid = "#9400D3",
    midpoint = 0.5,
    name = "Feature value",
    na.value = "lightgrey",
    guide = guide_colourbar(ticks.colour = NA)
  ) +
  coord_flip() +
  theme_classic()


# ============================================================================
# Module 3: Group Comparisons (ICT and CCPI)
# ============================================================================

library(tidyverse)
library(readxl)
library(writexl)
library(car)
library(rstatix)
library(coin)

# ----------------------------------------------------------------------------
# 3.1 ICT Group Comparison
# ----------------------------------------------------------------------------

test <- read_excel("C:\\Users\\HP\\Desktop\\RMdata_selected.xlsx", guess_max = 10000)

test_clean <- test %>%
  filter(!is.na(ICT_NETUSER_2023), !is.na(continent), !is.na(Ratio)) %>%
  group_by(continent) %>%
  mutate(
    continent_median = quantile(ICT_NETUSER_2023, probs = 1/3, na.rm = TRUE),
    ICT_Group = ifelse(ICT_NETUSER_2023 > continent_median, "High ICT", "Low ICT")
  ) %>%
  ungroup() %>%
  filter(!continent %in% c("North America", "Seven seas (open ocean)")) %>%
  mutate(ICT_Group = factor(ICT_Group, levels = c("Low ICT", "High ICT")))

# Strict statistical test function based on normality
group_test_strict <- function(group1_data, group2_data, group1_name, group2_name) {
  
  ks_test1 <- ks.test(scale(group1_data), "pnorm")
  ks_test2 <- ks.test(scale(group2_data), "pnorm")
  
  norm1 <- ks_test1$p.value > 0.05
  norm2 <- ks_test2$p.value > 0.05
  
  if(norm1 && norm2) {
    levene_test <- car::leveneTest(c(group1_data, group2_data), 
                                   factor(rep(c(group1_name, group2_name), 
                                              c(length(group1_data), length(group2_data)))))
    var_equal <- levene_test$`Pr(>F)`[1] > 0.05
  } else {
    var_equal <- NA
    levene_test <- list(`Pr(>F)` = c(NA, NA))
  }
  
  if(norm1 && norm2) {
    if(var_equal) {
      test_result <- t.test(group1_data, group2_data, var.equal = TRUE)
      test_method <- "Independent t-test (equal variance)"
    } else {
      test_result <- t.test(group1_data, group2_data, var.equal = FALSE)
      test_method <- "Independent t-test (unequal variance)"
    }
    p_diff <- test_result$p.value
    
    n1 <- length(group1_data)
    n2 <- length(group2_data)
    sd_pooled <- sqrt(((n1-1)*var(group1_data) + (n2-1)*var(group2_data)) / (n1+n2-2))
    effect_size <- (mean(group1_data) - mean(group2_data)) / sd_pooled
    effect_name <- "Cohen's d"
    
  } else {
    test_result <- wilcox.test(group1_data, group2_data, exact = FALSE)
    test_method <- "Mann-Whitney U test"
    p_diff <- test_result$p.value
    
    effect_size <- rstatix::wilcox_effsize(
      data = data.frame(
        value = c(group1_data, group2_data),
        group = factor(rep(c(group1_name, group2_name), 
                           c(length(group1_data), length(group2_data))))
      ),
      formula = value ~ group
    )$effsize
    effect_name <- "r"
  }
  
  return(list(
    group1_n = length(group1_data),
    group2_n = length(group2_data),
    group1_mean = mean(group1_data),
    group2_mean = mean(group2_data),
    group1_median = median(group1_data),
    group2_median = median(group2_data),
    group1_sd = sd(group1_data),
    group2_sd = sd(group2_data),
    ks_p1 = ks_test1$p.value,
    ks_p2 = ks_test2$p.value,
    normality_group1 = ifelse(norm1, "Normal", "Non-normal"),
    normality_group2 = ifelse(norm2, "Normal", "Non-normal"),
    levene_p = ifelse(!is.na(var_equal), levene_test$`Pr(>F)`[1], NA),
    variance_equal = ifelse(!is.na(var_equal), ifelse(var_equal, "Yes", "No"), "Not applicable"),
    test_method = test_method,
    p_value = p_diff,
    effect_size = effect_size,
    effect_name = effect_name,
    effect_direction = ifelse(test_method == "Mann-Whitney U test", 
                              ifelse(effect_size > 0, "Group1 > Group2", "Group1 < Group2"),
                              ifelse(effect_size > 0, "Group1 > Group2", "Group1 < Group2"))
  ))
}

continents <- unique(test_clean$continent)

ict_results_list <- list()

for(cont in continents) {
  cat("\n=== ", cont, ": High ICT vs Low ICT (strict rule) ===\n")
  
  continent_data <- test_clean %>% filter(continent == cont)
  
  high_data <- continent_data %>% 
    filter(ICT_Group == "High ICT") %>% 
    pull(Ratio)
  
  low_data <- continent_data %>% 
    filter(ICT_Group == "Low ICT") %>% 
    pull(Ratio)
  
  if(length(high_data) >= 2 & length(low_data) >= 2) {
    result <- group_test_strict(high_data, low_data, 
                                paste(cont, "High ICT"), 
                                paste(cont, "Low ICT"))
    
    result$continent <- cont
    ict_results_list[[cont]] <- result
    
    print(result)
  } else {
    cat("Insufficient data, skipping", cont, "\n")
    cat("High ICT group sample size:", length(high_data), "\n")
    cat("Low ICT group sample size:", length(low_data), "\n")
  }
}

if(length(ict_results_list) > 0) {
  ict_results_summary <- map_dfr(ict_results_list, function(x) {
    tibble(
      Continent = x$continent,
      High_ICT_N = x$group1_n,
      Low_ICT_N = x$group2_n,
      High_ICT_Mean = round(x$group1_mean, 3),
      Low_ICT_Mean = round(x$group2_mean, 3),
      High_ICT_Median = round(x$group1_median, 3),
      Low_ICT_Median = round(x$group2_median, 3),
      Normality_High = x$normality_group1,
      Normality_Low = x$normality_group2,
      Test_Method = x$test_method,
      P_Value = format.pval(x$p_value, digits = 3),
      Effect_Size = round(x$effect_size, 3),
      Effect_Name = x$effect_name,
      Effect_Direction = x$effect_direction,
      Significance = case_when(
        x$p_value < 0.001 ~ "***",
        x$p_value < 0.01 ~ "**",
        x$p_value < 0.05 ~ "*",
        x$p_value < 0.1 ~ ".",
        TRUE ~ "ns"
      )
    )
  })
  
  cat("\n=== Summary of ICT group comparisons by continent (strict normality rule) ===\n")
  print(ict_results_summary)
}

cat("\n=== All continents combined: High ICT vs Low ICT (strict rule) ===\n")
all_high_ict_data <- test_clean %>% 
  filter(ICT_Group == "High ICT") %>% 
  pull(Ratio)

all_low_ict_data <- test_clean %>% 
  filter(ICT_Group == "Low ICT") %>% 
  pull(Ratio)

if(length(all_high_ict_data) >= 2 & length(all_low_ict_data) >= 2) {
  overall_ict_result <- group_test_strict(all_high_ict_data, all_low_ict_data, 
                                          "All Continents High ICT", 
                                          "All Continents Low ICT")
  print(overall_ict_result)
  
  overall_ict_summary <- tibble(
    Continent = "All Continents",
    High_ICT_N = overall_ict_result$group1_n,
    Low_ICT_N = overall_ict_result$group2_n,
    High_ICT_Mean = round(overall_ict_result$group1_mean, 3),
    Low_ICT_Mean = round(overall_ict_result$group2_mean, 3),
    High_ICT_Median = round(overall_ict_result$group1_median, 3),
    Low_ICT_Median = round(overall_ict_result$group2_median, 3),
    Normality_High = overall_ict_result$normality_group1,
    Normality_Low = overall_ict_result$normality_group2,
    Test_Method = overall_ict_result$test_method,
    P_Value = format.pval(overall_ict_result$p_value, digits = 3),
    Effect_Size = round(overall_ict_result$effect_size, 3),
    Effect_Name = overall_ict_result$effect_name,
    Effect_Direction = overall_ict_result$effect_direction,
    Significance = case_when(
      overall_ict_result$p_value < 0.001 ~ "***",
      overall_ict_result$p_value < 0.01 ~ "**",
      overall_ict_result$p_value < 0.05 ~ "*",
      overall_ict_result$p_value < 0.1 ~ ".",
      TRUE ~ "ns"
    )
  )
  
  final_ict_results <- bind_rows(ict_results_summary, overall_ict_summary)
  cat("\n=== Final ICT results summary (including overall comparison) ===\n")
  print(final_ict_results)
}

# ----------------------------------------------------------------------------
# 3.2 CCPI Group Comparison
# ----------------------------------------------------------------------------

test <- read_excel("C:\\Users\\HP\\Desktop\\RMdata_selected.xlsx", guess_max = 10000)

test_clean <- test %>%
  filter(!is.na(CCPI_Score), !is.na(continent), !is.na(Ratio)) %>%
  group_by(continent) %>%
  mutate(
    continent_median = quantile(CCPI_Score, probs = 1/2, na.rm = TRUE),
    CCPI_Group = ifelse(CCPI_Score > continent_median, "High CCPI", "Low CCPI")
  ) %>%
  ungroup() %>%
  filter(!continent %in% c("North America", "South America", "Oceania", "NA")) %>%
  mutate(CCPI_Group = factor(CCPI_Group, levels = c("Low CCPI", "High CCPI")))

# Strict test function (same as above but redefined here for clarity)
group_test_strict <- function(group1_data, group2_data, group1_name, group2_name) {
  
  ks_test1 <- ks.test(scale(group1_data), "pnorm")
  ks_test2 <- ks.test(scale(group2_data), "pnorm")
  
  norm1 <- ks_test1$p.value > 0.05
  norm2 <- ks_test2$p.value > 0.05
  
  if(norm1 && norm2) {
    levene_test <- car::leveneTest(c(group1_data, group2_data), 
                                   factor(rep(c(group1_name, group2_name), 
                                              c(length(group1_data), length(group2_data)))))
    var_equal <- levene_test$`Pr(>F)`[1] > 0.05
  } else {
    var_equal <- NA
    levene_test <- list(`Pr(>F)` = c(NA, NA))
  }
  
  if(norm1 && norm2) {
    if(var_equal) {
      test_result <- t.test(group1_data, group2_data, var.equal = TRUE)
      test_method <- "Independent t-test (equal variance)"
    } else {
      test_result <- t.test(group1_data, group2_data, var.equal = FALSE)
      test_method <- "Independent t-test (unequal variance)"
    }
    p_diff <- test_result$p.value
    
    n1 <- length(group1_data)
    n2 <- length(group2_data)
    sd_pooled <- sqrt(((n1-1)*var(group1_data) + (n2-1)*var(group2_data)) / (n1+n2-2))
    effect_size <- (mean(group1_data) - mean(group2_data)) / sd_pooled
    effect_name <- "Cohen's d"
    
  } else {
    test_result <- wilcox.test(group1_data, group2_data, exact = FALSE)
    test_method <- "Mann-Whitney U test"
    p_diff <- test_result$p.value
    
    effect_size <- rstatix::wilcox_effsize(
      data = data.frame(
        value = c(group1_data, group2_data),
        group = factor(rep(c(group1_name, group2_name), 
                           c(length(group1_data), length(group2_data))))
      ),
      formula = value ~ group
    )$effsize
    effect_name <- "r"
  }
  
  return(list(
    group1_n = length(group1_data),
    group2_n = length(group2_data),
    group1_mean = mean(group1_data),
    group2_mean = mean(group2_data),
    group1_median = median(group1_data),
    group2_median = median(group2_data),
    group1_sd = sd(group1_data),
    group2_sd = sd(group2_data),
    ks_p1 = ks_test1$p.value,
    ks_p2 = ks_test2$p.value,
    normality_group1 = ifelse(norm1, "Normal", "Non-normal"),
    normality_group2 = ifelse(norm2, "Normal", "Non-normal"),
    levene_p = ifelse(!is.na(var_equal), levene_test$`Pr(>F)`[1], NA),
    variance_equal = ifelse(!is.na(var_equal), ifelse(var_equal, "Yes", "No"), "Not applicable"),
    test_method = test_method,
    p_value = p_diff,
    effect_size = effect_size,
    effect_name = effect_name,
    effect_direction = ifelse(test_method == "Mann-Whitney U test", 
                              ifelse(effect_size > 0, "Group1 > Group2", "Group1 < Group2"),
                              ifelse(effect_size > 0, "Group1 > Group2", "Group1 < Group2"))
  ))
}

continents <- unique(test_clean$continent)

strict_results_list <- list()

for(cont in continents) {
  cat("\n=== ", cont, ": High CCPI vs Low CCPI (strict rule) ===\n")
  
  continent_data <- test_clean %>% filter(continent == cont)
  
  high_data <- continent_data %>% 
    filter(CCPI_Group == "High CCPI") %>% 
    pull(Ratio)
  
  low_data <- continent_data %>% 
    filter(CCPI_Group == "Low CCPI") %>% 
    pull(Ratio)
  
  if(length(high_data) >= 2 & length(low_data) >= 2) {
    result <- group_test_strict(high_data, low_data, 
                                paste(cont, "High CCPI"), 
                                paste(cont, "Low CCPI"))
    
    result$continent <- cont
    strict_results_list[[cont]] <- result
    
    print(result)
  } else {
    cat("Insufficient data, skipping", cont, "\n")
    cat("High CCPI group sample size:", length(high_data), "\n")
    cat("Low CCPI group sample size:", length(low_data), "\n")
  }
}

if(length(strict_results_list) > 0) {
  strict_results_summary <- map_dfr(strict_results_list, function(x) {
    tibble(
      Continent = x$continent,
      High_CCPI_N = x$group1_n,
      Low_CCPI_N = x$group2_n,
      High_CCPI_Mean = round(x$group1_mean, 3),
      Low_CCPI_Mean = round(x$group2_mean, 3),
      High_CCPI_Median = round(x$group1_median, 3),
      Low_CCPI_Median = round(x$group2_median, 3),
      Normality_High = x$normality_group1,
      Normality_Low = x$normality_group2,
      Test_Method = x$test_method,
      P_Value = format.pval(x$p_value, digits = 3),
      Effect_Size = round(x$effect_size, 3),
      Effect_Name = x$effect_name,
      Effect_Direction = x$effect_direction,
      Significance = case_when(
        x$p_value < 0.001 ~ "***",
        x$p_value < 0.01 ~ "**",
        x$p_value < 0.05 ~ "*",
        x$p_value < 0.1 ~ ".",
        TRUE ~ "ns"
      )
    )
  })
  
  cat("\n=== Summary of CCPI group comparisons by continent (strict normality rule) ===\n")
  print(strict_results_summary)
}

cat("\n=== All continents combined: High CCPI vs Low CCPI (strict rule) ===\n")
all_high_data <- test_clean %>% 
  filter(CCPI_Group == "High CCPI") %>% 
  pull(Ratio)

all_low_data <- test_clean %>% 
  filter(CCPI_Group == "Low CCPI") %>% 
  pull(Ratio)

if(length(all_high_data) >= 2 & length(all_low_data) >= 2) {
  overall_strict_result <- group_test_strict(all_high_data, all_low_data, 
                                             "All Continents High CCPI", 
                                             "All Continents Low CCPI")
  print(overall_strict_result)
  
  overall_strict_summary <- tibble(
    Continent = "All Continents",
    High_CCPI_N = overall_strict_result$group1_n,
    Low_CCPI_N = overall_strict_result$group2_n,
    High_CCPI_Mean = round(overall_strict_result$group1_mean, 3),
    Low_CCPI_Mean = round(overall_strict_result$group2_mean, 3),
    High_CCPI_Median = round(overall_strict_result$group1_median, 3),
    Low_CCPI_Median = round(overall_strict_result$group2_median, 3),
    Normality_High = overall_strict_result$normality_group1,
    Normality_Low = overall_strict_result$normality_group2,
    Test_Method = overall_strict_result$test_method,
    P_Value = format.pval(overall_strict_result$p_value, digits = 3),
    Effect_Size = round(overall_strict_result$effect_size, 3),
    Effect_Name = overall_strict_result$effect_name,
    Effect_Direction = overall_strict_result$effect_direction,
    Significance = case_when(
      overall_strict_result$p_value < 0.001 ~ "***",
      overall_strict_result$p_value < 0.01 ~ "**",
      overall_strict_result$p_value < 0.05 ~ "*",
      overall_strict_result$p_value < 0.1 ~ ".",
      TRUE ~ "ns"
    )
  )
  
  final_strict_results <- bind_rows(strict_results_summary, overall_strict_summary)
  cat("\n=== Final CCPI results summary (strict rule, including overall comparison) ===\n")
  print(final_strict_results)
}


# ============================================================================
# Module 4: Grouped Boxplots (ICT and CCPI)
# ============================================================================

library(tidyverse)
library(readxl)
library(writexl)

test <- read_excel("C:\\Users\\HP\\Desktop\\RMdata_selected.xlsx", sheet=2, guess_max = 10000)

# CCPI boxplot
test_clean <- test %>%
  filter(!is.na(CCPI_Score), !is.na(continent), !is.na(Ratio)) %>%
  group_by(continent) %>%
  mutate(
    continent_median = quantile(CCPI_Score, probs = 1/3, na.rm = TRUE),
    CCPI_Group = ifelse(CCPI_Score > continent_median, "High CCPI", "Low CCPI")
  ) %>%
  ungroup() %>%
  filter(!continent %in% c("North America", "South America", "Oceania", "NA")) %>%
  mutate(CCPI_Group = factor(CCPI_Group, levels = c("Low CCPI", "High CCPI")))

continent_order <- levels(as.factor(test_clean$continent))
n_continents <- length(continent_order)

ggplot(test_clean, aes(x = continent, y = Ratio, fill = CCPI_Group)) +
  geom_vline(xintercept = seq(1.5, n_continents - 0.5, by = 1),
             linetype = "dashed", 
             color = "gray50", 
             alpha = 0.7,
             size = 0.5) +
  geom_boxplot(alpha = 0.8,
               position = position_dodge(width = 0.5),
               outlier.alpha = 0.5,
               width = 0.3) +
  labs(x = "",
       y = "Reporting Ratio",
       fill = "") +
  scale_fill_manual(values = c("Low CCPI" = "#5D3A9B", "High CCPI" = "#E7B800")) +
  theme_bw() +
  theme(
    legend.position = "top",
    panel.grid.major.x = element_blank()
  )

# ICT boxplot
test_clean <- test %>%
  filter(!is.na(ICT_NETUSER_2023), !is.na(continent), !is.na(Ratio)) %>%
  group_by(continent) %>%
  mutate(
    continent_median = quantile(ICT_NETUSER_2023, probs = 1/3, na.rm = TRUE),
    ICT_Group = ifelse(ICT_NETUSER_2023 > continent_median, "High ICT", "Low ICT")
  ) %>%
  ungroup() %>%
  filter(!continent %in% c("Oceania", "North America", "Seven seas (open ocean)", "South America")) %>%
  mutate(ICT_Group = factor(ICT_Group, levels = c("Low ICT", "High ICT")))

continent_order <- levels(as.factor(test_clean$continent))
n_continents <- length(continent_order)

ggplot(test_clean, aes(x = continent, y = Ratio, fill = ICT_Group)) +
  geom_vline(xintercept = seq(1.5, n_continents - 0.5, by = 1),
             linetype = "dashed", 
             color = "gray50", 
             alpha = 0.7,
             size = 0.5) +
  geom_boxplot(alpha = 0.8,
               position = position_dodge(width = 0.5),
               outlier.alpha = 0.5,
               width = 0.3) +
  labs(x = "",
       y = "",
       fill = "") +
  scale_fill_manual(values = c("Low ICT" = "#D45087", "High ICT" = "#2FC1B7")) +
  theme_bw() +
  theme(
    legend.position = "top",
    panel.grid.major.x = element_blank()
  )


# ============================================================================
# Module 5: Linear Regression and Plots for Language Proximity (Africa)
# ============================================================================

# 5.1 Overall fit line

test <- read_excel("C:\\Users\\HP\\Desktop\\RMdata_selected.xlsx", sheet=2, guess_max = 10000)

Africa <- test %>%
  filter(continent == "Africa")

ENG <- c("南非","肯尼亚","坦桑尼亚","赞比亚","埃塞俄比亚","乌干达","津巴布韦","博茨瓦纳","纳米比亚",
         "马拉维","卢旺达","莱索托","尼日利亚","加纳","塞拉里昂","利比里亚","冈比亚","南苏丹",
         "索马里","厄立特里亚")
FRA <- c("刚果民主共和国（扎伊尔）","乍得","尼日尔","马里","马达加斯加",
         "中非","刚果共和国","科特迪瓦（象牙海岸）","布基纳法索","塞内加尔","贝宁","多哥",
         "布隆迪","吉布提共和国","喀麦隆")
ARB <- c("埃及","突尼斯","利比亚","苏丹","阿尔及利亚","毛里塔尼亚","摩洛哥")
POR <- c("安哥拉","莫桑比克","几内亚比绍","佛得角","圣多美和普林西比","赤道几内亚")

Africa <- Africa %>%
  mutate(lan_group = "Other") %>%
  mutate(lan_group = ifelse(country1 %in% ENG, "English", lan_group)) %>%
  mutate(lan_group = ifelse(country1 %in% FRA, "French", lan_group)) %>%
  mutate(lan_group = ifelse(country1 %in% ARB, "Arabic", lan_group)) %>%
  mutate(lan_group = ifelse(country1 %in% POR, "Portuguese", lan_group))

lan_groups <- unique(Africa$lan_group)
lan_groups <- lan_groups[!is.na(lan_groups)]

Africa$lan_group <- factor(
  Africa$lan_group,
  levels = c("English", "French", "Portuguese", "Arabic", "Other")
)

country_means <- Africa %>%
  group_by(country1, lan_group, LPN2CommonLanguage_2024) %>%
  summarise(
    mean_ratio = mean(Ratio, na.rm = TRUE),
    n_points = n(),
    .groups = "drop"
  )

overall_model <- lm(Ratio ~ LPN2CommonLanguage_2024, data = Africa)
overall_p <- summary(overall_model)$coefficients[2,4]

p_stars <- ifelse(overall_p < 0.001, "***",
                  ifelse(overall_p < 0.01, "**",
                         ifelse(overall_p < 0.05, "*", "")))

color_mapping <- scale_color_brewer(palette = "Set1")
fill_mapping <- scale_fill_brewer(palette = "Set1")

ggplot(Africa, aes(x = LPN2CommonLanguage_2024, y = Ratio)) +
  geom_point(aes(color = lan_group), alpha = 0.1, size = 2) +
  geom_point(
    data = country_means,
    aes(x = LPN2CommonLanguage_2024, y = mean_ratio, color = lan_group),
    alpha = 1,
    size = 3
  ) +
  geom_smooth(
    data = Africa,
    aes(x = LPN2CommonLanguage_2024, y = Ratio),
    method = "lm",
    se = TRUE,
    color = "black",
    fill = "grey35",
    alpha = 0.3,
    size = 0.8
  ) +
  color_mapping +
  fill_mapping +
  guides(
    color = guide_legend(title = "Language Group", override.aes = list(alpha = 1, size = 2)),
    fill = "none"
  ) +
  labs(
    x = "Language Proximity Index",
    y = ""
  ) +
  coord_cartesian(ylim = c(0.5, 0.75)) +
  theme_minimal() +
  theme(
    text = element_text(size = 28),
    plot.title = element_text(size = 24, hjust = 0.5),
    axis.title.x = element_text(size = 24),
    axis.title.y = element_text(size = 24),
    axis.text.x = element_text(size = 24),
    axis.text.y = element_text(size = 24),
    legend.position = "bottom"
  )

# 5.2 Arabic language subgroup

arabic_data <- Africa %>% filter(lan_group == "Arabic")
x_range_arabic <- range(arabic_data$LPN2CommonLanguage_2024, na.rm = TRUE)
y_range_arabic <- range(arabic_data$Ratio, na.rm = TRUE)

Africa_arabic <- Africa %>%
  mutate(plot_group = ifelse(lan_group == "Arabic", "Arabic", "non-Arabic"))

country_means_arabic <- country_means %>%
  mutate(plot_group = ifelse(lan_group == "Arabic", "Arabic", "non-Arabic"))

simple_colors_arabic <- c("Arabic" = "#984EA3", "non-Arabic" = "grey70")

arabic_lm <- lm(Ratio ~ LPN2CommonLanguage_2024, data = arabic_data)
arabic_beta <- coef(arabic_lm)[2]
arabic_p <- summary(arabic_lm)$coefficients[2,4]

arabic_p_star <- ifelse(arabic_p < 0.001, "***",
                        ifelse(arabic_p < 0.01, "**",
                               ifelse(arabic_p < 0.05, "*", "")))

arabic_label <- paste0("β = ", round(arabic_beta, 3), 
                       " ",
                       ", p = ", format(arabic_p, digits = 3), arabic_p_star)

ggplot(Africa_arabic, aes(x = LPN2CommonLanguage_2024, y = Ratio)) +
  geom_point(aes(color = plot_group), alpha = 0.1, size = 2) +
  geom_point(
    data = country_means_arabic,
    aes(x = LPN2CommonLanguage_2024, y = mean_ratio, color = plot_group),
    alpha = 1,
    size = 3
  ) +
  geom_smooth(
    data = Africa %>% filter(lan_group == "Arabic"),
    aes(x = LPN2CommonLanguage_2024, y = Ratio),
    method = "lm",
    se = TRUE,
    color = "#984EA3",
    fill = "#984EA3",
    alpha = 0.2,
    size = 0.8
  ) +
  coord_cartesian(x = x_range_arabic, ylim = c(0.5, 0.75)) +
  scale_color_manual(values = simple_colors_arabic) +
  guides(
    color = guide_legend(title = "Language Group", override.aes = list(alpha = 1, size = 3)),
    fill = "none"
  ) +
  labs(
    x = "",
    y = ""
  ) +
  theme_minimal() +
  theme(
    text = element_text(size = 20),
    plot.title = element_text(size = 24, hjust = 0.5),
    axis.title.x = element_text(size = 24),
    axis.title.y = element_text(size = 24),
    axis.text.x = element_text(size = 24),
    axis.text.y = element_text(size = 24),
    legend.position = "bottom"
  )