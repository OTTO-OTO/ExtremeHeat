# ============================================================================
# Libraries
# ============================================================================
library(readxl)
library(dplyr)
library(tidyr)
library(ggplot2)
library(caret)
library(xgboost)
library(SHAPforxgboost)
library(car)
library(rstatix)
library(boot)
library(purrr)

# ============================================================================
# Configuration
# ============================================================================
SEED_MAIN <- 42
IN_PATH   <- "C:/Users/HP/Desktop/Figure5_RawData.xlsx"

# ============================================================================
# Data loading and global imputation
# ============================================================================
raw_data <- read_excel(IN_PATH, guess_max = 10000)
cat("Rows:", nrow(raw_data), " Columns:", ncol(raw_data), "\n")

cols_to_impute <- names(raw_data)[6:13]   # 8 variables excluding CCPI

if (!"income_grp" %in% names(raw_data)) stop("Column income_grp not found.")

col_classes <- sapply(raw_data[cols_to_impute], function(x) class(x)[1])
if (!all(col_classes %in% c("numeric", "integer", "double"))) {
  stop("Non-numeric variables found in columns 6-13: ",
       paste(names(col_classes)[!col_classes %in% c("numeric","integer","double")],
             collapse = ", "))
}

imputed_data <- raw_data %>%
  group_by(income_grp) %>%
  mutate(across(
    all_of(cols_to_impute),
    ~ if_else(is.na(.x) & !is.na(income_grp),
              median(.x, na.rm = TRUE),
              .x)
  )) %>%
  ungroup()

outcome_col  <- names(imputed_data)[1]
feature_cols <- names(imputed_data)[6:13]

if (any(is.na(imputed_data[[outcome_col]]))) {
  imputed_data <- imputed_data %>% filter(!is.na(.data[[outcome_col]]))
}

# ============================================================================
# Shared helpers
# ============================================================================
eval_metrics <- function(actual, predicted) {
  rss  <- sum((actual - predicted)^2)
  tss  <- sum((actual - mean(actual))^2)
  list(R2   = 1 - rss / tss,
       RMSE = sqrt(mean((actual - predicted)^2)),
       MAE  = mean(abs(actual - predicted)))
}

impute_fold <- function(train_df, test_df, cols) {
  medians <- train_df %>%
    group_by(income_grp) %>%
    summarise(across(all_of(cols),
                     ~ median(.x, na.rm = TRUE),
                     .names = "med_{.col}"),
              .groups = "drop")
  
  train_imp <- train_df %>%
    group_by(income_grp) %>%
    mutate(across(all_of(cols),
                  ~ if_else(is.na(.x) & !is.na(income_grp),
                            median(.x, na.rm = TRUE), .x))) %>%
    ungroup()
  
  test_imp <- test_df %>% left_join(medians, by = "income_grp")
  for (col in cols) {
    med_col <- paste0("med_", col)
    if (med_col %in% names(test_imp)) {
      test_imp[[col]] <- ifelse(
        is.na(test_imp[[col]]) & !is.na(test_imp$income_grp),
        test_imp[[med_col]],
        test_imp[[col]]
      )
    }
  }
  list(train = train_imp,
       test  = test_imp %>% select(-starts_with("med_")))
}

# ============================================================================
# XGBoost model and SHAP bar plot
# ============================================================================
m1_data <- raw_data %>% filter(!is.na(.data[[outcome_col]]))
X_m1    <- as.data.frame(m1_data[, feature_cols])
y_m1    <- m1_data[[outcome_col]]
income  <- m1_data$income_grp

set.seed(SEED_MAIN)
train_index <- sample(1:nrow(X_m1), size = floor(0.8 * nrow(X_m1)))
X_train_raw <- X_m1[train_index, , drop = FALSE]
y_train     <- y_m1[train_index]
X_test_raw  <- X_m1[-train_index, , drop = FALSE]
y_test      <- y_m1[-train_index]
income_train <- income[train_index]
income_test  <- income[-train_index]

train_df <- cbind(X_train_raw, income_grp = income_train)
test_df  <- cbind(X_test_raw,  income_grp = income_test)
imp      <- impute_fold(train_df, test_df, cols_to_impute)

X_train <- as.data.frame(imp$train[, feature_cols, drop = FALSE])
X_test  <- as.data.frame(imp$test[,  feature_cols, drop = FALSE])

set.seed(SEED_MAIN)
val_index <- sample(1:nrow(X_train), size = floor(0.1 * nrow(X_train)))
X_val <- X_train[val_index, , drop = FALSE]
y_val <- y_train[val_index]
X_tr  <- X_train[-val_index, , drop = FALSE]
y_tr  <- y_train[-val_index]

dtrain <- xgb.DMatrix(data = as.matrix(X_tr), label = y_tr)
dval   <- xgb.DMatrix(data = as.matrix(X_val), label = y_val)
dtest  <- xgb.DMatrix(data = as.matrix(X_test), label = y_test)

param_grid <- expand.grid(
  eta              = c(0.05, 0.1, 0.2),
  max_depth        = c(5, 6, 7),
  subsample        = c(0.6, 0.8, 1.0),
  colsample_bytree = c(0.6, 0.8, 1.0),
  reg_lambda       = c(1),
  min_child_weight = c(1),
  stringsAsFactors = FALSE
)
cat("Hyperparameter combinations:", nrow(param_grid), "\n")

set.seed(SEED_MAIN)
cv_folds <- createFolds(y_train, k = 5, list = TRUE)
tuning_results <- data.frame()

for (i in 1:nrow(param_grid)) {
  p <- as.list(param_grid[i, ])
  p$objective   <- "reg:squarederror"
  p$eval_metric <- "rmse"
  fold_rmse <- c()
  
  for (f in 1:5) {
    val_idx <- cv_folds[[f]]
    tr_idx  <- setdiff(seq_along(y_train), val_idx)
    
    dtrain_fold <- xgb.DMatrix(as.matrix(X_train[tr_idx, , drop = FALSE]),
                               label = y_train[tr_idx])
    dval_fold   <- xgb.DMatrix(as.matrix(X_train[val_idx, , drop = FALSE]),
                               label = y_train[val_idx])
    
    set.seed(SEED_MAIN)
    model_fold <- xgb.train(params = p, data = dtrain_fold,
                            nrounds = 1000, early_stopping_rounds = 50,
                            watchlist = list(val = dval_fold),
                            verbose = 0)
    
    preds_fold <- predict(model_fold, dval_fold,
                          iteration_range = c(0, model_fold$best_iteration + 1))
    fold_rmse[f] <- sqrt(mean((y_train[val_idx] - preds_fold)^2))
  }
  
  tuning_results <- rbind(tuning_results,
                          data.frame(param_grid[i, ],
                                     mean_rmse = mean(fold_rmse),
                                     sd_rmse   = sd(fold_rmse)))
  
  if (i %% 10 == 0) cat("Completed", i, "of", nrow(param_grid), "\n")
}

best_idx <- which.min(tuning_results$mean_rmse)
best_params <- as.list(tuning_results[best_idx, 1:6])
best_params$objective   <- "reg:squarederror"
best_params$eval_metric <- "rmse"

cat("\nBest parameter combination:\n"); print(best_params)
cat("Best CV mean RMSE:", tuning_results$mean_rmse[best_idx], "\n")

set.seed(SEED_MAIN)
xgb_model <- xgb.train(params = best_params, data = dtrain,
                       nrounds = 1000, early_stopping_rounds = 50,
                       watchlist = list(train = dtrain, val = dval),
                       verbose = 0)

preds <- predict(xgb_model, dtest,
                 iteration_range = c(0, xgb_model$best_iteration + 1))
metrics <- eval_metrics(y_test, preds)
cat("\nXGBoost test metrics:\n"); print(metrics)



# ============================================================================
# SHAP bar plot with error bars (absolute values, right-aligned)
# ============================================================================
shap_values <- shap.values(xgb_model = xgb_model,
                           X_train = as.matrix(X_train))
shap_data <- shap.prep(xgb_model = xgb_model,
                       X_train = as.matrix(X_train),
                       top_n = ncol(X_train))

shap_abs_mat <- abs(shap_values$shap_score)

shap_importance_abs <- colMeans(shap_abs_mat)
shap_sd_abs         <- apply(shap_abs_mat, 2, sd)

enhanced_importance <- data.frame(
  Feature       = names(shap_importance_abs),
  Abs_Direction = shap_importance_abs,
  Direction     = colMeans(shap_values$shap_score),
  SD            = shap_sd_abs[names(shap_importance_abs)]
) %>%
  arrange(desc(Abs_Direction)) %>%
  mutate(
    Effect = ifelse(Direction > 0, "Positive", "Negative"),
    xmin   = pmax(Abs_Direction - SD, 0),
    xmax   = Abs_Direction + SD
  )

p_bar <- ggplot(enhanced_importance,
                aes(x = Abs_Direction,
                    y = reorder(Feature, Abs_Direction),
                    fill = Effect)) +
  geom_col(width = 0.6) +
  geom_errorbar(aes(xmin = xmin, xmax = xmax),
                width = 0.2, color = "grey20", size = 0.5) +
  scale_fill_manual(values = c("Positive" = "#D95F02", "Negative" = "#1B7D8C")) +
  labs(fill = "Overall Effect", x = "Mean |SHAP value|", y = "") +
  theme_classic() +
  theme(
    text             = element_text(size = 14),
    axis.title.x     = element_text(size = 14),
    axis.text.y      = element_text(size = 12),
    axis.ticks.x     = element_blank(),
    legend.position  = "bottom",
    panel.grid       = element_blank(),
    panel.background = element_rect(fill = "white", colour = NA)
  ) +
  expand_limits(x = c(0, max(enhanced_importance$xmax) * 1.05)) +
  geom_vline(xintercept = 0, color = "grey40", linetype = "solid", size = 0.5)

print(p_bar)



# ============================================================================
# Group-level SHAP importance: bar chart and Nightingale rose plot
# ============================================================================
feature_groups <- c(
  "GDP percapita(gdp_percapita_historical_2020)"       = "Socioeconomic Status",
  "Precipitation(cmip5_prAdjust_2024_mm_year)"         = "Socioeconomic Status",
  "Language Proximity(LPN2CommonLanguage_2024)"        = "Socioeconomic Status",
  "Population Density(pop_density_2020)"               = "Socioeconomic Status",
  "CCVI(ccvi_2024)"                                     = "Climate Vulnerability",
  "Climate Sensitivity(NDGAIN_VulSensitivity_2023)"    = "Climate Vulnerability",
  "Climate Capacity(NDGAIN_VulCapacity_2023)"          = "Climate Vulnerability",
  "ICT Level(ICT_NETUSER_2023）"                       = "Monitoring Ability"
)

plot_data <- enhanced_importance %>%
  mutate(Group = feature_groups[Feature]) %>%
  filter(!is.na(Group))

group_colors <- c(
  "Socioeconomic Status" = "#B3C1D4",
  "Climate Vulnerability" = "#D1B8A0",
  "Monitoring Ability"   = "#B0B0B0"
)

# --- Horizontal bar chart ---
group_summary <- plot_data %>%
  group_by(Group) %>%
  summarise(Total_Importance = sum(Abs_Direction, na.rm = TRUE)) %>%
  mutate(Percentage = Total_Importance / sum(Total_Importance) * 100)

p_group <- ggplot(group_summary,
                  aes(x = Percentage,
                      y = reorder(Group, Percentage),
                      fill = Group)) +
  geom_col(width = 0.7) +
  scale_fill_manual(values = group_colors) +
  labs(x = "Contribution to total SHAP importance (%)", y = "") +
  theme_classic() +
  theme(
    legend.position  = "none",
    panel.grid       = element_blank(),
    panel.background = element_rect(fill = "white", colour = NA)
  ) +
  geom_text(aes(label = sprintf("%.1f%%", Percentage)),
            hjust = -0.2, size = 4)

print(p_group)

# --- Nightingale rose plot ---
rose_data <- plot_data %>%
  arrange(desc(Abs_Direction)) %>%
  mutate(
    width_raw    = Abs_Direction,
    width        = width_raw / sum(width_raw) * 2 * pi,
    start_angle  = cumsum(lag(width, default = 0)),
    rank         = row_number(),
    inner_radius = 1.5 + 0.15 * (rank - 1),
    inner_height = 0.4,
    outer_height = 0.6,
    end_angle    = start_angle + width
  )

inner_color <- ifelse(rose_data$rank %% 2 == 0, "white", "#EBEBED")

p_rose <- ggplot(rose_data) +
  geom_rect(
    aes(xmin = start_angle, xmax = end_angle,
        ymin = inner_radius, ymax = inner_radius + inner_height),
    fill = inner_color,
    color = "white", size = 0.3
  ) +
  geom_rect(
    aes(xmin = start_angle, xmax = end_angle,
        ymin = inner_radius + inner_height,
        ymax = inner_radius + inner_height + outer_height,
        fill = Group),
    color = "white", size = 0.3
  ) +
  scale_fill_manual(values = group_colors, name = "Group") +
  coord_polar(theta = "x", start = 0) +
  theme_void() +
  theme(
    legend.position = "bottom",
    legend.title    = element_blank(),
    legend.text     = element_text(size = 10)
  )

print(p_rose)


# ============================================================================
# Module 2: SHAP summary scatter plot (REMOVABLE — for internal use only)
# ============================================================================

# Prepare complete-case data for Module 2
X_m2 <- as.data.frame(imputed_data[, feature_cols])
y_m2 <- imputed_data[[outcome_col]]
complete_idx <- complete.cases(X_m2) & !is.na(y_m2)
X_m2 <- X_m2[complete_idx, , drop = FALSE]
y_m2 <- y_m2[complete_idx]

set.seed(SEED_MAIN)
train_index_m2 <- sample(1:nrow(X_m2), size = floor(0.8 * nrow(X_m2)))
X_train_m2 <- X_m2[train_index_m2, , drop = FALSE]
y_train_m2 <- y_m2[train_index_m2]
X_test_m2  <- X_m2[-train_index_m2, , drop = FALSE]
y_test_m2  <- y_m2[-train_index_m2]

set.seed(SEED_MAIN)
val_index_m2 <- sample(1:nrow(X_train_m2), size = floor(0.1 * nrow(X_train_m2)))
X_val_m2 <- X_train_m2[val_index_m2, , drop = FALSE]
y_val_m2 <- y_train_m2[val_index_m2]
X_tr_m2  <- X_train_m2[-val_index_m2, , drop = FALSE]
y_tr_m2  <- y_train_m2[-val_index_m2]

dtrain_m2 <- xgb.DMatrix(data = as.matrix(X_tr_m2), label = y_tr_m2)
dval_m2   <- xgb.DMatrix(data = as.matrix(X_val_m2), label = y_val_m2)
dtest_m2  <- xgb.DMatrix(data = as.matrix(X_test_m2), label = y_test_m2)

set.seed(SEED_MAIN)
xgb_model_m2 <- xgb.train(params = best_params, data = dtrain_m2,
                          nrounds = 1000, early_stopping_rounds = 50,
                          watchlist = list(train = dtrain_m2, val = dval_m2),
                          verbose = 0)

preds_m2 <- predict(xgb_model_m2, dtest_m2,
                    iteration_range = c(0, xgb_model_m2$best_iteration + 1))
cat("\nModule 2 test metrics:\n")
print(eval_metrics(y_test_m2, preds_m2))

# SHAP values for Module 2
shap_values_m2 <- shap.values(xgb_model = xgb_model_m2,
                              X_train = as.matrix(X_train_m2))
shap_data_m2 <- shap.prep(xgb_model = xgb_model_m2,
                          X_train = as.matrix(X_train_m2),
                          top_n = ncol(X_train_m2))

p_scatter_m2 <- shap.plot.summary(shap_data_m2) +
  theme(
    text             = element_text(size = 28),
    axis.title.x     = element_text(size = 28),
    axis.title.y     = element_text(size = 28),
    axis.text.x      = element_text(size = 24),
    axis.text.y      = element_text(size = 24),
    legend.position  = "bottom",
    legend.direction = "horizontal",
    legend.key.width = unit(2, "cm"),
    legend.key.height = unit(0.4, "cm"),
    legend.text      = element_text(size = 15),
    legend.title     = element_text(size = 0, vjust = 1.3),
    panel.grid       = element_blank(),
    panel.background = element_rect(fill = "white", colour = NA)
  ) +
  scale_color_gradient2(
    low      = "#1B7D8C",
    high     = "#D95F02",
    mid      = "#D6C6E0",
    midpoint = 0.5,
    name     = "Feature value",
    na.value = "lightgrey",
    guide    = guide_colourbar(ticks.colour = NA)
  ) +
  scale_y_continuous(expand = expansion(mult = c(0.10, 0.10))) +
  coord_flip() +
  theme_classic() +
  theme(
    plot.margin = margin(10, 30, 10, 10)
  )
print(p_scatter_m2)




# ============================================================================
# Group comparison: ICT and CCPI
# ============================================================================
ict_col  <- names(imputed_data)[grepl("ICT",  names(imputed_data), ignore.case = TRUE)]
ccpi_col <- names(imputed_data)[grepl("CCPI", names(imputed_data), ignore.case = TRUE)]
if (length(ict_col) == 0 || length(ccpi_col) == 0) {
  stop("ICT or CCPI column not found.")
}

df_group <- imputed_data %>%
  select(Ratio     = all_of(outcome_col),
         continent,
         ICT       = all_of(ict_col),
         CCPI      = all_of(ccpi_col)) %>%
  filter(continent %in% c("Africa", "Asia", "Europe")) %>%
  mutate(continent = factor(continent, levels = c("Africa", "Asia", "Europe")))

boot_median_diff_ci <- function(x, y, n_boot = 2000, conf = 0.95) {
  set.seed(SEED_MAIN)
  boot_stat <- function(data, indices) {
    d <- data[indices, ]
    median(d$value[d$group == "G1"]) - median(d$value[d$group == "G2"])
  }
  df_boot <- data.frame(value = c(x, y),
                        group = factor(rep(c("G1","G2"), c(length(x), length(y)))))
  b  <- boot(df_boot, boot_stat, R = n_boot)
  ci <- boot.ci(b, type = "perc", conf = conf)$percent
  c(lower = ci[4], upper = ci[5])
}

boot_mwu_r_ci <- function(x, y, n_boot = 2000, conf = 0.95) {
  set.seed(SEED_MAIN)
  df_boot <- data.frame(value = c(x, y),
                        group = factor(rep(c("G1","G2"), c(length(x), length(y)))))
  boot_stat <- function(data, indices) {
    d <- data[indices, ]
    rstatix::wilcox_effsize(d, value ~ group)$effsize
  }
  b  <- boot(df_boot, boot_stat, R = n_boot)
  ci <- boot.ci(b, type = "perc", conf = conf)$percent
  c(lower = ci[4], upper = ci[5])
}

boot_cohen_d_ci <- function(x, y, n_boot = 2000, conf = 0.95) {
  set.seed(SEED_MAIN)
  df_boot <- data.frame(value = c(x, y),
                        group = factor(rep(c("G1","G2"), c(length(x), length(y)))))
  boot_stat <- function(data, indices) {
    d <- data[indices, ]
    x1 <- d$value[d$group == "G1"]; x2 <- d$value[d$group == "G2"]
    n1 <- length(x1); n2 <- length(x2)
    sd_pooled <- sqrt(((n1-1)*var(x1) + (n2-1)*var(x2)) / (n1+n2-2))
    (mean(x1) - mean(x2)) / sd_pooled
  }
  b  <- boot(df_boot, boot_stat, R = n_boot)
  ci <- boot.ci(b, type = "perc", conf = conf)$percent
  c(lower = ci[4], upper = ci[5])
}

group_test_strict <- function(g1, g2, name1, name2) {
  sw1 <- tryCatch(shapiro.test(g1), error = function(e) NULL)
  sw2 <- tryCatch(shapiro.test(g2), error = function(e) NULL)
  norm1 <- !is.null(sw1) && sw1$p.value > 0.05
  norm2 <- !is.null(sw2) && sw2$p.value > 0.05
  
  if (norm1 && norm2) {
    lev <- car::leveneTest(c(g1, g2),
                           factor(rep(c(name1, name2), c(length(g1), length(g2)))))
    var_equal <- lev$`Pr(>F)`[1] > 0.05
    tt <- if (var_equal) t.test(g1, g2, var.equal = TRUE)
    else            t.test(g1, g2, var.equal = FALSE)
    test_method <- if (var_equal) "t-test (equal variance)" else "Welch t-test"
    p_val    <- tt$p.value
    mean_diff <- mean(g1) - mean(g2)
    ci_diff   <- tt$conf.int
    n1 <- length(g1); n2 <- length(g2)
    sd_pooled <- sqrt(((n1-1)*var(g1) + (n2-1)*var(g2)) / (n1+n2-2))
    effect_size <- (mean(g1) - mean(g2)) / sd_pooled
    effect_name <- "Cohen's d"
    ci_effect   <- boot_cohen_d_ci(g1, g2)
  } else {
    wt <- wilcox.test(g1, g2, exact = FALSE)
    test_method <- "Mann-Whitney U"
    p_val    <- wt$p.value
    mean_diff <- median(g1) - median(g2)
    ci_diff   <- boot_median_diff_ci(g1, g2)
    effect_size <- rstatix::wilcox_effsize(
      data.frame(value = c(g1, g2),
                 group = factor(rep(c(name1, name2), c(length(g1), length(g2))))),
      formula = value ~ group)$effsize
    effect_name <- "r"
    ci_effect   <- boot_mwu_r_ci(g1, g2)
  }
  
  list(group1_n = length(g1), group2_n = length(g2),
       group1_mean = mean(g1), group2_mean = mean(g2),
       group1_median = median(g1), group2_median = median(g2),
       group1_sd = sd(g1), group2_sd = sd(g2),
       test_method = test_method, p_value = p_val,
       mean_diff = mean_diff,
       ci_diff_lower = ci_diff[1], ci_diff_upper = ci_diff[2],
       effect_size = effect_size, effect_name = effect_name,
       ci_effect_lower = ci_effect[1], ci_effect_upper = ci_effect[2])
}

assign_groups <- function(df, var_name) {
  df %>%
    filter(!is.na(.data[[var_name]]), !is.na(Ratio), !is.na(continent)) %>%
    group_by(continent) %>%
    mutate(q33 = quantile(.data[[var_name]], probs = 1/3, na.rm = TRUE),
           q67 = quantile(.data[[var_name]], probs = 2/3, na.rm = TRUE),
           group = case_when(.data[[var_name]] <= q33 ~ "Low",
                             .data[[var_name]] >= q67 ~ "High",
                             TRUE ~ NA_character_)) %>%
    ungroup() %>%
    filter(!is.na(group)) %>%
    mutate(group = factor(group, levels = c("Low", "High")))
}

print_group_stats <- function(summary_tbl, var_label) {
  cat("\n------------------------------------------------------------\n")
  cat("Statistics for", var_label, "\n")
  cat("------------------------------------------------------------\n")
  out <- summary_tbl %>%
    select(Continent, High_N, Low_N, Effect_Size, Effect_Name, Effect_CI, P_Adj) %>%
    arrange(factor(Continent, levels = c("Africa", "Asia", "Europe", "All")))
  print(out, n = Inf)
  cat("------------------------------------------------------------\n\n")
}

run_comparison <- function(df, var_name, var_label) {
  df_var <- assign_groups(df, var_name)
  results_list <- list()
  
  for (cont in c("Africa", "Asia", "Europe")) {
    sub  <- df_var %>% filter(continent == cont)
    high <- sub$Ratio[sub$group == "High"]
    low  <- sub$Ratio[sub$group == "Low"]
    if (length(high) >= 2 & length(low) >= 2) {
      res <- group_test_strict(high, low,
                               paste(cont, "High"), paste(cont, "Low"))
      res$continent <- cont
      res$variable  <- var_label
      results_list[[cont]] <- res
    }
  }
  
  high_all <- df_var$Ratio[df_var$group == "High"]
  low_all  <- df_var$Ratio[df_var$group == "Low"]
  if (length(high_all) >= 2 & length(low_all) >= 2) {
    res_all <- group_test_strict(high_all, low_all, "All High", "All Low")
    res_all$continent <- "All"
    res_all$variable  <- var_label
    results_list[["All"]] <- res_all
  }
  
  summary_tbl <- map_dfr(results_list, function(x) {
    tibble(Variable     = x$variable,
           Continent    = x$continent,
           High_N       = x$group1_n,
           Low_N        = x$group2_n,
           High_Mean    = round(x$group1_mean, 3),
           Low_Mean     = round(x$group2_mean, 3),
           High_Median  = round(x$group1_median, 3),
           Low_Median   = round(x$group2_median, 3),
           High_SD      = round(x$group1_sd, 3),
           Low_SD       = round(x$group2_sd, 3),
           Test         = x$test_method,
           P_Raw        = x$p_value,
           Mean_Diff    = round(x$mean_diff, 3),
           Mean_Diff_CI = paste0("[", round(x$ci_diff_lower, 3), ", ",
                                 round(x$ci_diff_upper, 3), "]"),
           Effect_Size  = round(x$effect_size, 3),
           Effect_Name  = x$effect_name,
           Effect_CI    = paste0("[", round(x$ci_effect_lower, 3), ", ",
                                 round(x$ci_effect_upper, 3), "]"))
  })
  
  summary_tbl$P_Adj <- p.adjust(summary_tbl$P_Raw, method = "BH")
  summary_tbl$Significance <- case_when(
    summary_tbl$P_Adj < 0.001 ~ "***",
    summary_tbl$P_Adj < 0.01  ~ "**",
    summary_tbl$P_Adj < 0.05  ~ "*",
    summary_tbl$P_Adj < 0.1   ~ ".",
    TRUE ~ "ns"
  )
  summary_tbl
}

summary_ict  <- run_comparison(df_group, "ICT",  "ICT")
summary_ccpi <- run_comparison(df_group, "CCPI", "CCPI")

plot_boxplot <- function(df, var_name, var_label, fill_colors) {
  df_var <- assign_groups(df, var_name)
  ggplot(df_var, aes(x = continent, y = Ratio, fill = group)) +
    geom_vline(xintercept = seq(1.5, 2.5, by = 1),
               linetype = "dashed", color = "gray50",
               alpha = 0.7, size = 0.5) +
    geom_boxplot(alpha = 0.8,
                 position = position_dodge(width = 0.5),
                 outlier.alpha = 0.5, width = 0.3) +
    labs(x = "", y = "Reporting Ratio", fill = "", title = var_label) +
    scale_fill_manual(values = fill_colors) +
    theme_bw() +
    theme(legend.position = "top",
          panel.grid.major.x = element_blank())
}

print(plot_boxplot(df_group, "ICT", "ICT",
                   c("Low" = "#D45087", "High" = "#2FC1B7")))
print_group_stats(summary_ict, "ICT")

print(plot_boxplot(df_group, "CCPI", "CCPI",
                   c("Low" = "#5D3A9B", "High" = "#E7B800")))
print_group_stats(summary_ccpi, "CCPI")



# 分组比较箱线图
p_ict  <- plot_boxplot(df_group, "ICT",  "ICT",
                       c("Low" = "#D45087", "High" = "#2FC1B7"))
p_ccpi <- plot_boxplot(df_group, "CCPI", "CCPI",
                       c("Low" = "#5D3A9B", "High" = "#E7B800"))
print(p_ict)
print(p_ccpi)

# ============================================================================
# Language proximity regression
# ============================================================================
lpn_col <- names(imputed_data)[grepl("LPN2CommonLanguage", names(imputed_data))]
if (length(lpn_col) == 0) lpn_col <- names(imputed_data)[grepl("LPN", names(imputed_data))]
if (length(lpn_col) == 0) stop("No LPN column found.")
lpn_col <- lpn_col[1]

country_col <- names(imputed_data)[grepl("^country1$", names(imputed_data))]
iso_col     <- names(imputed_data)[grepl("Alpha_3_Co|alpha_3_co",
                                         names(imputed_data), ignore.case = TRUE)]
if (length(iso_col) == 0) stop("ISO3 column not found.")
iso_col <- iso_col[1]

Africa <- imputed_data %>%
  filter(continent == "Africa") %>%
  select(country1 = all_of(country_col),
         iso3     = all_of(iso_col),
         Ratio    = all_of(outcome_col),
         LPN      = all_of(lpn_col)) %>%
  filter(!is.na(Ratio), !is.na(LPN))

ENG_ISO <- c("ZAF","KEN","TZA","ZMB","ETH","UGA","ZWE","BWA","NAM",
             "MWI","RWA","LSO","NGA","GHA","SLE","LBR","GMB","SSD",
             "SOM","ERI")
FRA_ISO <- c("COD","TCD","NER","MLI","MDG","CAF","COG","CIV","BFA",
             "SEN","BEN","TGO","BDI","DJI","CMR")
ARB_ISO <- c("EGY","TUN","LBY","SDN","DZA","MRT","MAR")
POR_ISO <- c("AGO","MOZ","GNB","CPV","STP","GNQ")

Africa <- Africa %>%
  mutate(lan_group = case_when(
    iso3 %in% ENG_ISO ~ "English",
    iso3 %in% FRA_ISO ~ "French",
    iso3 %in% ARB_ISO ~ "Arabic",
    iso3 %in% POR_ISO ~ "Portuguese",
    TRUE ~ "Other"
  )) %>%
  mutate(lan_group = factor(lan_group,
                            levels = c("English","French","Portuguese","Arabic","Other")))

fit_frac_logit <- function(data, label) {
  m  <- glm(Ratio ~ LPN, data = data, family = quasibinomial(link = "logit"))
  m0 <- glm(Ratio ~ 1,   data = data, family = quasibinomial(link = "logit"))
  
  beta <- coef(m)["LPN"]
  se   <- sqrt(diag(vcov(m)))["LPN"]
  z    <- beta / se
  p    <- 2 * pnorm(-abs(z))
  ci   <- confint.default(m)["LPN", ]
  
  ll_binom <- function(model, y) {
    p_hat <- predict(model, type = "response")
    p_hat <- pmin(pmax(p_hat, 1e-10), 1 - 1e-10)
    sum(y * log(p_hat) + (1 - y) * log(1 - p_hat))
  }
  y       <- data$Ratio
  ll_full <- ll_binom(m,  y)
  ll_null <- ll_binom(m0, y)
  n       <- nobs(m)
  k       <- length(coef(m)) - 1
  mcfadden     <- 1 - ll_full / ll_null
  mcfadden_adj <- 1 - (ll_full - k) / ll_null
  
  p_hat        <- fitted(m)
  ame_per_unit <- mean(beta * p_hat * (1 - p_hat))
  lpn_sd       <- sd(data$LPN, na.rm = TRUE)
  ame_per_sd   <- ame_per_unit * lpn_sd
  
  list(label = label, model = m, n = n, beta = beta, se = se, z = z, p = p,
       ci_lower = ci[1], ci_upper = ci[2],
       lpn_sd = lpn_sd, ame_per_sd = ame_per_sd,
       mcfadden = mcfadden, mcfadden_adj = mcfadden_adj)
}

fit_africa <- fit_frac_logit(Africa, "Africa (overall)")
arabic_data <- Africa %>% filter(lan_group == "Arabic")
fit_arabic  <- fit_frac_logit(arabic_data, "Arabic subgroup")

print_language_stats <- function(fit, label) {
  cat("\n------------------------------------------------------------\n")
  cat("Statistics for", label, "\n")
  cat("------------------------------------------------------------\n")
  cat("Sample size (grid cells):", fit$n, "\n")
  cat("Coefficient (logit scale):", round(fit$beta, 4), "\n")
  cat("Standard error:", round(fit$se, 4), "\n")
  cat("z value:", round(fit$z, 4), "\n")
  cat("p value:", format.pval(fit$p, digits = 4), "\n")
  cat("95% CI (logit scale): [",
      round(fit$ci_lower, 4), ", ", round(fit$ci_upper, 4), "]\n", sep = "")
  cat("LPN SD:", round(fit$lpn_sd, 4), "\n")
  cat("Change in reporting ratio per 1-SD increase in LPN:",
      round(fit$ame_per_sd, 4), "\n")
  cat("McFadden pseudo R2:", round(fit$mcfadden, 4), "\n")
  cat("Adjusted McFadden pseudo R2:", round(fit$mcfadden_adj, 4), "\n")
  cat("------------------------------------------------------------\n\n")
}

# Africa overall plot
lpn_seq <- seq(min(Africa$LPN), max(Africa$LPN), length.out = 200)
pred_df <- data.frame(LPN = lpn_seq)
pred    <- predict(fit_africa$model, newdata = pred_df,
                   type = "response", se.fit = TRUE)
pred_df$fit   <- pred$fit
pred_df$lower <- pmax(pred$fit - 1.96 * pred$se.fit, 0)
pred_df$upper <- pmin(pred$fit + 1.96 * pred$se.fit, 1)

country_means <- Africa %>%
  group_by(country1, lan_group) %>%
  summarise(mean_ratio = mean(Ratio, na.rm = TRUE),
            LPN = mean(LPN, na.rm = TRUE), .groups = "drop")


p_africa <- ggplot(Africa, aes(x = LPN, y = Ratio)) +
    geom_point(aes(color = lan_group), alpha = 0.1, size = 2) +
    geom_point(data = country_means,
               aes(x = LPN, y = mean_ratio, color = lan_group),
               alpha = 1, size = 3) +
    geom_ribbon(data = pred_df, aes(x = LPN, ymin = lower, ymax = upper),
                inherit.aes = FALSE, fill = "grey35", alpha = 0.3) +
    geom_line(data = pred_df, aes(x = LPN, y = fit),
              inherit.aes = FALSE, color = "black", size = 0.8) +
    scale_color_brewer(palette = "Set1") +
    guides(color = guide_legend(title = "Language Group",
                                override.aes = list(alpha = 1, size = 2))) +
    labs(x = "Common Language Proximity Index", y = "") +
    theme_minimal() +
    theme(text = element_text(size = 28),
          axis.title.x = element_text(size = 24),
          axis.title.y = element_text(size = 24),
          axis.text.x  = element_text(size = 24),
          axis.text.y  = element_text(size = 24),
          legend.position = "bottom")+
  coord_cartesian(ylim = c(0.5, 0.75))

print(p_africa)

print_language_stats(fit_africa, "Africa (overall)")

# Arabic subgroup plot
lpn_seq_ar <- seq(min(arabic_data$LPN), max(arabic_data$LPN), length.out = 200)
pred_df_ar <- data.frame(LPN = lpn_seq_ar)
pred_ar    <- predict(fit_arabic$model, newdata = pred_df_ar,
                      type = "response", se.fit = TRUE)
pred_df_ar$fit   <- pred_ar$fit
pred_df_ar$lower <- pmax(pred_ar$fit - 1.96 * pred_ar$se.fit, 0)
pred_df_ar$upper <- pmin(pred_ar$fit + 1.96 * pred_ar$se.fit, 1)

country_means_ar <- arabic_data %>%
  group_by(country1, lan_group) %>%
  summarise(mean_ratio = mean(Ratio, na.rm = TRUE),
            LPN = mean(LPN, na.rm = TRUE), .groups = "drop")


p_arabic <- ggplot(arabic_data, aes(x = LPN, y = Ratio)) +
    geom_point(aes(color = lan_group), alpha = 0.1, size = 2) +
    geom_point(data = country_means_ar,
               aes(x = LPN, y = mean_ratio, color = lan_group),
               alpha = 1, size = 3) +
    geom_ribbon(data = pred_df_ar, aes(x = LPN, ymin = lower, ymax = upper),
                inherit.aes = FALSE, fill = "#984EA3", alpha = 0.2) +
    geom_line(data = pred_df_ar, aes(x = LPN, y = fit),
              inherit.aes = FALSE, color = "#984EA3", size = 1) +
    scale_color_manual(values = c("Arabic" = "#984EA3")) +
    guides(color = guide_legend(title = "Language Group",
                                override.aes = list(alpha = 1, size = 3))) +
    labs(x = "Common Language Proximity Index", y = "") +
    theme_minimal() +
    theme(text = element_text(size = 20),
          axis.title.x = element_text(size = 24),
          axis.title.y = element_text(size = 24),
          axis.text.x  = element_text(size = 24),
          axis.text.y  = element_text(size = 24),
          legend.position = "bottom")+
  coord_cartesian(ylim = c(0.5, 0.75))

print(p_arabic)

print_language_stats(fit_arabic, "Arabic subgroup")







# ============================================================================
# Supplementary: Geographic cross-validation (k-means on grid centroids, 5 folds)
#   Baselines: global mean, regional mean, geographic OLS, socioeconomic OLS.
# ============================================================================
ncc_raw <- read_excel(IN_PATH, guess_max = 10000)
df_cv <- ncc_raw %>%
  select(Ratio = all_of(outcome_col),
         continent, income_grp,
         all_of(feature_cols),
         lon, lat) %>%
  filter(!is.na(Ratio), !is.na(lon), !is.na(lat))

set.seed(SEED_MAIN)
df_cv$fold <- kmeans(scale(df_cv[, c("lon", "lat")]),
                     centers = 5, nstart = 25, iter.max = 100)$cluster

socio_vars <- feature_cols[
  grepl("gdp_percapita",         feature_cols, ignore.case = TRUE) |
    grepl("pop_density",           feature_cols, ignore.case = TRUE) |
    grepl("NDGAIN_VulSensitivity", feature_cols, ignore.case = TRUE) |
    grepl("NDGAIN_VulCapacity",    feature_cols, ignore.case = TRUE)
]

oof <- list(xgb       = rep(NA_real_, nrow(df_cv)),
            global    = rep(NA_real_, nrow(df_cv)),
            regional  = rep(NA_real_, nrow(df_cv)),
            geo_ols   = rep(NA_real_, nrow(df_cv)),
            socio_ols = rep(NA_real_, nrow(df_cv)))

fold_metrics <- data.frame(fold = integer(),
                           N_train = integer(),
                           N_test  = integer(),
                           xgb_R2 = numeric(),
                           xgb_RMSE = numeric(),
                           xgb_MAE = numeric())
for (f in 1:5) {
  tr_idx <- which(df_cv$fold != f)
  te_idx <- which(df_cv$fold == f)
  
  imp <- impute_fold(df_cv[tr_idx, ], df_cv[te_idx, ], cols_to_impute)
  tr  <- imp$train
  te  <- imp$test
  
  X_tr_full <- as.data.frame(tr[, feature_cols, drop = FALSE])
  y_tr_full <- tr$Ratio
  X_te      <- as.data.frame(te[, feature_cols, drop = FALSE])
  y_te      <- te$Ratio
  
  set.seed(SEED_MAIN)
  val_idx <- sample(1:nrow(X_tr_full), floor(0.1 * nrow(X_tr_full)))
  dtrain  <- xgb.DMatrix(as.matrix(X_tr_full[-val_idx, ]), label = y_tr_full[-val_idx])
  dval    <- xgb.DMatrix(as.matrix(X_tr_full[val_idx, ]),  label = y_tr_full[val_idx])
  dtest   <- xgb.DMatrix(as.matrix(X_te), label = y_te)
  
  set.seed(SEED_MAIN)
  model_f <- xgb.train(params = best_params_m1, data = dtrain,
                       nrounds = 1000, early_stopping_rounds = 50,
                       watchlist = list(train = dtrain, val = dval),
                       verbose = 0)
  
  preds_f <- predict(model_f, dtest,
                     iteration_range = c(0, model_f$best_iteration + 1))
  oof$xgb[te_idx] <- preds_f
  
  m <- eval_metrics(y_te, preds_f)
  fold_metrics <- rbind(fold_metrics,
                        data.frame(fold = f,
                                   N_train = nrow(tr),
                                   N_test  = nrow(te),
                                   xgb_R2   = m$R2,
                                   xgb_RMSE = m$RMSE,
                                   xgb_MAE  = m$MAE))
  
  global_mean <- mean(tr$Ratio, na.rm = TRUE)
  oof$global[te_idx] <- global_mean
  
  cont_means <- tr %>% group_by(continent) %>%
    summarise(cont_mean = mean(Ratio, na.rm = TRUE), .groups = "drop")
  oof$regional[te_idx] <- te %>%
    left_join(cont_means, by = "continent") %>%
    mutate(p = ifelse(is.na(cont_mean), global_mean, cont_mean)) %>%
    pull(p)
  
  oof$geo_ols[te_idx] <- predict(lm(Ratio ~ lon + lat, data = tr), newdata = te)
  
  if (length(socio_vars) >= 1) {
    socio_vars_quoted <- paste0("`", socio_vars, "`", collapse = " + ")
    fml <- as.formula(paste("Ratio ~", socio_vars_quoted))
    oof$socio_ols[te_idx] <- predict(lm(fml, data = tr), newdata = te)
  }
}

overall <- data.frame(
  Model = c("XGBoost (geographic CV)", "Global mean", "Regional mean",
            "Geographic OLS", "Socioeconomic OLS"),
  R2 = NA_real_, RMSE = NA_real_, MAE = NA_real_
)

for (i in seq_len(nrow(overall))) {
  preds <- oof[[c("xgb", "global", "regional", "geo_ols", "socio_ols")[i]]]
  ok    <- !is.na(preds) & !is.na(df_cv$Ratio)
  if (sum(ok) > 0) {
    m <- eval_metrics(df_cv$Ratio[ok], preds[ok])
    overall[i, c("R2", "RMSE", "MAE")] <- round(c(m$R2, m$RMSE, m$MAE), 4)
  }
}

cat("\n============================================================\n")
cat("OOF model comparison (geographic CV)\n")
cat("============================================================\n")
print(overall)

cat("\nXGBoost fold-to-fold variability:\n")
cat("R2   mean:", round(mean(fold_metrics$xgb_R2),   4),
    "| SD:", round(sd(fold_metrics$xgb_R2),   4), "\n")
cat("RMSE mean:", round(mean(fold_metrics$xgb_RMSE), 4),
    "| SD:", round(sd(fold_metrics$xgb_RMSE), 4), "\n")
cat("MAE  mean:", round(mean(fold_metrics$xgb_MAE),  4),
    "| SD:", round(sd(fold_metrics$xgb_MAE),  4), "\n")


