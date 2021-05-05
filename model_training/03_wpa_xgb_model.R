library(dplyr)
library(readr)
library(stringr)
library(tidyverse)
library(nnet)
library(mgcv)
library(xgboost)

#--- Load and Filter Data -----
pbp_full <- readRDS(file = "pbp_full.rds")

model_name <- "xgb_wp_spread_model"
figure_name <- "figures/xgb_wp_spread_cv_loso_calibration_results.png"
## Filter out OT games for now
# Create the EP model dataset that only includes plays with basic seven
# types of next scoring events along with the following play types:
# Field Goal, No Play, Pass, Punt, Run, Sack, Spike

# Remove  [8] "Extra Point Missed","Extra Point Good","Timeout","End of Half","End of Game","Uncategorized"
remove_plays <-
  c(
    "Extra Point Missed",
    "Extra Point Good",
    "Timeout",
    "End Period",
    "End of Half",
    "End of Game",
    "Defensive 2pt Conversion",
    "Two Point Rush",
    "Uncategorized",
    "Penalty (Kickoff)",
    "Kickoff",
    "Kickoff (Safety)",
    "Kickoff Return (Offense)",
    "Kickoff Return Touchdown",
    "Kickoff Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown"
  )


## need to remove games with 
pbp_full <- pbp_full %>% 
  filter(down > 0) %>%
  filter(!(play_type %in% remove_plays)) %>%
  filter(!is.na(down)) %>%
  mutate(
    Next_Score = case_when(
      NSH == 7 ~ "TD",
      NSH == 3 ~ "FG",
      NSH == 2 ~ "Safety",
      NSH == -2 ~ "Opp_Safety",
      NSH == -3  ~ "Opp_FG",
      NSH == -7 ~ "Opp_TD",
      TRUE ~ "No_Score"),
    label = case_when(
      Next_Score == "TD" ~ 0,
      Next_Score == "FG" ~ 2,
      Next_Score == "Safety" ~ 4,
      Next_Score == "Opp_Safety" ~ 5,
      Next_Score == "Opp_FG"  ~ 3,
      Next_Score == "Opp_TD" ~ 1,
      TRUE ~ 6),
    Next_Score = forcats::fct_relevel(factor(Next_Score), "No_Score"),
    label = as.factor(label),
    Goal_To_Go = ifelse(
      str_detect(play_type, "Field Goal"),
      ydstogo >= (yardline_100 - 17),
      ydstogo >= yardline_100
    ),
    Under_two = half_seconds_remaining <= 120,
    down_1 = ifelse(down == 1, 1, 0),
    down_2 = ifelse(down == 2, 1, 0),
    down_3 = ifelse(down == 3,1,0),
    down_4 = ifelse(down == 4, 1, 0)
    #id_play = as.numeric(id_play,digits=20)
  ) %>% filter(!is.na(game_id)) %>% 
  filter(!(game_id %in% c(400603838, 401020760,400933849,
                          400547737, 400547739, 401012806,
                          401021693, 400787470, 401112262, 
                          401114227, 401147693, 401015042,
                          400986609, 400763439))) %>% 
  filter(!is.na(posteam_score_differential)) 

model_data <- pbp_full %>% 
  select("label","season",
         "game_seconds_remaining",
         "half_seconds_remaining","yardline_100",
         "ydstogo","down_1","down_2","down_3","down_4",
         "posteam_score_differential","ScoreDiff_W")

# 
model_data <- model_data %>%
  mutate(
    label = as.numeric(.data$label),
    label = .data$label - 1
  )

seasons <- unique(model_data$season)

xgb_ep_model <- xgboost::xgb.load('models/xgb_ep_model.model')
preds <- as.data.frame(
  matrix(predict(xgb_ep_model, as.matrix(model_data %>% 
                                           select(-label,-season, -ScoreDiff_W))), 
         ncol = 7, byrow = TRUE)
)
colnames(preds) <- c(
  "Touchdown", "Opp_Touchdown", "Field_Goal", "Opp_Field_Goal",
  "Safety", "Opp_Safety", "No_Score"
)
weights <- c(0, 3, -3, -2, -7, 2, 7)
preds$ep <- apply(preds, 1, function(row) {
  sum(row * weights)
})
pbp_full <- bind_cols(pbp_full, preds) 
games_list <- read.csv("cfb_games_info_2002_2020.csv")
length(unique(games_list$game_id))
pbp_full <- pbp_full %>%
  dplyr::left_join(games_list %>% 
                     dplyr::select(.data$game_id,.data$home_points,.data$away_points), 
                   by="game_id") 
model_data <- pbp_full %>% 
  dplyr::mutate(
    ExpScoreDiff = .data$posteam_score_differential + .data$ep,
    ExpScoreDiff_Time_Ratio = .data$ExpScoreDiff/(.data$game_seconds_remaining + 1),
    winner = ifelse(home_points > away_points, home_team_name,
                    ifelse(home_points < away_points, away_team_name, "TIE"))
  ) %>% dplyr::filter(!is.na(game_id)) %>% 
  dplyr::filter(!(game_id %in% c(400603838, 401020760,400933849,
                                 400547737, 400547739, 401012806,
                                 401021693, 400787470, 401112262, 
                                 401114227, 401147693, 401015042,
                                 400986609, 400763439))) %>% 
  dplyr::filter(!is.na(posteam_score_differential)) %>% 
  dplyr::rename(spread_time = .data$start_spread_time) %>% 
  dplyr::mutate(label = ifelse(posteam_name == winner, 1, 0)) %>%
  dplyr::filter(!is.na(label)) %>% 
  dplyr::select("label","season","spread_time",
                "game_seconds_remaining", "half_seconds_remaining",
                "ExpScoreDiff_Time_Ratio","posteam_score_differential",
                "down","ydstogo","yardline_100",
                "posteam_timeouts","defteam_timeouts","ScoreDiff_W")




nrounds = 534
params <-
  list(
    booster = "gbtree",
    objective = "binary:logistic",
    eval_metric = c("logloss"),
    eta = 0.05,
    gamma = .79012017,
    subsample= 0.9224245,
    colsample_bytree= 5/12,
    max_depth = 5,
    min_child_weight = 7
  )


wp_spread_training <- function(model_data = model_data, seasons = seasons){
  p <- progressr::progressor(along = seasons)
  
  cv_results <- furrr::future_map_dfr(seasons, function(x) {
    test_data <- model_data %>%
      filter(season == x) %>%
      select(-season) %>% 
      as.data.frame()
    train_data <- model_data %>%
      filter(season != x) %>%
      select(-season) %>% 
      as.data.frame()
    
    full_train <- xgboost::xgb.DMatrix(model.matrix(~ . + 0, 
                                                    data = train_data %>% select(-label,-ScoreDiff_W)),
                                       label = train_data$label,
                                       weight=train_data$ScoreDiff_W,
    )
    xgb_wp_spread_model <- xgboost::xgboost(params = params, 
                                            data = full_train, 
                                            nrounds = nrounds, verbose = 0)
    xgboost::xgb.save(xgb_wp_spread_model, glue::glue("models/{model_name}.model"))
    preds <- as.data.frame(
      matrix(predict(xgb_wp_spread_model, as.matrix(test_data %>% select(-label, -ScoreDiff_W))), byrow = TRUE)
    )
    colnames(preds) <- "win_prob"
    cv_data <- bind_cols(test_data, preds) %>% mutate(season = x)
    p(sprintf("x=%s", as.integer(x)))
    return(cv_data)
  })
  return(cv_results)
}
options(future.globals.maxSize= 1250*1024^2)
future::plan("multisession")
progressr::with_progress({
  cv_results <- wp_spread_training(model_data=model_data,seasons=seasons)
})

set.seed(2013) #GoNoles
full_train <- xgboost::xgb.DMatrix(model.matrix(~ . + 0, 
                                                data = model_data %>% 
                                                  dplyr::select(-label,-season, -ScoreDiff_W)),
                                   label = model_data$label,
                                   weight = model_data$ScoreDiff_W
)
wp_model_spread <- xgboost::xgboost(params = params, 
                                    data = full_train, 
                                    nrounds = nrounds, 
                                    verbose = 2)

importance <- xgboost::xgb.importance(feature_names = colnames(full_train), model = wp_model_spread)
xgboost::xgb.ggplot.importance(importance_matrix = importance)

xgboost::xgb.save(wp_model_spread, glue::glue("models/{model_name}.model"))
wp_model_spread <- xgb.load("models/xgb_wp_spread_model.model")
pbp_full <- pbp_full %>% 
  dplyr::mutate(
    ExpScoreDiff = .data$posteam_score_differential + .data$ep,
    ExpScoreDiff_Time_Ratio = .data$ExpScoreDiff/(.data$game_seconds_remaining + 1),
    winner = ifelse(home_points > away_points, home_team_name,
                    ifelse(home_points < away_points, away_team_name, "TIE"))
  ) %>% dplyr::filter(!is.na(game_id)) %>% 
  dplyr::filter(!(game_id %in% c(400603838, 401020760,400933849,
                                 400547737, 400547739, 401012806,
                                 401021693, 400787470, 401112262, 
                                 401114227, 401147693, 401015042,
                                 400986609, 400763439))) %>% 
  dplyr::filter(!is.na(posteam_score_differential)) %>% 
  dplyr::rename(spread_time = .data$start_spread_time) %>% 
  dplyr::mutate(label = ifelse(posteam_name == winner, 1, 0)) %>%
  dplyr::filter(!is.na(label)) %>% 
  dplyr::bind_cols(cv_results %>% dplyr::select(win_prob))

wp_cv_loso_calibration_results <- pbp_full %>%
  # Create binned probability column:
  dplyr::mutate(bin_pred_prob = round(win_prob / 0.05) * .05) %>%
  # Group by both the qtr and bin_pred_prob:
  dplyr::group_by(qtr, bin_pred_prob) %>%
  # Calculate the calibration results:
  dplyr::summarize(n_plays = dplyr::n(), 
                   n_wins = length(which(label == 1)),
                   bin_actual_prob = n_wins / n_plays) %>%
  dplyr::ungroup() 


# Create a label data frame for the chart:
ann_text <- data.frame(x = c(.25, 0.75), y = c(0.75, 0.25), 
                       lab = c("More times\nthan expected", "Fewer times\nthan expected"),
                       qtr = factor("1st Quarter"))

# Calculate the calibration error values:  
wp_cv_cal_error <- wp_cv_loso_calibration_results %>% 
  dplyr::mutate(cal_diff = abs(bin_pred_prob - bin_actual_prob)) %>%
  dplyr::group_by(qtr) %>% 
  dplyr::summarize(
    weight_cal_error = weighted.mean(cal_diff, n_plays, na.rm = TRUE),
    n_plays = n(),
    n_wins = sum(n_wins, na.rm = TRUE))

# Overall weighted calibration error:
print("Weighted calibration error:")
with(wp_cv_cal_error, weighted.mean(weight_cal_error, n_wins))
# 0.008924582

cal_error <- round(with(wp_cv_cal_error, weighted.mean(weight_cal_error, n_wins)),5)
print(cal_error)

library(extrafont)
extrafont::loadfonts(device = "win", quiet = TRUE)
library(tidyverse)
library(ggplot2)
library(ggthemes)
library(ggrepel)
library(gganimate)
library(gifski)
library(gt)
library(webshot)


plot_caption <- glue::glue("Overall Weighted Calibration Error: {cal_error}")

# Create a label data frame for the chart:
ann_text <- data.frame(x = c(0.25, 0.75), 
                       y = c(0.75, 0.25), 
                       lab = c("More times\nthan expected", "Fewer times\nthan expected")
)
options(scipen = 9000)

library(magick)
#--- Thomas Mock a real one ----
add_logo <- function(plot_path, logo_path, logo_position, logo_scale = 10){
  
  # Requires magick R Package https://github.com/ropensci/magick
  
  # Useful error message for logo position
  if (!logo_position %in% c("top right", "top left", "bottom right", "bottom left")) {
    stop("Error Message: Uh oh! Logo Position not recognized\n  Try: logo_positon = 'top left', 'top right', 'bottom left', or 'bottom left'")
  }
  
  # read in raw images
  plot <- magick::image_read(plot_path)
  logo_raw <- magick::image_read(logo_path)
  
  # get dimensions of plot for scaling
  plot_height <- magick::image_info(plot)$height
  plot_width <- magick::image_info(plot)$width
  
  # default scale to 1/10th width of plot
  # Can change with logo_scale
  logo <- magick::image_scale(logo_raw, as.character(plot_width/logo_scale))
  
  # Get width of logo
  logo_width <- magick::image_info(logo)$width
  logo_height <- magick::image_info(logo)$height
  
  # Set position of logo
  # Position starts at 0,0 at top left
  # Using 0.01 for 1% - aesthetic padding
  
  if (logo_position == "top right") {
    x_pos = plot_width - logo_width - 0.05 * plot_width
    y_pos = 0.05 * plot_height
  } else if (logo_position == "top left") {
    x_pos = 0.05 * plot_width
    y_pos = 0.05 * plot_height
  } else if (logo_position == "bottom right") {
    x_pos = plot_width - logo_width - 0.12 * plot_width
    y_pos = plot_height - logo_height - 0.08 * plot_height
  } else if (logo_position == "bottom left") {
    x_pos = 0.1 * plot_width
    y_pos = plot_height - logo_height - .17 * plot_height
  }
  
  # Compose the actual overlay
  magick::image_composite(plot, logo, offset = paste0("+", x_pos, "+", y_pos))
  
}
# new: # 0.008924582, old: 0.02411699
plot_caption <- glue::glue("Overall Weighted Calibration Error: {cal_error}")
# Create the calibration chart:
wp_cv_loso_calibration_results %>%
  mutate(qtr = fct_recode(factor(qtr), 
                          "1st Quarter" = "1", 
                          "2nd Quarter" = "2",
                          "3rd Quarter" = "3", 
                          "4th Quarter" = "4")) %>% 
  ggplot() +
  geom_point(aes(x = bin_pred_prob, y = bin_actual_prob, size = n_plays))+
  geom_smooth(aes(x = bin_pred_prob, y = bin_actual_prob), method = "loess", size = 0.5) +
  geom_abline(slope = 1, intercept = 0, color = "black", lty = 0.5) +
  coord_equal() +   
  scale_x_continuous(limits = c(0,1)) + 
  scale_y_continuous(limits = c(0,1)) + 
  labs(title = "Calibration plots for Win Probability Model",
       subtitle = "Leave-One-Season-Out Cross-Validation, Spread-adjusted WP Model - cfbfastR",
       caption = plot_caption,
       size = "Number of plays",
       x = "Estimated Win Probability",
       y = "Observed Win Probability") + 
  geom_text(data = ann_text, aes(x = x, y = y, label = lab), size = 2.5) +
  theme(
    legend.title = element_text(size = 9, margin=margin(t=0.2,r=0,b=0.2,l=0.2,unit=c("mm")),  family = "Gill Sans MT",face = "bold"),
    legend.text = element_text(size = 9, margin=margin(t=0.2,r=0,b=0.2,l=0.2,unit=c("mm")), family = "Gill Sans MT",face = "bold"),
    legend.background = element_rect(fill = "grey95"),
    legend.key = element_rect(fill = "grey99"),
    legend.key.width = unit(1.5,"mm"),
    legend.key.size = unit(2.0,"mm"),
    legend.margin=margin(t = 0.4,b = 1.4,l=0.4,r=0.4,unit=c('mm')),
    legend.position = "bottom",
    legend.direction = "horizontal",
    legend.box.background = element_rect(colour = "#500f1b"),
    strip.background = element_rect(fill = "grey95"),
    strip.text = element_text(size=6,colour = "black", face = "bold", family = "Gill Sans MT"),
    axis.title.x = element_text(size = 8, margin=margin(0,0,0,0, unit=c("mm")),  family = "Gill Sans MT",face = "bold"),
    axis.text.x = element_text(size = 8, margin=margin(1.4,1.4,-0.1,0, unit=c("mm")),  family = "Gill Sans MT"),
    axis.title.y = element_text(size = 8, margin=margin(0,0,0,0, unit=c("mm")),  family = "Gill Sans MT",face = "bold"),
    axis.text.y = element_text(size = 8, margin=margin(0,0.1,0,0, unit=c("mm")),   family = "Gill Sans MT"),
    plot.title = element_text(size = 10, margin=margin(t=0, r=0, b=0.2,l=0, unit=c("mm")), 
                              lineheight=0.7, family = "Gill Sans MT", face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 8, margin=margin(t=0, r=0, b=0.2,l=0, unit=c("mm")), 
                                 lineheight=0.7, family = "Gill Sans MT", hjust = 0.5),
    plot.caption = element_text(size = 8, margin=margin(t=1, r=0, b=1,l=0, unit=c("mm")), 
                                lineheight=0.7, family = "Gill Sans MT", face = "bold"),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    panel.background = element_rect(fill = "grey95", color="black"),
    plot.background = element_rect(fill = "grey99", color="black"),
    plot.margin=unit(c(0.3,0.3,0.3,0.3),"cm"))+
  facet_wrap(~qtr, ncol = 4)+ 
  scale_size(range=c(0,3),breaks=c(5000,10000,20000,300000),labels=c(5000,10000,20000,300000),guide="legend")+
  ggsave("figures/wp_spread_cv_loso_calibration_results.png", height = 101.6, width = 152.4,units=c('mm'),type="cairo")


WPA_Big_Plot <- "figures/wp_spread_cv_loso_calibration_results.png"
WPA_Big_Plot_logo <- add_logo(
  plot_path = WPA_Big_Plot,
  logo_path = "logo.png",
  logo_position = "bottom left",
  logo_scale = 11
)
magick::image_write(WPA_Big_Plot_logo, "figures/wp_spread_cv_loso_calibration_results.png")

nrounds <- 525
params <-
  list(
    booster = "gbtree",
    objective = "binary:logistic",
    eval_metric = c("logloss"),
    eta = 0.025,
    gamma = 0.8,
    subsample = 0.8,
    colsample_bytree = 0.8,
    max_depth = 5,
    min_child_weight = 4
  )



wp_naive_training <- function(model_data = model_data, seasons = seasons){
  p <- progressr::progressor(along = seasons)
  
  cv_naive_results <- furrr::future_map_dfr(seasons, function(x) {
    test_data <- model_data %>%
      dplyr::filter(season == x) %>%
      dplyr::select(-season,-spread_time) %>% 
      as.data.frame()
    train_data <- model_data %>%
      dplyr::filter(season != x) %>%
      dplyr::select(-season,-spread_time) %>% 
      as.data.frame()
    
    full_train_naive <- xgboost::xgb.DMatrix(model.matrix(~ . + 0, 
                                                          data = train_data %>% 
                                                            dplyr::select(-label, -ScoreDiff_W)),
                                             label = train_data$label,
                                             weight = train_data$ScoreDiff_W
    )
    xgb_wp_naive_model <- xgboost::xgboost(params = params, 
                                           data = full_train_naive, 
                                           nrounds = nrounds, 
                                           verbose = 0)
    xgboost::xgb.save(xgb_wp_naive_model, glue::glue("models/xgb_wp_naive_model.model"))
    preds <- as.data.frame(
      matrix(predict(xgb_wp_naive_model, as.matrix(test_data %>% 
                                                     dplyr::select(-label, -ScoreDiff_W))), byrow = TRUE)
    )
    colnames(preds) <- "win_prob_naive"
    cv_data <- dplyr::bind_cols(test_data, preds) %>% 
      dplyr::mutate(season = x)
    p(sprintf("x=%s", as.integer(x)))
    return(cv_data)
  })  
  return(cv_naive_results)
}

options(future.globals.maxSize= 1250*1024^2)
future::plan("multisession")
progressr::with_progress({
  cv_naive_results <- wp_naive_training(model_data = model_data, seasons = seasons)
})

set.seed(2013) #GoNoles
full_train_naive <- xgboost::xgb.DMatrix(model.matrix(~ . + 0, 
                                                      data = model_data %>% 
                                                        dplyr::select(-label,-season,-spread_time,-ScoreDiff_W)),
                                         label = model_data$label,
                                         weight = model_data$ScoreDiff_W
)
wp_model_naive <- xgboost::xgboost(params = params, 
                                   data = full_train_naive, 
                                   nrounds = nrounds, 
                                   verbose = 2)

importance_naive <- xgboost::xgb.importance(feature_names = colnames(full_train_naive), 
                                            model = wp_model_naive)
xgboost::xgb.ggplot.importance(importance_matrix = importance_naive)

xgboost::xgb.save(wp_model_naive, glue::glue("models/xgb_wp_naive_model.model"))
wp_model_naive <- xgb.load("models/xgb_wp_naive_model.model")

pbp_full <- pbp_full %>% 
  dplyr::bind_cols(cv_naive_results %>% 
                     dplyr::select(win_prob_naive))

wp_cv_naive_loso_calibration_results <- pbp_full %>%
  # Create binned probability column:
  dplyr::mutate(bin_pred_prob = round(win_prob_naive / 0.05) * .05) %>%
  # Group by both the qtr and bin_pred_prob:
  dplyr::group_by(qtr, bin_pred_prob) %>%
  # Calculate the calibration results:
  dplyr::summarize(n_plays = dplyr::n(), 
                   n_wins = length(which(label == 1)),
                   bin_actual_prob = n_wins / n_plays) %>%
  dplyr::ungroup() 


# Create a label data frame for the chart:
ann_text <- data.frame(x = c(.25, 0.75), y = c(0.75, 0.25), 
                       lab = c("More times\nthan expected", "Fewer times\nthan expected"),
                       qtr = factor("1st Quarter"))

# Calculate the calibration error values:  
wp_cv_naive_cal_error <- wp_cv_naive_loso_calibration_results %>% 
  dplyr::mutate(cal_diff = abs(bin_pred_prob - bin_actual_prob)) %>%
  dplyr::group_by(qtr) %>% 
  dplyr::summarize(
    weight_cal_error = weighted.mean(cal_diff, n_plays, na.rm = TRUE),
    n_plays = dplyr::n(),
    n_wins = sum(n_wins, na.rm = TRUE))

# Overall weighted calibration error:
print("Weighted calibration error:")
with(wp_cv_naive_cal_error, weighted.mean(weight_cal_error, n_wins))
# 0.008924582

cv_naive_cal_error <- round(with(wp_cv_naive_cal_error, weighted.mean(weight_cal_error, n_wins)),5)
print(cv_naive_cal_error)

library(extrafont)
extrafont::loadfonts(device = "win", quiet = TRUE)
library(tidyverse)
library(ggplot2)
library(ggthemes)
library(ggrepel)
library(gganimate)
library(gifski)
library(gt)
library(webshot)


plot_caption <- glue::glue("Overall Weighted Calibration Error: {cv_naive_cal_error}")

# Create a label data frame for the chart:
ann_text <- data.frame(x = c(0.25, 0.75), 
                       y = c(0.75, 0.25), 
                       lab = c("More times\nthan expected", "Fewer times\nthan expected")
)
options(scipen = 9000)

# new: # 0.008924582, old: 0.02411699
plot_caption <- glue::glue("Overall Weighted Calibration Error: {cv_naive_cal_error}")
# Create the calibration chart:
wp_cv_naive_loso_calibration_results %>%
  dplyr::mutate(qtr = fct_recode(factor(qtr), 
                                 "1st Quarter" = "1", 
                                 "2nd Quarter" = "2",
                                 "3rd Quarter" = "3", 
                                 "4th Quarter" = "4")) %>% 
  ggplot() +
  geom_point(aes(x = bin_pred_prob, y = bin_actual_prob, size = n_plays))+
  geom_smooth(aes(x = bin_pred_prob, y = bin_actual_prob), method = "loess", size = 0.5) +
  geom_abline(slope = 1, intercept = 0, color = "black", lty = 0.5) +
  coord_equal() +   
  scale_x_continuous(limits = c(0,1)) + 
  scale_y_continuous(limits = c(0,1)) + 
  labs(title = "Calibration plots for Win Probability Model",
       subtitle = "Leave-One-Season-Out Cross-Validation, Naive WP Model - cfbfastR",
       caption = plot_caption,
       size = "Number of plays",
       x = "Estimated Win Probability",
       y = "Observed Win Probability") + 
  geom_text(data = ann_text, aes(x = x, y = y, label = lab), size = 2.5) +
  theme(
    legend.title = element_text(size = 9, margin=margin(t=0.2,r=0,b=0.2,l=0.2,unit=c("mm")),  family = "Gill Sans MT",face = "bold"),
    legend.text = element_text(size = 9, margin=margin(t=0.2,r=0,b=0.2,l=0.2,unit=c("mm")), family = "Gill Sans MT",face = "bold"),
    legend.background = element_rect(fill = "grey95"),
    legend.key = element_rect(fill = "grey99"),
    legend.key.width = unit(1.5,"mm"),
    legend.key.size = unit(2.0,"mm"),
    legend.margin=margin(t = 0.4,b = 1.4,l=0.4,r=0.4,unit=c('mm')),
    legend.position = "bottom",
    legend.direction = "horizontal",
    legend.box.background = element_rect(colour = "#500f1b"),
    strip.background = element_rect(fill = "grey95"),
    strip.text = element_text(size=6,colour = "black", face = "bold", family = "Gill Sans MT"),
    axis.title.x = element_text(size = 8, margin=margin(0,0,0,0, unit=c("mm")),  family = "Gill Sans MT",face = "bold"),
    axis.text.x = element_text(size = 8, margin=margin(1.4,1.4,-0.1,0, unit=c("mm")),  family = "Gill Sans MT"),
    axis.title.y = element_text(size = 8, margin=margin(0,0,0,0, unit=c("mm")),  family = "Gill Sans MT",face = "bold"),
    axis.text.y = element_text(size = 8, margin=margin(0,0.1,0,0, unit=c("mm")),   family = "Gill Sans MT"),
    plot.title = element_text(size = 10, margin=margin(t=0, r=0, b=0.2,l=0, unit=c("mm")), 
                              lineheight=0.7, family = "Gill Sans MT", face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 8, margin=margin(t=0, r=0, b=0.2,l=0, unit=c("mm")), 
                                 lineheight=0.7, family = "Gill Sans MT", hjust = 0.5),
    plot.caption = element_text(size = 8, margin=margin(t=1, r=0, b=1,l=0, unit=c("mm")), 
                                lineheight=0.7, family = "Gill Sans MT", face = "bold"),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    panel.background = element_rect(fill = "grey95", color="black"),
    plot.background = element_rect(fill = "grey99", color="black"),
    plot.margin=unit(c(0.3,0.3,0.3,0.3),"cm"))+
  facet_wrap(~qtr, ncol = 4)+ 
  scale_size(range=c(0,3),breaks=c(5000,10000,20000,300000),labels=c(5000,10000,20000,300000),guide="legend")+
  ggsave("figures/wp_naive_cv_loso_calibration_results.png", height = 101.6, width = 152.4,units=c('mm'),type="cairo")


WPA_Big_Plot <- "figures/wp_naive_cv_loso_calibration_results.png"
WPA_Big_Plot_logo <- add_logo(
  plot_path = WPA_Big_Plot,
  logo_path = "logo.png",
  logo_position = "bottom left",
  logo_scale = 11
)
magick::image_write(WPA_Big_Plot_logo, "figures/wp_naive_cv_loso_calibration_results.png")

