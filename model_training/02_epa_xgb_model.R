library(dplyr)
library(readr)
library(stringr)
library(tidyverse)
library(nnet)
library(mgcv)
library(xgboost)

#--- Load and Filter Data -----
pbp_full <- readRDS(file = "pbp_full.rds")

model_name <- "xgb_ep_model"
figure_name <- "figures/xgb_ep_cv_loso_calibration_results.png"
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
model_data <- pbp_full %>% 
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
    down_3 = ifelse(down == 3, 1,0),
    down_4 = ifelse(down == 4, 1, 0)
    #id_play = as.numeric(id_play,digits=20)
  ) %>% filter(!is.na(game_id)) %>% 
  filter(!(game_id %in% c(400603838, 401020760,400933849,
                          400547737, 400547739, 401012806,
                          401021693, 400787470, 401112262, 
                          401114227, 401147693, 401015042,
                          400986609, 400763439))) %>% 
  filter(!is.na(posteam_score_differential),
         !is.na(game_seconds_remaining),
         !is.na(half_seconds_remaining),
         !is.na(yardline_100),
         !is.na(ydstogo)) %>% 
  select("label","season",
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

nrounds <- 525
params <-
  list(
    booster = "gbtree",
    objective = "multi:softprob",
    eval_metric = c("mlogloss"),
    nthread = 4,
    num_class = 7,
    eta = 0.025,
    gamma = 1,
    subsample = 0.8,
    colsample_bytree = 0.8,
    max_depth = 5,
    min_child_weight = 1
  )

ep_training <- function(model_data = model_data, seasons = seasons){
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
    
    full_train <- xgboost::xgb.DMatrix(model.matrix(~ . + 0, data = train_data %>% select(-label, -ScoreDiff_W)),
                                       label = train_data$label, weight = train_data$ScoreDiff_W
    )
    xgb_ep_model <- xgboost::xgboost(params = params, data = full_train, nrounds = nrounds, verbose = 0)
    xgboost::xgb.save(xgb_ep_model, glue::glue("models/{model_name}.model"))
    
    preds <- as.data.frame(
      matrix(predict(xgb_ep_model, as.matrix(test_data %>% select(-label, -ScoreDiff_W))), ncol = 7, byrow = TRUE)
    )
    colnames(preds) <- c(
      "Touchdown", "Opp_Touchdown", "Field_Goal", "Opp_Field_Goal",
      "Safety", "Opp_Safety", "No_Score"
    )
    
    cv_data <- bind_cols(test_data, preds) %>% mutate(season = x)
    p(sprintf("x=%s", as.integer(x)))
    return(cv_data)
  })
  return(cv_results)
}

options(future.globals.maxSize= 1250*1024^2)
future::plan("multisession")
progressr::with_progress({
  cv_results <- ep_training(model_data = model_data, 
                            seasons = seasons)
})
write.csv(cv_results,file=gzfile(glue::glue("pbp_model_preds/csv/play_by_play_ep.csv.gz")),row.names = FALSE)
saveRDS(cv_results,glue::glue("pbp_model_preds/rds/play_by_play_{y}_ep.rds"))
arrow::write_parquet(cv_results, glue::glue("pbp_model_preds/parquet/play_by_play_ep.parquet"))
cv_results <- arrow::read_parquet(glue::glue("pbp_model_preds/parquet/play_by_play_ep.parquet"))

# get the BINS for the calibration plot
plot <- cv_results %>%
  select(Touchdown, Opp_Touchdown, Field_Goal, Opp_Field_Goal, Safety, Opp_Safety, No_Score, label) %>%
  pivot_longer(-label, names_to = "type", values_to = "pred_prob") %>%
  mutate(bin_pred_prob = round(pred_prob / 0.05) * .05) %>%
  mutate(outcome = case_when(
    label == 0 ~ "Touchdown",
    label == 1 ~ "Opp_Touchdown",
    label == 2 ~ "Field_Goal",
    label == 3 ~ "Opp_Field_Goal",
    label == 4 ~ "Safety",
    label == 5 ~ "Opp_Safety",
    label == 6 ~ "No_Score"
  )) %>%
  group_by(type, bin_pred_prob) %>%
  mutate(correct = if_else(outcome == type, 1, 0)) %>%
  summarize(
    n_plays = n(),
    n_outcome = sum(correct),
    bin_actual_prob = n_outcome / n_plays
  )

ann_text <- data.frame(
  x = c(.25, 0.75), y = c(0.75, 0.25),
  lab = c("More times\nthan expected", "Fewer times\nthan expected"),
  next_score_type = factor("No Score (0)")
)


# calibration error
cv_cal_error <- plot %>%
  ungroup() %>%
  mutate(cal_diff = abs(bin_pred_prob - bin_actual_prob)) %>%
  group_by(type) %>%
  summarize(
    weight_cal_error = weighted.mean(cal_diff, n_plays, na.rm = TRUE),
    n_scoring_event = sum(n_outcome, na.rm = TRUE)
  )

round(with(cv_cal_error, weighted.mean(weight_cal_error, n_scoring_event)), 4)

# Overall weighted calibration error: -----
print("Weighted calibration error:")

cal_error <- sprintf('%.5f', round(with(cv_cal_error, weighted.mean(weight_cal_error, n_scoring_event)),7))
print(cal_error)
# new: 0.00949, old: 0.01307456

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
    x_pos = 0.05 * plot_width
    y_pos = plot_height - logo_height - 0.05 * plot_height
  }
  
  # Compose the actual overlay
  magick::image_composite(plot, logo, offset = paste0("+", x_pos, "+", y_pos))
  
}

# # Create the calibration chart:
plot %>% 
  ungroup() %>% 
  mutate(
    type = fct_relevel(
      type,
      "Opp_Safety", "Opp_Field_Goal",
      "Opp_Touchdown", "No_Score", "Safety",
      "Field_Goal", "Touchdown"
    ),
    type = fct_recode(type,
                      "-Field Goal (-3)" = "Opp_Field_Goal",
                      "-Safety (-2)" = "Opp_Safety",
                      "-Touchdown (-7)" = "Opp_Touchdown",
                      "Field Goal (3)" = "Field_Goal",
                      "No Score (0)" = "No_Score",
                      "Touchdown (7)" = "Touchdown",
                      "Safety (2)" = "Safety"
    )
  ) %>%
  ggplot() +
  geom_point(aes(x = bin_pred_prob, y = bin_actual_prob, size = n_plays))+
  geom_smooth(aes(x = bin_pred_prob, y = bin_actual_prob), method = "loess") +
  geom_abline(slope = 1, intercept = 0, color = "black", lty = 2) +
  coord_equal() +   geom_text(data = ann_text,aes(x = x, y = y, label = lab)) +
  scale_x_continuous(limits = c(0,1)) + 
  scale_y_continuous(limits = c(0,1)) + 
  labs(title = "Calibration plots for Expected Points Model by Scoring Event",
       subtitle = "Leave-One-Season-Out Cross-Validation, EP Model - cfbfastR",
       caption = plot_caption,
       size = "Number of plays",
       x = "Estimated Next Score Probability",
       y = "Observed Next Score Probability") + 
  theme(
    legend.title = element_text(size = 11, margin=margin(t=0.2,r=0,b=0.2,l=0.2,unit=c("mm")), family = "Gill Sans MT", face = "bold"),
    legend.text = element_text(size = 11, margin=margin(t=0.2,r=0,b=0.2,l=0.2,unit=c("mm")), family = "Gill Sans MT"),
    legend.background = element_rect(fill = "grey99"),
    legend.key = element_rect(fill = "grey99"),
    legend.key.width = unit(2.5,"mm"),
    legend.key.size = unit(3.0,"mm"),
    legend.margin=margin(t = 0.4,b = 0.4,l=0.4,r=0.4,unit=c('mm')),
    legend.position = c(0.895, 0.33),
    legend.direction = "vertical",
    legend.box.background = element_rect(colour = "#500f1b"),
    axis.title.x = element_text(size = 14, margin=margin(0,0,0,0, unit=c("mm")), family = "Gill Sans MT", face = "bold"),
    axis.text.x = element_text(size = 13, margin=margin(0,0,-0.1,0, unit=c("mm")), family = "Gill Sans MT"),
    axis.title.y = element_text(size = 14, margin=margin(0,0,0,0, unit=c("mm")), family = "Gill Sans MT", face = "bold"),
    axis.text.y = element_text(size = 13, margin=margin(0,0.1,0,0, unit=c("mm")), family = "Gill Sans MT"),
    plot.title = element_text(size = 16, margin=margin(t=0, r=0, b=0.2,l=0, unit=c("mm")), 
                              lineheight=0.7, family = "Gill Sans MT", face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 16, margin=margin(t=0, r=0, b=0.2,l=0, unit=c("mm")), 
                                 lineheight=0.7, family = "Gill Sans MT", hjust = 0.5),
    plot.caption = element_text(size = 16, margin=margin(t=3, r=0, b=0,l=0, unit=c("mm")), 
                                lineheight=0.7, family = "Gill Sans MT", face = "bold"),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    panel.background = element_rect(fill = "grey95", color="black"),
    plot.background = element_rect(fill = "grey99", color="black"),
    plot.margin=unit(c(1.5,0.3,0.3,0.3),"cm"),
    strip.background = element_rect(fill = "grey95"),
    strip.text = element_text(size=12, margin=margin(t=0, r=0, b=0.2,l=0,unit=c("mm")),
                              lineheight=0.7, family = "Gill Sans MT", face = "bold"))+      
  facet_wrap(~ type, ncol = 4)+  
  ggsave(figure_name, height = 9/1.2, width = 16/1.2)


EPA_Big_Plot <- figure_name
EPA_Big_Plot_logo <- add_logo(
  plot_path = EPA_Big_Plot,
  logo_path = "logo.png",
  logo_position = "bottom right",
  logo_scale = 11
)
magick::image_write(EPA_Big_Plot_logo, figure_name)
