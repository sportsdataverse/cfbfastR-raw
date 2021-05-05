
library(tidyverse)
library(dplyr)
library(stringr)
library(arrow)
source('model_training/06_data_ingest_utils.R')

years_vec <- 2002:2020

# --- read from play_by_play_{year}.parquet ---------
# future::plan("multisession")
progressr::with_progress({
  p <- progressr::progressor(along = years_vec)
  pbp_games <- purrr::map_dfr(years_vec, function(y){
    pbp_g <- arrow::read_parquet(glue::glue("pbp_train/parquet/play_by_play_{y}.parquet"))
    p(sprintf("y=%s", as.integer(y)))
    return(pbp_g)
  })
})


length(unique(pbp_games$game_id))
# games_list <- read.csv("cfb_games_info_2002_2020.csv")
# length(unique(games_list$game_id))
length(unique(pbp_games$game_id))


if(all(stringr::str_detect(colnames(pbp_games),"week")==FALSE)==TRUE){
  print("adding weeks")
  pbp_games <- pbp_games %>%
    dplyr::left_join(games_list %>% 
                       dplyr::select(.data$game_id,.data$week), by="game_id")
}

no_dresult <- pbp_games %>% 
  dplyr::filter(is.na(drive_result_detailed)) 


# --- select columns for pbp final ---------
pbp <- pbp_games %>% 
  dplyr::select("season","week","game_id","drive_id", "play_id",
                "home_team_name","home_score","away_team_name","away_score",
                "game_play_number", "qtr","clock_display_value","text", "type_text",
                "stat_yardage",
                "start_posteam_id","start_posteam_name",
                
                "start_defteam_id","start_defteam_name",
                "start_posteam_type",
                "start_posteam_receives_2h_kickoff","start_posteam_score_differential",
                "start_down","start_distance","start_ydstogo","start_yardline_100",
                "start_game_seconds_remaining",
                "start_half_seconds_remaining",
                "start_pos_team_timeouts", "start_def_pos_team_timeouts",
                "penalty_flag","penalty_1st_conv",
                "end_posteam_name","end_defteam_name","change_of_poss","change_of_posteam",
                "yds_penalty", "drive_display_result",
                "drive_is_score","drive_offensive_plays",
                "drive_result_detailed","drive_points", 
                "down_1","down_2", "down_3","down_4",
                "kickoff_play","kick_play","start_spread_time","game_spread",
                "home_team_spread","clock_minutes","clock_seconds",
                "offense_score_play","defense_score_play", "scoring_play")
colnames(pbp)[16:30] <- gsub("start_", "", colnames(pbp)[16:30])

pbp_games_own_KO_recovery <- pbp_games %>% 
  dplyr::filter(
    (.data$type_text == 'Kickoff Team Fumble Recovery')|
      (.data$type_text == 'Kickoff Team Fumble Recovery Touchdown')
  )

pbp_games = pbp_games %>%
  dplyr::rename(
    posteam_id = start_posteam_id,
    posteam_name = start_posteam_name,
    defteam_id = start_defteam_id,
    defteam_name = start_defteam_name,
    posteam_receives_2h_kickoff = start_posteam_receives_2h_kickoff,
    home = start_posteam_type,
    down = start_down,
    distance = start_distance,
    ydstogo = start_ydstogo,
    yardline_100 = start_yardline_100,
    game_seconds_remaining = start_game_seconds_remaining,
    half_seconds_remaining = start_half_seconds_remaining,
    posteam_timeouts = start_pos_team_timeouts, 
    defteam_timeouts = start_def_pos_team_timeouts) 

clean_all_years = pbp_games %>% 
  dplyr::mutate(
    # make sure all kick off downs are -1
    down = ifelse(down == 5 &
                    stringr::str_detect(type_text, "Kickoff"),-1, down)
  ) %>% dplyr::filter(down < 5)

## Figure out the next score now
## clean_drive <- cfbscrapR::clean_drive_info(clean_all_years)

## Remove OT games
OT_games = clean_all_years %>% 
  dplyr::group_by(game_id) %>%
  dplyr::summarize(max_per = max(qtr)) 
zero_games = clean_all_years %>% 
  dplyr::group_by(game_id) %>%
  dplyr::summarize(min_per = min(qtr)) 
zero_games_lst = zero_games %>% 
  dplyr::filter(min_per < 1) %>% 
  dplyr::pull(game_id)
OT_games_lst = OT_games %>% 
  dplyr::filter(max_per > 4) %>% 
  dplyr::pull(game_id)

clean_all_years_OT_game_info <- clean_all_years %>% 
  dplyr::filter(game_id %in% OT_games_lst) %>% 
  dplyr::select(game_id, season, week, home_team_name, away_team_name)
df_OT_game_info <- as.data.frame(dplyr::distinct(clean_all_years_OT_game_info))
write.csv(df_OT_game_info,"OT_game_ids.csv", row.names = FALSE)

clean_all_years_OT <- clean_all_years %>% 
  dplyr::filter(game_id %in% OT_games_lst) 

# write.csv(clean_all_years_OT, "data-raw/raw_data/clean_all_years_OT_2014_2019.csv", row.names = FALSE)
# write.csv(clean_all_years_OT %>% filter(period>=5),"data-raw/raw_data/OT_only_2014_2019.csv", row.names = FALSE)

clean_all_years_no_OT = clean_all_years %>%  
  dplyr::filter(!(game_id %in% OT_games_lst))

## ESPN doesn't report full games in some instances, and that really throws things off.
## get rid of these. Thanks
check_for_full_game = clean_all_years_no_OT %>%  
  dplyr::filter(qtr == 4) %>% 
  dplyr::group_by(game_id, clock_minutes) %>%
  dplyr::summarize(val = n()) %>% 
  dplyr::filter(clock_minutes == min(clock_minutes))

keep_full_games = check_for_full_game %>% 
  dplyr::filter(clock_minutes == 0) %>% 
  dplyr::pull(game_id)

clean_all_years_no_OT <- clean_all_years_no_OT %>% 
  dplyr::filter(game_id %in% keep_full_games)

nas_drive_result_new <- clean_all_years_no_OT %>% 
  dplyr::filter(is.na(drive_result_detailed), qtr <= 4) 

all_years_na_games <- clean_all_years_no_OT %>% 
  dplyr::filter(game_id %in% unique(nas_drive_result_new$game_id))

df_play_types <- data.frame(play_types = unique(pbp$type_text))
all_play_types <- data.frame(play_types = unique(pbp$type_text))
all_drive_types <- data.frame(new_drive_result = unique(pbp$drive_result_detailed))

remove_plays <-
  c(
    "Extra Point Missed",
    "Extra Point Good",
    "Timeout",
    "Kickoff",
    "Penalty (Kickoff)",
    "Kickoff Return (Offense)",
    "Kickoff Return Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Kickoff Touchdown"
  )
clean_removed <- clean_all_years %>% 
  dplyr::filter(!(type_text %in% remove_plays),
                !(game_id %in% OT_games_lst),
                !(game_id %in% zero_games_lst)) %>% 
  dplyr::mutate(
    drive_id = as.numeric(drive_id),
    drive_result_detailed = ifelse(is.na(drive_result_detailed), 
                                   drive_display_result, 
                                   drive_result_detailed)
  )
clean_removed %>% dplyr::filter(is.na(drive_is_score))
clean_removed %>% dplyr::filter(qtr>4)
clean_removed %>% dplyr::filter(qtr<1)
## get the next score half
## with the drive_details
source('model_training/06_data_ingest_utils.R')
clean_removed$drive_id <- as.numeric(clean_removed$drive_id)
# drive_df = clean_removed %>% arrange(drive_id)
# score_plays <- which(drive_df$scoring_play == TRUE & 
#                        !str_detect(drive_df$drive_result_detailed, regex("END HALF|END GAME|Uncategorized", ignore_case = TRUE)))

# read from play_by_play{year}.parquet
games <- unique(clean_removed$game_id)
clean_removed <- clean_removed %>% dplyr::rename(play_type = .data$type_text)
clean_next_score_drive <- purrr::map_dfr(games,
                                         function(x) {
                                           clean_removed %>%
                                             dplyr::filter(game_id == x) %>%
                                             find_game_next_score_half()
                                         })

# drive, and the next drives score details
# join this back to the pbp
clean_next_score_drive  <- clean_next_score_drive %>% 
  dplyr::mutate(
    Next_Score = dplyr::case_when(
      NSH ==  7 ~ "TD",
      NSH ==  3 ~ "FG",
      NSH == -2 ~ "Safety",
      NSH ==  2 ~ "Opp_Safety",
      NSH == -3 ~ "Opp_FG",
      NSH == -7 ~ "Opp_TD",
      TRUE ~      "No_Score"
    )
  )
 
pbp_full <- clean_next_score_drive 
clean_drive_results <- data.frame(new_drive_result = unique(clean_next_score_drive$new_drive_result ))

pbp_full <- pbp_full %>% 
  dplyr::filter(game_id != 400603838) %>%
  dplyr::group_by(game_id) %>% 
  dplyr::mutate(
    # calculate absolute score difference
    abs_diff = abs(posteam_score_differential),
    # Calculate the drive difference between the next score drive and the
    # current play drive:
    Drive_Score_Dist = DSH - as.numeric(drive_id)) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(
    # Create a weight column based on difference in drives between play
    # and next score:
    Drive_Score_Dist_W = (max(Drive_Score_Dist, na.rm = TRUE) - Drive_Score_Dist) /
      (max(Drive_Score_Dist, na.rm = TRUE) - min(Drive_Score_Dist, na.rm = TRUE)),
    # Create a weight column based on score differential:
    ScoreDiff_W = (max(abs_diff, na.rm = TRUE) - abs_diff) /
      (max(abs_diff, na.rm = TRUE) - min(abs_diff, na.rm = TRUE)),
    # Add these weights together and scale again:
    Total_W = Drive_Score_Dist_W + ScoreDiff_W,
    Total_W_Scaled = (Total_W - min(Total_W, na.rm = TRUE)) /
      (max(Total_W, na.rm = TRUE) - min(Total_W, na.rm = TRUE))
  )



pbp_full<- pbp_full %>%
  dplyr::arrange(-season,game_id, qtr, play_id)
saveRDS(pbp_full, "pbp_full.rds")

