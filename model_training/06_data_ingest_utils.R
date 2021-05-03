find_game_next_score_half <- function(drive_df){
  drive_df$drive_id <- as.numeric(drive_df$drive_id)
  drive_df = drive_df %>% arrange(drive_id)
  score_plays <- which(drive_df$drive_is_score == TRUE, 
                       str_detect(drive_df$drive_result_detailed, 
                                   regex("END HALF|END GAME|Uncategorized", ignore_case = TRUE))==FALSE)
  
  final_df = lapply(1:nrow(drive_df), 
                    find_next_score,
                    score_plays_i = score_plays,
                    dat_drive = drive_df) %>% 
    bind_rows()
  
  final_df2 = cbind(drive_df, final_df)
  return(final_df2)
}

find_next_score <- function(play_i, score_plays_i, dat_drive){
  defense_tds <- c("Missed Field Goal Return Touchdown", "Blocked Field Goal Touchdown", 
                   "Uncategorized Touchdown",
                   "Blocked Punt Touchdown", "Fumble Recovery (Opponent) Touchdown",
                   "Fumble Return Touchdown", "Interception Return Touchdown",
                   "Punt Return Touchdown", "Kickoff Fumble Recovery Touchdown")
  next_score_i <- score_plays_i[which(score_plays_i >= play_i)[1]]
  
  if( is.na(next_score_i) |
      dat_drive$qtr[play_i] <= 2 & dat_drive$qtr[next_score_i] %in% c(3,4) |
      dat_drive$qtr[play_i] %in% c(3, 4) & dat_drive$qtr[next_score_i] > 4){
    
    if(is.na(next_score_i) & dat_drive$qtr[play_i] <= 2){
      score_drive <- max(dat_drive$drive_id[dat_drive$qtr == 2])
      next_score <- 0
      return(data.frame(NSH = next_score,
                        DSH = score_drive))}
    if(is.na(next_score_i) & dat_drive$qtr[play_i] %in% c(3,4)){
      score_drive <- max(dat_drive$drive_id[dat_drive$qtr == 4])
      next_score <- 0
      return(data.frame(NSH = next_score,
                        DSH = score_drive))}
    else{ 
      if(dat_drive$qtr[play_i] <= 2 & dat_drive$qtr[next_score_i] %in% c(3,4)){
        score_drive <- max(dat_drive$drive_id[dat_drive$qtr == 2])
        next_score <- 0
        return(data.frame(NSH = next_score,
                          DSH = score_drive))
      } 
      if (dat_drive$qtr[play_i] >= 3 & dat_drive$qtr[next_score_i] > 4){
        score_drive <- max(dat_drive$drive_id[dat_drive$qtr == 4])
        next_score <- 0
        return(data.frame(NSH = next_score,
                          DSH = score_drive))
      }
    }
  } else{
    score_drive <- dat_drive$drive_id[next_score_i]
    # Identify current and next score teams
    # if they are the same then you are good
    # if it is different then flip the negative sign
    current_team <- dat_drive$offense_play[play_i]
    
    ## If the defense scores
    ## we need to make sure the next_score_team is correct
    next_score_team <- dat_drive$offense_play[next_score_i]
    
    if(dat_drive$drive_result_detailed[next_score_i] %in% defense_tds){
      next_score_team <- dat_drive$defense_play[next_score_i]
    }
    
    if(dat_drive$drive_result_detailed[next_score_i] %in% defense_tds){
      if(identical(current_team, next_score_team)){
        next_score <- -1 * dat_drive$drive_points[next_score_i]
      } else {
        next_score <- dat_drive$drive_points[next_score_i]
      }
    } else {
      # Now normal cases of offensive touchdowns
      if(identical(current_team, next_score_team)){
        # if same then you score
        next_score <- dat_drive$drive_points[next_score_i]
      } else {
        # if different, then other team scored
        next_score <- -1 * dat_drive$drive_points[next_score_i]
      }
    }
    return(data.frame(NSH = next_score,
                      DSH = score_drive))
  }
}