
library(tidyverse)
library(dplyr)
library(stringr)
library(arrow)

years_vec <- 2002:2020
# --- compile into play_by_play_{year}.parquet ---------
future::plan("multisession")
progressr::with_progress({
  p <- progressr::progressor(along = years_vec)
  pbp_games <- purrr::map_dfr(years_vec, function(y){
    
    pbp_g <- data.frame()
    pbp_list <- list.files(path = glue::glue('pbp_json_final/{y}/'))
    print(glue::glue('pbp_json_final/{y}/'))
    pbp_g <- furrr::future_map_dfr(pbp_list, function(x){
      pbp <- jsonlite::fromJSON(glue::glue('pbp_json_final/{y}/{x}'))$plays
      return(pbp)
    })
    pbp_g <- pbp_g %>% janitor::clean_names()
    write.csv(pbp_g,file=gzfile(glue::glue("pbp_final/csv/play_by_play_{y}.csv.gz")),row.names = FALSE)
    saveRDS(pbp_g,glue::glue("pbp_final/rds/play_by_play_{y}.rds"))
    arrow::write_parquet(pbp_g, glue::glue("pbp_final/parquet/play_by_play_{y}.parquet"))
    p(sprintf("y=%s", as.integer(y)))
    return(pbp_g)
  })
})