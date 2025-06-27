#--------------------------------------#
## Project: NEON-archive
## Script purpose: Grab and zip each variable scores/forecasts/targets
## Date: 2025-06-16
## Author: Freya Olsson
#--------------------------------------#

# Set up ------------------------------
library(tidyverse)
aws_region <- "sdsc"
aws_endpoint <- "osn.xsede.org"

# define all variables
P1D_vars <- c('P1D' = "chla",'P1D' =  "gcc_90", 'P1D' =  "le", 'P1D' =  "nee",
              'P1D' = "oxygen", 'P1D' =  "rcc_90", 'P1D' = "temperature")
P1W_vars <- c('P1W' = "abundance", 'P1W' = "richness",'P1W' = "amblyomma_americanum")
PT30M_vars <- c('PT30M' = "le", 'PT30M' = "nee")

archive_vars <- c(P1D_vars) # add others in here if needed!

archive_location <- file.path(here::here(), "archive")
dir.create(archive_location, showWarnings = F)

# do you want to check the files before zipping?
apply_checks <- F


# get the forecasts and zip -----------
message('Getting forecasts')
# each zip is called the variable name and duration and then within that is partioned by model_id
# e.g. forecasts_P1D_chla/model_id=climatology

for (var in archive_vars) {
  message(var)
  
  # Access forecast catalogue
  s3_location <- paste0("bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/duration=",
                        names(var), "/variable=",var)
  
  message('Accessing bucket')
  df <- arrow::s3_bucket(bucket = s3_location, 
                         endpoint_override = paste(aws_region, aws_endpoint, sep = "."),
                         anonymous = T) |> 
    arrow::open_dataset() 
  
  # Check forecasts before zipping 
  # Checks for duplicates, missing forecasts
  # This could be very slow! Might need to go my model_id??
  if (apply_checks) {
    message('Checking for duplicates and missing forecasts')
    df |>
      filter(!is.na(prediction))  |>
      collect() |>
      group_by(reference_datetime, datetime, variable, model_id, duration, family, parameter, site_id, project_id) |>
      slice_head(n=1) |> 
      arrow::write_dataset(path = file.path(archive_location, 
                                            paste("forecasts",names(var), var, sep = "_"), 
                                            paste("forecasts",names(var), var, sep = "_")),
                           hive_style = TRUE,
                           partitioning = c("model_id"))
    message('Checked and written')
  } else {
    # Write locally
    message('Writing locally')
    arrow::write_dataset(df,
                         path = file.path(archive_location, 
                                          paste("forecasts",names(var), var, sep = "_"), 
                                          paste("forecasts",names(var), var, sep = "_")),
                         hive_style = TRUE,
                         partitioning = c("model_id"))
    
  }
  
  
  # Zip up
  message('Zipping data')
  setwd(file.path(archive_location, paste("forecasts",names(var), var, sep = "_")))
  files2zip <- fs::dir_ls(recurse = TRUE)
  files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
  utils::zip(zipfile = file.path(archive_location, paste("forecasts",names(var), var, sep = "_")), files = files2zip)
  
  setwd(archive_location)
}

# get the scores and zip -----------
message('Getting scores')
# each zip is called the variable name and duration and then within that is partioned by model_id
# e.g. scores_P1D_chla/model_id=climatology

for (var in archive_vars) {
  message(var)
  
  # Access forecast catalogue
  s3_location <- paste0("bio230014-bucket01/challenges/scores/bundled-parquet/project_id=neon4cast/duration=",
                        names(var), "/variable=",var)
  
  message('Accessing bucket')
  df <- arrow::s3_bucket(bucket = s3_location, 
                         endpoint_override = paste(aws_region, aws_endpoint, sep = "."),
                         anonymous = T) |> 
    arrow::open_dataset() 
  
  # Check scores before zipping 
  # Checks for duplicates, missing scores
  # This could be very slow! Might need to go my model_id??
  if (apply_checks) {
    message('Checking for duplicates and missing forecasts')
    df |>
      filter(!is.na(mean))  |>
      collect() |>
      group_by(reference_datetime, datetime, variable, model_id, duration, family, site_id, project_id) |>
      slice_head(n=1) |> 
      arrow::write_dataset(path = file.path(archive_location, 
                                            paste("scores",names(var), var, sep = "_"), 
                                            paste("scores",names(var), var, sep = "_")),
                           hive_style = TRUE,
                           partitioning = c("model_id"))
    message('Checked and written')
    
  } else {
      # Write locally
  message('Writing locally')
  arrow::write_dataset(df,
                       path = file.path(archive_location, 
                                        paste("scores",names(var), var, sep = "_"), 
                                        paste("scores",names(var), var, sep = "_")),
                       hive_style = TRUE,
                       partitioning = c("model_id"))
  }

  
  # Zip up
  message('Zipping data')
  setwd(file.path(archive_location, paste("scores",names(var), var, sep = "_")))
  files2zip <- fs::dir_ls(recurse = TRUE)
  files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
  utils::zip(zipfile = file.path(archive_location, paste("scores",names(var), var, sep = "_")), files = files2zip)
  
  setwd(archive_location)
}


# get targets and zip ---------------------------
message('get targets')
# each theme has a different targets file in a seperate csv file
# download and zip together
targets_url <- paste0("https://", 
                      paste(aws_region, aws_endpoint, sep = "."), 
                      "/bio230014-bucket01/challenges/targets/project_id=neon4cast/")

P1D_themes <- c('P1D' = "aquatics",'P1D' =  "phenology", 'P1D' =  "terrestrial_daily")
P1W_themes <- c('P1W' = "ticks",'P1W' =  "beetles")
PT30M_themes <- c("PT30M" = "terrestrial_30min")

archive_themes <- c(P1D_themes, P1W_themes)
save_targets <- 'targets/targets'
dir.create(file.path(archive_location, save_targets), recursive = T)

for (i in 1:length(archive_themes)) {
  message(archive_themes[i])
  message('download')
  read_csv(paste0(targets_url, "duration=", names(archive_themes[i]), "/", archive_themes[i], "-targets.csv.gz")) |> 
    write_csv(file.path(archive_location,
                        save_targets,
                        paste0(paste('targets', names(archive_themes[i]), archive_themes[i], sep = '_'), '.csv.gz')))
}


message('Zipping data')
setwd(file.path(archive_location, 'targets'))
files2zip <- fs::dir_ls(recurse = TRUE)
files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
utils::zip(zipfile = file.path(archive_location, "targets"), files = files2zip)

setwd(archive_location)

# get NOAA drivers and zip ------------
## get stage 3 -------


