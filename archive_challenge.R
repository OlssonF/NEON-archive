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

archive_vars <- c(P1D_vars, PT30M_vars) # add others in here if needed!

archive_location <- file.path(here::here(), "archive")
dir.create(archive_location, showWarnings = F)

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
                         endpoint_override = "sdsc.osn.xsede.org",
                         anonymous = T) |> 
    arrow::open_dataset() 
  
  # Write locally
  message('Writing locally')
  arrow::write_dataset(df,
                       path = file.path(archive_location, 
                                        paste("forecasts",names(var), var, sep = "_"), 
                                        paste("forecasts",names(var), var, sep = "_")),
                       hive_style = TRUE,
                       partitioning = c("model_id"))
  
  # Check forecasts before zipping 
  # Checks for duplicates, missing forecasts
  # message('Checking for duplicates')
  # local_df <- arrow::open_dataset(file.path(archive_location, paste0("forecasts_P1D_",var), paste0("forecasts_P1D_",var))) |> 
  #   filter(!is.na(prediction))  |> 
  #   collect() |> 
  #   group_by(reference_datetime, datetime, variable, model_id, duration, family, parameter, site_id, project_id) |> 
  #   slice_head(n=1) 
  
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
                         endpoint_override = "sdsc.osn.xsede.org",
                         anonymous = T) |> 
    arrow::open_dataset() 
  
  # Write locally
  message('Writing locally')
  arrow::write_dataset(df,
                       path = file.path(archive_location, 
                                        paste("scores",names(var), var, sep = "_"), 
                                        paste("scores",names(var), var, sep = "_")),
                       hive_style = TRUE,
                       partitioning = c("model_id"))
  
  # Check scores before zipping 
  # Checks for duplicates, missing scores
  # message('Checking for duplicates')
  # local_df <- arrow::open_dataset(file.path(archive_location, paste0("forecasts_P1D_",var), paste0("forecasts_P1D_",var))) |> 
  #   filter(!is.na(prediction))  |> 
  #   collect() |> 
  #   group_by(reference_datetime, datetime, variable, model_id, duration, family, parameter, site_id, project_id) |> 
  #   slice_head(n=1) 
  
  # Zip up
  message('Zipping data')
  setwd(file.path(archive_location, paste("scores",names(var), var, sep = "_")))
  files2zip <- fs::dir_ls(recurse = TRUE)
  files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
  utils::zip(zipfile = file.path(archive_location, paste("scores",names(var), var, sep = "_")), files = files2zip)
  
  setwd(archive_location)
}
