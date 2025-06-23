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

# define variables
P1D_vars <- c("chla", "gcc_90", "le", "nee", "oxygen", "rcc_90", "temperature")
P1W_vars <- c("abundance", "richness", "amblyomma_americanum")
PT30M_vars <- c("le", "nee")


archive_location <- file.path(here::here(), "archive")
dir.create(archive_location, showWarnings = F)

# get the forecasts and zip -----------
# each zip is called the variable name and duration and then within that is partioned by model_id
# e.g. forecasts_P1D_chla/model_id=climatology

for (var in P1D_vars) {
  message(var)
  
  # Access forecast catalogue
  s3_location <- paste0("bio230014-bucket01/challenges/forecasts/bundled-parquet/project_id=neon4cast/duration=P1D/variable=",var)
  
  message('Accessing bucket')
  df <- arrow::s3_bucket(bucket = s3_location, 
                         endpoint_override = "sdsc.osn.xsede.org",
                         anonymous = T) |> 
    arrow::open_dataset() 
  
  # Write locally
  message('Writing locally')
  arrow::write_dataset(df,
                       path = file.path(archive_location, paste0("forecasts_P1D_",var), paste0("forecasts_P1D_",var)),
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
  setwd(file.path(archive_location, paste0("forecasts_P1D_",var)))
  files2zip <- fs::dir_ls(recurse = TRUE)
  files2zip <- files2zip[stringr::str_detect(files2zip, pattern = "DS_Store", negate = TRUE)][-1]
  utils::zip(zipfile = file.path(archive_location, paste0("forecasts_P1D_",var)), files = files2zip)
  
  setwd(archive_location)
}


