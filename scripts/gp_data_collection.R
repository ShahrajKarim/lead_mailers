## This script is used to collect a list of GPs under the NHS in Leeds
## First a list of GPs are downloaded
## Then postcodes are filtered for based on whether they fall within the LAD corresponding to Leeds
## This code can be reworked with the use of an API but the NHS is considering to take the API down

library(dplyr)
library(googledrive)
library(readr)
library(tidyr)
library(stringr)

# Source file from: https://files.digital.nhs.uk/assets/ods/current/epraccur.zip
  gp_data <- read.csv("raw_data/epraccur.csv", header = FALSE)
 
# Assign column names as depicted in pdf in zip
  
  colnames(gp_data) <- c(
    "Organisation Code",
    "Name",
    "National Grouping",
    "High Level Health Geography",
    "Address Line 1",
    "Address Line 2",
    "Address Line 3",
    "Address Line 4",
    "Address Line 5",
    "Postcode",
    "Open Date",
    "Close Date",
    "Status Code",
    "Organisation Sub-Type Code",
    "Commissioner",
    "Join Provider/Purchaser Date",
    "Left Provider/Purchaser Date",
    "Contact Telephone Number",
    "Null_19",
    "Null_20",
    "Null_21",
    "Amended Record Indicator",
    "Null_23",
    "Provider/Purchaser",
    "Null_25",
    "Prescribing Setting",
    "Null_27"
    )

# Keep GPs which are open only active
  
  gp_data <- gp_data %>%
    filter(`Status Code` == "A")
  
# Keep variables needed for contact

  gp_data <- gp_data |>
     select(
       "Name",
       "Address Line 1",
       "Address Line 2",
       "Address Line 3",
       "Address Line 4",
       "Address Line 5",
       "Postcode",
       "Contact Telephone Number",
       "Status Code"
       )
 
# Load LAD / postcode mapping file

  mapping_file <- drive_get("Lead_Map_Project/UK/predictors/leeds_data/PCD_OA21_LSOA21_MSOA21_LAD_MAY25_UK_LU.csv") |>
    drive_read_string() |>
    read_csv()

# Filter mapping file for postcodes within Leeds

  leeds_mapping_file <- mapping_file |>
    filter(ladcd == "E08000035") |>
    select(pcd7, pcd8, pcds) |>
    pivot_longer(
      cols = c(pcd7, pcd8, pcds),
      names_to = "source",
      values_to = "postcode"
      ) |>
    select(postcode)

# Obtain additional postcodes from residential address file
  
  residential_address_data <- read_csv("processed_data/residential_address_data.csv") |>
    select(pcds) |>
    rename(postcode = pcds)
  
# Append to mapping df and keep distinct observations
  
  leeds_mapping_file <- bind_rows(leeds_mapping_file, residential_address_data) %>%
    distinct()
  
# Save mapping file with the mention of whether it is an LS postcode or not
  
  leeds_mapping_file <- leeds_mapping_file %>%
    mutate(ls_tag = ifelse(substr(postcode, 1, 2) == "LS", 1, 0))
  
  write.csv(leeds_mapping_file, "processed_data/leeds_postcodes.csv", row.names = FALSE)
  
# Keep GP's that meet that meet either of the following criteria:
# (i) Has a postcode that falls within the Leeds LAD
# (ii) Has an address line that contains the string "Leeds"
  
  addr_cols <- c("Address Line 1","Address Line 2","Address Line 3","Address Line 4","Address Line 5")
  
  gp_leeds <- gp_data %>%
    filter(
      Postcode %in% leeds_mapping_file$postcode |
        if_any(all_of(addr_cols), ~ str_detect(coalesce(.x, ""), regex("\\bLeeds\\b", ignore_case = TRUE)))
    )

# Write file
  
  write.csv(gp_leeds, "processed_data/gp_script_data.csv", row.names = FALSE)
  