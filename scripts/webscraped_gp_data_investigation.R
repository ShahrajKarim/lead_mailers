# Investigation into web scraped GP data
# This script investigates which GPs from the webscraped dataset fall within the NHS' official GP dataset
# Note that this script is currently matching using the name and postcode combination but names could vary slightly
# Some form of fuzzy matching with manual checking needs to be implemented to validate numbers [WIP - fuzzy matching needs to be fixed]

library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(fuzzyjoin)
library(stringdist)

# Set seed
set.seed(76695)

# Import webscraped data
webscraped_data <- read_csv("processed_data/webscraped_gps.csv") |>
  select( - "...1") |>
  rename("Name" = "name") |>
  mutate(Name = toupper(Name))

postcode_pattern <- "[A-Z]{1,2}[0-9][0-9A-Z]? ?[0-9][A-Z]{2}" 

webscraped_data <- webscraped_data |>
  mutate(Postcode = str_extract(address, postcode_pattern))

# Import my dataset
gp_script_data <- read_csv("processed_data/gp_script_data.csv")
additional_gps <- read_csv("processed_data/additional_gps.csv") # List of GPs beginning with LS but not in gp_script_data
combined_gp_script <- bind_rows(gp_script_data, additional_gps)

# Import full list of GP data

    gp_full_data <- read.csv("raw_data/epraccur.csv", header = FALSE)
    
    # Assign column names as depicted in pdf in zip
    
    colnames(gp_full_data) <- c(
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
    
    # Keep GPs which are open only active - hashed out for now.
    
    #gp_full_data <- gp_full_data |>
    #filter(`Status Code` == "A")
    
    # Replace status codes
    
    gp_full_data <- gp_full_data |>
      mutate(
        `Status Code` = ifelse(`Status Code` == "A", "Active", `Status Code`),
        `Status Code` = ifelse(`Status Code` == "C", "Closed", `Status Code`),
        `Status Code` = ifelse(`Status Code` == "D", "Dormant", `Status Code`),
        `Status Code` = ifelse(`Status Code` == "P", "Proposed", `Status Code`)
      )
    
    # Keep variables needed for contact
    
    gp_full_data <- gp_full_data |>
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


# Calculate statics based on GPs in web scraped data (relative to GP script dataset)

web_merge_script <- webscraped_data |>
  left_join(combined_gp_script, by = c("Name", "Postcode"))

inactive_count <- web_merge_script |>
  summarise(
    inactive_count = sum(!is.na(`Status Code`) & `Status Code` != "Active")
  ) # Gives 6 closed GP's 

active_count <- web_merge_script |>
  summarise(
    active_count = sum(`Status Code` == "Active", na.rm = TRUE)
  ) # 94 active GPs

merge_count <- web_merge_script |>
  summarise(
    merge_rate = sum(!is.na(`Address Line 1`))
  ) # 100

# Calculate statics based on GPs in webscraped data (relative to Full GP dataset)

web_merge_full <- webscraped_data |>
  left_join(gp_full_data, by = c("Name", "Postcode"))

inactive_count_full <- web_merge_full |>
  summarise(
    inactive_count = sum(!is.na(`Status Code`) & `Status Code` != "Active")
  ) # Gives 13 closed GP's [only 1 of these is permanently closed]

active_count_full <- web_merge_full |>
  summarise(
    active_count = sum(`Status Code` == "Active", na.rm = TRUE)
  ) # 203 active GPs

merge_count_full <- web_merge_full |>
  summarise(
    merge_rate = sum(!is.na(`Address Line 1`))
  ) # 216

# Calculate statics based on GPs in GP script dataset (relative to GP script dataset)

script_merge_web  <- combined_gp_script |>
  left_join(webscraped_data, by = c("Name", "Postcode"))

merge_count_script <- script_merge_web |>
  summarise(
    merge_rate = sum(!is.na(address))
  ) # 100

inactive_count_merge <- script_merge_web |>
  summarise(
    inactive_count = sum(!is.na(`Status Code`) & `Status Code` != "Active" & !is.na(address))
  ) # Gives 6 closed GP's [only X of these are permanently closed]

inactive_count_not_merge <- script_merge_web |>
  summarise(
    inactive_count = sum(!is.na(`Status Code`) & `Status Code` != "Active" & is.na(address))
  ) # 86 closed GP's

active_count_not_merge <- script_merge_web |>
  summarise(
    active_count = sum(`Status Code` == "Active" & is.na(address))
  ) # 87 active GPs


######### Now repeat this exercise using fuzzy matching [WIP] #########

# Webscraped data relative to GPs to full GP list

web_full_fuzzy <- fuzzy_left_join(
  webscraped_data,
  gp_full_data,
  by = c("Name" = "Name", "Postcode" = "Postcode"),
  match_fun = list(
    `==`,
    function(a, b) stringdist(a, b, method = "cosine") <= 0.2
  )) |>
  mutate(cosine_dist = stringdist(Name.x, Name.y, method = "cosine"),
         cosine_sim  = 1 - cosine_dist)

# Webscraped data relative to GPs to GP script data

web_script_fuzzy <- fuzzy_left_join(
  webscraped_data,
  gp_script_data,
  by = c("Name" = "Name", "Postcode" = "Postcode"),
  match_fun = list(
    `==`,
    function(a, b) stringdist(a, b, method = "cosine") <= 0.2
  )) |>
  mutate(cosine_dist = stringdist(Name.x, Name.y, method = "cosine"),
         cosine_sim  = 1 - cosine_dist)

# Script data relative to webscraped data

script_web_fuzzy <- fuzzy_left_join(
  gp_script_data,
  webscraped_data,
  by = c("Name" = "Name", "Postcode" = "Postcode"),
  match_fun = list(
    `==`,
    function(a, b) stringdist(a, b, method = "cosine") <= 0.2
  )) |>
  mutate(cosine_dist = stringdist(Name.x, Name.y, method = "cosine"),
         cosine_sim  = 1 - cosine_dist)