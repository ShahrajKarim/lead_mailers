## This script calls in census data at the OA, LSOA and MSOA level for Leeds
## Currently this calls in data for country of birth, ethnicity and number of households with youngest dependent child aged between 0 and 9.

library(dplyr)
library(nomisr)
library(purrr)
library(readr)
library(googledrive)

# Define function to get census data
# "geography" is related specifically to the code used by nomisr e.g. "TYPE150"
# "geo_col" is used to rename the geography column to know the geographic unit associated with the "geography" variable e.g. oa21cd
get_census_data <- function(geography, geo_col) {
  
  # Country of Birth Data
  COB_values <- c(1, 1002, 6, 7, 8, 9, 10, 11)
  COB_names <- c("uk_COB", "europe_eu_countries_COB", "europe_non_eu_countries_COB", 
                 "africa_COB", "asia_middle_east_COB", "americas_carribean_COB", 
                 "antarctica_oceana_COB", "british_overseas_COB")
  
  COB_data <- map2(COB_values, COB_names, ~ nomis_get_data(
    id = "NM_2024_1", time = "latest", geography = geography, C2021_COB_12 = .x, measures = 20100,
    select = c("GEOGRAPHY_CODE", "OBS_VALUE")
  ) %>%
    rename(!!sym(.y) := OBS_VALUE, !!geo_col := GEOGRAPHY_CODE))
  
  # Ethnicity Data
  ethnicity_values <- c(12, 13, 11, 12, 14, 16, 15, 17, 1, 2, 3, 4, 5, 18, 19)
  ethnicity_names <- c("bangladesh_ethnicity", "chinese_ethnicity", "indian_ethnicity",
                       "pakistani_ethnicity", "asian_other_ethnicity", "african_ethnicity",
                       "caribbean_ethnicity", "afro_caribbean_other_ethnicity",
                       "white_english_ethnicity", "white_irish_ethnicity",
                       "white_gypsy_irish_traveller_ethnicity", "white_roma_ethnicity",
                       "white_other_ethnicity", "arab_ethnicity", "any_other_ethnicity")
  
  ethnicity_data <- map2(ethnicity_values, ethnicity_names, ~ nomis_get_data(
    id = "NM_2041_1", time = "latest", geography = geography, measures = 20100, C2021_ETH_20 = .x,
    select = c("GEOGRAPHY_CODE", "OBS_VALUE")
  ) %>%
    rename(!!sym(.y) := OBS_VALUE, !!geo_col := GEOGRAPHY_CODE))
  
  # Load lookup file
  lookup_data <- read.csv("raw_data/geography_lookup_Dec_2021.csv") %>%
    rename_with(tolower) %>%  # Convert column names to lowercase
    select(ends_with("cd"))   # Keep only columns ending with "cd"
  
  # Merge all datasets
  dataframes <- c(list(lookup_data), COB_data, ethnicity_data)
  merged_data <- reduce(dataframes, left_join, by = geo_col)
  
  # Filter for Leeds LAD22CD
  leeds_data <- merged_data %>%
    filter(lad22cd == "E08000035") %>%
    distinct(!!sym(geo_col), .keep_all = TRUE) %>%
    select(-matches("NMW|ObjectId"))
  
  return(leeds_data)
}

# Call in data for OA level
leeds_census_data_2021_oa <- get_census_data("TYPE150", "oa21cd")

# Call in data for LSOA level
leeds_census_data_2021_lsoa <- get_census_data("TYPE151", "lsoa21cd")

# Call in data for MSOA level
leeds_census_data_2021_msoa <- get_census_data("TYPE152", "msoa21cd")

# Use ONS data on eligible households based on age of child - downloaded on March 7th 2025
# The ONS groups dependent children in the following age bands:[0,4], [5,9], [10,15], [16,18]
# In this script eligible children are defined to be in the age bands [0,9]

dep_child_age_data <- drive_get("Lead_Map_Project/UK/predictors/leeds_data/household_dependent_child_age_ons.csv") |>
  drive_read_string() |>
  read_csv()

dep_child_age_data <- dep_child_age_data %>%
  rename(
    oa21cd = 1,                  # Rename first column
    dep_child_category = 3          # Rename third column
  )

dep_child_age_data <- dep_child_age_data %>%
  left_join(lookup_data, by = "oa21cd")

# Select Leeds' LAD
dep_child_age_data <- dep_child_age_data %>%
  filter(lad22cd == "E08000035")

# Define the geographic levels to loop over
geo_levels <- c("oa21cd", "lsoa21cd", "msoa21cd")

# Create aggregate statistics for children with dependent children for each geographic unit
results <- lapply(geo_levels, function(geo) {
  if (geo %in% colnames(dep_child_age_data)) {  # Check if the column exists
    dep_child_age_data %>%
      group_by(across(all_of(geo))) %>%
      summarise(
        total_number_of_households = sum(Observation, na.rm = TRUE),
        eligible = sum(Observation[dep_child_category %in% c(1, 2)], na.rm = TRUE),
        share_of_eligible = 100 * eligible / total_number_of_households,
        .groups = "drop"  # Ensure ungrouping after summarising
      ) %>%
      select(all_of(geo), total_number_of_households, eligible, share_of_eligible)  # Keep only required columns
  } else {
    message(paste("Skipping:", geo, "not found in dataset"))
    return(NULL)  # Return NULL if the column does not exist
  }
})

dep_child_age_data_oa <- results[[1]]
dep_child_age_data_lsoa <- results[[2]]
dep_child_age_data_msoa <- results[[3]]

# Merge OA-level data
leeds_census_data_2021_oa <- leeds_census_data_2021_oa %>%
  left_join(dep_child_age_data_oa, by = "oa21cd")
write_csv(leeds_census_data_2021_oa, "processed_data/leeds_census_data_2021_oa.csv")

# Merge LSOA-level data
leeds_census_data_2021_lsoa <- leeds_census_data_2021_lsoa %>%
  left_join(dep_child_age_data_lsoa, by = "lsoa21cd") %>%
  select(-oa21cd)
write_csv(leeds_census_data_2021_lsoa, "processed_data/leeds_census_data_2021_lsoa.csv")

# Merge MSOA-level data
leeds_census_data_2021_msoa <- leeds_census_data_2021_msoa %>%
  left_join(dep_child_age_data_msoa, by = "msoa21cd") %>%
  select(-oa21cd, -lsoa21cd)
write_csv(leeds_census_data_2021_msoa, "processed_data/leeds_census_data_2021_msoa.csv")