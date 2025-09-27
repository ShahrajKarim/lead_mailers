# This script samples from the full list of addresses in Leeds
# Selected addresses can fall under three different treatment groups:
# Control, Treatment 1 and Treatment 2
# In order to compute sampling weights, eligibility for each LSOA is calculated by using the proportion of households with a child under the age of 8 years old.

# libraries
library(tidyverse)
library(tidyr)
library(stringr)

# Call in script with relevant functions
source("scripts/sampling_functions.R") # Change name 

# EXPERIMENT SETTINGS
n_letters <- 150000
n_pilot <- 30000 # 20% of total

# Set seed
set.seed(2025)

# Load the data
lsoa_data <- read_csv("processed_data/leeds_census_data_2021_lsoa.csv")
oa_data <- read_csv("processed_data/leeds_census_data_2021_oa.csv")
address_data <- read_csv("processed_data/tagged_address_data.csv")

# Create eligibility weights
lsoa_data <- lsoa_data %>% 
  mutate(w_eligible_lsoa = eligible / sum(eligible))

oa_data <- oa_data %>% 
  mutate(w_eligible_oa = eligible / sum(eligible))

# Merge and clean data
merged <- address_data %>% 
  left_join(oa_data, by = "oa21cd", suffix = c("", "_oa")) %>%
  left_join(lsoa_data, by = "lsoa21cd", suffix = c("", "_lsoa")) %>%
  filter(tag_hotel == 0, tag_vacant == 0, tag_unconventional_household == 0) %>%  # keep households only
  mutate(
    house_age_old = if_else(epc_age_end <= 1966, 1, 0),
    house_age_unknown = is.na(epc_age_end)
  )


# Summarize address data by LSOA
lsoa_summary <- merged %>% 
  group_by(lsoa21cd, w_eligible_lsoa) %>% 
  summarise(
    n_addresses = n(),
    n_old     = sum(house_age_old == 1, na.rm = TRUE),
    n_new     = sum(house_age_old == 0, na.rm = TRUE),
    n_unknown = sum(house_age_unknown)
  ) %>% 
  ungroup() %>% 
  mutate(
    ratio_old     = n_old / n_addresses,
    ratio_new     = n_new / n_addresses,
    ratio_unknown = n_unknown / n_addresses
  )

# Perform baseline allocation - numbers based on LSOA 
baseline_allocated <- create_baseline_allocation(lsoa_summary, n_letters)

# Sample addresses based on baseline allocations
sampled_addresses <- sample_addresses_from_allocation(baseline_allocated, merged, seed = 2025)

# Check the allocation results
sampled_addresses %>% 
  filter(!is.na(treatment)) %>% 
  count(treatment, name = "n_allocated") %>% 
  mutate(proportion = n_allocated / sum(n_allocated))

# Summary by house age and treatment
sampled_addresses %>% 
  filter(!is.na(treatment)) %>% 
  mutate(
    house_age_cat = case_when(
      house_age_old == 1 ~ "old",
      house_age_old == 0 ~ "new",
      house_age_unknown == 1 ~ "unknown"
    )
  ) %>% 
  count(house_age_cat, treatment, name = "n_allocated") %>% 
  arrange(house_age_cat, treatment)

# Total allocated vs target
print(paste("Target letters:", n_letters, "\n"))
print(paste("Actually allocated:", sum(!is.na(sampled_addresses$treatment)), "\n"))

# Save resulting dataset
write.csv(sampled_addresses, "processed_data/pilot_sampled_addresses.csv", row.names = FALSE)