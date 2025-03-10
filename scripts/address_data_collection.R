## This script is used to collect address data for Leeds.
## The Leeds City Council provided data on council tax bands - this gives a list of residential properties in Leeds, but it is missing the postcode.
## Properties are matched against UPRN using the ONS UPRN dataset and EPC dataset to fill in missing postcodes.
## There are still some missing postcodes so the remaining addresses are matched against the EPC dataset using the door number and the street name.
## Using this process, only 0.39% of postcodes are missing.

library(dplyr)
library(tidyr)
library(readr)
library(lubridate)
library(stringr)
library(googledrive)

# Load council tax data, uprn data, epc data:

council_tax_data <- read.csv("raw_data/leeds_council_tax_data.csv") %>%
  rename_with(tolower)

uprn_data <- drive_get("Lead_Map_Project/UK/predictors/leeds_data/ONS_UPRN_NOV_2024_YH.csv") |>
  drive_read_string() |>
  read_csv()

uprn_data <- uprn_data %>%
  rename_with(tolower) %>%
  mutate(uprn_data_column = 1)
  
epc_data <- drive_get("Lead_Map_Project/UK/predictors/leeds_data/EPC_certificates.csv") |>
  drive_read_string() |>
  read_csv()

epc_data <- epc_data %>%
  rename_with(tolower) %>%
  mutate(epc_data_column = 1) %>%  # Create a column with 1s
  distinct(address1, uprn, .keep_all = TRUE) %>%  # Remove duplicates based on address1 and uprn
  mutate(
    # Extract the door number (first number in address1)
    door_number = str_extract(address1, "^[0-9]+"),
    
    # Remove leading numbers, spaces, and commas from address1 and convert to uppercase
    cleaned_addr1 = str_to_upper(str_replace(address1, "^[0-9]+[ ,]*", ""))
  )

tax_uprn_merge <- council_tax_data %>%
  left_join(uprn_data, by = "uprn")

# Remove duplicates
tax_uprn_merge <- tax_uprn_merge %>%
  distinct(addr1, addr2, addr3, .keep_all = TRUE)

# Check merge rate for CouncilTax-UPRN data
uprn_data_merge_no <- sum(tax_uprn_merge$uprn_data_column == 1, na.rm = TRUE)
print(uprn_data_merge_no)
# 376,127 of 376,186 properties were merged

# Generate `door_number` by extracting leading numbers from `addr1`
tax_uprn_merge <- tax_uprn_merge %>%
  mutate(door_number = ifelse(str_detect(addr1, "^[0-9]+"), 
                              str_extract(addr1, "^[0-9]+"), 
                              NA))

# Generate `cleaned_addr1` by removing the leading numbers and space from `addr1`
tax_uprn_merge <- tax_uprn_merge %>%
  mutate(cleaned_addr1 = str_replace(addr1, "^[0-9]+ ", ""))

# Replace `uprn` with 1 if it is missing (NA)
tax_uprn_merge <- tax_uprn_merge %>%
  mutate(uprn = ifelse(is.na(uprn), 1, uprn))

# Merge EPC data with council_tax-UPRN dataset
tax_uprn_epc_merge <- tax_uprn_merge %>%
  full_join(epc_data, by = "uprn", relationship = "many-to-many")

# Remove duplicates based on addr1, addr2, addr3
tax_uprn_epc_merge <- tax_uprn_epc_merge %>%
  distinct(addr1, addr2, addr3, .keep_all = TRUE)

# Check merge rate
epc_merge_no <- sum(tax_uprn_epc_merge$epc_data_column == 1, na.rm = TRUE)
print(epc_merge_no)
# 255,833 of 376,186 properties were merged

tax_uprn_epc_merge <- tax_uprn_epc_merge %>%
  select(-epc_data_column, -uprn_data_column)

# Remove the EPC cleaned address and door number column
tax_uprn_epc_merge <- tax_uprn_epc_merge %>%
  rename_with(~ str_replace(.x, "cleaned_addr1.x", "cleaned_addr1")) %>%  # Rename cleaned_addr1.x
  rename_with(~ str_replace(.x, "door_number.x", "door_number")) %>%  # Rename door_number.x
  select(-cleaned_addr1.y, -door_number.y)  # Remove cleaned_addr1.y and door_number.y

# Keep most recent energy certificate from EPC data - may have one property with multiple EPC certificates
tax_uprn_epc_merge <- tax_uprn_epc_merge %>%
  group_by(uprn) %>%
  slice_max(order_by = inspection_date, n = 1) %>%
  ungroup()

# Count rows where `construction_age_band` is empty or "NO DATA!" and part of merged records - 144,632
count_missing <- sum(is.na(tax_uprn_epc_merge$construction_age_band) | 
                       tax_uprn_epc_merge$construction_age_band %in% c("", "NO DATA!"))
print(count_missing)

# Replace `pcds` with `postcode` if `pcds` is empty
tax_uprn_epc_merge <- tax_uprn_epc_merge %>%
  mutate(pcds = ifelse(is.na(pcds) | pcds == "", postcode, pcds))

# Drop `postcode` column since we have `pcds`
tax_uprn_epc_merge <- tax_uprn_epc_merge %>%
  select(-postcode)

# Attempt to merge remaining addresses with EPC certificates using door number and address lines
# This first involves cleaning the EPC data

epc_data_clean <- epc_data %>%
  # Extract numbers from `cleaned_addr1` and store in `door_number`
  mutate(
    door_number = ifelse(str_detect(cleaned_addr1, "[0-9]+"), str_extract(cleaned_addr1, "[0-9]+"), door_number),
    
    # If `cleaned_addr1` contains "flat" or "apartment", set both `cleaned_addr1` and `door_number` to ""
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("flat", ignore_case = TRUE)), "", cleaned_addr1),
    door_number = ifelse(str_detect(cleaned_addr1, regex("flat", ignore_case = TRUE)), "", door_number),
    
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("apartment", ignore_case = TRUE)), "", cleaned_addr1),
    door_number = ifelse(str_detect(cleaned_addr1, regex("apartment", ignore_case = TRUE)), "", door_number),
    
    # If `cleaned_addr1` is empty, extract street name from `address2`
    cleaned_addr1 = ifelse(cleaned_addr1 == "", str_to_upper(str_replace(address2, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    # Repeat cleaning process of "flat" or "apartment"
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("flat", ignore_case = TRUE)), "", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("apartment", ignore_case = TRUE)), "", cleaned_addr1),
    
    # If `cleaned_addr1` is empty, extract street name from `address3`
    cleaned_addr1 = ifelse(cleaned_addr1 == "", str_to_upper(str_replace(address3, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    
    # Remove leading spaces, commas, and numbers from `cleaned_addr1`
    cleaned_addr1 = str_replace(cleaned_addr1, "^( |, )+", ""),
    cleaned_addr1 = str_replace(cleaned_addr1, "^[0-9]+[ ,]*", ""),
    cleaned_addr1 = str_replace(cleaned_addr1, "^[^,]*, *", ""),
    
    # Remove anything with "FLOOR" - these are usually ground floor flats, basement flats or first floor flats. Better to use address2/3 for it
    cleaned_addr1 = str_replace(cleaned_addr1, ".*FLOOR[ ]*", ""),
    
    # Trim whitespace
    cleaned_addr1 = str_trim(cleaned_addr1),
    
    # Extract numbers from `address2` if `address1` contains "flat"
    door_number = ifelse(str_detect(address1, regex("flat", ignore_case = TRUE)) & str_detect(address2, "[0-9]+"),
                         str_extract(address2, "[0-9]+"), door_number),
    
    # Extract numbers from `address2` if `address1` contains "apartment"
    door_number = ifelse(str_detect(address1, regex("apartment", ignore_case = TRUE)) & str_detect(address2, "[0-9]+"),
                         str_extract(address2, "[0-9]+"), door_number),
    
    # If `door_number` is empty but `address1` contains numbers, extract those numbers
    door_number = ifelse(door_number == "" & str_detect(address1, "[0-9]+"),
                         str_extract(address1, "[0-9]+"), door_number),
    
    # Assign "X" to `door_number` if no numbers exist in address1, address2, or address3
    door_number = ifelse(!str_detect(address1, "[0-9]") & !str_detect(address2, "[0-9]") & !str_detect(address3, "[0-9]"),
                         "X", door_number),
    
    # If `door_number` is empty, extract street name from `address2` or `address3`
    cleaned_addr1 = ifelse(door_number == "", str_to_upper(str_replace(address2, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    door_number = ifelse(door_number == "" & str_detect(address2, "[0-9]+"), str_extract(address2, "[0-9]+"), door_number),
    cleaned_addr1 = ifelse(door_number == "", str_to_upper(str_replace(address3, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    door_number = ifelse(door_number == "" & str_detect(address3, "[0-9]+"), str_extract(address3, "[0-9]+"), door_number),
    
    # Remove "X" if set earlier
    door_number = ifelse(door_number == "X", "", door_number)
  ) %>%
  
  # Sort by cleaned_addr1
  arrange(cleaned_addr1) %>%
  
  # Clean up text (similar to `sieve()` in Stata)
  mutate(cleaned_addr1 = str_replace_all(cleaned_addr1, "[0-9\\-(),/&]", ""),
         cleaned_addr1 = str_replace(cleaned_addr1, "^A[ ]+", ""),
         cleaned_addr1 = str_replace(cleaned_addr1, "^B[ ]+", ""),
         cleaned_addr1 = str_replace(cleaned_addr1, "^C[ ]+", "")) %>%
  
  # Standardize known address corrections
  mutate(
    cleaned_addr1 = case_when(
      address3 == "Goodman Street" ~ "GOODMAN STREET",
      address3 == "THE AVENUE" ~ "THE AVENUE",
      address2 == "Atkinson Street," ~ "ATKINSON STREET",
      address3 == "Chapeltown Road" ~ "CHAPELTOWN ROAD",
      address3 == "Bennett Road" ~ "BENNETT ROAD",
      address3 == "Merrion Street" ~ "MERRION STREET",
      TRUE ~ cleaned_addr1  # Keeps existing values if no condition is met
    )
  ) %>%
  
  # Apply regex-based replacements for "Cross Flatts" addresses
  mutate(
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts avenue") & cleaned_addr1 == "", "CROSS FLATTS AVENUE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts mount") & cleaned_addr1 == "", "CROSS FLATTS MOUNT", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts place") & cleaned_addr1 == "", "CROSS FLATTS PLACE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts parade") & cleaned_addr1 == "", "CROSS FLATTS PARADE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts road") & cleaned_addr1 == "", "CROSS FLATTS ROAD", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts row") & cleaned_addr1 == "", "CROSS FLATTS ROW", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts grove") & cleaned_addr1 == "", "CROSS FLATTS GROVE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts crescent") & cleaned_addr1 == "", "CROSS FLATTS CRESCENT", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts terrace") & cleaned_addr1 == "", "CROSS FLATTS TERRACE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "cross flatts street") & cleaned_addr1 == "", "CROSS FLATTS STREET", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "shakespeare towers") & cleaned_addr1 == "", "SHAKESPEARE TOWERS", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(address1), "westfield road") & cleaned_addr1 == "", "WESTFIELD ROAD", cleaned_addr1)
  ) %>%
  
  # Any other addresses which has Flatts in the address lines:
  mutate(
  cleaned_addr1 = ifelse(
    (is.na(cleaned_addr1) | cleaned_addr1 == "") & str_detect(address1, "Flatts"),
    str_to_upper(str_replace(address1, "^[0-9, ]+", "")),  # Remove leading numbers & commas, convert to uppercase
    cleaned_addr1)  # Keep existing value if condition not met
   ) %>%
  
  mutate(
    cleaned_addr1 = ifelse(
      (is.na(cleaned_addr1) | cleaned_addr1 == "") & str_detect(address2, "Flatts"),
      str_to_upper(str_replace(address2, "^[0-9, ]+", "")),  # Remove leading numbers & commas, convert to uppercase
      cleaned_addr1)  # Keep existing value if condition not met
  ) %>%
  
  ## Uses string following punctuation and number for remaining empty strings that contain "Flat" in address line 1
  mutate(
    cleaned_addr1 = ifelse(
      (is.na(cleaned_addr1) | cleaned_addr1 == "") & str_detect(address1, regex("Flat", ignore_case = TRUE)),
      str_to_upper(str_replace(address1, "(?i)^flat[^A-Za-z]*", "")),  # Remove "Flat" + any non-letter characters
      cleaned_addr1)  # Keep existing values
    ) %>%
  
  # Some remaining entries for cleaned_addr1 still need to be filled in
  mutate(
    cleaned_addr1 = ifelse(cleaned_addr1 == "", str_to_upper(str_replace(address2, "^[0-9]+[ ,]*", "")), cleaned_addr1),
  ) %>%
  
  # Some addresses have flat letter's - remove this by replacing it and removing the whitespace
  mutate(
    cleaned_addr1 = str_replace(cleaned_addr1, "^[A-Z] ", "")  # Removes single letter + space at start
  ) %>%
  
  # Some door numbers have the floor number - replace the door number entry with numbers from the second line of address
  mutate(
    door_number = ifelse(
      str_detect(address1, regex("FLOOR", ignore_case = TRUE)) & str_detect(address2, "[0-9]+"), 
      str_extract(address2, "[0-9]+"),  # Extracts the first number in address2
      door_number)  # Keep existing value if conditions are not met
  ) %>%
  
  mutate(
    # Remove single-letter values and replace with the next available address line
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, "^[A-Z]$"), 
                           coalesce(address2, address3, address1), 
                           cleaned_addr1),
    
    # Remove leading whitespace before letters
    cleaned_addr1 = str_trim(cleaned_addr1),
    
    # Remove leading symbols like "-"
    cleaned_addr1 = str_remove(cleaned_addr1, "^-+\\s*"),
    
    # Remove numbers again
    cleaned_addr1 = str_remove(cleaned_addr1, "^[0-9]+[ ,]*")
  ) %>%
  
  mutate(
    # Remove leading numbers and any punctuation at the start
    cleaned_addr1 = str_remove(cleaned_addr1, "^[0-9]+[A-Za-z]*[ ,]*"),  
    
    # Remove any leading special characters or stray spaces
    cleaned_addr1 = str_remove(cleaned_addr1, "^[^A-Za-z]+\\s*"),  
    
    # Ensure all text is uppercase
    cleaned_addr1 = str_to_upper(cleaned_addr1)
  ) %>%
  
  # Select columns of interest
  select(address1, address2, address3, cleaned_addr1, door_number, construction_age_band, postcode) %>%
  
  # Rename columns
  rename(construction_age_band_new = construction_age_band, postcode_new = postcode)

# Now clean the merged dataset that we obtained earlier

tax_uprn_epc_merge_clean <- tax_uprn_epc_merge %>%
  # Extract numbers from `cleaned_addr1` and store in `door_number`
  mutate(
    door_number = ifelse(str_detect(cleaned_addr1, "[0-9]+"), str_extract(cleaned_addr1, "[0-9]+"), door_number),
    
    # If `cleaned_addr1` contains "flat" or "apartment", set both `cleaned_addr1` and `door_number` to ""
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("flat", ignore_case = TRUE)), "", cleaned_addr1),
    door_number = ifelse(str_detect(cleaned_addr1, regex("flat", ignore_case = TRUE)), "", door_number),
    
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("apartment", ignore_case = TRUE)), "", cleaned_addr1),
    door_number = ifelse(str_detect(cleaned_addr1, regex("apartment", ignore_case = TRUE)), "", door_number),
    
    # If `cleaned_addr1` is empty, extract street name from `addr2`
    cleaned_addr1 = ifelse(cleaned_addr1 == "", str_to_upper(str_replace(addr2, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    # Repeat cleaning process of "flat" or "apartment"
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("flat", ignore_case = TRUE)), "", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, regex("apartment", ignore_case = TRUE)), "", cleaned_addr1),
    
    # If `cleaned_addr1` is empty, extract street name from `addr3`
    cleaned_addr1 = ifelse(cleaned_addr1 == "", str_to_upper(str_replace(addr3, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    
    # Remove leading spaces, commas, and numbers from `cleaned_addr1`
    cleaned_addr1 = str_replace(cleaned_addr1, "^( |, )+", ""),
    cleaned_addr1 = str_replace(cleaned_addr1, "^[0-9]+[ ,]*", ""),
    cleaned_addr1 = str_replace(cleaned_addr1, "^[^,]*, *", ""),
    
    # Remove anything with "FLOOR" - these are usually ground floor flats, basement flats or first floor flats. Better to use address2/3 for it
    cleaned_addr1 = str_replace(cleaned_addr1, ".*FLOOR[ ]*", ""),
    
    # Trim whitespace
    cleaned_addr1 = str_trim(cleaned_addr1),
    
    # Extract numbers from `addr2` if `addr1` contains "flat"
    door_number = ifelse(str_detect(addr1, regex("flat", ignore_case = TRUE)) & str_detect(addr2, "[0-9]+"),
                         str_extract(addr2, "[0-9]+"), door_number),
    
    # Extract numbers from `addr2` if `addr1` contains "apartment"
    door_number = ifelse(str_detect(addr1, regex("apartment", ignore_case = TRUE)) & str_detect(addr2, "[0-9]+"),
                         str_extract(addr2, "[0-9]+"), door_number),
    
    # If `door_number` is empty but `addr1` contains numbers, extract those numbers
    door_number = ifelse(door_number == "" & str_detect(addr1, "[0-9]+"),
                         str_extract(addr1, "[0-9]+"), door_number),
    
    # Assign "X" to `door_number` if no numbers exist in address1, address2, or address3
    door_number = ifelse(!str_detect(addr1, "[0-9]") & !str_detect(address2, "[0-9]") & !str_detect(address3, "[0-9]"),
                         "X", door_number),
    
    # If `door_number` is empty, extract street name from `addr2` or `addr3`
    cleaned_addr1 = ifelse(door_number == "", str_to_upper(str_replace(addr2, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    door_number = ifelse(door_number == "" & str_detect(addr2, "[0-9]+"), str_extract(addr2, "[0-9]+"), door_number),
    cleaned_addr1 = ifelse(door_number == "", str_to_upper(str_replace(addr3, "^[0-9]+[ ,]*", "")), cleaned_addr1),
    door_number = ifelse(door_number == "" & str_detect(addr3, "[0-9]+"), str_extract(addr3, "[0-9]+"), door_number),
    
    # Remove "X" if set earlier
    door_number = ifelse(door_number == "X", "", door_number)
  ) %>%
  
  # Sort by cleaned_addr1
  arrange(cleaned_addr1) %>%
  
  # Clean up text (similar to `sieve()` in Stata)
  mutate(cleaned_addr1 = str_replace_all(cleaned_addr1, "[0-9\\-(),/&]", ""),
         cleaned_addr1 = str_replace(cleaned_addr1, "^A[ ]+", ""),
         cleaned_addr1 = str_replace(cleaned_addr1, "^B[ ]+", ""),
         cleaned_addr1 = str_replace(cleaned_addr1, "^C[ ]+", "")) %>%
  
  # Standardize known address corrections
  mutate(
    cleaned_addr1 = case_when(
      addr3 == "Goodman Street" ~ "GOODMAN STREET",
      addr3 == "THE AVENUE" ~ "THE AVENUE",
      addr2 == "Atkinson Street," ~ "ATKINSON STREET",
      addr3 == "Chapeltown Road" ~ "CHAPELTOWN ROAD",
      addr3 == "Bennett Road" ~ "BENNETT ROAD",
      addr3 == "Merrion Street" ~ "MERRION STREET",
    TRUE ~ cleaned_addr1)
  ) %>%
  
  # Apply regex-based replacements for "Cross Flatts" addresses
  mutate(
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts avenue") & cleaned_addr1 == "", "CROSS FLATTS AVENUE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts mount") & cleaned_addr1 == "", "CROSS FLATTS MOUNT", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts place") & cleaned_addr1 == "", "CROSS FLATTS PLACE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts parade") & cleaned_addr1 == "", "CROSS FLATTS PARADE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts road") & cleaned_addr1 == "", "CROSS FLATTS ROAD", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts row") & cleaned_addr1 == "", "CROSS FLATTS ROW", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts grove") & cleaned_addr1 == "", "CROSS FLATTS GROVE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts crescent") & cleaned_addr1 == "", "CROSS FLATTS CRESCENT", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts terrace") & cleaned_addr1 == "", "CROSS FLATTS TERRACE", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "cross flatts street") & cleaned_addr1 == "", "CROSS FLATTS STREET", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "shakespeare towers") & cleaned_addr1 == "", "SHAKESPEARE TOWERS", cleaned_addr1),
    cleaned_addr1 = ifelse(str_detect(str_to_lower(addr1), "westfield road") & cleaned_addr1 == "", "WESTFIELD ROAD", cleaned_addr1)
  ) %>%
  
  # Any other addresses which has Flatts in the address lines:
  mutate(
    cleaned_addr1 = ifelse(
      (is.na(cleaned_addr1) | cleaned_addr1 == "") & str_detect(addr1, "Flatts"),
      str_to_upper(str_replace(addr1, "^[0-9, ]+", "")),  # Remove leading numbers & commas, convert to uppercase
      cleaned_addr1)  # Keep existing value if condition not met
  ) %>%
  
  mutate(
    cleaned_addr1 = ifelse(
      (is.na(cleaned_addr1) | cleaned_addr1 == "") & str_detect(addr2, "Flatts"),
      str_to_upper(str_replace(addr2, "^[0-9, ]+", "")),  # Remove leading numbers & commas, convert to uppercase
      cleaned_addr1)  # Keep existing value if condition not met
  ) %>%
  
  # Uses string following punctuation and number for remaining empty strings that contain "Flat" in address line 1
  mutate(
    cleaned_addr1 = ifelse(
      (is.na(cleaned_addr1) | cleaned_addr1 == "") & str_detect(addr1, regex("Flat", ignore_case = TRUE)),
      str_to_upper(str_replace(addr1, "(?i)^flat[^A-Za-z]*", "")),  # Remove "Flat" + any non-letter characters
      cleaned_addr1)  # Keep existing values
  ) %>%
  
  # Some remaining entries for cleaned_addr1 still need to be filled in
  mutate(
  cleaned_addr1 = ifelse(cleaned_addr1 == "", str_to_upper(str_replace(addr2, "^[0-9]+[ ,]*", "")), cleaned_addr1),
  ) %>%
  
  # Some addresses have flat letter's - remove this by replacing it and removing the whitespace
  mutate(
    cleaned_addr1 = str_replace(cleaned_addr1, "^[A-Z] ", "")  # Removes single letter + space at start
  ) %>%
  
  # Some door numbers have the floor number - replace the door number entry with numbers from the second line of address
  mutate(
    door_number = ifelse(
      str_detect(addr1, regex("FLOOR", ignore_case = TRUE)) & str_detect(addr2, "[0-9]+"), 
      str_extract(addr2, "[0-9]+"),  # Extracts the first number in address2
      door_number)  # Keep existing value if conditions are not met
  ) %>%
  
  mutate(
    # Remove single-letter values and replace with the next available address line
    cleaned_addr1 = ifelse(str_detect(cleaned_addr1, "^[A-Z]$"), 
                           coalesce(addr2, addr3, addr1), 
                           cleaned_addr1),
    
    # Remove leading whitespace before letters
    cleaned_addr1 = str_trim(cleaned_addr1),
    
    # Remove leading symbols like "-"
    cleaned_addr1 = str_remove(cleaned_addr1, "^-+\\s*"),
    
    # Remove numbers again
    cleaned_addr1 = str_remove(cleaned_addr1, "^[0-9]+[ ,]*")
  )

# Select relevant columns

tax_uprn_epc_merge_clean <- tax_uprn_epc_merge_clean %>%
  select(addr1, addr2, addr3, addr4, uprn, pcds, oa21cd, lsoa21cd, msoa21cd, 
         door_number, cleaned_addr1, address1, address2, address3, construction_age_band)

# Merge the cleaned datasets together using door number and cleaned_addr1

merged_data <- tax_uprn_epc_merge_clean %>%
  full_join(epc_data_clean, by = c("door_number", "cleaned_addr1"), relationship = "many-to-many")

merged_data <- merged_data %>%
  distinct(addr1, addr2, .keep_all = TRUE)

merged_data <- merged_data %>%
  distinct(addr1, uprn, .keep_all = TRUE)

merged_data <- merged_data %>%
  distinct(address1.y, address2.y, uprn, .keep_all = TRUE)

merged_data <- merged_data %>%
  distinct(address1.x, address2.x, uprn, .keep_all = TRUE)

merged_data <- merged_data %>%
  mutate(
    pcds = ifelse(is.na(pcds), postcode_new, pcds),  # Replace NA pcds with postcode_new
    construction_age_band = ifelse(is.na(construction_age_band), construction_age_band_new, construction_age_band)  # Replace NA construction_age_band with construction_age_band_new
  )

# Manually filling in postcodes

merged_data <- merged_data %>%
  mutate(
    pcds = case_when(
      addr1 == "1ST FLOOR FLAT" & addr2 == "100 TOWN STREET" & (is.na(pcds) | pcds == "") ~ "LS12 3HN",
      addr1 == "270B DEWSBURY ROAD" & addr2 == "HUNSLET" & (is.na(pcds) | pcds == "") ~ "LS11 6JQ",
      addr1 == "7A MAFEKING MOUNT" & addr2 == "BEESTON" & (is.na(pcds) | pcds == "") ~ "LS11 7BX",
      addr1 == "1 ACACIA COURT" & addr2 == "SEACROFT" & (is.na(pcds) | pcds == "") ~ "LS14 6ZP",
      cleaned_addr1 == "ACACIA FOLD" & (is.na(pcds) | pcds == "") ~ "LS14 6ZR",
      cleaned_addr1 == "ACACIA GARTH" & (is.na(pcds) | pcds == "") ~ "LS14 6ZP",
      cleaned_addr1 == "ACACIA TERRACE" & (is.na(pcds) | pcds == "") ~ "LS14 6ZQ",
      cleaned_addr1 == "MERCER WEST" & (is.na(pcds) | pcds == "") ~ "LS2 7FA",
      cleaned_addr1 == "MADISON EAST" & (is.na(pcds) | pcds == "") ~ "LS2 7PA",
      cleaned_addr1 == "Q3 RESIDENCE" & (is.na(pcds) | pcds == "") ~ "LS1 2FP",
      cleaned_addr1 == "CALVERT CRESCENT"& (is.na(pcds) | pcds == "") ~ "LS10 4WU",
      cleaned_addr1 == "QUARRY AVENUE" & (is.na(pcds) | pcds == "") ~ "LS25 4FR",
      cleaned_addr1 == "BLACKTHORN LANE" & (is.na(pcds) | pcds == "") ~ "LS16 6TJ",
      cleaned_addr1 == "BLUEBELL WAY" & (is.na(pcds) | pcds == "") ~ "LS14 6WD",
      cleaned_addr1 == "MOORLAND LANE" & (is.na(pcds) | pcds == "") ~ "BD4 0DU",
      cleaned_addr1 == "FARMER STREET" & (is.na(pcds) | pcds == "") ~ "WF3 3SY",
      cleaned_addr1 == "IRIS CRESCENT" & (is.na(pcds) | pcds == "") ~ "LS14 6WY",
      cleaned_addr1 == "ELDER WAY" & (is.na(pcds) | pcds == "") ~ "LS7 2FG",
      cleaned_addr1 == "HAZEL MOUNT" & (is.na(pcds) | pcds == "") ~ "LS25 1FF",
      cleaned_addr1 == "HETCHELL HOUSE" & (is.na(pcds) | pcds == "") ~ "LS14 3FT",
      cleaned_addr1 == "MILLSTONE WAY" & (is.na(pcds) | pcds == "") ~ "LS16 6FZ",
      cleaned_addr1 == "SOUTHWAITE PLACE" & (is.na(pcds) | pcds == "") ~ "LS14 6SR",
      cleaned_addr1 == "COLLEGE DRIVE" & (is.na(pcds) | pcds == "") ~ "LS18 4GZ",
      cleaned_addr1 == "WILLOW CHASE" & (is.na(pcds) | pcds == "") ~ "LS7 2FS",
      cleaned_addr1 == "WOODLAND GRANGE" & (is.na(pcds) | pcds == "") ~ "LS7 2FT",
      cleaned_addr1 == "MOORLAND GARDENS" & (is.na(pcds) | pcds == "") ~ "LS17 6JT",
      cleaned_addr1 == "ROSE CLOSE" & (is.na(pcds) | pcds == "") ~ "BD11 1FY",
      TRUE ~ pcds  # Keeps existing pcds if no conditions match
    )
  )

# Now fill in missing MSOA/LSOA/OA's:

# Use OA/LSOA/MSOA from addresses from the same postcode
merged_data <- merged_data %>%
  group_by(pcds) %>%  # Group by postcode
  fill(oa21cd, lsoa21cd, msoa21cd, .direction = "downup") %>%  # Fill missing values
  ungroup()

# Two more properties left which are missing the OA/LSOA/MSOA, replace these manually:
# 130 Chapel House Cardigan Road:
merged_data <- merged_data %>%
  mutate(
    oa21cd = ifelse(door_number == "130" & cleaned_addr1 == "CARDIGAN ROAD" & (is.na(oa21cd) | oa21cd == ""), "E00057642", oa21cd),
    lsoa21cd = ifelse(door_number == "130" & cleaned_addr1 == "CARDIGAN ROAD" & (is.na(lsoa21cd) | lsoa21cd == ""), "E01011441", lsoa21cd),
    msoa21cd = ifelse(door_number == "130" & cleaned_addr1 == "CARDIGAN ROAD" & (is.na(msoa21cd) | msoa21cd == ""), "E02002373", msoa21cd)
  )

# 3 Heather Row
merged_data <- merged_data %>%
  mutate(
    oa21cd = ifelse(pcds == "LS16 8FF" & (is.na(oa21cd) | oa21cd == ""), "E00057406", oa21cd),
    lsoa21cd = ifelse(pcds == "LS16 8FF" & (is.na(lsoa21cd) | lsoa21cd == ""), "E01011385", lsoa21cd),
    msoa21cd = ifelse(pcds == "LS16 8FF" & (is.na(msoa21cd) | msoa21cd == ""), "E02002345", msoa21cd)
  )

merged_data <- merged_data %>%
  mutate(
    addr1 = coalesce(address1.x, address1.y, addr1),
    addr2 = coalesce(address2.x, address2.y, addr2),
    addr3 = coalesce(address3.x, address3.y, addr3)
  ) %>%
  select(addr1, addr2, addr3, addr4, uprn, pcds, oa21cd, lsoa21cd, 
         msoa21cd, door_number, cleaned_addr1, construction_age_band)

write.csv(merged_data, "processed_data/full_address_data.csv", row.names = FALSE)
