## This script calls in census data at the OA level for Leeds

library(dplyr)
library(nomisr)
library(purrr)
library(readr)  


child_count_0to4_years <- nomis_get_data(id = "NM_2221_1", C_SEX = 0, geography = "TYPE150", C2021_AGE_24 = 1, C2021_AGE_24 = 2,
                         select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
  rename("under_yo4_count" = "OBS_VALUE", "oa21cd"= "GEOGRAPHY_CODE")

1

## Country of Birth Data

uk_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 1, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("uk_COB" = "OBS_VALUE", "oa21cd"= "GEOGRAPHY_CODE")

europe_eu_countries_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 1002, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("europe_eu_countries_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

europe_non_eu_countries_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 6, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("europe_non_eu_countries_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

africa_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 7, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("africa_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

asia_middle_east_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 8, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("asia_middle_east_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

americas_carribean_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 9, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("americas_carribean_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

antarctica_oceana_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 10, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("antarctica_oceana_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

british_overseas_COB <- nomis_get_data(id = "NM_2024_1", time = "latest", geography = "TYPE150", C2021_COB_12 = 11, measures = 20100,
                                 select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                 rename("british_overseas_COB" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

## Ethnicity Data

bangladesh_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 12,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("bangladesh_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

chinese_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 13,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("chinese_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

indian_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 11,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("indian_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

pakistani_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 12,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("pakistani_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

asian_other_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 14,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("asian_other_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

african_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 16,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("african_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

caribbean_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 15,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("caribbean_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

afro_caribbean_other_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 17,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("afro_caribbean_other_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

white_english_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 1,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("white_english_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

white_irish_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 2,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("white_irish_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

white_gypsy_irish_traveller_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 3,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("white_gypsy_irish_traveller_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

white_roma_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 4,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("white_roma_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

white_other_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 5,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("white_other_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

arab_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 18,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("arab_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")

any_other_ethnicity <- nomis_get_data(id = "NM_2041_1", time = "latest", geography = "TYPE150", measures = 20100, C2021_ETH_20 = 19,
                                select = c("GEOGRAPHY_CODE", "OBS_VALUE")) %>%
                                rename("any_other_ethnicity" = "OBS_VALUE", "oa21cd" = "GEOGRAPHY_CODE")


## Merge all data

oa_lsoa_msoa_lookup_Dec_2021 <- read.csv("../raw_data/geography_lookup_Dec_2021.csv")

oa_lsoa_msoa_lookup_Dec_2021 <- oa_lsoa_msoa_lookup_Dec_2021 %>%
                                rename(oa21cd = OA21CD)

dataframes <- list(
  oa_lsoa_msoa_lookup_Dec_2021,
  child_count_0to4_years, uk_COB, europe_eu_countries_COB, europe_non_eu_countries_COB, 
  africa_COB, asia_middle_east_COB, americas_carribean_COB, antarctica_oceana_COB, 
  british_overseas_COB, bangladesh_ethnicity, chinese_ethnicity, indian_ethnicity, 
  pakistani_ethnicity, asian_other_ethnicity, african_ethnicity, caribbean_ethnicity, 
  afro_caribbean_other_ethnicity, white_english_ethnicity, white_irish_ethnicity, 
  white_gypsy_irish_traveller_ethnicity, white_roma_ethnicity, white_other_ethnicity, 
  arab_ethnicity, any_other_ethnicity
)


oa21cd_data <- reduce(dataframes, left_join, by = "oa21cd")

leeds_eligbility_data <- oa21cd_data %>%
  filter(LAD22CD == "E08000035")

census_data_2011_msoa %>%
  write_csv("../processed_data/census_data_2021_oa.csv")
