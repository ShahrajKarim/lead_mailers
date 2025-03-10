# The list of residential addresses showed that we had 376k homes, but census data showed we had 342k households.
# This script aims to understand the reasons why there are 10% more homes than households in Leeds.
# We investigate vacant properties in Leeds at the LSOA level and find that this accounts for 50% of the difference between households and residential properties in Leeds.

library(readxl)
library(dplyr)
library(stringr)
library(ggplot2)

vacant_property_data <- read_excel("raw_data/numberofvacantdwellingsandsecondhomes.xlsx", 
                                   sheet = "1c") %>%
  rename(
    lsoa21cd = 1,         
    lsoa_name = 2,         
    vacant_dwellings = 3,  
    second_home = 4        
  )

vacant_property_data <- vacant_property_data %>%
  filter(str_detect(lsoa_name, "Leeds"))

# Note that counts that say "c" are less than 10. Clean by replacing with 0 (so we underestimate how many houses are vacant)
vacant_property_data <- vacant_property_data %>%
  mutate(
    vacant_dwellings = ifelse(vacant_dwellings == "c", 0, vacant_dwellings),
    second_home = ifelse(second_home == "c", 0, second_home)
  )

# Second homes in this case do not have a usual resident so add vacant dwellings with second homes

vacant_property_data <- vacant_property_data %>%
  group_by(lsoa21cd, lsoa_name) %>%
  summarise(
    vacant_dwellings = sum(as.numeric(vacant_dwellings), na.rm = TRUE),
    second_home = sum(as.numeric(second_home), na.rm = TRUE),
    total_vacant = vacant_dwellings + second_home,
    .groups = "drop"
  )

# There is a minimm of 15,610 vacant properties in Leeds - which explains ~50% of vacant properties in Leeds
total_vacant_properties <- sum(vacant_property_data$total_vacant, na.rm = TRUE)
print(total_vacant_properties)

property_data <- read.csv("processed_data/full_address_data.csv")

lsoa_counts <- property_data %>%
  count(lsoa21cd, name = "property_count")

vacant_property_data <- vacant_property_data %>%
  left_join(lsoa_counts, by = "lsoa21cd")

vacant_property_data <- vacant_property_data %>%
  mutate(vacant_proportion = 100 * total_vacant / property_count)

ggplot(vacant_property_data, aes(x = vacant_proportion)) +  
  geom_histogram(binwidth = 1, fill = "blue", color = "black", alpha = 0.7) +  
  labs(title = "Distribution of Vacant Properties in Leeds by LSOA",
       x = "Proportion of Properties Vacant",
       y = "Number of LSOAs") +
  theme_minimal()