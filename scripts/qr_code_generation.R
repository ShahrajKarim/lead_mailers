## This script aims to generate personalised QR codes for use in Qualtrics
# For the purpose of this exercise I am using a sample of 10 addresses to test if this works and to prevent using data

library(qrcode)
library(purrr)
library(dplyr)
library(magrittr)
library(grid)
library(png) 

address_data <- read.csv("processed_data/residential_address_data.csv")

# Generate unique identifiers

# address_data <- address_data |> mutate(unique_id = sample(1e6:9.999999e6, n(), replace = FALSE))

address_data <- address_data |>
  slice_sample(n = 10) # sample 10 

# Anonymous ID for Qualtrics

url <- "https://qualtricsxmdmvj4pg46.qualtrics.com/jfe/form/SV_0GSBWqyDtnrcDnE" # this code does not work currently

address_data <- address_data |>
  mutate(qualtrics_link = paste0(url, "?id=", unique_id))

# QR code name

address_data <- address_data %>%
  mutate(qr_path = paste0("qr_codes/qr_", unique_id, ".png"))

# Generate the QR codes

walk2(address_data$qualtrics_link, address_data$qr_path, function(link, path) {
  qr <- qr_code(link)
  png(path, width = 300, height = 300, bg = "white")  
  grid::grid.raster(!qr)
  dev.off()
})

write.csv(address_data, "processed_data/sample_residential_addresses.csv") # Save this sample for future reproducibility when trialing

