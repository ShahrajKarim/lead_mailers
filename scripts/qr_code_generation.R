## This script aims to generate personalised QR codes for use in Qualtrics
# For the purpose of this exercise I am using a sample of 10 addresses to test if this works and to prevent using data

# To measure time
start <- Sys.time()

library(qrcode)
library(purrr)
library(dplyr)
library(magrittr)
library(grid)
library(png) 

address_data <- read.csv("processed_data/pilot_sampled_addresses.csv")

# Change this to the actual file directory
# qualtrics_link_file <- "qr_codes/export-EMD_MydSxeKIFKwQtbX-2025-09-27T16-56-12-124Z.csv"

# Load in links for QR codes

urls <- read_csv(qualtrics_link_file) |>
  right_join(address_data, by = c("External Data Reference" = "External.Data.Reference"))

# QR code name

urls <- urls %>%
  mutate(qr_path = paste0("qr_codes/qr_", `External Data Reference`, ".png"))

# urls <- urls[1:1000, ]

# Generate the QR codes

walk2(urls$Link, urls$qr_path, function(link, path) {
  qr <- qr_code(link)
  png(path, width = 300, height = 300, bg = "white")  
  grid::grid.raster(!qr)
  dev.off()
})

# csv file for 
write.csv(urls, "processed_data/test_links_250825.csv", row.names = FALSE)

# To measure time
end <- Sys.time()
print(end - start)

# write.csv(address_data, "processed_data/sample_residential_addresses.csv") # Save this sample for future reproducibility when trialing

