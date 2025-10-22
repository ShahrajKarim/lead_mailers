## This script aims to generate personalised QR codes for use in Qualtrics
# The user must insert the file directory of qualtrics_link_file prior to running the code.
# The user must also change the file directory which they want to use to save the png images of qr_codes.

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
qualtrics_link_file <- "qr_codes/export-EMD_MydSxeKIFKwQtbX-2025-09-27T16-56-12-124Z.csv"

# Load in links for QR codes

urls <- read_csv(qualtrics_link_file) |>
  select("External Data Reference", "Link") |>
  right_join(address_data, by = c("External Data Reference" = "External.Data.Reference"))

# QR code name

# QR code file directory & file name - please change to relevant file directory when running script. 
qr_dir <- "qr_codes/qr_"

urls <- urls %>%
  mutate(qr_path = paste0("qr_codes/qr_", `External Data Reference`, ".png"))

# Generate the QR codes

walk2(urls$Link, urls$qr_path, function(link, path) {
  qr <- qr_code(link)
  png(path, width = 300, height = 300, bg = "white")  
  grid::grid.raster(!qr)
  dev.off()
})

# csv file for 
write.csv(urls, "processed_data/qr_code_data.csv", row.names = FALSE)

# To measure time
end <- Sys.time()
print(end - start)


