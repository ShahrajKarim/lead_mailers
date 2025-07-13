# This script aims to inject address data and QR codes into outreach letters

library(officer)
library(magrittr)
library(dplyr)

# Load address data with QR codes

address_data <- read.csv("processed_data/sample_residential_addresses.csv") # Using sample now - replace with full data once complete

# Loop to generate word documents

for (i in 1:nrow(address_data)) {
  person <- address_data[i, ]
  
  # Load the template outreach letter
  doc <- read_docx("letters/letter_template.docx") %>%
    body_replace_all_text("<<addr1>>", person$addr1, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr2>>", person$addr2, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr3>>", person$addr3, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr4>>", person$addr4, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<pcds>>", person$pcds, only_at_cursor = FALSE)
  
  # Replace <<qr>> with image
  if (!is.na(person$qr_path) && file.exists(person$qr_path)) {
    img_obj <- external_img(
      src = person$qr_path,
      width = 1.45 / 2.54,
      height = 1.29 / 2.54
    )
    
    doc <- body_replace_img_at_bkm(doc, bookmark = "qr_code", value = img_obj)
  }
  
  # Save each letter with the name of unique_id
  print(doc, target = paste0("letters/letter_", person$unique_id, ".docx"))
}