# This script aims to inject address data and QR codes into outreach letters

library(officer)
library(magrittr)
library(dplyr)
library(tidyr)
library(readr)

# Load address data with QR codes

qr_data <- read.csv("processed_data/qr_code_data.csv") |>
  filter(!is.na(treatment)) |>
  rename(mailer_id = `External Data Reference`)

survey_path <- "INSERT SURVEY PATH HERE"

survey_data <- read.csv(survey_path)

merged <- left_join(
  qr_data,
  survey_data,
  by = "External Data Reference"
  )

control <- merged |>
  filter(treatment == control)

treat_1 <- merged |>
  filter(treatment == general_risk)

treat_2 <- merged |>
  filter(treatment == personalized)

# Load in date
date <- format(Sys.Date(), "%d %B %Y")

# Loop to generate word documents
  
control_letter <- read_docx("letters/control_letter_september06_V1.docx") # Change to relevant file directory
treat_1_letter <- read_docx("letters/treat1_letter_september06_V1.docx") # Change to relevant file directory
treat_2_letter <- read_docx("letters/treat2_letter_september06_V1.docx") #Change to relevant file directory

for (i in 1:nrow(control)) {
  
  person <- control[i, ]
  doc <- control_letter %>%
    body_replace_all_text("<POSTCODE>", person$pcds, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr1>>", person$addr1, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr2>>", person$addr2, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr3>>", person$addr3, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr4>>", ifelse(is.na(person$addr4[[1]]), "", paste0(sample$addr4[[1]], " ")), only_at_cursor = FALSE) %>%
    body_replace_all_text("<<DATE>>", date, only_at_cursor = FALSE)
  
  # Replace <<qr>> with image
  
  if (!is.na(person$qr_path) && file.exists(person$qr_path)) {
    img_obj <- external_img(
      src = person$qr_path,
      width = 3.6 / 2.54,
      height = 3.6 / 2.54
      )

      doc <- body_replace_img_at_bkm(doc, bookmark = "qr_code", value = img_obj)
    }

    # Save each letter with the name of unique_id
    print(doc, target = paste0("letters/letter_", person$mailer_id, ".docx"))
    
}

for (i in 1:nrow(treat_1)) {
  
  person <- treat_1[i, ]
  doc <- control_letter %>%
    body_replace_all_text("<POSTCODE>", person$pcds, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr1>>", person$addr1, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr2>>", person$addr2, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr3>>", person$addr3, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr4>>", ifelse(is.na(person$addr4[[1]]), "", paste0(sample$addr4[[1]], " ")), only_at_cursor = FALSE) %>%
    body_replace_all_text("<<DATE>>", date, only_at_cursor = FALSE)
  
  # Replace <<qr>> with image
  
  if (!is.na(person$qr_path) && file.exists(person$qr_path)) {
    img_obj <- external_img(
      src = person$qr_path,
      width = 3.6 / 2.54,
      height = 3.6 / 2.54
    )
    
    doc <- body_replace_img_at_bkm(doc, bookmark = "qr_code", value = img_obj)
  }
  
  # Save each letter with the name of unique_id
  print(doc, target = paste0("letters/letter_", person$mailer_id, ".docx"))
  
}
 
for (i in 1:nrow(treat_2)) {
  
  person <- treat_2[i, ]
  doc <- control_letter %>%
    body_replace_all_text("<POSTCODE>", person$pcds, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr1>>", person$addr1, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr2>>", person$addr2, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr3>>", person$addr3, only_at_cursor = FALSE) %>%
    body_replace_all_text("<<addr4>>", ifelse(is.na(person$addr4[[1]]), "", paste0(sample$addr4[[1]], " ")), only_at_cursor = FALSE) %>%
    body_replace_all_text("<<DATE>>", date, only_at_cursor = FALSE)
  
  # Replace <<qr>> with image
  
  if (!is.na(person$qr_path) && file.exists(person$qr_path)) {
    img_obj <- external_img(
      src = person$qr_path,
      width = 3.6 / 2.54,
      height = 3.6 / 2.54
    )
    
    doc <- body_replace_img_at_bkm(doc, bookmark = "qr_code", value = img_obj)
  }
  
  # Save each letter with the name of unique_id
  print(doc, target = paste0("letters/letter_", person$mailer_id, ".docx"))
  
} 