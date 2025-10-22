# This script aims to generate a list of GPs to use in our survey. This is based on the NHS GP seach option from this website: https://www.nhs.uk/service-search/find-a-gp/

# We have 371,858 total postcodes and 18,174 unique postcodes in our residentialaddresses.csv file.
# However, many of these postcodes are close together and have significant overlap in terms of the GPs returned in the search.
# Additionally, scraping for each postcode would be incredibly time intensive and inefficient. 
# Therefore, in order to get a comprehensive list of GPs that residents may go to, we stratify the zip codes to the postcode sector level
# E.g. LS23 7XX and randomly sampled a postcode from each group. 
# We get a list of 40 postcodes in each sample.
# We then scrape the 50 closest GPs from each postcode in our sample and keep a list of the unique GPs.
# We repeat this process 10 times. Each sample level list returns anywhere from 306 to 312 GPs.
# Then, we merge our sample list of GPs from across our 10 samples and are left with a list of 317 GPs.

# Load libraries
library(tidyverse)
library(rvest)
library(xml2)
library(parallel)

# Set seed
set.seed(2025)

# ----------------------------- #
# Paths and holding folder
# ----------------------------- #
hold_dir <- "processed_data/tmp_gp"
if (!dir.exists(hold_dir)) dir.create(hold_dir, recursive = TRUE)

# ----------------------------- #
# Read and prepare postcode data
# ----------------------------- #
total_postcode <- read.csv("processed_data/residential_address_data.csv")

unique_postcode <- total_postcode %>%
  select(pcds) %>%
  distinct() %>%
  mutate(
    area  = gsub(" .*", "", pcds),
    zip   = substr(pcds, nchar(pcds) - 2, nchar(pcds)),
    index = paste0(area, " ", substr(zip, 1, 1))
  )

postcode_df_list <- list()

# List of postcode areas present in data
pc_list <- unique(unique_postcode$area)
pc_list <- pc_list[!is.na(pc_list)]

# ----------------------------- #
# Helper: flatten a list-like vector
# ----------------------------- #
tolist <- function(x) {
  templist <- c()
  for (i in seq(1, length(x))) {
    templist <- append(templist, x[[i]])
  }
  return(templist)
}

# ----------------------------- #
# Scraper for one dataframe of postcodes (one area at a time)
# ----------------------------- #
get_gp_list <- function(postcodes) {
  base_url <- "https://www.nhs.uk/service-search/find-a-gp/results/"
  
  gp <- data.frame(
    name = character(),
    address = character(),
    phone = character(),
    stringsAsFactors = FALSE
  )
  
  for (pc in postcodes$pcds) {
    
    # Build URL for the postcode page
    area_code <- sub(" .*", "", pc)
    post_code <- sub(".*? ", "", pc)
    url <- paste(base_url, area_code, "%20", post_code, sep = "")
    
    html <- tryCatch(read_html(url), error = function(e) NULL)
    if (is.null(html)) next
    
    # GP names
    test       <- html %>% html_elements("li")
    test_name  <- test %>% html_elements("h2") %>% html_text2()
    gp_names   <- sub(".*for ", "", test_name)
    gp_list    <- tolist(gp_names)
    
    # Addresses and phones
    add <- html %>% html_elements("li") %>% html_elements("p")
    
    address_list <- c()
    base  <- "//p[@id='address_0']"
    
    phone_list <- data.frame(index = character(), phone = character())
    p_base <- "//p[@id='phone_0']"
    
    for (j in seq(0, (length(gp_list) - 1))) {
      
      id <- paste0("address_", j)
      find_index <- gsub("address_0", id, base)
      
      nodes <- xml_find_all(add, find_index)
      node_text <- html_text2(nodes)
      node_text <- sub(".*organisation is ", "", node_text)
      address_list <- append(address_list, node_text)
      
      id_p <- paste0("phone_", j)
      find_index_p <- gsub("phone_0", id_p, p_base)
      
      nodes_p <- xml_find_all(add, find_index_p)
      node_text_p <- html_text2(nodes_p)
      node_text_p <- sub(".*organisation is ", "", node_text_p)
      phone_list[nrow(phone_list) + 1, ] <- c(as.character(j), node_text_p)
    }
    
    gp_info <- data.frame(
      name = gp_list,
      address = address_list,
      phone = phone_list$phone,
      stringsAsFactors = FALSE
    )
    
    gp <- full_join(gp, gp_info, by = c("name", "address", "phone"))
    
    gp$phone <- ifelse(nchar(gp$phone) > 4, gp$phone, NA)
    
    gp <- gp %>% filter(!duplicated(gp))
  }
  
  return(gp)
}

# ----------------------------- #
# Wrapper: run scraper for one area index; write interim chunk
# ----------------------------- #
get_list <- function(i) {
  library(tidyverse)
  library(rvest)
  library(xml2)
  library(parallel)
  
  data <- postcode_df_list[[i]]
  temp_list <- get_gp_list(data)
  temp_list <- temp_list %>% mutate(source_id = i)
  
  # Write interim CSV per area index
  out_path <- file.path(hold_dir, sprintf("gp_chunk_%03d.csv", i))
  suppressWarnings(write.csv(temp_list, out_path, row.names = FALSE))
  
  return(temp_list)
}

# ----------------------------- #
# Sampling rounds and parallel scraping
# ----------------------------- #
master_list <- list()

for (index in seq(1, 10)) {
  
  # Build the sampled postcode list per area (one postcode per sector per area)
  for (i in seq(1, length(pc_list))) {
    temp <- unique_postcode %>%
      filter(area == pc_list[i]) %>%
      group_by(index) %>%
      slice_sample(n = 1) %>%
      ungroup()
    postcode_df_list[[i]] <- temp
  }
  
  # Clear previous interim files for this round
  old_chunks <- list.files(hold_dir, pattern = "^gp_chunk_\\d+\\.csv$", full.names = TRUE)
  if (length(old_chunks)) file.remove(old_chunks)
  
  # Parallel execution
  num_cores <- max(1L, detectCores() - 1L)
  cl <- makeCluster(num_cores)
  
  # Load packages on workers
  clusterEvalQ(cl, { library(tidyverse); library(rvest); library(xml2); NULL })
  
  # Export required objects/functions
  clusterExport(cl,
                c("tolist", "get_gp_list", "get_list", "postcode_df_list", "hold_dir"),
                envir = environment())
  
  gp_list_df <- parLapply(cl, seq_along(postcode_df_list), get_list)
  
  stopCluster(cl)
  
  # Collapse interim files written by get_list(); fallback to in-memory if needed
  chunk_files <- list.files(hold_dir, pattern = "^gp_chunk_\\d+\\.csv$", full.names = TRUE)
  if (length(chunk_files)) {
    round_df <- bind_rows(lapply(chunk_files, read.csv, stringsAsFactors = FALSE))
  } else {
    round_df <- bind_rows(gp_list_df)
  }
  
  master_list[[index]] <- round_df
}

# ----------------------------- #
# Combine rounds and clean
# ----------------------------- #
master_all <- bind_rows(master_list, .id = "index")

master_all <- master_all %>% distinct(name, .keep_all = TRUE)

# Extra duplicate handling
master_all <- master_all[!duplicated(master_all[c("name", "address")]), ]

master_all$name <- str_to_title(master_all$name)
master_all$address <- paste0(
  str_to_title(substr(master_all$address, 1, nchar(master_all$address) - 8)),
  toupper(substr(master_all$address, nchar(master_all$address) - 7, nchar(master_all$address)))
)

master_all <- master_all %>%
  group_by(name) %>%
  mutate(name = if (n() > 1) paste(name, row_number()) else name) %>%
  ungroup()

if ("Clarendon At Manningham Health Centre" %in% master_all$name &
    "Clarendon Medical Centre" %in% master_all$name) {
  master_all <- master_all[master_all$name != "Clarendon At Manningham Health Centre", ]
}

master_all <- master_all %>% filter(!startsWith(name, "Inactive Profile"))

master_all <- master_all %>% arrange(name)

master_all <- master_all %>% select(-"index", -"source_id")

# ----------------------------- #
# Write final output
# ----------------------------- #
write.csv(master_all, "processed_data/webscraped_gp_data.csv", row.names = FALSE)