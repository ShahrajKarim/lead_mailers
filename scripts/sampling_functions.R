library(dplyr)
library(stringr)
library(dplyr)

# ----- Baseline Allocation Functions ----- #

# Function to allocate exactly n items given a weight vector.
allocate_exactly_n <- function(weights, n) {
  exact     <- weights * n
  int_part  <- floor(exact)
  frac_part <- exact - int_part
  remaining <- n - sum(int_part)
  additional <- numeric(length(weights))
  if (remaining > 0) {
    chosen <- sample(seq_along(weights), size = remaining, prob = frac_part, replace = FALSE)
    additional[chosen] <- 1
  }
  int_part + additional
}

# Function to allocate a total number to groups given their ratios.
allocate_by_ratios <- function(ratios, total) {
  exact <- ratios * total
  ints  <- floor(exact)
  remainder <- total - sum(ints)
  if (remainder > 0) {
    extra_order <- order(exact - ints, decreasing = TRUE)
    ints[extra_order[seq_len(remainder)]] <- ints[extra_order[seq_len(remainder)]] + 1
  }
  ints
}

### Functions to simulate treatment allocation counts by house type.
# For old houses, allocate to three treatments: control, general_risk, personalized.
old_treatments <- function(n) {
  if(n > 0) {
    counts <- as.vector(rmultinom(1, n, prob = rep(1/3, 3)))
    names(counts) <- c("control", "general_risk", "personalized")
    counts
  } else {
    c(control = 0, general_risk = 0, personalized = 0)
  }
}

# For new houses, only allocate "control" and "general_risk".
new_treatments <- function(n) {
  if(n > 0) {
    counts <- as.vector(rmultinom(1, n, prob = rep(1/2, 2)))
    names(counts) <- c("control", "general_risk")
    counts
  } else {
    c(control = 0, general_risk = 0)
  }
}

unknown_treatments <- function(n) {
  if(n > 0) {
    counts <- as.vector(rmultinom(1, n, prob = rep(1/2, 2)))
    names(counts) <- c("control", "general_risk")
    counts
  } else {
    c(control = 0, general_risk = 0)
  }
}

# Main function to create the baseline allocation.
# Assumes lsoa_summary has the following columns:
# - lsoa21cd, w_eligible_lsoa, ratio_old, ratio_new, ratio_unknown, n_lsoa (to be computed)
# Also, n_letters is the overall sampling budget.
create_baseline_allocation <- function(lsoa_summary, n_letters) {
  
  # Allocate exactly n_letters to each LSOA based on eligibility weight.
  lsoa_summary <- lsoa_summary %>%
    mutate(n_lsoa = allocate_exactly_n(w_eligible_lsoa, n_letters))
  
  # Allocate letters to each age group within each LSOA.
  lsoa_summary <- lsoa_summary %>%
    rowwise() %>%
    mutate(alloc = list(allocate_by_ratios(c(ratio_old, ratio_new, ratio_unknown), n_lsoa))) %>%
    ungroup() %>%
    mutate(
      n_old_alloc     = map_dbl(alloc, 1),
      n_new_alloc     = map_dbl(alloc, 2),
      n_unknown_alloc = map_dbl(alloc, 3)
    ) %>%
    select(-alloc)
  
  # Assign treatment arms via multinomial draws.
  final_allocations <- lsoa_summary %>%
    rowwise() %>%
    mutate(
      old_alloc     = list(old_treatments(n_old_alloc)),
      new_alloc     = list(new_treatments(n_new_alloc)),
      unknown_alloc = list(unknown_treatments(n_unknown_alloc))
    ) %>%
    ungroup() %>%
    select(lsoa21cd, n_lsoa, n_old_alloc, n_new_alloc, n_unknown_alloc, 
           old_alloc, new_alloc, unknown_alloc)
  
  # Create final data frame.
  old_df <- final_allocations %>% 
    select(lsoa21cd, n_lsoa, n_old_alloc, old_alloc) %>% 
    mutate(old_df = map(old_alloc, ~ tibble(treatment = names(.x), count = .x))) %>% 
    select(-old_alloc) %>% 
    unnest(old_df) %>% 
    mutate(house_age = "old") %>% 
    rename(n_age_alloc = n_old_alloc)
  
  new_df <- final_allocations %>% 
    select(lsoa21cd, n_lsoa, n_new_alloc, new_alloc) %>% 
    mutate(new_df = map(new_alloc, ~ tibble(treatment = names(.x), count = .x))) %>% 
    select(-new_alloc) %>% 
    unnest(new_df) %>% 
    mutate(house_age = "new") %>% 
    rename(n_age_alloc = n_new_alloc)
  
  unknown_df <- final_allocations %>% 
    select(lsoa21cd, n_lsoa, n_unknown_alloc, unknown_alloc) %>% 
    mutate(unknown_df = map(unknown_alloc, ~ tibble(treatment = names(.x), count = .x))) %>% 
    select(-unknown_alloc) %>% 
    unnest(unknown_df) %>% 
    mutate(house_age = "unknown") %>% 
    rename(n_age_alloc = n_unknown_alloc)
  
  final_allocation <- bind_rows(old_df, new_df, unknown_df) %>% 
    arrange(lsoa21cd, house_age, treatment) %>% 
    select(lsoa21cd, house_age, treatment, count, n_lsoa, n_age_alloc)
  
  return(final_allocation)
}



# ----- Address sampling function ----- #

sample_addresses_from_allocation <- function(baseline_allocation, address_data, seed = 2025) {
  
  # Set seed
  set.seed(seed)
  
  address_data <- address_data %>% mutate(.row_id = row_number()) # temporary row id ()
  
  baseline_allocation <- baseline_allocation %>% filter(count > 0)
  
  out <- list(); j <- 0L
  
  for (lsoa in unique(baseline_allocation$lsoa21cd)) {
    
    alloc_lsoa <- baseline_allocation %>% filter(lsoa21cd == lsoa)
    lsoa_pool  <- address_data %>% filter(lsoa21cd == lsoa)
    
    for (age in unique(alloc_lsoa$house_age)) {
      
      age_pool <- switch(age,
                         "old"     = which(!is.na(lsoa_pool$house_age_old)     & lsoa_pool$house_age_old == 1),
                         "new"     = which(!is.na(lsoa_pool$house_age_old)     & lsoa_pool$house_age_old == 0),
                         "unknown" = which(!is.na(lsoa_pool$house_age_unknown) & lsoa_pool$house_age_unknown),
                         integer(0)
      )
      
      pool_df <- lsoa_pool[age_pool, , drop = FALSE]
      alloc_age <- alloc_lsoa %>% filter(house_age == age)
      
      for (i in seq_len(nrow(alloc_age))) {
        
        n_draw <- alloc_age$count[i]
        n <- nrow(pool_df)
        
        if (n_draw == 0L) next
        
        sample_rows <- sample.int(n, size = n_draw, replace = FALSE)
        
        selection <- pool_df[sample_rows, , drop = FALSE] %>%
          mutate(
            house_age = age,
            treatment = alloc_age$treatment[i],
            count     = n_draw
          )
        
        # remove sampled rows from the pool before next treatment draw
        pool_df <- pool_df[-sample_rows, , drop = FALSE]
        
        j <- j + 1L
        out[[j]] <- selection %>%
          select(.row_id, treatment, count, house_age)
      }
    }
  }
  
  assignments <- bind_rows(out)
  
  result <- address_data %>%
    left_join(assignments, by = ".row_id") %>%
    mutate(count = coalesce(count, 0L)) %>%
    select(addr1, addr2, addr3, addr4, uprn, pcds, oa21cd, lsoa21cd, msoa21cd, door_number, epc_age_start, epc_age_end, house_age_old, house_age_unknown, treatment)
  
  return(result)
  
}
