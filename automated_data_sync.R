# Enhanced automated_data_sync.R
# This version creates a flag file when new data is synced,
# which helps the app detect when to reapply pitch type modifications

library(RCurl)
library(readr)
library(dplyr)
library(lubridate)
library(stringr)

# Load CSV filtering utilities
source("csv_filter_utils.R")

# FTP credentials (practice and v3 use separate accounts)
PRACTICE_FTP <- list(
  host = "ftp.trackmanbaseball.com",
  user = "Jared%20Gaynor",
  pass = "Wev4SdE2a8"
)
V3_FTP <- list(
  host = "ftp.trackmanbaseball.com",
  user = "UNewMexico",
  pass = "nPyLmUW9gJ"
)
PRACTICE_FTP$userpwd <- paste0(PRACTICE_FTP$user, ":", PRACTICE_FTP$pass)
V3_FTP$userpwd <- paste0(V3_FTP$user, ":", V3_FTP$pass)

# Local data directories
LOCAL_DATA_DIR      <- "data/"
LOCAL_PRACTICE_DIR  <- file.path(LOCAL_DATA_DIR, "practice")
LOCAL_V3_DIR        <- file.path(LOCAL_DATA_DIR, "v3")

# Ensure data directories exist
dir.create(LOCAL_DATA_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(LOCAL_PRACTICE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(LOCAL_V3_DIR, recursive = TRUE, showWarnings = FALSE)

# Function to list files in FTP directory
list_ftp_files <- function(ftp_path, creds) {
  url <- paste0("ftp://", creds$host, ftp_path)
  tryCatch({
    files <- getURL(url, userpwd = creds$userpwd, ftp.use.epsv = FALSE, dirlistonly = TRUE)
    strsplit(files, "\n")[[1]]
  }, error = function(e) {
    cat("Error listing files in", ftp_path, ":", e$message, "\n")
    character(0)
  })
}

# Function to download CSV file (no filtering - app will handle filtering)
download_csv <- function(remote_file, local_file, creds) {
  filename <- basename(remote_file)
  if (should_exclude_csv(filename)) {
    return(FALSE)
  }
  if (file.exists(local_file)) {
    cat("Skipping existing file:", basename(local_file), "\n")
    return(FALSE)
  }
  
  url <- paste0("ftp://", creds$host, remote_file)
  tryCatch({
    temp_file <- tempfile(fileext = ".csv")
    bin <- RCurl::getBinaryURL(url, userpwd = creds$userpwd, ftp.use.epsv = FALSE)
    writeBin(bin, temp_file)
    data <- read_csv(temp_file, show_col_types = FALSE)
    
    if (nrow(data) > 0) {
      write_csv(data, local_file)
      cat("Downloaded", nrow(data), "rows to", local_file, "\n")
      unlink(temp_file)
      return(TRUE)
    } else {
      cat("No data found in", remote_file, "\n")
      unlink(temp_file)
      return(FALSE)
    }
  }, error = function(e) {
    cat("Error processing", remote_file, ":", e$message, "\n")
    return(FALSE)
  })
}

# Function to sync practice data (2025 folder with MM/DD structure)
sync_practice_data <- function() {
  cat("Syncing practice data...\n")
  years <- as.character(2025:year(Sys.Date()))
  downloaded_count <- 0
  
  for (yr in years) {
    practice_base_path <- paste0("/practice/", yr, "/")
    months <- list_ftp_files(practice_base_path, PRACTICE_FTP)
    month_dirs <- months[grepl("^\\d{2}$", months)]
    
    for (month_dir in month_dirs) {
      month_path <- paste0(practice_base_path, month_dir, "/")
      cat("Checking practice month:", yr, month_dir, "\n")
      
      days <- list_ftp_files(month_path, PRACTICE_FTP)
      day_dirs <- days[grepl("^\\d{2}$", days)]
      
      for (day_dir in day_dirs) {
        day_path <- paste0(month_path, day_dir, "/")
        cat("Processing practice date:", yr, "/", month_dir, "/", day_dir, "\n")
        
        files_in_day <- list_ftp_files(day_path, PRACTICE_FTP)
        csv_files <- files_in_day[grepl("\\.csv$", files_in_day, ignore.case = TRUE)]
        csv_files <- csv_files[!grepl("playerpositioning", csv_files, ignore.case = TRUE)]
        
        for (file in csv_files) {
          remote_path <- paste0(day_path, file)
          local_path <- file.path(LOCAL_PRACTICE_DIR, paste0("practice_", yr, "_", month_dir, "_", day_dir, "_", file))
          
          if (download_csv(remote_path, local_path, PRACTICE_FTP)) {
            downloaded_count <- downloaded_count + 1
          }
          
          Sys.sleep(0.1)
        }
      }
    }
  }
  
  cat("Practice sync complete:", downloaded_count, "files downloaded\n")
  return(downloaded_count > 0)
}

# Function to check if file date is in allowed ranges
is_date_in_range <- function(file_path) {
  date_match <- stringr::str_match(file_path, "(20\\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01])")
  
  if (is.na(date_match[1])) {
    return(TRUE)
  }
  
  file_date <- as.Date(paste(date_match[2], date_match[3], date_match[4], sep = "-"))
  start_date <- as.Date("2025-08-10")
  return(file_date >= start_date)
}

# Function to sync v3 data with date filtering
sync_v3_data <- function() {
  cat("Syncing v3 data with date filtering...\n")
  years <- as.character(2025:year(Sys.Date()))
  downloaded_count <- 0
  
  for (yr in years) {
    v3_base_path <- paste0("/v3/", yr, "/")
    
    months <- list_ftp_files(v3_base_path, V3_FTP)
    month_dirs <- months[grepl("^\\d{2}$", months)]
    
    for (month_dir in month_dirs) {
      month_path <- paste0(v3_base_path, month_dir, "/")
      cat("Checking month:", yr, month_dir, "\n")
      
      days <- list_ftp_files(month_path, V3_FTP)
      day_dirs <- days[grepl("^\\d{2}$", days)]
      
      for (day_dir in day_dirs) {
        day_path <- paste0(month_path, day_dir, "/")
        full_date_path <- paste0(yr, "/", month_dir, "/", day_dir)
        
        if (!is_date_in_range(full_date_path)) {
          next
        }
        
        cat("Processing date:", full_date_path, "\n")
        
        files_in_day <- list_ftp_files(day_path, V3_FTP)
        
        if ("CSV" %in% files_in_day) {
          csv_path <- paste0(day_path, "CSV/")
          csv_files <- list_ftp_files(csv_path, V3_FTP)
          csv_files <- csv_files[grepl("\\.csv$", csv_files, ignore.case = TRUE)]
          csv_files <- csv_files[!grepl("playerpositioning", csv_files, ignore.case = TRUE)]
          
          for (file in csv_files) {
            remote_path <- paste0(csv_path, file)
            local_path <- file.path(LOCAL_V3_DIR, paste0("v3_", yr, "_", month_dir, "_", day_dir, "_", file))
            
            if (download_csv(remote_path, local_path, V3_FTP)) {
              downloaded_count <- downloaded_count + 1
            }
            
            Sys.sleep(0.1)
          }
        } else {
          csv_files <- files_in_day[grepl("\\.csv$", files_in_day, ignore.case = TRUE)]
          csv_files <- csv_files[!grepl("playerpositioning|unverified", csv_files, ignore.case = TRUE)]
          
          for (file in csv_files) {
            remote_path <- paste0(day_path, file)
            local_path <- file.path(LOCAL_V3_DIR, paste0("v3_", yr, "_", month_dir, "_", day_dir, "_", file))
            
            if (download_csv(remote_path, local_path, V3_FTP)) {
              downloaded_count <- downloaded_count + 1
            }
            
            Sys.sleep(0.1)
          }
        }
      }
    }
  }
  
  cat("V3 sync complete:", downloaded_count, "files downloaded\n")
  return(downloaded_count > 0)
}

# Function to remove duplicate data across all CSV files
deduplicate_files <- function() {
  cat("Starting deduplication process...\n")
  csv_files <- list.files(LOCAL_DATA_DIR, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  
  if (length(csv_files) == 0) {
    cat("No CSV files found for deduplication\n")
    return(FALSE)
  }
  
  all_data <- list()
  
  for (file in csv_files) {
    tryCatch({
      data <- read_csv(file, show_col_types = FALSE, col_types = cols(.default = "c"))
      if (nrow(data) > 0) {
        data$SourceFile <- basename(file)
        all_data[[length(all_data) + 1]] <- data
      }
    }, error = function(e) {
      cat("Error reading", file, ":", e$message, "\n")
    })
  }
  
  if (length(all_data) == 0) {
    cat("No valid data found in CSV files\n")
    return(FALSE)
  }
  
  combined_data <- bind_rows(all_data)
  key_cols <- c("Date", "Pitcher", "Batter", "PitchNo", "PlateLocSide", "PlateLocHeight",
                "RelSpeed", "TaggedPitchType", "Balls", "Strikes")
  available_key_cols <- intersect(key_cols, names(combined_data))
  
  if (length(available_key_cols) == 0) {
    cat("Warning: No key columns found for deduplication. Using all columns.\n")
    available_key_cols <- setdiff(names(combined_data), "SourceFile")
  }
  
  original_count <- nrow(combined_data)
  deduplicated_data <- combined_data %>%
    distinct(across(all_of(available_key_cols)), .keep_all = TRUE)
  
  duplicates_removed <- original_count - nrow(deduplicated_data)
  
  if (duplicates_removed > 0) {
    cat("Removed", duplicates_removed, "duplicate rows\n")
    for (source_file in unique(deduplicated_data$SourceFile)) {
      file_data <- deduplicated_data %>%
        filter(SourceFile == source_file) %>%
        select(-SourceFile)
      
      if (nrow(file_data) > 0) {
        original_path <- csv_files[basename(csv_files) == source_file]
        if (length(original_path) == 1) {
          write_csv(file_data, original_path)
          cat("Rewrote", original_path, "with", nrow(file_data), "unique rows\n")
        }
      }
    }
  } else {
    cat("No duplicates found\n")
  }
  
  return(duplicates_removed > 0)
}

# Main sync function
main_sync <- function() {
  cat("Starting UNM data sync at", as.character(Sys.time()), "\n")
  start_time <- Sys.time()
  last_sync_file <- file.path(LOCAL_DATA_DIR, "last_sync.txt")
  if (!file.exists(last_sync_file)) {
    cat("First run detected - cleaning old data files\n")
    old_files <- list.files(LOCAL_DATA_DIR, pattern = "\\.(csv|txt)$", full.names = TRUE, recursive = TRUE)
    if (length(old_files) > 0) {
      file.remove(old_files)
      cat("Cleaned", length(old_files), "old data files\n")
    }
  } else {
    cat("Incremental sync - keeping existing files\n")
  }
  
  practice_updated <- sync_practice_data()
  v3_updated <- sync_v3_data()
  
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  cat("Data sync completed in", round(duration, 2), "minutes\n")
  
  if (practice_updated || v3_updated) {
    deduplicate_files()
  }
  
  writeLines(as.character(Sys.time()), file.path(LOCAL_DATA_DIR, "last_sync.txt"))
  if (practice_updated || v3_updated) {
    writeLines(as.character(Sys.time()), file.path(LOCAL_DATA_DIR, "new_data_flag.txt"))
  }
  
  return(practice_updated || v3_updated)
}

if (!interactive()) {
  data_updated <- main_sync()
  if (!data_updated) {
    cat("No new data found during sync - this is normal for incremental sync\n")
  } else {
    cat("New data was downloaded and processed\n")
  }
  cat("Sync completed successfully\n")
}
