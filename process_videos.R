#!/usr/bin/env Rscript
#
# Manual/iPhone video uploader to Cloudinary (updates data/video_map_manual.csv)
# Mirrors PCU workflow for adding non-TrackMan videos.

suppressPackageStartupMessages({
  library(httr2)
  library(readr)
  library(dplyr)
  library(glue)
  library(stringr)
  library(purrr)
})

now_iso <- function() format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

ensure_env <- function(name) {
  val <- Sys.getenv(name, unset = "")
  if (!nzchar(val)) stop(glue("Environment variable '{name}' must be set."))
  val
}

parse_video_filename <- function(filename, folder_name) {
  number_match <- str_extract(filename, "\\d+")
  if (is.na(number_match)) {
    warning(glue("Could not extract number from iPhone video filename: {filename}"))
    return(NULL)
  }
  csv_file <- find_matching_csv_by_folder(folder_name)
  list(
    session_id = folder_name,
    play_id = paste0("pitch_", number_match),
    filename = filename,
    csv_file = csv_file,
    sort_number = as.numeric(number_match)
  )
}

find_matching_csv_by_folder <- function(folder_name) {
  csv_files <- list.files("data", pattern = "\\.csv$", full.names = TRUE)
  csv_files <- csv_files[!grepl("video_map\\.csv$", csv_files)]
  if (length(csv_files) == 0) {
    warning("No CSV files found in data/ directory")
    return(NULL)
  }
  exact_match <- csv_files[grepl(paste0("^", folder_name), basename(csv_files))]
  if (length(exact_match) > 0) {
    cat(glue("  📅 Matched folder '{folder_name}' to CSV: {basename(exact_match[1])}\n"))
    return(exact_match[1])
  }
  cat(glue("  ⚠️  Could not find CSV matching folder '{folder_name}'\n"))
  cat(glue("     Available CSV files:\n"))
  for (csv in csv_files) {
    cat(glue("       - {basename(csv)}\n"))
  }
  NULL
}

upload_to_cloudinary <- function(file_path, session_id, play_id, camera_slot, cloud_name, preset) {
  cat(glue("  📤 Uploading {basename(file_path)} to Cloudinary...\n"))
  public_id <- glue("trackman/{session_id}/{camera_slot}/{substr(tolower(play_id), 1, 12)}")
  upload_url <- glue("https://api.cloudinary.com/v1_1/{cloud_name}/video/upload")
  response <- request(upload_url) |>
    req_body_multipart(
      upload_preset = preset,
      file = curl::form_file(file_path),
      public_id = public_id,
      resource_type = "video"
    ) |>
    req_timeout(300) |>
    req_perform()
  if (resp_status(response) >= 400) {
    stop(glue("Cloudinary upload failed: {resp_body_string(response)}"))
  }
  result <- resp_body_json(response)
  cat(glue("  ✅ Uploaded: {result$secure_url}\n"))
  result
}

update_video_map <- function(session_id, play_id, camera_slot, cloudinary_result, video_map_path) {
  if (file.exists(video_map_path)) {
    video_map <- read_csv(video_map_path, show_col_types = FALSE)
    if ("uploaded_at" %in% names(video_map)) {
      video_map$uploaded_at <- as.character(video_map$uploaded_at)
    }
  } else {
    dir.create(dirname(video_map_path), showWarnings = FALSE, recursive = TRUE)
    video_map <- tibble()
  }
  new_row <- tibble(
    session_id = session_id,
    play_id = tolower(play_id),
    camera_slot = camera_slot,
    camera_name = "Phone",
    camera_target = "",
    video_type = "Manual",
    azure_blob = "",
    azure_md5 = "",
    cloudinary_url = cloudinary_result$secure_url,
    cloudinary_public_id = cloudinary_result$public_id,
    uploaded_at = now_iso()
  )
  video_map <- bind_rows(video_map, new_row) %>% distinct()
  write_csv(video_map, video_map_path)
  cat(glue("  📝 Updated video_map_manual.csv\n"))
}

process_video_folder <- function(session_folder, cloud_name, preset, video_map_path) {
  folder_name <- basename(session_folder)
  cat(glue("📁 Processing session folder: {folder_name}\n"))
  camera_folders <- list.dirs(session_folder, recursive = FALSE)
  camera_folders <- camera_folders[basename(camera_folders) %in% c("VideoClip2", "VideoClip3")]
  if (length(camera_folders) == 0) {
    cat("  ⚠️  No VideoClip2 or VideoClip3 folders found\n")
    return(character())
  }
  processed_files <- character()
  for (camera_folder in camera_folders) {
    camera_slot <- basename(camera_folder)
    cat(glue("  📹 Processing {camera_slot} videos...\n"))
    video_files <- list.files(
      camera_folder, 
      pattern = "\\.(mp4|mov|avi|mkv|webm)$", 
      ignore.case = TRUE,
      full.names = TRUE
    )
    if (length(video_files) == 0) {
      cat(glue("    ℹ️  No video files found in {camera_slot}\n"))
      next
    }
    video_info <- list()
    for (video_file in video_files) {
      parsed <- parse_video_filename(basename(video_file), folder_name)
      if (!is.null(parsed)) {
        parsed$full_path <- video_file
        video_info[[length(video_info) + 1]] <- parsed
      }
    }
    if (length(video_info) == 0) {
      cat(glue("    ⚠️  No valid video files found in {camera_slot}\n"))
      next
    }
    video_info <- video_info[order(sapply(video_info, function(x) x$sort_number))]
    cat(glue("    📱 Found {length(video_info)} iPhone videos, processing in order:\n"))
    for (info in video_info) {
      cat(glue("       {info$filename} → {info$play_id}\n"))
    }
    for (i in seq_along(video_info)) {
      info <- video_info[[i]]
      tryCatch({
        cat(glue("  🎥 Processing: {info$filename} (sequence {i})\n"))
        cloudinary_result <- upload_to_cloudinary(
          info$full_path, 
          info$session_id, 
          info$play_id, 
          camera_slot, 
          cloud_name, 
          preset
        )
        update_video_map(
          info$session_id, 
          info$play_id, 
          camera_slot, 
          cloudinary_result, 
          video_map_path
        )
        processed_files <- c(processed_files, info$full_path)
        cat(glue("  ✅ Successfully processed {info$filename} → sequence {i}\n\n"))
      }, error = function(e) {
        cat(glue("  ❌ Failed to process {info$filename}: {e$message}\n\n"))
      })
    }
  }
  processed_files
}

cleanup_processed_files <- function(processed_files) {
  if (length(processed_files) == 0) return()
  cat(glue("🧹 Cleaning up {length(processed_files)} processed files...\n"))
  for (file_path in processed_files) {
    if (file.exists(file_path)) {
      unlink(file_path)
      cat(glue("  🗑️  Deleted: {basename(file_path)}\n"))
    }
  }
  processed_dirs <- unique(dirname(processed_files))
  for (dir_path in processed_dirs) {
    if (length(list.files(dir_path)) == 0) {
      unlink(dir_path, recursive = TRUE)
      cat(glue("  🗑️  Deleted empty directory: {basename(dir_path)}\n"))
    }
  }
}

main <- function() {
  cat("🎬 Starting video processing...\n\n")
  cloud_name <- ensure_env("CLOUDINARY_CLOUD_NAME")
  preset <- ensure_env("CLOUDINARY_UPLOAD_PRESET")
  video_processing_dir <- "video_processing"
  video_map_path <- "data/video_map_manual.csv"
  if (!dir.exists(video_processing_dir)) {
    cat("ℹ️  No video_processing directory found. Nothing to process.\n")
    return()
  }
  date_folders <- list.dirs(video_processing_dir, recursive = FALSE)
  date_folders <- date_folders[grepl("videos_\\d{4}-\\d{2}-\\d{2}$", basename(date_folders))]
  if (length(date_folders) == 0) {
    cat("ℹ️  No video date folders found. Nothing to process.\n")
    return()
  }
  date_folders <- date_folders[order(basename(date_folders))]
  all_processed_files <- character()
  for (date_folder in date_folders) {
    processed_files <- process_video_folder(date_folder, cloud_name, preset, video_map_path)
    all_processed_files <- c(all_processed_files, processed_files)
  }
  if (length(all_processed_files) > 0) {
    cleanup_processed_files(all_processed_files)
    cat(glue("\n🎉 Successfully processed {length(all_processed_files)} videos!\n"))
  } else {
    cat("\nℹ️  No videos were processed.\n")
  }
}

main()
