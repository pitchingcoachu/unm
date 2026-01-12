# Emergency backup script for pitch modifications
# This can be run manually to ensure modifications are properly backed up

library(DBI)
library(RSQLite)
library(readr)

# Find the modifications database
get_modifications_db_path <- function() {
  override <- Sys.getenv("PITCH_MOD_DB_PATH", unset = "")
  if (nzchar(override)) {
    path <- path.expand(override)
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    return(path)
  }
  if (file.access(".", 2) == 0) return("pitch_modifications.db")
  alt <- file.path(tools::R_user_dir("pcu_pitch_dashboard", which = "data"), "pitch_modifications.db")
  dir.create(dirname(alt), recursive = TRUE, showWarnings = FALSE)
  alt
}

# Get export path
get_modifications_export_path <- function() {
  override <- Sys.getenv("PITCH_MOD_EXPORT_PATH", unset = "")
  if (nzchar(override)) {
    path <- path.expand(override)
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    return(path)
  }
  file.path("data", "pitch_type_modifications_export.csv")
}

# Backup function
backup_modifications <- function() {
  db_path <- get_modifications_db_path()
  export_path <- get_modifications_export_path()
  
  if (!file.exists(db_path)) {
    cat("No database found at:", db_path, "\n")
    return(FALSE)
  }
  
  tryCatch({
    con <- dbConnect(SQLite(), db_path)
    on.exit(dbDisconnect(con), add = TRUE)
    
    mods <- dbGetQuery(con, "SELECT * FROM modifications ORDER BY created_at")
    
    if (nrow(mods) > 0) {
      dir.create(dirname(export_path), recursive = TRUE, showWarnings = FALSE)
      write_csv(mods, export_path)
      cat("Backed up", nrow(mods), "modifications to:", export_path, "\n")
      
      # Also create a timestamped backup
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      backup_path <- file.path("data", paste0("pitch_type_modifications_backup_", timestamp, ".csv"))
      write_csv(mods, backup_path)
      cat("Created timestamped backup:", backup_path, "\n")
      
      return(TRUE)
    } else {
      cat("No modifications found in database\n")
      return(FALSE)
    }
  }, error = function(e) {
    cat("Error backing up modifications:", e$message, "\n")
    return(FALSE)
  })
}

# Run backup if called directly
if (!interactive()) {
  result <- backup_modifications()
  if (result) {
    cat("Backup completed successfully\n")
  } else {
    cat("Backup failed or no data to backup\n")
  }
}