
#Reverse GeoCode

#This is to work on larg photo collectons from multiple sources

# ---------------------------------------------------------
# Load necessary packages
# ---------------------------------------------------------
library(exifr)         # EXIF reading
library(dplyr)         # data manipulation
library(tidygeocoder)  # reverse geocoding
library(digest)        # hashing (dedup across sources)

# ---------------------------------------------------------
# Rename Photos Function with:
#  - Hash-based deduplication across sources
#  - Output into per-year folders
#  - PER-YEAR hash registries: Archive/<year>/hash_registry_<year>.csv
#  - Robust bind_rows() via explicit type standardisation
#  - Optional dry_run
# ---------------------------------------------------------
rename_photos_with_geocoding <- function(
    old_folder,
    new_folder,
    dry_run = FALSE,
    hash_algo = "sha256"
) {
  
  # -----------------------------
  # Helpers
  # -----------------------------
  add_if_missing <- function(x, col, value) {
    if (!col %in% names(x)) x[[col]] <- value
    x
  }
  
  # Standardise columns + types so bind_rows() never errors
  standardise_cols <- function(x) {
    x <- add_if_missing(x, "place_name", NA_character_)
    x <- add_if_missing(x, "date_label", NA_character_)
    x <- add_if_missing(x, "loc_label", NA_character_)
    x <- add_if_missing(x, "index_within_group", NA_integer_)
    x <- add_if_missing(x, "file_extension", NA_character_)
    x <- add_if_missing(x, "new_filename", NA_character_)
    x <- add_if_missing(x, "year_folder", NA_character_)
    x <- add_if_missing(x, "old_path", NA_character_)
    x <- add_if_missing(x, "new_dir", NA_character_)
    x <- add_if_missing(x, "new_path", NA_character_)
    x <- add_if_missing(x, "exists_already", as.logical(NA))
    x <- add_if_missing(x, "status", NA_character_)
    
    x %>%
      mutate(
        place_name         = as.character(place_name),
        date_label         = as.character(date_label),
        loc_label          = as.character(loc_label),
        index_within_group = as.integer(index_within_group),
        file_extension     = as.character(file_extension),
        new_filename       = as.character(new_filename),
        year_folder        = as.character(year_folder),
        old_path           = as.character(old_path),
        new_dir            = as.character(new_dir),
        new_path           = as.character(new_path),
        exists_already     = as.logical(exists_already),
        status             = as.character(status)
      )
  }
  
  # -----------------------------
  # 0) Ensure output folder exists
  # -----------------------------
  if (!dir.exists(new_folder)) dir.create(new_folder, recursive = TRUE)
  
  # -----------------------------
  # 0b) Load ALL existing per-year hash registries
  #     (so duplicates across Dropbox/iCloud are skipped even if processed separately)
  # -----------------------------
  known_hashes <- character(0)
  
  registry_files <- list.files(
    path = new_folder,
    pattern = "^hash_registry_\\d{4}\\.csv$",
    recursive = TRUE,
    full.names = TRUE
  )
  
  if (length(registry_files) > 0) {
    for (rf in registry_files) {
      reg <- tryCatch(read.csv(rf, stringsAsFactors = FALSE), error = function(e) NULL)
      if (!is.null(reg) && "file_hash" %in% names(reg)) {
        known_hashes <- c(known_hashes, as.character(reg$file_hash))
      }
    }
  }
  known_hashes <- unique(known_hashes)
  
  # -----------------------------
  # 1) Read EXIF metadata
  # -----------------------------
  meta <- read_exif(
    path      = old_folder,
    recursive = TRUE,
    quiet     = TRUE
  )
  
  if (nrow(meta) == 0) {
    message("No image files found in the specified folder (or its subfolders).")
    return(invisible(NULL))
  }
  
  # -----------------------------
  # 2) Extract fields
  # -----------------------------
  df <- meta %>%
    select(SourceFile, DateTimeOriginal, GPSLatitude, GPSLongitude) %>%
    mutate(
      DateTimeOriginal = ifelse(is.na(DateTimeOriginal) | DateTimeOriginal == "",
                                NA_character_, DateTimeOriginal),
      GPSLatitude  = ifelse(is.na(GPSLatitude),  NA, GPSLatitude),
      GPSLongitude = ifelse(is.na(GPSLongitude), NA, GPSLongitude)
    )
  
  # -----------------------------
  # 3) Parse datetime + derive year/date/time
  # -----------------------------
  df <- df %>%
    rowwise() %>%
    mutate(
      parsed_datetime = tryCatch(
        as.POSIXct(DateTimeOriginal, format = "%Y:%m:%d %H:%M:%S", tz = "UTC"),
        error = function(e) NA
      ),
      photo_date = if (!is.na(parsed_datetime)) format(parsed_datetime, "%Y%m%d") else NA_character_,
      photo_time = if (!is.na(parsed_datetime)) format(parsed_datetime, "%H%M%S") else NA_character_,
      photo_year = if (!is.na(parsed_datetime)) format(parsed_datetime, "%Y")     else "unknown_year"
    ) %>%
    ungroup() %>%
    mutate(
      photo_date = as.character(photo_date),
      photo_time = as.character(photo_time),
      photo_year = as.character(photo_year)
    )
  
  # -----------------------------
  # 4) Hash for dedup (before geocoding)
  # -----------------------------
  df <- df %>%
    rowwise() %>%
    mutate(
      file_hash = tryCatch(
        digest(file = SourceFile, algo = hash_algo, serialize = FALSE),
        error = function(e) NA_character_
      )
    ) %>%
    ungroup() %>%
    mutate(
      file_hash = as.character(file_hash),
      is_known_duplicate = !is.na(file_hash) & (file_hash %in% known_hashes)
    )
  
  df_to_process <- df %>% filter(!is_known_duplicate)
  
  if (nrow(df_to_process) == 0) {
    message("All files appear to be duplicates (already present in per-year hash registries). Nothing to do.")
    out <- df %>% mutate(status = ifelse(is_known_duplicate, "skipped_hash_duplicate", "unknown"))
    return(invisible(out))
  }
  
  # -----------------------------
  # 5) Reverse geocode (only non-dupes with GPS)
  # -----------------------------
  df_with_gps <- df_to_process %>%
    filter(!is.na(GPSLatitude) & !is.na(GPSLongitude))
  
  if (nrow(df_with_gps) > 0) {
    df_geo <- df_with_gps %>%
      reverse_geocode(
        lat     = GPSLatitude,
        long    = GPSLongitude,
        method  = "osm",
        address = "place_name"
      )
    
    df_to_process <- df_to_process %>%
      left_join(df_geo %>% select(SourceFile, place_name), by = "SourceFile")
  } else {
    df_to_process$place_name <- NA_character_
  }
  
  # -----------------------------
  # 6) Clean place name
  # -----------------------------
  df_to_process <- df_to_process %>%
    mutate(place_name = gsub("[^A-Za-z0-9]+", "_", place_name))
  
  # -----------------------------
  # 7) Labels
  # -----------------------------
  df_to_process <- df_to_process %>%
    mutate(
      date_label = ifelse(is.na(photo_date) | photo_date == "", "unknown", photo_date),
      loc_label  = ifelse(is.na(place_name) | place_name == "", "noGPS", place_name),
      date_label = as.character(date_label),
      loc_label  = as.character(loc_label)
    )
  
  # -----------------------------
  # 8) Index within (date, location)
  # -----------------------------
  df_grouped <- df_to_process %>%
    arrange(parsed_datetime) %>%
    group_by(date_label, loc_label) %>%
    mutate(index_within_group = row_number()) %>%
    ungroup()
  
  # -----------------------------
  # 9) New filename
  # -----------------------------
  df_grouped <- df_grouped %>%
    rowwise() %>%
    mutate(
      file_extension = as.character(tools::file_ext(SourceFile)),
      new_filename = case_when(
        date_label == "unknown" ~ paste0("unknown_", index_within_group, ".", file_extension),
        loc_label  == "noGPS"   ~ paste0(date_label, "_", index_within_group, ".", file_extension),
        TRUE                    ~ paste0(date_label, "_", loc_label, "_", index_within_group, ".", file_extension)
      )
    ) %>%
    ungroup()
  
  # -----------------------------
  # 10) Year folders
  # -----------------------------
  df_grouped <- df_grouped %>%
    mutate(
      year_folder = as.character(photo_year),
      old_path    = SourceFile,
      new_dir     = file.path(new_folder, year_folder),
      new_path    = file.path(new_dir, new_filename)
    )
  
  # -----------------------------
  # 11) Create year folders
  # -----------------------------
  if (!dry_run) {
    unique_dirs <- unique(df_grouped$new_dir)
    for (d in unique_dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)
  }
  
  # -----------------------------
  # 12) Copy with skip-if-exists
  # -----------------------------
  df_grouped <- df_grouped %>%
    mutate(
      exists_already = file.exists(new_path),
      status = NA_character_
    )
  
  if (dry_run) {
    df_grouped <- df_grouped %>%
      mutate(status = ifelse(exists_already, "would_skip_exists", "would_copy"))
  } else {
    for (i in seq_len(nrow(df_grouped))) {
      oldp <- df_grouped$old_path[i]
      newp <- df_grouped$new_path[i]
      
      if (file.exists(newp)) {
        df_grouped$status[i] <- "skipped_exists"
        next
      }
      
      ok <- file.copy(from = oldp, to = newp, overwrite = FALSE)
      df_grouped$status[i] <- ifelse(ok, "copied", "copy_failed")
    }
  }
  
  # -----------------------------
  # 13) Build skipped duplicates rows
  # -----------------------------
  skipped_dupes <- df %>%
    filter(is_known_duplicate) %>%
    mutate(
      place_name = NA_character_,
      date_label = as.character(ifelse(is.na(photo_date) | photo_date == "", "unknown", photo_date)),
      loc_label  = NA_character_,
      index_within_group = NA_integer_,
      file_extension = as.character(tools::file_ext(SourceFile)),
      new_filename = NA_character_,
      year_folder  = as.character(photo_year),
      old_path     = SourceFile,
      new_dir      = NA_character_,
      new_path     = NA_character_,
      exists_already = as.logical(NA),
      status       = "skipped_hash_duplicate"
    )
  
  # -----------------------------
  # 14) Standardise + bind safely
  # -----------------------------
  df_grouped   <- standardise_cols(df_grouped)
  skipped_dupes <- standardise_cols(skipped_dupes)
  out <- bind_rows(df_grouped, skipped_dupes)
  
  # -----------------------------
  # 15) Update PER-YEAR registries with successfully copied photos
  #     Writes: new_folder/<year>/hash_registry_<year>.csv
  # -----------------------------
  if (!dry_run) {
    to_add <- out %>%
      filter(status == "copied", !is.na(file_hash)) %>%
      transmute(
        photo_year    = as.character(year_folder),
        file_hash     = as.character(file_hash),
        source_file   = old_path,
        archived_file = new_path,
        added_utc     = format(Sys.time(), tz = "UTC")
      )
    
    if (nrow(to_add) > 0) {
      by_year <- split(to_add, to_add$photo_year)
      
      for (yr in names(by_year)) {
        year_dir <- file.path(new_folder, yr)
        dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
        
        registry_file <- file.path(year_dir, paste0("hash_registry_", yr, ".csv"))
        
        yr_df <- by_year[[yr]] %>% select(-photo_year)
        
        if (!file.exists(registry_file)) {
          write.csv(yr_df, registry_file, row.names = FALSE)
        } else {
          suppressWarnings(
            write.table(
              yr_df,
              registry_file,
              sep = ",",
              row.names = FALSE,
              col.names = FALSE,
              append = TRUE
            )
          )
        }
      }
    }
  }
  
  message("Done. Output root: ", new_folder)
  if (dry_run) {
    message("NOTE: dry_run=TRUE (no files copied, registries not updated).")
  } else {
    message("Per-year registries written under: ", new_folder, "/<year>/hash_registry_<year>.csv")
  }
  
  return(invisible(out))
}

# ---------------------------------------------------------
# Example usage
# ---------------------------------------------------------
# Dropbox ingest
# result_dropbox <- rename_photos_with_geocoding(
#   old_folder = "./DropboxDump",
#   new_folder = "./Archive",
#   dry_run = FALSE
# )
#
# iCloud ingest (duplicates skipped via per-year registries)
# result_icloud <- rename_photos_with_geocoding(
#   old_folder = "./iCloudDump",
#   new_folder = "./Archive",
#   dry_run = FALSE
# )



# ---------------------------------------------------------
# Example usage
# ---------------------------------------------------------
# Dropbox ingest
 result_dropbox <- rename_photos_with_geocoding(
   old_folder = "./Example",
   new_folder = "./Out",
   dry_run = FALSE
 )
#
# iCloud ingest (duplicates will be skipped by hash registry)
# result_icloud <- rename_photos_with_geocoding(
#   old_folder = "./iCloudDump",
#   new_folder = "./Archive",
#   dry_run = FALSE
# )
