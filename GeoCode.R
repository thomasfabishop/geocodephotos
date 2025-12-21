
# ---------------------------------------------------------
# Example Usage
# ---------------------------------------------------------
old_folder <- "./Example"            # Path to your folder with original photos
new_folder <- "./Out"    # Path to the output folder for renamed photos
result_df <- rename_photos_with_geocoding(old_folder, new_folder)
#
head(result)  # Inspect how photos got renamed

# ---------------------------------------------------------
# Load necessary packages
# ---------------------------------------------------------
library(exifr)         # For reading EXIF data
library(dplyr)         # For data manipulation
library(tidyr)         # For data manipulation (optional)
library(tidygeocoder)  # For reverse geocoding

# ---------------------------------------------------------
# Rename Photos Function with Reverse Geocoding
# ---------------------------------------------------------
rename_photos_with_geocoding <- function(old_folder, new_folder) {
  
  # 1. Read EXIF metadata (recursive = TRUE)
  meta <- read_exif(
    path      = old_folder, 
    recursive = TRUE, 
    quiet     = TRUE
  )
  
  if (nrow(meta) == 0) {
    message("No image files found in the specified folder (or its subfolders).")
    return(invisible(NULL))
  }
  
  # 2. Extract fields of interest
  df <- meta %>%
    select(SourceFile, DateTimeOriginal, GPSLatitude, GPSLongitude) %>%
    mutate(
      # Convert empty strings to NA
      DateTimeOriginal = ifelse(is.na(DateTimeOriginal) | DateTimeOriginal == "", NA, DateTimeOriginal),
      GPSLatitude      = ifelse(is.na(GPSLatitude), NA, GPSLatitude),
      GPSLongitude     = ifelse(is.na(GPSLongitude), NA, GPSLongitude)
    )
  
  # 3. Parse the EXIF date/time into POSIXct
  df <- df %>%
    rowwise() %>%
    mutate(
      parsed_datetime = tryCatch(
        as.POSIXct(DateTimeOriginal, format = "%Y:%m:%d %H:%M:%S", tz = "UTC"),
        error = function(e) NA
      ),
      photo_date = if (!is.na(parsed_datetime)) format(parsed_datetime, "%Y%m%d") else NA_character_,
      photo_time = if (!is.na(parsed_datetime)) format(parsed_datetime, "%H%M%S") else NA_character_
    ) %>%
    ungroup()
  
  # 4. Reverse Geocode: for rows that have valid GPS, get a place name
  #    (Using the OSM 'Nominatim' service here. If you have many images, 
  #     consider batching or using an API key for other services.)
  df_with_gps <- df %>%
    filter(!is.na(GPSLatitude) & !is.na(GPSLongitude))
  
  if (nrow(df_with_gps) > 0) {
    # Reverse geocode
    df_geo <- df_with_gps %>%
      reverse_geocode(
        lat    = GPSLatitude,
        long   = GPSLongitude,
        method = "osm",           # or "google", "census", "iq", etc.
        address = "place_name"
      )
    
    # Join back to the main df
    df <- df %>%
      left_join(
        df_geo %>% select(SourceFile, place_name), 
        by = "SourceFile"
      )
  } else {
    # If no rows had GPS, just add 'place_name' = NA
    df$place_name <- NA
  }
  
  # 5. Clean the place_name for filenames (remove spaces, special chars)
  df <- df %>%
    mutate(
      # E.g., "Los Angeles, CA" -> "Los_Angeles_CA"
      place_name = gsub("[^A-Za-z0-9]+", "_", place_name)  # replace any non-alphanumeric with underscore
    )
  
  # 6. Build a “date_label” and “loc_label” (location) that can be used for grouping
  df <- df %>%
    mutate(
      # If date is missing -> "unknown"
      date_label = ifelse(is.na(photo_date), "unknown", photo_date),
      
      # If place_name is missing -> noGPS or geocode-failed
      loc_label  = ifelse(is.na(place_name) | place_name == "", "noGPS", place_name)
    )
  
  # 7. Sort by parsed_datetime for indexing
  df_grouped <- df %>%
    arrange(parsed_datetime) %>%
    group_by(date_label, loc_label) %>%
    mutate(index_within_group = row_number()) %>%
    ungroup()
  
  # 8. Construct the new filename
  #    - if date_label == "unknown" => "unknown_index"
  #    - else if loc_label == "noGPS" => "YYYYMMDD_index"
  #    - else => "YYYYMMDD_placeName_index"
  df_grouped <- df_grouped %>%
    rowwise() %>%
    mutate(
      file_extension = tools::file_ext(SourceFile),
      
      new_filename = case_when(
        date_label == "unknown" ~ paste0("unknown_", index_within_group, ".", file_extension),
        loc_label  == "noGPS"   ~ paste0(date_label, "_", index_within_group, ".", file_extension),
        TRUE                    ~ paste0(date_label, "_", loc_label, "_", index_within_group, ".", file_extension)
      )
    ) %>%
    ungroup()
  
  # 9. Create new folder if not exist
  if (!dir.exists(new_folder)) {
    dir.create(new_folder, recursive = TRUE)
  }
  
  # 10. Copy (or rename) the original files to the new folder
  df_grouped <- df_grouped %>%
    mutate(
      old_path = SourceFile,
      new_path = file.path(new_folder, new_filename)
    )
  
  for (i in seq_len(nrow(df_grouped))) {
    oldp <- df_grouped$old_path[i]
    newp <- df_grouped$new_path[i]
    
    # copy = keeps original in place; rename = moves it
    file.copy(from = oldp, to = newp, overwrite = FALSE)
    # If you want to move (rename) instead of copy, do:
    # file.rename(from = oldp, to = newp)
  }
  
  message("Renaming process complete. See new folder: ", new_folder)
  
  return(invisible(df_grouped))
}


