# School-specific overrides for the shared app.
# Copy this file to another repo and keep the same structure when you need to customize colors, logos, APIs, etc.
school_config <- list(
  team_code = "UNM",
  # Additional school-code markers used in TrackMan team columns (optional).
  # These are checked alongside team_code during allowed-player verification.
  team_code_markers = c("UNL_REB", "UNM", "MEX_LOB"),
  allowed_pitchers = c(
    "White, Tommy",
    "Alvarez, Diego",
    "Woltz, Ethin",
    "Cornelius, Matt",
    "Grubbs, Talor",
    "Romero, Matt",
    "Jackson, Samuel",
    "Mogen, Cristian",
    "Corkrean, Cooper",
    "Barnett, Logan",
    "Lee, Seth",
    "Cunningham, Ty",
    "Wiseman, Luke",
    "Barnhouse, Josh",
    "Do, Tyler",
    "Lehmbeck, Blake",
    "Castillo, Ryan",
    "Edgerton, Seth",
    "Gergen, Jacob",
    "Jacobson, Reid"
  ),
  allowed_hitters = c(
    "Wentworth, TP",
    "LeBlanc, Bryce",
    "Lund, Ethan",
    "Fyke, Kai",
    "Rhodes, Stormy",
    "Wech, Noah",
    "Brown, Matthew",
    "Phillips, Brennan",
    "Blake, Drew",
    "Glendinning, Lucas",
    "Golden, Josiah",
    "Kennedy, Jake",
    "Barrett, Hudson",
    "Zagar, Kyler",
    "Albright, Gaige",
    "Sramek, Caden",
    "Jennings, Parker",
    "Burns, Zane",
    "Winslow, Drew",
    "Pearcy, Kyle",
    "Turner, Cael",
    "Pesca, Mario",
    "Watkins, Hunter",
    "Thompson, Brock",
    "Meola, Aidan",
    "Bowen, Terrance",
    "Smithwick, Campbell",
    "Shull, Garrett",
    "Indomenico, Remo",
    "Ortiz, Avery",
    "Wallace, Danny",
    "Brueggemann, Colin",
    "Ritchie, Kollin",
    "Conover, Alex",
    "Norman, Sebastian",
    "Essex, Ezra",
    "Saunders, Evan",
    "Pladson, Cole",
    "Schambow, Quinn",
    "Kennedy, Ty",
    "Francisco, Brady",
    "Pomeroy, Deacon"
  ),
  allowed_campers = c(
    "Bowman, Brock",
    "Daniels, Tyke",
    "Pearson, Blake",
    "Rodriguez, Josiah",
    "James, Brody",
    "Nevarez, Matthew",
    "Nunes, Nolan",
    "Parks, Jaeden",
    "Hill, Grant",
    "McGinnis, Ayden",
    "Morton, Ryker",
    "McGuire, John",
    "Willson, Brandon",
    "Lauterbach, Camden",
    "Turnquist, Dylan",
    "Bournonville, Tanner",
    "Evans, Lincoln",
    "Gnirk, Will",
    "Mann, Tyson",
    "Neneman, Chase",
    "Warmus, Joaquin",
    "Kapadia, Taylor",
    "Stoner, Timothy",
    "Bergloff, Cameron",
    "Hamm, Jacob",
    "Hofmeister, Ben",
    "Moo, Eriksen",
    "Peltz, Zayden",
    "Huff, Tyler",
    "Moseman, Cody"
  ),
  colors = list(
    primary             = "#ba0c2f",   # crimson from UNM logo
    accent              = "#ba0c2f",   # bold red gradient start
    accent_secondary    = "#9fa0a2",   # grey accent from logo
    background          = "#ffffff",   # clean white page base
    background_secondary= "#f4f4f4"    # light off-white secondary tone
    
  ),
  logo = "UNMlogo.png",
  coaches_emails = c(
    "mlopez41@unm.edu",
    "jstill@unm.edu"
  ),
  notes_api = list(
    base_url = "https://script.google.com/macros/s/AKfycbyo6HKhHDSoUNRWtmFpto7BLqBxWU2nG5-ifkmsDTYqHq5mBFGhtdsmIGvb25xFsNh0/exec",
    token = "unmbaseball"
  ),
  extra = list(
    school_name = "unm",
    ftp_folder = "trackman",
    cloudinary_folder = "trackman"
  )
)

colorize_css <- function(css, accent, accent_secondary, background, background_secondary) {
  accent_rgb <- paste(grDevices::col2rgb(accent), collapse = ",")
  accent_secondary_rgb <- paste(grDevices::col2rgb(accent_secondary), collapse = ",")
  css <- gsub("#e35205", accent, css, fixed = TRUE)
  css <- gsub("#ff8c1a", accent_secondary, css, fixed = TRUE)
  css <- gsub("rgba(227,82,5", paste0("rgba(", accent_rgb), css, fixed = TRUE)
  css <- gsub("rgba(227, 82, 5", paste0("rgba(", accent_rgb), css, fixed = TRUE)
  css <- gsub("rgba(255,140,26", paste0("rgba(", accent_secondary_rgb), css, fixed = TRUE)
  css <- gsub("rgba(255, 140, 26", paste0("rgba(", accent_secondary_rgb), css, fixed = TRUE)
  css <- gsub("#f5f7fa", background, css, fixed = TRUE)
  css <- gsub("#e8ecf1", background_secondary, css, fixed = TRUE)
  css
}
