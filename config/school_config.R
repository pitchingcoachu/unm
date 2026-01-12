# School-specific overrides for the shared app.
# Copy this file to another repo and keep the same structure when you need to customize colors, logos, APIs, etc.
school_config <- list(
  team_code = "OSU",
  # Player filters
  allowed_pitchers = c(
    "Lee, Aidan",
    "Limas, Jacob",
    "Higginbottom, Elijah",
    "Cunnings, Cam",
    "Moeller, Luke",
    "Smith, Jace",
    "Frey, Chase",
    "Ahern, Garrett",
    "McGuire, Tommy",
    "Robb, Nicholas",
    "Guerrero, JT",
    "Gregory, Billy",
    "Penzkover, Gunnar",
    "Lewis, JT",
    "Kiemele, Cody",
    "Cohen, Andrew",
    "Lyon, Andrew",
    "Johns, Tanner",
    "Toney, Brock",
    "Sloan, Landon",
    "Key, Chance",
    "Orr, Dillon",
    "Yates, Zach",
    "New, Cody"
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
    primary             = "#0d1224",   # deep navy used in the dark-mode radial gradient (gcu/app.R:17666-17674)
    accent              = "#667eea",   # start of the active-tab/btn gradient (gcu/app.R:17464-17515)
    accent_secondary    = "#764ba2",   # end of that same gradient
    background          = "#f5f7fa",   # light page background (gcu/app.R:17135)
    background_secondary= "#e8ecf1"   # the matching secondary background tone
    
  ),
  logo = "OSUlogo.png",
  coaches_emails = c(
    "Blake.hawksworth@okstate.edu",
    "Payton.stevens@okstate.edu",
    "Trey.cobb@okstate.edu",
    "jared.s.gaynor@gmail.com",
    "Victor.Romero@okstate.edu",
    "J.Holliday@okstate.edu",
    "Mark.Ginther@okstate.edu",
    "hub.roberts@okstate.edu"
  ),
  notes_api = list(
    base_url = "https://script.google.com/macros/s/AKfycby8_RuLj5hKxi129ru32cpEojVimffD2msCSl-I9r9a1LfZe9Ht-yLPbiDHVatm48g/exec",
    token = "OSUbaseball"
  ),
  extra = list(
    school_name = "Oklahoma State",
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
