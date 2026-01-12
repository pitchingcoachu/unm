# School Config Overrides

This folder holds per-school configuration values so the shared `app.R` logic can stay generic. When you copy `shared-app` to a new repository, update `school_config.R` with that school's specific data:

- `team_code`: short code used in filters and branding (e.g., `"OSU"`) and replaces hard-coded `"OSU"` references.
- `colors`: a palette map that can include `primary`, `accent`, `accent_secondary`, `background`, and `background_secondary` to tweak gradients.
- `logo`: path to the school logo asset rendered in the navbar.
- `coaches_emails`: any coach-specific notification recipients.
- `notes_api`: base URL/credentials for the notes integration.
- `ftp`, `extra`, etc.: any other school-specific secrets or overrides (like `cloudinary_folder` or `school_name`).
- `colorize_css`: helper available after sourcing the config; use it when you need to inject school-specific colors into the main CSS block.

The helper `school_setting(name, default)` in `app.R` pulls these values with fallbacks so `app.R` does not need to change between schools.
