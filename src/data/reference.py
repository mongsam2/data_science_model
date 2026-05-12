"""State code, FIPS, and FAF5 reference tables."""
from __future__ import annotations

# (abbr, full name, FIPS code, Census region)
# Census regions: NE=Northeast, MW=Midwest, S=South, W=West
STATES: list[tuple[str, str, int, str]] = [
    ("AL", "Alabama", 1, "S"),
    ("AK", "Alaska", 2, "W"),
    ("AZ", "Arizona", 4, "W"),
    ("AR", "Arkansas", 5, "S"),
    ("CA", "California", 6, "W"),
    ("CO", "Colorado", 8, "W"),
    ("CT", "Connecticut", 9, "NE"),
    ("DE", "Delaware", 10, "S"),
    ("DC", "District of Columbia", 11, "S"),
    ("FL", "Florida", 12, "S"),
    ("GA", "Georgia", 13, "S"),
    ("HI", "Hawaii", 15, "W"),
    ("ID", "Idaho", 16, "W"),
    ("IL", "Illinois", 17, "MW"),
    ("IN", "Indiana", 18, "MW"),
    ("IA", "Iowa", 19, "MW"),
    ("KS", "Kansas", 20, "MW"),
    ("KY", "Kentucky", 21, "S"),
    ("LA", "Louisiana", 22, "S"),
    ("ME", "Maine", 23, "NE"),
    ("MD", "Maryland", 24, "S"),
    ("MA", "Massachusetts", 25, "NE"),
    ("MI", "Michigan", 26, "MW"),
    ("MN", "Minnesota", 27, "MW"),
    ("MS", "Mississippi", 28, "S"),
    ("MO", "Missouri", 29, "MW"),
    ("MT", "Montana", 30, "W"),
    ("NE", "Nebraska", 31, "MW"),
    ("NV", "Nevada", 32, "W"),
    ("NH", "New Hampshire", 33, "NE"),
    ("NJ", "New Jersey", 34, "NE"),
    ("NM", "New Mexico", 35, "W"),
    ("NY", "New York", 36, "NE"),
    ("NC", "North Carolina", 37, "S"),
    ("ND", "North Dakota", 38, "MW"),
    ("OH", "Ohio", 39, "MW"),
    ("OK", "Oklahoma", 40, "S"),
    ("OR", "Oregon", 41, "W"),
    ("PA", "Pennsylvania", 42, "NE"),
    ("RI", "Rhode Island", 44, "NE"),
    ("SC", "South Carolina", 45, "S"),
    ("SD", "South Dakota", 46, "MW"),
    ("TN", "Tennessee", 47, "S"),
    ("TX", "Texas", 48, "S"),
    ("UT", "Utah", 49, "W"),
    ("VT", "Vermont", 50, "NE"),
    ("VA", "Virginia", 51, "S"),
    ("WA", "Washington", 53, "W"),
    ("WV", "West Virginia", 54, "S"),
    ("WI", "Wisconsin", 55, "MW"),
    ("WY", "Wyoming", 56, "W"),
]

FIPS_TO_ABBR: dict[int, str] = {fips: abbr for abbr, _, fips, _ in STATES}
ABBR_TO_FIPS: dict[str, int] = {abbr: fips for abbr, _, fips, _ in STATES}
ABBR_TO_NAME: dict[str, str] = {abbr: name for abbr, name, _, _ in STATES}
NAME_TO_ABBR: dict[str, str] = {name: abbr for abbr, name, _, _ in STATES}
ABBR_TO_REGION: dict[str, str] = {abbr: region for abbr, _, _, region in STATES}

# FRED regional CPI series (CPI is not published at state level)
FRED_REGIONAL_CPI: dict[str, str] = {
    "NE": "CUUR0100SA0",  # Northeast
    "MW": "CUUR0200SA0",  # Midwest
    "S": "CUUR0300SA0",   # South
    "W": "CUUR0400SA0",   # West
}

# FAF5 mode codes
FAF5_MODE_TRUCK = 1
