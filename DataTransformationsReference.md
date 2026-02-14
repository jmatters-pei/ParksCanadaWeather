# DATA TRANSFORMATION REFERENCE GUIDE
## Quick Reference for All Data Cleaning, Imputation, and Aggregation

**Version:** 2.5  
**Last Updated:** February 14, 2026  
**Purpose:** Complete listing of all data transformations applied by weather_processing.py

---

## 📊 TABLE OF CONTENTS

1. [Data Cleaning Operations](#data-cleaning-operations)
2. [Missing Data Imputation](#missing-data-imputation)
3. [Data Aggregation Methods](#data-aggregation-methods)
4. [Data Validation & Bounds](#data-validation--bounds)
5. [Column Transformations](#column-transformations)
6. [Quick Reference Tables](#quick-reference-tables)

---

## 🧹 DATA CLEANING OPERATIONS

### 1. Column Name Standardization

**Operations Applied:**
- Split column names at parentheses or underscores, keep first part
- Convert to title case
- Apply standard replacements

**Standard Replacements:**
| Original Name(s) | Standardized To |
|------------------|-----------------|
| wind gust  speed, wind gust speed, gust speed | Wind Gust Speed |
| avg wind speed, average wind speed, wind spd, windspd | Wind Speed |
| accumulated rain, precip. amount | Rain |
| temp | Temperature |
| wind dir | Wind Direction |
| rel hum | Rh |
| date/time | Date/Time |
| *contains 'dew'* | Dew |

**Example:**
- `Temperature (°C)` → `Temperature`
- `Wind_Speed_mph` → `Wind`
- `Relative Humidity` → `Rh`

**Code Location:** Lines 251-296 (`clean_columns` function)

---

### 2. Column Removal

**Columns Dropped:**
- Columns containing "serial" (any case)
- Columns containing "battery" (any case)
- Duplicate column names (keeps first occurrence)
- Constant columns (0 or 1 unique values, except 'station')

**Example Removals:**
- `Serial_Number`
- `Battery_Voltage`
- `Status` (if all values are the same)

**Code Location:** Lines 263-294

---

### 3. Duplicate Column Merging

**Operation:**
- When multiple columns have the same name (after standardization)
- Uses backward fill across columns to merge values
- Keeps first column, drops duplicates

**Example:**
```
Temperature (from sensor A): [20, NaN, 22]
Temperature (from sensor B): [NaN, 21, NaN]
Merged Temperature:          [20, 21, 22]
```

**Code Location:** Lines 588-608 (`merge_duplicate_columns`)

---

### 4. Datetime Processing

**Operations Applied:**

1. **ECCC Date/Time Column:**
   - Parse to UTC timezone
   - Rename to `Datetime_UTC`
   - Drop original `Date/Time` column

2. **Separate Date and Time Columns:**
   - Combine into single datetime: `date + ' ' + time`
   - Parse to UTC timezone
   - Drop original separate columns

3. **Standardization:**
   - All datetime columns converted to UTC
   - Unparseable values set to NaT (Not a Time)
   - Data sorted by datetime

**Code Location:** Lines 609-645 (`process_datetime_columns`)

---

### 5. Data Type Conversions

**Operations:**
- All numeric columns converted from object/string to numeric
- Non-numeric values coerced to NaN
- Datetime columns converted to pandas datetime64

**Example:**
- `"23.5"` (string) → `23.5` (float)
- `"N/A"` (string) → `NaN` (missing)

**Code Location:** Lines 746-752

---

### 6. Duplicate Row Handling

**Detection:**
- Duplicate rows identified and counted
- Logged in data quality reports

**Current Behavior:**
- Duplicates are logged but NOT removed
- Allows manual review of duplicate patterns

**Code Location:** Lines 396-398

---

### 7. Empty Row Removal

**Removed:**
- Rows where ALL data columns are NaN
- Keeps rows with at least one valid measurement

**Example:**
```
Row 1: Datetime=2022-01-01, Temp=NaN, RH=NaN, Wind=NaN  → REMOVED
Row 2: Datetime=2022-01-01, Temp=20, RH=NaN, Wind=NaN   → KEPT
```

**Code Location:** Lines 678-681

---

### 8. Character Encoding Fixes

**Applied During File Loading:**

1. **Primary:** UTF-8 encoding
2. **Fallback:** Latin1 encoding (if UTF-8 fails)
3. **Skip:** Bad lines that can't be parsed

**Handles:**
- Special characters (°, ñ, é, etc.)
- Different file encodings from various sources

**Code Location:** Lines 141-155

---

## 🔧 MISSING DATA IMPUTATION

### Imputation Strategy Overview

**Three-Tier Approach:**
1. **Tier 1:** Linear interpolation (smallest assumption)
2. **Tier 2:** Forward/backward fill (moderate assumption)
3. **Tier 3:** Variable-specific rules (domain knowledge)

**Threshold Rule:**
- If >25% missing for a station-variable → **SKIP IMPUTATION**
- Rationale: Likely sensor failure, too uncertain to impute

---

### TIER 1: Linear Interpolation

**Method:** Time-based linear interpolation

**Parameters:**
- Maximum gap: 3 hours
- Direction: Both forward and backward
- Method: Linear based on time distance

**Formula:**
```
y = y₁ + (y₂ - y₁) × (t - t₁) / (t₂ - t₁)
```

**Applied To:** ALL numeric variables

**Example:**
```
Time:  10:00  11:00  12:00  13:00  14:00
Temp:   20     ?      ?      23     24

Imputed:
11:00 = 20 + (23-20) × 1/3 = 21.0°C
12:00 = 20 + (23-20) × 2/3 = 22.0°C
```

**Imputation Flag:** `1` (interpolated)

**Code Location:** Lines 842-867

---

### TIER 2: Forward/Backward Fill

**Method:** 
- Forward Fill (LOCF): Last Observation Carried Forward
- Backward Fill (NOCB): Next Observation Carried Backward

**Parameters:**
- Forward fill limit: 6 hours
- Backward fill limit: 3 hours

**Applied To:** (Slowly-changing variables only)
- Temperature
- Dew Point
- Relative Humidity (Rh)
- Wind Speed

**NOT Applied To:** (Rapidly-changing variables)
- Rain
- Wind Direction
- Wind Gust Speed

**Example (Forward Fill):**
```
Time:  10:00  11:00  12:00  13:00  14:00
Temp:   20     ?      ?      ?      23

Forward filled:
11:00 = 20 (copied forward)
12:00 = 20 (copied forward)
13:00 = 20 (copied forward)
```

**Imputation Flag:** `2` (forward/backward filled)

**Code Location:** Lines 873-891

---

### TIER 3: Variable-Specific Rules

#### Rain Imputation

**Rule:** Missing rain = 0 mm

**Rationale:**
- Rain gauges often record 0 as missing/blank
- Absence of rain recording = no rain occurred
- Conservative assumption for fire risk

**Example:**
```
Rain: [2.5, NaN, NaN, 1.2, NaN, 3.0]
→     [2.5, 0.0, 0.0, 1.2, 0.0, 3.0]
```

**Imputation Flag:** `3` (calculated)

**Code Location:** Lines 896-902

---

#### Wind Gust Speed Imputation

**Rule:** If gust missing but wind speed available → Use wind speed

**Rationale:**
- Gust speed ≥ average wind speed (by definition)
- Using average wind speed is conservative estimate
- Better than leaving blank

**Example:**
```
Wind Speed:      [15, 18, 20, 12]
Wind Gust Speed: [25, ?, 30, ?]
→                [25, 18, 30, 12]
```

**Imputation Flag:** `3` (calculated)

**Code Location:** Lines 905-913

---

#### Relative Humidity (Rh) Calculation

**Rule:** Calculate from Temperature and Dew Point using Magnus formula

**Formula:**
```
e_dew = exp((17.625 × Dew) / (243.04 + Dew))
e_temp = exp((17.625 × Temp) / (243.04 + Temp))
Rh = 100 × (e_dew / e_temp)
Rh = clip(Rh, 0, 100)
```

**Constants:**
- a = 17.625 (Magnus constant)
- b = 243.04°C (Magnus constant)

**Applied When:**
- Rh is missing
- Temperature is available
- Dew Point is available

**Example:**
```
Temperature: 20°C
Dew Point:   15°C
→ Calculated Rh: ~73.4%
```

**Imputation Flag:** `3` (calculated)

**Code Location:** Lines 700-727, 916-926

---

### Imputation Flags

**Every imputed column gets a corresponding flag column:**

| Flag Value | Meaning | Method |
|------------|---------|--------|
| 0 | Original data | No imputation |
| 1 | Interpolated | Linear interpolation (Tier 1) |
| 2 | Filled | Forward/backward fill (Tier 2) |
| 3 | Calculated | Variable-specific rule (Tier 3) |

**Flag Column Names:**
- Data column: `Temperature`
- Flag column: `Temperature_imputed`

**Example DataFrame:**
```
Datetime_UTC         Temperature  Temperature_imputed
2022-01-01 10:00:00      20.5            0  (original)
2022-01-01 11:00:00      21.2            1  (interpolated)
2022-01-01 12:00:00      22.0            2  (forward filled)
2022-01-01 13:00:00       0.0            3  (calculated: Rain)
```

---

## 📈 DATA AGGREGATION METHODS

### Hourly Aggregation

**Time Window:** ±30 minutes around each hour

**Example:**
- For 14:00 hour: Include data from 13:30 to 14:30

**Aggregation by Variable Type:**

| Variable Type | Method | Examples |
|---------------|--------|----------|
| Average values | Mean | Temperature, Dew Point, Rh, Wind Speed |
| Maximum values | Max | Wind Gust Speed |
| Totals | Sum | Rain (precipitation) |
| Directional | Circular Mean | Wind Direction |

**Circular Mean for Wind Direction:**
```
Formula:
1. Convert degrees to radians
2. Calculate mean of sine components: sin_mean
3. Calculate mean of cosine components: cos_mean
4. Angle = arctan2(sin_mean, cos_mean)
5. Convert back to degrees
```

**Why Circular Mean:**
- Regular mean of 350° and 10° = 180° ❌ (wrong!)
- Circular mean of 350° and 10° = 0° ✅ (correct!)

**Output:** One row per hour per station

**Code Location:** Lines 982-1044 (`create_hourly_aggregates`)

---

### Daily Aggregation

**Time Window:** Entire day (00:00 to 23:59)

**Aggregation by Variable Type:**

| Variable | Min | Max | Mean | Sum | Other |
|----------|-----|-----|------|-----|-------|
| Temperature | ✓ | ✓ | ✓ | | |
| Dew Point | ✓ | ✓ | ✓ | | |
| Rh (Humidity) | ✓ | ✓ | ✓ | | |
| Wind Speed | | ✓ | ✓ | | |
| Wind Gust Speed | | ✓ | | | |
| Rain | | | | ✓ | |
| Wind Direction | | | | | Most common |

**Output Columns:**
- `Temperature_min`, `Temperature_max`, `Temperature_mean`
- `Rain_total` (sum of all precipitation)
- `Wind_Direction_mode` (most frequent direction)

**Output:** One row per day per station

**Code Location:** Lines 1046-1087 (`create_daily_aggregates`)

---

## ✅ DATA VALIDATION & BOUNDS

### Temperature Bounds

**Valid Range:** -40°C to +40°C

**Rationale:** 
- Historical PEI climate extremes
- Values outside range likely sensor errors

**Action:**
- Values < -40°C → Set to NaN
- Values > +40°C → Set to NaN

**Code Location:** Lines 929-932

---

### Relative Humidity Bounds

**Valid Range:** 0% to 100%

**Rationale:**
- Physical limits of relative humidity
- 0% = completely dry air
- 100% = saturated air

**Action:**
- Values clipped to [0, 100]
- Negative values → 0%
- Values > 100% → 100%

**Code Location:** Lines 935-938

---

### Dew Point Bounds

**Valid Range:** -50°C to +50°C

**Rationale:**
- Catches sensor and imputation errors

**Action:**
- Values outside range → Set to NaN

**Code Location:** Lines 45-46 (CONFIG)

---

### Physical Relationship Validation

**Dew Point ≤ Temperature**

**Check:** Dew point should never exceed air temperature

**Action:** (Not currently implemented, future enhancement)
- Flag or correct violations
- Dew > Temp indicates sensor error

---

## 🔄 COLUMN TRANSFORMATIONS

### Columns Added

| Column Name | Description | When Added |
|-------------|-------------|------------|
| `station` | Station identifier | During data loading |
| `Datetime_UTC` | Standardized datetime (UTC) | During datetime processing |
| `*_imputed` | Imputation flags (0-3) | During imputation |
| `hour_label` | Hour marker for aggregation | During hourly aggregation |
| `minutes_from_hour` | Distance from hour marker | During hourly aggregation |

---

### Columns Removed

| Column Pattern | Reason |
|----------------|--------|
| `Serial*`, `Battery*` | Not useful for analysis |
| `Date/Time` | Replaced by `Datetime_UTC` |
| Separate `Date` and `Time` | Combined into `Datetime_UTC` |
| Duplicate columns | Merged into single column |
| Constant columns | Contain no information |
| `hour_label`, `minutes_from_hour` | Temporary columns for aggregation |

---

### Columns Renamed

| Original | Renamed To | During |
|----------|------------|--------|
| `Date/Time` | `Datetime_UTC` | Datetime processing |
| `Temp` | `Temperature` | Column standardization |
| `Rel Hum` | `Rh` | Column standardization |
| Various wind columns | `Wind Speed`, `Wind Gust Speed` | Column standardization |

---

## 📋 QUICK REFERENCE TABLES

### Processing Pipeline Summary

| Step | Operation | Input | Output |
|------|-----------|-------|--------|
| 1 | Load Local Files | CSV files in folders | List of DataFrames |
| 2 | Download ECCC | API calls | List of DataFrames |
| 3 | Clean Columns | Raw column names | Standardized names |
| 4 | Combine Data | Multiple DataFrames | Single DataFrame |
| 5 | Process Datetime | Various date formats | `Datetime_UTC` |
| 6 | Validate Types | Mixed types | Numeric + datetime |
| 7 | Clean Data | Raw data | Validated data |
| 8 | Impute Missing | Data with gaps | Complete data |
| 9 | Aggregate Hourly | All observations | Hourly summaries |
| 10 | Aggregate Daily | All observations | Daily summaries |
| 11 | Quality Report | Final data | Statistics CSV |
| 12 | Save Outputs | Processed data | 4 CSV files |

---

### Data Quality Thresholds

| Threshold | Value | Purpose |
|-----------|-------|---------|
| Imputation skip | >25% missing | Avoid unreliable imputation |
| Interpolation limit | 3 hours | Maximum gap for linear interpolation |
| Forward fill limit | 6 hours | Maximum forward fill |
| Backward fill limit | 3 hours | Maximum backward fill |
| Hourly window | ±30 minutes | Time range for hourly aggregation |

---

### Variable Treatment Summary

| Variable | Impute Tier 1 | Impute Tier 2 | Impute Tier 3 | Bounds Check | Hourly Agg | Daily Agg |
|----------|---------------|---------------|---------------|--------------|------------|-----------|
| Temperature | ✓ (3h) | ✓ (6h) | | -40 to 40°C | Mean | Min/Max/Mean |
| Dew Point | ✓ (3h) | ✓ (6h) | | -100 to 100°C | Mean | Min/Max/Mean |
| Rh | ✓ (3h) | ✓ (6h) | Calculate from T/Dew | 0-100% | Mean | Min/Max/Mean |
| Wind Speed | ✓ (3h) | ✓ (6h) | | | Mean | Max/Mean |
| Wind Gust | ✓ (3h) | ✗ | Use Wind Speed | | Max | Max |
| Wind Direction | ✓ (3h) | ✗ | | | Circular Mean | Mode |
| Rain | ✓ (3h) | ✗ | Missing = 0 | | Sum | Sum |

**Legend:**
- ✓ = Applied
- ✗ = Not applied
- (Xh) = Maximum gap in hours

---

### Output Files Summary

| File | Size | Rows | Use Case |
|------|------|------|----------|
| PEINP_all_weather_data.csv | ~240 MB | ~800,000 | Detailed analysis, ML training |
| PEINP_hourly_weather_data.csv | ~5 MB | ~35,000 | Time-series analysis |
| PEINP_daily_weather_data.csv | ~500 KB | ~1,200 | Daily summaries, trends |
| PEINP_data_quality_report.csv | ~50 KB | ~80 | Quality assessment |