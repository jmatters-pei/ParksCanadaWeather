# Parks Canada Weather Data Processing Pipeline

**A Python-based system for processing, cleaning, and analyzing weather data from multiple stations at or near Prince Edward Island National Park.**

---

## Quick Start Guide
Ensure that your local weather data is in the following folder C:\WeatherData\Data 
Ensure that there is on subfolder for each data station. Ensure none of the file names have spaces in them. The name of the station in the end product will be the same as the first level subfolder under Data. The data should be in CSV format.


Run the cleaning.py file. It will produce 5 reports. 

A CSV with all the cleaned data PEINP_all_weather_data.csv
A CSV with aggregated data by hour PEINP_hourly_weather_data.csv
A CSV with aggregated data by day PEINP_daily_weather_data.csv
A Data Quality Report PEINP_data_quality_report.csv
A log outlining the results of the main steps of the cleaning process weather_processing.log


## What This System Does

### Input
The system processes weather data from two sources:

1. **Local CSV Files** (from multiple weather stations)
   - Located in: `C:\WeatherData\Data`
   - Format: CSV files organized by station in subdirectories
   - Variables: Temperature, humidity, wind, rain, etc.

2. **Environment Canada Climate Data** (ECCC)
   - Station: Stanhope (Station ID: 6545)
   - Period: 2022-present
   - Automatically downloaded via API

### Processing

**Step 1: Data Collection**
- Recursively scans local folders for CSV files
- Downloads ECCC data month-by-month
- Caches downloads to avoid repeated API calls

**Step 2: Data Cleaning**
- Standardizes column names across sources
- Handles character encoding issues (UTF-8, Latin1)
- Removes duplicate and constant columns
- Validates data ranges (temperature, humidity, etc.)

**Step 3: Missing Data Imputation**
- **Tier 1:** Linear interpolation for gaps < 3 hours
- **Tier 2:** Forward/backward fill for slowly-changing variables (< 6 hours)
- **Tier 3:** Variable-specific rules (e.g., missing rain = 0)
- Skips imputation if >25% missing (probable sensor failure)

**Step 4: Aggregation**
- Creates hourly averages (±30 minute windows)
- Creates daily summaries (min, max, mean)
- Uses circular mean for wind direction

### Output

Four CSV files are generated:

1. **`PEINP_all_weather_data.csv`** (~240 MB)
   - All cleaned and imputed observations
   - All stations combined
   - Includes imputation flags

2. **`PEINP_hourly_weather_data.csv`**
   - Hourly aggregated data
   - Suitable for time-series analysis

3. **`PEINP_daily_weather_data.csv`**
   - Daily summaries (min/max/mean)
   - Compact dataset for trends

4. **`PEINP_data_quality_report.csv`**
   - Missing data statistics per station-variable
   - Imputation counts by method
   - Descriptive statistics (mean, median, IQR)

---

## 💻 Prerequisites

### Required Software

You need these programs installed on your computer:

1. **Python 3.8 or higher**
   - Download from: https://www.python.org/downloads/

2. **Git** (for version control)
   - Download from: https://git-scm.com/downloads

3. **Text Editor or IDE** (choose one)
   - VS Code (recommended): https://code.visualstudio.com/

### Required Data

- Weather station CSV files in: `C:\WeatherData\Data\`
- Organize by station: `C:\WeatherData\Data\StationName\file.csv`
- Internet connection (for ECCC downloads)

---

## 🚀 Installation Guide

### Step 1: Verify Python Installation

Open **Command Prompt** or **PowerShell** and type:

```bash
python --version
```

You should see something like: `Python 3.11.5`

If you get an error, reinstall Python and make sure to check "Add to PATH".

### Step 2: Create Project Folder

```bash
# Create and navigate to project folder
mkdir C:\WeatherData\ParksCanadaWeather
cd C:\WeatherData\ParksCanadaWeather
```

### Step 3: Download the Code

**Option A: Using Git** (recommended)

```bash
git clone https://github.com/jmatters-pei/ParksCanadaWeather.git
cd ParksCanadaWeather
```

**Option B: Manual Download**

1. Go to: https://github.com/jmatters-pei/ParksCanadaWeather
2. Click green "Code" button → "Download ZIP"
3. Extract to: `C:\WeatherData\ParksCanadaWeather`

### Step 4: Install Required Python Packages

```bash
# Install dependencies
pip install pandas numpy

# Verify installation
pip list
```

You should see `pandas` and `numpy` in the list.

### Step 5: Verify Data Folder Structure

Make sure your data is organized like this:

```
C:\WeatherData\
├── ParksCanadaWeather\        ← Your code is here
│   ├── weather_processing.py
│   ├── requirements.txt
│   └── README.md
└── Data\                      ← Your data is here
    ├── Station1\
    │   ├── 2022_data.csv
    │   └── 2023_data.csv
    ├── Station2\
    │   └── data.csv
    └── Station3\
        └── observations.csv
```

**Important:** The script expects data in `C:\WeatherData\Data\`

---

## 🎬 How to Use

### For Complete Beginners (No Python Experience)

#### Method 1: Run from VS Code (Easiest)

1. **Open VS Code**

2. **Open the project folder:**
   - File → Open Folder
   - Select: `C:\WeatherData\ParksCanadaWeather`

3. **Open the Python file:**
   - Click on `weather_processing.py` in the left sidebar

4. **Run the script:**
   - Press **F5** (or click the play button in top-right)
   - Or: Right-click in code → "Run Python File in Terminal"

5. **Watch the progress:**
   - You'll see messages like "Loading files..." and "Imputing data..."
   - Processing takes 5-15 minutes depending on data size

6. **Check the outputs:**
   - Look in the same folder for 4 new CSV files

#### Method 2: Run from Command Line

1. **Open Command Prompt or PowerShell**

2. **Navigate to the project folder:**
   ```bash
   cd C:\WeatherData\ParksCanadaWeather
   ```

3. **Run the script:**
   ```bash
   python weather_processing.py
   ```

4. **Wait for completion:**
   - You'll see progress messages
   - "Pipeline completed successfully!" when done

5. **Find your outputs:**
   - They're in the same folder as the script

### What to Expect When Running

**Console output will show:**

```
============================================================
Starting Weather Data Processing Pipeline v2.5
With 25% Threshold + Data Quality + Dew Bounds Check
============================================================
2026-02-14 14:30:15 - INFO - Scanning local folder: C:\WeatherData\Data
2026-02-14 14:30:16 - INFO - Found 47 CSV files
2026-02-14 14:30:17 - INFO - Loading and cleaning 47 files...
2026-02-14 14:30:45 - INFO - Successfully loaded 45 files
2026-02-14 14:30:46 - INFO - Downloading ECCC Stanhope data (Station ID: 6545)
...
2026-02-14 14:40:30 - INFO - Starting Missing Data Imputation
...
2026-02-14 14:42:15 - INFO - Pipeline completed successfully!
============================================================
```

**Processing time:** 5-15 minutes (varies by data size)

**Log file:** `weather_processing.log` (detailed execution log)

---

## 📂 Understanding the Outputs

### Output Files

#### 1. `PEINP_all_weather_data.csv` (~240 MB)

**What it contains:**
- Every weather observation after cleaning and imputation
- All stations combined into one file
- Imputation flags showing how gaps were filled

**Columns:**
- `Datetime_UTC`: Timestamp (UTC timezone)
- `station`: Weather station name
- `Temperature`: Air temperature (°C)
- `Rh`: Relative humidity (%)
- `Dew`: Dew point temperature (°C)
- `Wind Speed`: Average wind speed (km/h)
- `Wind Gust Speed`: Maximum wind gust (km/h)
- `Wind Direction`: Wind direction (degrees)
- `Rain`: Precipitation amount (mm)
- `*_imputed`: Flags (0=original, 1=interpolated, 2=filled, 3=calculated)

**Use for:**
- Detailed analysis of individual observations
- Training machine learning models
- Investigating specific weather events

#### 2. `PEINP_hourly_weather_data.csv` (~5 MB)

**What it contains:**
- Hourly aggregated data (one row per hour per station)
- Observations within ±30 minutes of each hour are averaged

**Aggregation methods:**
- Temperature, humidity, wind speed: Mean
- Wind gust: Maximum
- Rain: Total (sum)
- Wind direction: Circular mean

**Use for:**
- Time-series analysis
- Hourly fire risk calculations
- Trend analysis

#### 3. `PEINP_daily_weather_data.csv` (~500 KB)

**What it contains:**
- Daily summaries (one row per day per station)

**Columns:**
- `*_min`: Minimum value for the day
- `*_max`: Maximum value for the day
- `*_mean`: Average value for the day
- `Rain_total`: Total daily precipitation

**Use for:**
- Daily summaries and reports
- Long-term trend analysis
- Quick data exploration

#### 4. `PEINP_data_quality_report.csv`

**What it contains:**
- Data quality metrics for each station-variable combination
- Missing data counts and percentages
- Imputation statistics by method
- Descriptive statistics (mean, median, quartiles)

**Key columns:**
- `missing_percent`: % of data that was missing
- `interpolated_count`: Values filled by interpolation
- `forward_backward_filled_count`: Values filled by forward/backward fill
- `calculated_count`: Values calculated from other variables
- `mean`, `median`, `min`, `max`: Descriptive statistics

**Use for:**
- Assessing data quality before analysis
- Identifying problematic sensors/stations
- Documenting data limitations in reports

### How to Open Output Files

**Excel:**
- Double-click the CSV file
- Or: Open Excel → File → Open → Select CSV

⚠️ **Warning:** Large files (PEINP_all_weather_data.csv) may be too big for Excel!

**Recommended for large files:**
- **Python:** `pd.read_csv('PEINP_all_weather_data.csv')`
- **R:** `read.csv('PEINP_all_weather_data.csv')`
- **Power BI:** Import data → Text/CSV
- **Tableau:** Connect → Text file

---

## 📁 Project Structure

```
ParksCanadaWeather/
│
├── weather_processing.py          # Main script (1,296 lines)
│
├── requirements.txt                # Python dependencies
│   ├── pandas>=1.5.0
│   └── numpy>=1.21.0
│
├── README.md                       # This file
│
├── .gitignore                      # Files excluded from Git
│   ├── *.csv                      # Don't commit data files!
│   ├── cache/                     # Don't commit cache
│   └── *.log                      # Don't commit logs
│
├── cache/                          # Cached ECCC downloads (auto-created)
│   ├── eccc_6545_2022_01.pkl
│   ├── eccc_6545_2022_02.pkl
│   └── ...
│
├── weather_processing.log          # Execution log (auto-created)
│
└── Outputs (auto-created):
    ├── PEINP_all_weather_data.csv
    ├── PEINP_hourly_weather_data.csv
    ├── PEINP_daily_weather_data.csv
    └── PEINP_data_quality_report.csv
```

### File Descriptions

**Python Script:**
- `weather_processing.py`: Main pipeline (all functions included)

**Configuration:** (inside the Python file)
- Lines 24-46: CONFIG dictionary with all settings
- Easily modify station ID, date range, imputation parameters

**Data Flow:**
```
Raw Data Sources
    ↓
Load & Clean (parallel processing)
    ↓
Combine All Stations
    ↓
Clean & Validate
    ↓
Impute Missing Values (3-tier strategy)
    ↓
Generate Outputs
```

---

## 🔧 Technical Details

### Configuration Options

Open `weather_processing.py` and find the `CONFIG` dictionary (lines 24-46):

```python
CONFIG = {
    'ECCC_STATION_ID': 6545,              # Stanhope station
    'ECCC_START_YEAR': 2022,              # Download from 2022 onward
    'API_DELAY': 0.5,                     # Seconds between downloads
    'LOCAL_DATA_PATH': r'C:\WeatherData\Data',  # Where your CSV files are
    'OUTPUT_ALL_DATA': 'PEINP_all_weather_data.csv',
    'OUTPUT_HOURLY': 'PEINP_hourly_weather_data.csv',
    'OUTPUT_DAILY': 'PEINP_daily_weather_data.csv',
    'OUTPUT_DATA_QUALITY': 'PEINP_data_quality_report.csv',
    'CACHE_DIR': 'cache',
    'MAX_WORKERS': 4,                     # Parallel threads
    'INTERPOLATE_LIMIT_HOURS': 3,         # Max gap for interpolation
    'FORWARD_FILL_LIMIT_HOURS': 6,        # Max forward fill
    'BACKWARD_FILL_LIMIT_HOURS': 3,       # Max backward fill
    'IMPUTATION_THRESHOLD_PCT': 25.0,     # Skip if >25% missing
    'TEMP_MIN': -40,                      # PEI temperature bounds (°C)
    'TEMP_MAX': 40,
    'RH_MIN': 0,                          # Humidity bounds (%)
    'RH_MAX': 100,
    'DEW_MIN': -100,                      # Dew point bounds (°C)
    'DEW_MAX': 100,
}
```

**To change settings:**
1. Open `weather_processing.py` in a text editor
2. Find the CONFIG section (near top of file)
3. Change values as needed
4. Save the file
5. Run the script again

### Imputation Strategy (Technical)

**Tier 1: Linear Interpolation**
- Method: Time-based linear interpolation
- Limit: 3 hours maximum gap
- Formula: `y = y₁ + (y₂ - y₁) × (t - t₁) / (t₂ - t₁)`
- Applied to: All numeric variables

**Tier 2: Forward/Backward Fill**
- Method: Last observation carried forward (LOCF) / Next observation carried backward (NOCB)
- Limits: 6 hours forward, 3 hours backward
- Applied to: Temperature, Dew Point, Humidity, Wind Speed (slowly-changing variables)
- Not applied to: Rain, Wind Direction (rapidly-changing variables)

**Tier 3: Variable-Specific Rules**
- **Rain:** Missing = 0 mm (rain gauges record 0 as missing)
- **Wind Gust:** Missing but Wind Speed available → use Wind Speed
- **Relative Humidity:** Calculate from Temperature and Dew Point using Magnus formula
  - Formula: `RH = 100 × (e_dew / e_temp)` where `e = exp((17.625 × T) / (243.04 + T))`

**Threshold Check:**
- If >25% missing for a station-variable combination → skip imputation
- Rationale: Likely sensor failure, imputation would be unreliable

### Data Quality Validation

**Range checks:**
- Temperature: -40°C to +40°C (PEI climate bounds)
- Relative Humidity: 0% to 100% (physical limits)
- Dew Point: -100°C to +100°C (very conservative)
- Values outside ranges set to NaN

**Duplicate handling:**
- Duplicate column names: Merged using backward fill
- Duplicate rows: Identified and removed

### Performance Optimization

**Parallel Processing:**
- Uses ThreadPoolExecutor with 4 workers
- Files loaded simultaneously (4 at a time)
- Reduces total load time by ~75%

**Caching:**
- ECCC downloads cached as pickle files
- Avoids repeated API calls
- Cache location: `cache/` folder
- Cache naming: `eccc_{station_id}_{year}_{month}.pkl`

**Memory Management:**
- Garbage collection after major operations
- DataFrames deleted when no longer needed
- Data types optimized (int64 → int32, float64 → float32)

### Dependencies

**Core libraries:**
- `pandas>=1.5.0`: Data manipulation and analysis
- `numpy>=1.21.0`: Numerical computing

**Standard library modules:**
- `concurrent.futures`: Parallel processing
- `logging`: Execution logging
- `pathlib`: File path operations
- `urllib.request`: HTTP downloads
- `pickle`: Object serialization
- `datetime`: Date/time handling
- `re`: Regular expressions
- `json`: JSON parsing
- `gc`: Garbage collection
- `os`: Operating system interface
- `time`: Time delays

No external dependencies beyond pandas and numpy!

---

## ❓ Troubleshooting

### Common Issues and Solutions

#### Issue 1: "ModuleNotFoundError: No module named 'pandas'"

**Problem:** Python packages not installed

**Solution:**
```bash
pip install pandas numpy
```

Or if you have multiple Python versions:
```bash
python -m pip install pandas numpy
```

---

#### Issue 2: "Local data path does not exist: C:\WeatherData\Data"

**Problem:** Data folder not found

**Solutions:**
1. Create the folder: `mkdir C:\WeatherData\Data`
2. Move your CSV files into that folder
3. Or change the path in CONFIG (line 28 of the script)

---

#### Issue 3: "Failed to download ECCC data"

**Problem:** No internet connection or ECCC website down

**Solutions:**
1. Check your internet connection
2. Try again later (ECCC site sometimes has maintenance)
3. The script will continue with local data only

---

#### Issue 4: Script runs but no output files

**Problem:** Likely an error during processing

**Solutions:**
1. Check `weather_processing.log` for error messages
2. Look at the console output for error details
3. Verify data files are valid CSV format

---

#### Issue 5: "Permission denied" error

**Problem:** File is open in another program

**Solution:**
1. Close Excel, R, or any program with output files open
2. Run the script again

---

#### Issue 6: Output files are empty or very small

**Problem:** No data passed validation or all data was filtered out

**Solutions:**
1. Check data quality report to see what happened
2. Review temperature/humidity bounds in CONFIG
3. Verify input data files contain valid observations

---

#### Issue 7: "Failed to load X files"

**Problem:** Some CSV files couldn't be read

**Possible causes:**
- Corrupted files
- Wrong encoding
- Not actually CSV format

**Solutions:**
1. Check `weather_processing.log` to see which files failed
2. Try opening failed files in Excel to verify they're valid
3. The script continues with files that did load

---

#### Issue 8: Processing is very slow

**Problem:** Large dataset or slow computer

**Solutions:**
1. Be patient (240 MB of data takes time!)
2. Reduce MAX_WORKERS in CONFIG if computer is slow
3. Check if antivirus is scanning files during processing

---

### Getting Help

**Check the log file:**
```bash
# Open the log file to see detailed error messages
notepad weather_processing.log
```

**Common log messages:**

- `INFO - Starting...`: Normal progress message
- `WARNING - Failed to load cache`: Not critical, will download
- `ERROR - Failed to load file`: Check that file
- `ERROR - Pipeline failed`: Something went wrong, check details

**Still stuck?**

1. Read the error message carefully
2. Check log file for details
3. Verify all prerequisites are met
4. Try with a small subset of data first

---

### Contributing

This is an academic project, but suggestions are welcome!

**To contribute:**
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request with description

---

## Acknowledgments

### Data Sources

**Environment and Climate Change Canada (ECCC)**
- Stanhope weather station data (Station ID: 6545)
- Historical climate data available via public API
- https://climate.weather.gc.ca/

**Parks Canada**
- Local weather station network data
- Field validation and domain expertise
- Project support and guidance

### Academic Support

**Post-Graduate Certificate in Data Analytics**
- Program: Holland College
- Instructor: Chris Stewart
- Cohort: 2025-2026

### Technical Resources

**Python Libraries:**
- pandas: McKinney, W. (2010). Data structures for statistical computing in Python
- NumPy: Harris, C.R. et al. (2020). Array programming with NumPy

**Meteorological Methods:**
- Magnus-Tetens formula for humidity calculations
- Circular statistics for wind direction averaging

---

## 📄 License

This project is licensed under the MIT License.

```
Copyright (c) 2026 Jonathan Matters

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 📞 Contact

**Project Author:** Jonathan Matters  
**Email:** jmatters@dal.ca  
**GitHub:** https://github.com/jmatters-pei/ParksCanadaWeather  
**LinkedIn:** www.linkedin.com/in/jonathan-matters-45701b54

---

## 📚 Citation

If you use this code or methodology in your research, please cite:

```
Jonathan Matters (2026). Parks Canada Weather Data Processing Pipeline.
GitHub repository: https://github.com/jmatters-pei/ParksCanadaWeather
```

---

**Last Updated:** February 14, 2026  
**Version:** 2.5  
**Python Version:** 3.8+  
**Status:** Production-ready ✅# ParksCanadaWeather