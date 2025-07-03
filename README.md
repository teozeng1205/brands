# Fare Brand Analysis and Basic Economy Detection

## Overview

This project performs comprehensive analysis of fare brands across US domestic markets to identify Basic Economy fare structures for each airline. The analysis includes advance purchase pattern analysis, multi-factor scoring for Basic Economy detection, and comprehensive visualizations.

## Key Features

- **US Domestic Focus**: Uses `metadata.airportlocation_extra` table to filter for US domestic markets only
- **Multi-Factor Basic Economy Detection**: Combines price rank, refundability, change fees, and brand name analysis
- **Advance Purchase Analysis**: Analyzes brand availability across 7, 14, and 21-day advance purchase windows
- **Comprehensive Visualizations**: Creates professional charts showing brand distribution, price patterns, and detection confidence
- **Production-Ready Deliverables**: Generates CSV reports and PNG visualizations with timestamps

## Files

### Core Analysis Script
- `fare_brand_analysis_enhanced.py` - Main analysis pipeline with comprehensive EDA and Basic Economy detection

### Supporting Files
- `rs_access_v1.py` - Redshift connection utility
- `.env.sh` - Environment variables for AWS credentials (not tracked in git)
- `.gitignore` - Excludes sensitive files and temporary outputs

### Output Files (Generated)
- `basic_economy_analysis_enhanced_YYYYMMDD_HHMMSS.csv` - Deliverable table with Basic Economy identification
- `fare_brand_analysis_visualizations_YYYYMMDD_HHMMSS.png` - Main visualization dashboard
- `summary_statistics_YYYYMMDD_HHMMSS.png` - Additional summary statistics

## Methodology

### Data Source
- **Dataset**: Common Output format from 3Victors
- **Filter**: US domestic markets only (using `metadata.airportlocation_extra`)
- **Date**: Sales date 20250629
- **Limit**: 75,000 records for analysis

### Basic Economy Detection Logic

The system uses a multi-factor scoring approach (0-100 points):

1. **Price Rank (40% weight)**
   - Rank 1 (cheapest): 40 points
   - Rank 2: 30 points  
   - Rank 3: 20 points

2. **Refundability (20% weight)**
   - <10% refundable: 20 points
   - <30% refundable: 10 points

3. **Change Fees (20% weight)**
   - >$100 average: 20 points
   - >$50 average: 10 points

4. **Brand Name Analysis (20% weight)**
   - Basic keywords (+15 points): 'basic', 'economy', 'main', 'standard', 'saver', 'light', 'essential'
   - Premium keywords (-15 points): 'first', 'business', 'premium', 'plus', 'comfort', 'extra', 'flex'

### Confidence Levels
- **High**: Score ≥ 70
- **Medium**: Score ≥ 50
- **Low**: Score < 50

## Usage

### Prerequisites
1. AWS credentials configured in `.env.sh`
2. Python packages: pandas, numpy, matplotlib, seaborn
3. Access to Redshift database

### Running the Analysis
```bash
# Source AWS credentials
source .env.sh

# Run the analysis
python fare_brand_analysis_enhanced.py
```

### Output
The script generates:
1. **Console output** with detailed analysis results
2. **CSV file** with airline-market combinations and Basic Economy identification
3. **Visualization files** showing brand distribution and patterns

## Sample Results

### Dataset Overview
- **Records**: 75,000 US domestic flights
- **Airlines**: 13 carriers (AA, B6, WN, DL, UA, AS, etc.)
- **Markets**: 591 unique origin-destination pairs
- **Fare Families**: 126 distinct brand types

### Basic Economy Detection
- **High Confidence**: ~700 airline-market combinations
- **Medium Confidence**: ~100 combinations
- **Low Confidence**: ~5 combinations

### Key Findings
- **AA**: Basic Economy identified in 267 markets
- **B6**: BLUE BASIC identified in 113 markets  
- **DL**: DELTA MAIN BASIC identified in 103 markets
- **WN**: Wanna Get Away Plus identified as Basic Economy equivalent

## Advance Purchase Analysis

The analysis categorizes advance purchase windows:
- **0-7 days**: Short-term bookings
- **8-14 days**: Medium-term bookings
- **15-21 days**: Long-term bookings
- **22-30 days**: Extended bookings
- **30+ days**: Very early bookings

Brand availability and pricing patterns are analyzed for each window to understand fare structure dynamics.

## Code Structure

The main script is organized into clear, documented functions:

1. `get_us_domestic_data()` - Fetches data from Redshift
2. `clean_and_prepare_data()` - Data cleaning and feature engineering
3. `perform_exploratory_data_analysis()` - Comprehensive EDA
4. `analyze_advance_purchase_patterns()` - Advance purchase analysis
5. `analyze_fare_brands_by_airline()` - Brand diversity analysis
6. `identify_basic_economy_candidates()` - Multi-factor Basic Economy detection
7. `create_visualizations()` - Professional chart generation
8. `create_deliverable_table()` - Final report generation

## Maintenance

- **Version**: 2.0
- **Last Updated**: 2025-07-03
- **Status**: Production-ready
- **Dependencies**: pandas, numpy, matplotlib, seaborn, rs_access_v1

## Notes

- All output files include timestamps for traceability
- The analysis focuses on outbound fare families for consistency
- Price analysis includes both inclusive and exclusive pricing
- Visualizations are saved in high-resolution PNG format
- The pipeline is designed to handle missing data gracefully

## Mission

As a Data Scientist, explore the Common Output dataset for domestic US markets and identify the range of fare brands offered by each airline, so that we can better understand brand structures and determine which fares are likely to represent Basic Economy—typically the lowest-cost and most restrictive options—and use these insights to inform our standard brand mapping efforts. 