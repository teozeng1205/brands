# Enhanced Fare Brand Analysis and Basic Economy Detection v2.0

## Overview

This project performs comprehensive analysis of fare brands across US domestic markets to identify Basic Economy fare structures for each airline. The enhanced v2.0 pipeline implements significant improvements over the original version, including market-specific price ranking, comprehensive advance purchase analysis, and enhanced Basic Economy detection algorithms.

## Key Features

- **Enhanced Data Coverage**: Complete market coverage with multiple sales dates (no 75K limit)
- **Market-Specific Analysis**: Price ranking within each origin-destination market for accurate detection
- **Advanced Purchase Pattern Analysis**: Detailed analysis across 7, 14, 21-day windows
- **Enhanced Basic Economy Detection**: Multi-factor scoring with improved methodology
- **Production-Ready Deliverables**: Comprehensive CSV reports and visualizations with all required columns
- **Comprehensive Documentation**: Detailed methodology and implementation guide

## Files

### Core Analysis Scripts
- `enhanced_fare_brand_analysis.py` - **Main enhanced pipeline** with all v2.0 improvements
- `pipeline_analysis_and_recommendations.py` - Analysis of pipeline gaps and recommendations

### Supporting Files
- `rs_access_v1.py` - Redshift connection utility
- `fare_brand_analysis_enhanced.py` - Original pipeline (for comparison)
- `.env.sh` - Environment variables for AWS credentials (not tracked in git)
- `.gitignore` - Excludes sensitive files and temporary outputs

### Documentation
- `ENHANCED_ANALYSIS_GUIDE.md` - Comprehensive guide for the enhanced pipeline
- `README.md` - This file

### Output Files (Generated)
- `enhanced_basic_economy_analysis_YYYYMMDD_HHMMSS.csv` - Enhanced deliverable table
- `enhanced_analysis_dashboard_YYYYMMDD_HHMMSS.png` - Comprehensive visualizations

## Enhanced Methodology v2.0

### Data Source
- **Dataset**: Common Output format from 3Victors
- **Filter**: US domestic markets only (using `metadata.airportlocation_extra`)
- **Date Range**: Multiple sales dates (20250625-20250629) for trend analysis
- **Coverage**: Complete market coverage (no record limits)

### Enhanced Basic Economy Detection Logic

The enhanced system uses an improved multi-factor scoring approach (0-100 points):

1. **Market-Specific Price Ranking (35% weight)**
   - Rank 1 in market (cheapest): 35 points
   - Rank 2 in market: 25 points  
   - Rank 3 in market: 15 points
   - Rank 4-5 in market: 10 points
   - Rank 6+ in market: 5 points

2. **Advance Purchase Availability (20% weight)**
   - Available 0-7 days: +8 points
   - Available 8-14 days: +6 points
   - Available 15-21 days: +4 points
   - Available 22-30 days: +2 points
   - Maximum: 20 points

3. **Restriction Analysis (25% weight)**
   - <5% refundable: 25 points
   - 5-15% refundable: 20 points
   - 15-30% refundable: 15 points
   - 30-50% refundable: 10 points
   - >50% refundable: 5 points

4. **Change Fee Analysis (10% weight)**
   - >$150 average: 10 points
   - $100-150 average: 8 points
   - $50-100 average: 6 points
   - <$50 average: 4 points

5. **Brand Name Analysis (10% weight)**
   - Basic keywords: +10 points ('basic', 'economy', 'main', 'standard', 'saver', 'light', 'essential')
   - Premium keywords: 0 points ('first', 'business', 'premium', 'plus', 'comfort', 'extra', 'flex')
   - Neutral: 5 points

### Enhanced Confidence Levels
- **Very High**: Score ≥ 80 (Strong Basic Economy candidate)
- **High**: Score 70-79 (Likely Basic Economy)
- **Medium**: Score 50-69 (Possible Basic Economy)
- **Low**: Score 30-49 (Unlikely Basic Economy)
- **Very Low**: Score < 30 (Not Basic Economy)

## Usage

### Prerequisites
1. AWS credentials configured in `.env.sh`
2. Python packages: pandas, numpy, matplotlib, seaborn, boto3, psycopg2-binary
3. Access to Redshift database

### Running the Enhanced Analysis
```bash
# Source AWS credentials
source .env.sh

# Run the enhanced analysis
python enhanced_fare_brand_analysis.py
```

### Output
The enhanced script generates:
1. **Console output** with detailed analysis results
2. **CSV file** with comprehensive airline-market combinations and Basic Economy identification
3. **Visualization files** showing enhanced brand distribution and patterns

## Enhanced Deliverable Table

The enhanced deliverable includes all required columns from acceptance criteria:

| Column | Description |
|--------|-------------|
| Airline | Carrier code |
| Source | Data source identifier |
| Market | Origin-destination pair |
| All_Detected_Brands | All fare brands found |
| Identified_Basic_Economy_Brands | High-confidence Basic Economy brands |
| Confidence_Score | Score 0-100 |
| Confidence_Level | Very High/High/Medium/Low/Very Low |
| Price_Rank_in_Market | Market-specific price rank |
| Avg_Price_Inclusive | Average inclusive price |
| Pct_Refundable | Percentage refundable |
| Avg_Change_Fee | Average change fee |
| Available_0_7_Days | Available 0-7 days out (Y/N) |
| Available_8_14_Days | Available 8-14 days out (Y/N) |
| Available_15_21_Days | Available 15-21 days out (Y/N) |
| Available_22_Plus_Days | Available 22+ days out (Y/N) |
| Analysis_Date | Date of analysis |
| Methodology_Version | Version identifier |

## Key Improvements v2.0

### 1. Market-Specific Context
- **Old**: Global price ranking across all markets
- **New**: Market-specific ranking within each origin-destination pair
- **Impact**: More accurate Basic Economy detection for each route

### 2. Complete Data Coverage
- **Old**: 75K record limit, single sales date
- **New**: Complete market coverage, multiple sales dates
- **Impact**: Better representation of airline brand strategies

### 3. Enhanced Advance Purchase Analysis
- **Old**: Basic bucketing of advance purchase windows
- **New**: Detailed analysis of brand availability across 7, 14, 21-day windows
- **Impact**: More sophisticated detection of restriction patterns

### 4. Improved Scoring Algorithm
- **Old**: 40% price, 20% refundability, 20% change fees, 20% brand name
- **New**: 35% market ranking, 20% advance purchase, 25% restrictions, 10% change fees, 10% brand name
- **Impact**: More accurate confidence scoring

### 5. Production-Ready Deliverables
- **Old**: Basic airline-market-brand identification
- **New**: Comprehensive table with all required columns including advance purchase availability
- **Impact**: Meets all acceptance criteria requirements

## Expected Results

### Dataset Overview
- **Records**: Complete US domestic flight coverage
- **Airlines**: 10-15 major carriers (AA, B6, WN, DL, UA, AS, etc.)
- **Markets**: 300-600 unique origin-destination pairs
- **Fare Families**: 100+ distinct brand types

### Basic Economy Detection
- **Very High Confidence**: ~200-400 airline-market combinations
- **High Confidence**: ~300-600 combinations
- **Medium Confidence**: ~200-400 combinations
- **Low Confidence**: ~100-200 combinations

### Key Findings
- **AA**: Basic Economy identified in 200+ markets
- **B6**: BLUE BASIC identified in 100+ markets  
- **DL**: DELTA MAIN BASIC identified in 100+ markets
- **WN**: Wanna Get Away Plus identified as Basic Economy equivalent

## Code Structure

The enhanced script is organized into clear, documented functions:

1. `get_enhanced_data()` - Fetches enhanced data from Redshift
2. `clean_and_enhance_data()` - Enhanced data cleaning and feature engineering
3. `add_market_specific_ranking()` - Market-specific price ranking
4. `analyze_brand_availability()` - Brand availability pattern analysis
5. `enhanced_basic_economy_scoring()` - Enhanced Basic Economy detection
6. `create_enhanced_deliverable()` - Enhanced deliverable table generation
7. `create_visualizations()` - Enhanced chart generation

## Maintenance

- **Version**: 2.0 Enhanced
- **Last Updated**: 2025-07-03
- **Status**: Production-ready
- **Dependencies**: pandas, numpy, matplotlib, seaborn, boto3, psycopg2-binary, rs_access_v1

## Mission

As a Data Scientist, explore the Common Output dataset for domestic US markets and identify the range of fare brands offered by each airline, so that we can better understand brand structures and determine which fares are likely to represent Basic Economy—typically the lowest-cost and most restrictive options—and use these insights to inform our standard brand mapping efforts.

## Documentation

For detailed implementation guide and methodology, see `ENHANCED_ANALYSIS_GUIDE.md`. 