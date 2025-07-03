# Fare Brand Analysis and Basic Economy Detection

## Overview

This project performs comprehensive analysis of fare brands across US domestic markets to identify Basic Economy fare structures for each airline. The analysis helps understand brand structures and determine which fares are likely to represent Basic Economy—typically the lowest-cost and most restrictive options.

## Mission

As a Data Scientist, explore the Common Output dataset for domestic US markets and identify the range of fare brands offered by each airline, so that we can better understand brand structures and determine which fares are likely to represent Basic Economy—typically the lowest-cost and most restrictive options—and use these insights to inform our standard brand mapping efforts.

## Files

- `fare_brand_analysis.py` - Main analysis script
- `rs_access_v1.py` - Redshift database access module
- `analysis_summary.py` - Comprehensive results analysis and insights
- `basic_economy_analysis_YYYYMMDD_HHMMSS.csv` - Generated deliverable table

## Requirements

- Python 3.8+
- pandas
- numpy
- matplotlib
- seaborn
- boto3
- psycopg2

## Installation

```bash
pip install pandas numpy matplotlib seaborn boto3 psycopg2
```

## Usage

1. Set up AWS credentials in `.env.sh`:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

2. Run the main analysis:
```bash
source .env.sh && python fare_brand_analysis.py
```

3. Generate insights summary:
```bash
python analysis_summary.py
```

## Methodology

### Data Inputs
- Uses the "Common Output" dataset provided by 3Victors
- Focuses only on US domestic itineraries using `metadata.airportlocation_extra` table
- Groups records by airline and origin-destination market

### Brand Identification
- For each airline and market combination, identifies all distinct fare brands present
- Analyzes brand availability over departure horizons of 7, 14, and 21 days out
- Uses a combination of features to determine which brand(s) are most restrictive

### Basic Economy Detection Logic
The algorithm uses a multi-factor scoring system:

1. **Price criteria (40% weight)**
   - Cheapest average price: +40 points
   - Second cheapest: +30 points
   - Third cheapest: +20 points

2. **Refundability (20% weight)**
   - Less than 10% refundable: +20 points
   - Less than 30% refundable: +10 points

3. **Change fee criteria (20% weight)**
   - High change fees (>$100): +20 points
   - Medium change fees (>$50): +10 points

4. **Brand name analysis (20% weight)**
   - Basic keywords (basic, economy, saver, etc.): +15 points
   - Premium keywords (first, business, premium, etc.): -15 points

### Confidence Levels
- **High**: Score ≥ 70
- **Medium**: Score 50-69
- **Low**: Score < 50

## Deliverables

### Main Table Structure
| Column | Description |
|--------|-------------|
| Airline | Carrier code |
| Market | Origin-Destination pair |
| All_Detected_Brands | Complete list of fare families in market |
| Identified_Basic_Economy_Brand | Algorithm-identified Basic Economy product |
| Confidence_Level | High/Medium/Low confidence rating |
| BE_Score | Numerical scoring (0-100) |

### Key Findings

#### Basic Economy Brands Identified:
- **American (AA)**: BASIC ECONOMY
- **JetBlue (B6)**: BLUE BASIC
- **United (UA)**: ECO-BASIC
- **Southwest (WN)**: Basic
- **Delta (DL)**: Basic Economy
- **Alaska (AS)**: SAVER

#### Success Metrics:
- **95.2% High Confidence Rate** (500/525 combinations)
- **60.7% Keyword Validation** (brands contain expected Basic Economy keywords)
- **525 airline-market combinations** analyzed
- **12 airlines** covered across **411 markets**

## Constraints & Assumptions

- Work is limited to domestic US markets at this stage
- Brand names may vary by airline and may not be standardized
- Uses `metadata.airportlocation_extra` table for US airport identification
- Focuses on outbound fare families for analysis

## Future Enhancements

- Extend to international markets
- Add more sophisticated brand name analysis
- Include fare rule analysis when available
- Implement machine learning for improved classification
- Add visualization capabilities for brand distribution

## Author

Data Science Team - 3Victors
Date: 2025-07-03 