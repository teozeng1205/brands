# Fare Brand Analysis and Basic Economy Detection

## Overview

This project performs comprehensive analysis of fare brands across US domestic markets to identify Basic Economy fare structures for each airline. The analysis includes advance purchase pattern analysis, multi-factor scoring for Basic Economy detection, and comprehensive visualizations.

## Project Structure

```
brands/
├── fare_brand_analysis_enhanced.py  # Main analysis script
├── rs_access_v1.py                  # Redshift connection utilities
├── README.md                        # This documentation
├── .gitignore                       # Git ignore rules
└── .env.sh                          # Environment variables (not tracked)
```

## Key Features

- **US Domestic Focus**: Uses `metadata.airportlocation_extra` table to filter for US domestic markets only
- **Multi-Factor Basic Economy Detection**: Combines price rank, refundability, change fees, and brand name analysis
- **Advance Purchase Analysis**: Analyzes brand availability across 7, 14, and 21-day advance purchase windows
- **Comprehensive Visualizations**: Creates detailed charts showing brand distribution, price patterns, and detection confidence
- **Confidence Scoring**: Provides High/Medium/Low confidence levels for Basic Economy identification

## Requirements

- Python 3.8+
- pandas
- numpy
- matplotlib
- seaborn
- AWS credentials (configured via `.env.sh`)

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install pandas numpy matplotlib seaborn
   ```
3. Set up AWS credentials in `.env.sh`:
   ```bash
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_DEFAULT_REGION=us-east-1
   ```

## Usage

Run the main analysis script:

```bash
source .env.sh && python fare_brand_analysis_enhanced.py
```

## Output Files

The script generates several output files with timestamps:

1. **CSV Deliverable**: `basic_economy_analysis_enhanced_YYYYMMDD_HHMMSS.csv`
   - Contains: Airline, Market, All Detected Brands, Identified Basic Economy Brand, Confidence Level, BE Score

2. **Visualizations**: 
   - `fare_brand_analysis_visualizations_YYYYMMDD_HHMMSS.png` - Main analysis charts
   - `summary_statistics_YYYYMMDD_HHMMSS.png` - Summary statistics charts

## Methodology

### Basic Economy Detection Logic

The script uses a multi-factor scoring system (0-100 points):

1. **Price Rank (40 points)**: Cheapest average price gets 40 points, second cheapest gets 30, etc.
2. **Refundability (20 points)**: Less than 10% refundable gets 20 points, less than 30% gets 10 points
3. **Change Fees (20 points)**: High change fees (>$100) get 20 points, moderate fees (>$50) get 10 points
4. **Brand Name Analysis (20 points)**: Basic keywords add 15 points, premium keywords subtract 15 points

### Confidence Levels

- **High**: Score ≥ 70 points
- **Medium**: Score ≥ 50 points  
- **Low**: Score < 50 points

### Advance Purchase Analysis

Brands are analyzed across five advance purchase windows:
- 0-7 days
- 8-14 days
- 15-21 days
- 22-30 days
- 30+ days

## Data Sources

- **Primary**: `common_output.common_output_format` - Main fare data
- **Filtering**: `metadata.airportlocation_extra` - US airport identification
- **Date Range**: Sales date 20250629 (configurable in script)

## Sample Results

The analysis typically covers:
- 75,000+ US domestic records
- 10-15 airlines
- 1,000+ markets
- 100+ unique fare families
- 90%+ high-confidence Basic Economy identifications

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure `.env.sh` is sourced before running
2. **Redshift Connection**: Check network access and credentials
3. **Memory**: Large datasets may require additional memory allocation

### Error Handling

The script includes comprehensive error handling and will:
- Print detailed error messages
- Continue processing when possible
- Generate partial results if some steps fail

## Contributing

When making changes:
1. Test with a subset of data first
2. Update documentation for any new features
3. Ensure all acceptance criteria are still met
4. Commit with descriptive messages

## License

Internal use only - 3Victors Data Science Team 