"""
Main Fare Brand Analysis and Basic Economy Detection
===================================================

This is the primary analysis script for identifying fare brands and Basic Economy detection
in US domestic markets using the 3Victors Common Output dataset.

Key Features:
- US domestic market filtering using metadata.airportlocation_extra
- Comprehensive brand identification by airline and market
- Advance purchase pattern analysis (7, 14, 21 days)
- Multi-factor Basic Economy scoring (price rank, refundability, change fees, brand names)
- Production-ready visualizations and deliverable tables
- High confidence detection with transparent methodology

Usage: python main_analysis.py

Outputs:
- basic_economy_analysis_YYYYMMDD_HHMMSS.csv (deliverable table)
- fare_brand_visualizations_YYYYMMDD_HHMMSS.png (summary charts)

Author: Data Science Team
Date: 2025-07-03
"""

import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import rs_access_v1 as rs

warnings.filterwarnings('ignore')
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)

# =============================
# 1. DATA EXTRACTION
# =============================
def get_us_domestic_data():
    """
    Fetch US domestic flight data from Redshift using metadata.airportlocation_extra.
    """
    print("\nFetching US domestic flight data from Redshift...")
    rs.assign_connection("ds")
    query = """
    SELECT 
        carrier, origin, destination, outbound_departure_date, outbound_fare_family,
        inbound_fare_family, price_inc, price_exc, tax, yqyr, currency, search_class, cabin,
        outbound_cabins, inbound_cabins, outbound_booking_class, inbound_booking_class,
        outbound_fare_basis, inbound_fare_basis, refundable, change_fee, los, sales_date,
        observation_date, customer, outbound_travel_stop_over, inbound_travel_stop_over
    FROM common_output.common_output_format 
    WHERE sales_date = 20250629
      AND origin IN (SELECT airportcode FROM metadata.airportlocation_extra WHERE countryname = 'United States')
      AND destination IN (SELECT airportcode FROM metadata.airportlocation_extra WHERE countryname = 'United States')
      AND origin != destination
      AND carrier IS NOT NULL
      AND outbound_fare_family IS NOT NULL
      AND price_inc > 0
    LIMIT 75000;
    """
    df = rs.rq(query)
    if df is not None and len(df) > 0:
        print(f"Fetched {len(df):,} records for US domestic markets.")
        return df
    print("No data returned from query.")
    return pd.DataFrame()

# =============================
# 2. DATA CLEANING & PREP
# =============================
def clean_and_prepare_data(df):
    """
    Clean and prepare the data for analysis, including advance purchase bucket assignment.
    """
    if df.empty:
        print("No data to clean.")
        return df
    df = df.copy()
    # Date conversions
    df['outbound_departure_date'] = pd.to_datetime(df['outbound_departure_date'], errors='coerce')
    df['observation_date'] = pd.to_datetime(df['observation_date'], errors='coerce')
    df['sales_date'] = pd.to_datetime(df['sales_date'], format='%Y%m%d', errors='coerce')
    # Advance purchase calculation
    df['days_to_departure'] = (df['outbound_departure_date'] - df['observation_date']).dt.days
    # Market identifier
    df['market'] = df['origin'] + '-' + df['destination']
    # Fare family cleaning
    df['outbound_fare_family'] = df['outbound_fare_family'].fillna('Unknown')
    df['inbound_fare_family'] = df['inbound_fare_family'].fillna('Unknown')
    df['primary_fare_family'] = df['outbound_fare_family']
    # Price and numeric cleaning
    df['price_inc'] = pd.to_numeric(df['price_inc'], errors='coerce')
    df['price_exc'] = pd.to_numeric(df['price_exc'], errors='coerce')
    df['change_fee'] = pd.to_numeric(df['change_fee'], errors='coerce')
    # Remove invalids
    df = df.dropna(subset=['carrier', 'market', 'primary_fare_family', 'price_inc'])
    df = df[df['price_inc'] > 0]
    df = df.dropna(subset=['days_to_departure'])
    df = df[df['days_to_departure'] >= 0]
    # Advance purchase bucket
    def categorize_advance_purchase(days):
        if days <= 7: return '0-7 days'
        elif days <= 14: return '8-14 days'
        elif days <= 21: return '15-21 days'
        elif days <= 30: return '22-30 days'
        else: return '30+ days'
    df['advance_purchase_bucket'] = df['days_to_departure'].apply(categorize_advance_purchase)
    return df

# =============================
# 3. EXPLORATORY DATA ANALYSIS
# =============================
def exploratory_data_analysis(df):
    """
    Print summary statistics and distributions for airlines, markets, brands, and prices.
    """
    print("\n--- DATASET OVERVIEW ---")
    print(f"Records: {len(df):,}, Airlines: {df['carrier'].nunique()}, Markets: {df['market'].nunique()}, Brands: {df['primary_fare_family'].nunique()}")
    print("\nTop Airlines:")
    print(df['carrier'].value_counts().head(10))
    print("\nTop Markets:")
    print(df['market'].value_counts().head(10))
    print("\nTop Fare Families:")
    print(df['primary_fare_family'].value_counts().head(15))
    print("\nPrice Stats:")
    print(df['price_inc'].describe())
    print("\nAdvance Purchase Buckets:")
    print(df['advance_purchase_bucket'].value_counts())
    return df

# =============================
# 4. ADVANCE PURCHASE ANALYSIS
# =============================
def analyze_advance_purchase_patterns(df):
    """
    Analyze brand availability and price by advance purchase window.
    """
    ap_analysis = df.groupby(['carrier', 'market', 'advance_purchase_bucket', 'primary_fare_family']).agg({
        'price_inc': ['count', 'mean', 'min'],
        'refundable': 'mean',
        'change_fee': 'mean'
    }).reset_index()
    ap_analysis.columns = ['carrier', 'market', 'advance_purchase_bucket', 'fare_family',
                          'record_count', 'avg_price', 'min_price', 'refundable_pct', 'avg_change_fee']
    print("\n--- ADVANCE PURCHASE PATTERNS ---")
    for bucket in ['0-7 days', '8-14 days', '15-21 days']:
        print(f"\n{bucket}:")
        bucket_data = ap_analysis[ap_analysis['advance_purchase_bucket'] == bucket].groupby('fare_family').agg({
            'record_count': 'sum', 'avg_price': 'mean'
        }).reset_index().sort_values('record_count', ascending=False)
        for _, row in bucket_data.head(8).iterrows():
            print(f"  {row['fare_family']}: {row['record_count']:,} records, avg ${row['avg_price']:.2f}")
    return ap_analysis

# =============================
# 5. BRAND ANALYSIS
# =============================
def analyze_fare_brands_by_airline(df):
    """
    Identify all brands per airline and market, with summary stats.
    """
    brand_analysis = df.groupby(['carrier', 'market']).agg({
        'primary_fare_family': lambda x: sorted(list(set(x))),
        'price_inc': ['count', 'mean', 'min', 'max'],
        'days_to_departure': ['mean', 'min', 'max']
    }).reset_index()
    brand_analysis.columns = ['carrier', 'market', 'all_brands', 'record_count', 
                             'avg_price', 'min_price', 'max_price', 
                             'avg_days_out', 'min_days_out', 'max_days_out']
    brand_analysis['num_brands'] = brand_analysis['all_brands'].apply(len)
    brand_analysis['brand_count_str'] = brand_analysis['all_brands'].apply(lambda x: ', '.join(x))
    print("\n--- BRAND DIVERSITY BY AIRLINE ---")
    print(brand_analysis.groupby('carrier')['num_brands'].mean().round(2).sort_values(ascending=False))
    return brand_analysis

# =============================
# 6. BASIC ECONOMY DETECTION
# =============================
def identify_basic_economy_candidates(df, brand_analysis):
    """
    Use a multi-factor score to flag the most restrictive/lowest fare as Basic Economy.
    """
    brand_metrics = df.groupby(['carrier', 'market', 'primary_fare_family']).agg({
        'price_inc': ['count', 'mean', 'min', 'max', 'std'],
        'refundable': 'mean',
        'change_fee': ['mean', 'max'],
        'days_to_departure': 'mean',
        'outbound_booking_class': lambda x: len(set(x))
    }).reset_index()
    brand_metrics.columns = ['carrier', 'market', 'fare_family', 'record_count', 
                           'avg_price', 'min_price', 'max_price', 'price_std',
                           'refundable_pct', 'avg_change_fee', 'max_change_fee',
                           'avg_days_out', 'booking_class_variety']
    brand_metrics['price_rank'] = brand_metrics.groupby(['carrier', 'market'])['avg_price'].rank(method='min')
    def calculate_basic_economy_score(row):
        score = 0
        if row['price_rank'] == 1: score += 40
        elif row['price_rank'] == 2: score += 30
        elif row['price_rank'] <= 3: score += 20
        if pd.notna(row['refundable_pct']):
            if row['refundable_pct'] < 0.1: score += 20
            elif row['refundable_pct'] < 0.3: score += 10
        if pd.notna(row['avg_change_fee']):
            if row['avg_change_fee'] > 100: score += 20
            elif row['avg_change_fee'] > 50: score += 10
        fare_family_lower = str(row['fare_family']).lower()
        basic_keywords = ['basic', 'economy', 'main', 'standard', 'saver', 'light', 'essential']
        premium_keywords = ['first', 'business', 'premium', 'plus', 'comfort', 'extra', 'flex']
        if any(keyword in fare_family_lower for keyword in basic_keywords): score += 15
        if any(keyword in fare_family_lower for keyword in premium_keywords): score -= 15
        return score
    brand_metrics['basic_economy_score'] = brand_metrics.apply(calculate_basic_economy_score, axis=1)
    basic_economy_candidates = brand_metrics.loc[
        brand_metrics.groupby(['carrier', 'market'])['basic_economy_score'].idxmax()
    ].copy()
    def assign_confidence(score):
        if score >= 70: return 'High'
        elif score >= 50: return 'Medium'
        else: return 'Low'
    basic_economy_candidates['confidence'] = basic_economy_candidates['basic_economy_score'].apply(assign_confidence)
    print("\n--- BASIC ECONOMY IDENTIFICATION ---")
    print(basic_economy_candidates['confidence'].value_counts())
    return basic_economy_candidates, brand_metrics

# =============================
# 7. VISUALIZATIONS
# =============================
def create_visualizations(df, ap_analysis, basic_economy_candidates):
    """
    Create and save summary visualizations for the analysis.
    """
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle('Fare Brand Analysis and Basic Economy Detection - US Domestic Markets', fontsize=16, fontweight='bold')
    # Airline distribution
    airline_counts = df['carrier'].value_counts().head(10)
    axes[0, 0].bar(airline_counts.index, airline_counts.values, color='skyblue')
    axes[0, 0].set_title('Top Airlines by Record Count')
    axes[0, 0].set_xlabel('Airline')
    axes[0, 0].set_ylabel('Number of Records')
    axes[0, 0].tick_params(axis='x', rotation=45)
    # Price distribution by airline
    top_airlines = df['carrier'].value_counts().head(6).index
    price_data = df[df['carrier'].isin(top_airlines)]
    price_data.boxplot(column='price_inc', by='carrier', ax=axes[0, 1])
    axes[0, 1].set_title('Price Distribution by Airline')
    axes[0, 1].set_xlabel('Airline')
    axes[0, 1].set_ylabel('Price (USD)')
    axes[0, 1].tick_params(axis='x', rotation=45)
    # Advance purchase distribution
    ap_counts = df['advance_purchase_bucket'].value_counts()
    axes[0, 2].pie(ap_counts.values, labels=ap_counts.index, autopct='%1.1f%%', startangle=90)
    axes[0, 2].set_title('Distribution by Advance Purchase Window')
    # Brand availability by advance purchase window
    if not ap_analysis.empty:
        ap_summary = ap_analysis.groupby(['advance_purchase_bucket', 'fare_family']).agg({'record_count': 'sum'}).reset_index()
        top_brands = ap_summary.groupby('fare_family')['record_count'].sum().nlargest(5).index
        ap_pivot = ap_summary[ap_summary['fare_family'].isin(top_brands)].pivot(
            index='advance_purchase_bucket', columns='fare_family', values='record_count').fillna(0)
        ap_pivot.plot(kind='bar', ax=axes[1, 0], width=0.8)
        axes[1, 0].set_title('Brand Availability by Advance Purchase Window')
        axes[1, 0].set_xlabel('Advance Purchase Window')
        axes[1, 0].set_ylabel('Number of Records')
        axes[1, 0].tick_params(axis='x', rotation=45)
        axes[1, 0].legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    # Basic Economy confidence distribution
    if not basic_economy_candidates.empty:
        conf_counts = basic_economy_candidates['confidence'].value_counts()
        colors = ['#2E8B57', '#FFA500', '#DC143C']
        axes[1, 1].bar(conf_counts.index, conf_counts.values, color=colors)
        axes[1, 1].set_title('Basic Economy Detection Confidence')
        axes[1, 1].set_xlabel('Confidence Level')
        axes[1, 1].set_ylabel('Number of Combinations')
        total = conf_counts.sum()
        for i, (conf, count) in enumerate(conf_counts.items()):
            pct = (count / total) * 100
            axes[1, 1].text(i, count + 5, f'{pct:.1f}%', ha='center', va='bottom')
    # Basic Economy brands by airline
    if not basic_economy_candidates.empty:
        be_by_airline = basic_economy_candidates[basic_economy_candidates['confidence'] == 'High'].groupby('carrier')['fare_family'].agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Unknown')
        axes[1, 2].bar(range(len(be_by_airline)), be_by_airline.values, color='lightcoral')
        axes[1, 2].set_title('High-Confidence Basic Economy Brands by Airline')
        axes[1, 2].set_xlabel('Airline')
        axes[1, 2].set_ylabel('Basic Economy Brand')
        axes[1, 2].set_xticks(range(len(be_by_airline)))
        axes[1, 2].set_xticklabels(be_by_airline.index, rotation=45)
    plt.tight_layout()
    viz_filename = f"fare_brand_visualizations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(viz_filename, dpi=300, bbox_inches='tight')
    print(f"Saved visualizations to: {viz_filename}")

# =============================
# 8. DELIVERABLE TABLE
# =============================
def create_deliverable_table(basic_economy_candidates, brand_analysis):
    """
    Create the final deliverable table for mapping and reporting.
    """
    deliverable = brand_analysis.merge(
        basic_economy_candidates[['carrier', 'market', 'fare_family', 'basic_economy_score', 'confidence']],
        left_on=['carrier', 'market'], right_on=['carrier', 'market'], how='left')
    deliverable_final = deliverable[['carrier', 'market', 'brand_count_str', 'fare_family', 'confidence', 'basic_economy_score']].copy()
    deliverable_final.columns = ['Airline', 'Market', 'All_Detected_Brands', 'Identified_Basic_Economy_Brand', 'Confidence_Level', 'BE_Score']
    deliverable_final = deliverable_final.sort_values(['Airline', 'Market'])
    output_filename = f"basic_economy_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    deliverable_final.to_csv(output_filename, index=False)
    print(f"Saved deliverable table to: {output_filename}")
    return deliverable_final

# =============================
# 9. MAIN EXECUTION
# =============================
def main():
    print("\n=== MAIN FARE BRAND ANALYSIS AND BASIC ECONOMY DETECTION ===")
    df = get_us_domestic_data()
    if df.empty:
        print("No data to analyze.")
        return
    df_clean = clean_and_prepare_data(df)
    if df_clean.empty:
        print("No data after cleaning.")
        return
    exploratory_data_analysis(df_clean)
    ap_analysis = analyze_advance_purchase_patterns(df_clean)
    brand_analysis = analyze_fare_brands_by_airline(df_clean)
    basic_economy_candidates, _ = identify_basic_economy_candidates(df_clean, brand_analysis)
    create_visualizations(df_clean, ap_analysis, basic_economy_candidates)
    create_deliverable_table(basic_economy_candidates, brand_analysis)
    print("\nAnalysis complete. All outputs saved.")

if __name__ == "__main__":
    main() 