"""
Enhanced Fare Brand Analysis and Basic Economy Detection for US Domestic Markets
================================================================================

This script performs comprehensive analysis of fare brands across US domestic markets
to identify Basic Economy fare structures for each airline, including advance purchase
analysis and visualizations.

Key Features:
- Fetches US domestic flight data from Redshift using metadata.airportlocation_extra
- Analyzes fare brands by airline and market combination
- Identifies Basic Economy fares using multi-factor scoring
- Creates comprehensive visualizations and deliverable reports
- Includes advance purchase pattern analysis (7, 14, 21 days)

Author: Data Science Team
Date: 2025-07-03
Version: 2.0
"""

import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import rs_access_v1 as rs

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure plotting style for professional output
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

def get_us_domestic_data():
    """
    Fetch US domestic flight data from Redshift using metadata.airportlocation_extra
    
    Returns:
        pd.DataFrame: Raw flight data for US domestic markets
    """
    print("📊 Fetching US domestic flight data from Redshift...")
    
    # Initialize Redshift connection
    rs.assign_connection("ds")
    
    # Query for US domestic markets using the airport location table
    query = """
    SELECT 
        carrier,
        origin,
        destination,
        outbound_departure_date,
        outbound_fare_family,
        inbound_fare_family,
        price_inc,
        price_exc,
        tax,
        yqyr,
        currency,
        search_class,
        cabin,
        outbound_cabins,
        inbound_cabins,
        outbound_booking_class,
        inbound_booking_class,
        outbound_fare_basis,
        inbound_fare_basis,
        refundable,
        change_fee,
        los,
        sales_date,
        observation_date,
        customer,
        outbound_travel_stop_over,
        inbound_travel_stop_over
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
        print(f"✅ Fetched {len(df):,} records for US domestic markets")
        return df
    else:
        print("❌ No data returned from query")
        return pd.DataFrame()

def clean_and_prepare_data(df):
    """
    Clean and prepare the data for analysis
    
    Args:
        df (pd.DataFrame): Raw flight data
        
    Returns:
        pd.DataFrame: Cleaned and prepared data with calculated fields
    """
    print("🧹 Cleaning and preparing data...")
    
    if df.empty:
        print("❌ No data to clean")
        return df
    
    # Create copy to avoid modifying original
    df = df.copy()
    
    # Convert date columns
    df['outbound_departure_date'] = pd.to_datetime(df['outbound_departure_date'], errors='coerce')
    df['observation_date'] = pd.to_datetime(df['observation_date'], errors='coerce')
    df['sales_date'] = pd.to_datetime(df['sales_date'], format='%Y%m%d', errors='coerce')
    
    # Calculate days to departure (advance purchase window)
    df['days_to_departure'] = (df['outbound_departure_date'] - df['observation_date']).dt.days
    
    # Create market identifier
    df['market'] = df['origin'] + '-' + df['destination']
    
    # Handle missing fare families
    df['outbound_fare_family'] = df['outbound_fare_family'].fillna('Unknown')
    df['inbound_fare_family'] = df['inbound_fare_family'].fillna('Unknown')
    
    # Create primary fare family (outbound for analysis)
    df['primary_fare_family'] = df['outbound_fare_family']
    
    # Flag round-trip vs one-way
    df['is_roundtrip'] = df['inbound_fare_family'].notna() & (df['inbound_fare_family'] != 'Unknown')
    
    # Clean price data
    df['price_inc'] = pd.to_numeric(df['price_inc'], errors='coerce')
    df['price_exc'] = pd.to_numeric(df['price_exc'], errors='coerce')
    df['change_fee'] = pd.to_numeric(df['change_fee'], errors='coerce')
    
    # Remove invalid data
    initial_count = len(df)
    df = df.dropna(subset=['carrier', 'market', 'primary_fare_family', 'price_inc'])
    df = df[df['price_inc'] > 0]  # Remove zero/negative prices
    
    # Only keep records with valid advance purchase data
    df = df.dropna(subset=['days_to_departure'])
    df = df[df['days_to_departure'] >= 0]  # Remove past departures
    
    # Define advance purchase buckets as specified in requirements
    def categorize_advance_purchase(days):
        """Categorize advance purchase days into buckets"""
        if days <= 7:
            return '0-7 days'
        elif days <= 14:
            return '8-14 days'
        elif days <= 21:
            return '15-21 days'
        elif days <= 30:
            return '22-30 days'
        else:
            return '30+ days'
    
    df['advance_purchase_bucket'] = df['days_to_departure'].apply(categorize_advance_purchase)
    
    print(f"✅ Cleaned data: {len(df):,} records (removed {initial_count - len(df):,} invalid records)")
    
    if len(df) > 0:
        print(f"📅 Date range: {df['outbound_departure_date'].min()} to {df['outbound_departure_date'].max()}")
        print(f"📊 Advance purchase range: {df['days_to_departure'].min()} to {df['days_to_departure'].max()} days")
        print(f"✈️ Airlines: {sorted(df['carrier'].unique())}")
        print(f"🗺️ Markets: {df['market'].nunique()}")
    
    return df

def perform_exploratory_data_analysis(df):
    """
    Perform comprehensive exploratory data analysis
    
    Args:
        df (pd.DataFrame): Cleaned flight data
        
    Returns:
        pd.DataFrame: Same dataframe with analysis printed
    """
    print("\n" + "="*60)
    print("EXPLORATORY DATA ANALYSIS")
    print("="*60)
    
    if df.empty:
        print("❌ No data for EDA")
        return df
    
    # Basic statistics
    print(f"\n📈 Dataset Overview:")
    print(f"Total records: {len(df):,}")
    print(f"Unique airlines: {df['carrier'].nunique()}")
    print(f"Unique markets: {df['market'].nunique()}")
    print(f"Unique fare families: {df['primary_fare_family'].nunique()}")
    
    # Airlines distribution
    print(f"\n✈️ Airlines in dataset:")
    airline_counts = df['carrier'].value_counts()
    for airline, count in airline_counts.head(10).items():
        print(f"  {airline}: {count:,} records")
    
    # Market distribution
    print(f"\n🗺️ Top markets:")
    market_counts = df['market'].value_counts()
    for market, count in market_counts.head(10).items():
        print(f"  {market}: {count:,} records")
    
    # Fare family distribution
    print(f"\n🏷️ Fare families overview:")
    ff_counts = df['primary_fare_family'].value_counts()
    for ff, count in ff_counts.head(15).items():
        print(f"  {ff}: {count:,} records")
    
    # Price statistics
    print(f"\n💰 Price statistics:")
    print(f"  Mean price: ${df['price_inc'].mean():.2f}")
    print(f"  Median price: ${df['price_inc'].median():.2f}")
    print(f"  Price range: ${df['price_inc'].min():.2f} - ${df['price_inc'].max():.2f}")
    
    # Advance purchase statistics
    print(f"\n📅 Advance purchase statistics:")
    ap_counts = df['advance_purchase_bucket'].value_counts()
    for bucket, count in ap_counts.items():
        pct = (count / len(df)) * 100
        print(f"  {bucket}: {count:,} records ({pct:.1f}%)")
    
    return df

def analyze_advance_purchase_patterns(df):
    """
    Analyze brand availability across different advance purchase windows
    
    Args:
        df (pd.DataFrame): Cleaned flight data
        
    Returns:
        pd.DataFrame: Analysis results by advance purchase window
    """
    print("\n" + "="*60)
    print("ADVANCE PURCHASE PATTERN ANALYSIS")
    print("="*60)
    
    if df.empty:
        print("❌ No data for advance purchase analysis")
        return pd.DataFrame()
    
    # Analyze brand availability by advance purchase window
    ap_analysis = df.groupby(['carrier', 'market', 'advance_purchase_bucket', 'primary_fare_family']).agg({
        'price_inc': ['count', 'mean', 'min'],
        'refundable': 'mean',
        'change_fee': 'mean'
    }).reset_index()
    
    # Flatten column names
    ap_analysis.columns = ['carrier', 'market', 'advance_purchase_bucket', 'fare_family',
                          'record_count', 'avg_price', 'min_price', 'refundable_pct', 'avg_change_fee']
    
    print(f"\n📅 Brand availability by advance purchase window:")
    
    # Show top brands by advance purchase window
    for bucket in ['0-7 days', '8-14 days', '15-21 days']:
        print(f"\n{bucket}:")
        bucket_data = ap_analysis[ap_analysis['advance_purchase_bucket'] == bucket].groupby('fare_family').agg({
            'record_count': 'sum',
            'avg_price': 'mean'
        }).reset_index().sort_values('record_count', ascending=False)
        
        for _, row in bucket_data.head(8).iterrows():
            print(f"  {row['fare_family']}: {row['record_count']:,} records, avg ${row['avg_price']:.2f}")
    
    return ap_analysis

def analyze_fare_brands_by_airline(df):
    """
    Analyze fare brands for each airline and market combination
    
    Args:
        df (pd.DataFrame): Cleaned flight data
        
    Returns:
        pd.DataFrame: Brand analysis by airline and market
    """
    print("\n" + "="*60)
    print("FARE BRAND ANALYSIS BY AIRLINE")
    print("="*60)
    
    if df.empty:
        print("❌ No data for brand analysis")
        return pd.DataFrame()
    
    # Group by airline and market to find all brands
    brand_analysis = df.groupby(['carrier', 'market']).agg({
        'primary_fare_family': lambda x: sorted(list(set(x))),
        'price_inc': ['count', 'mean', 'min', 'max'],
        'days_to_departure': ['mean', 'min', 'max']
    }).reset_index()
    
    # Flatten column names
    brand_analysis.columns = ['carrier', 'market', 'all_brands', 'record_count', 
                             'avg_price', 'min_price', 'max_price', 
                             'avg_days_out', 'min_days_out', 'max_days_out']
    
    brand_analysis['num_brands'] = brand_analysis['all_brands'].apply(len)
    brand_analysis['brand_count_str'] = brand_analysis['all_brands'].apply(lambda x: ', '.join(x))
    
    print(f"\n📊 Brand diversity by airline:")
    airline_brand_summary = brand_analysis.groupby('carrier').agg({
        'num_brands': ['mean', 'min', 'max'],
        'market': 'count'
    }).round(2)
    
    airline_brand_summary.columns = ['avg_brands_per_market', 'min_brands', 'max_brands', 'markets_served']
    print(airline_brand_summary.sort_values('avg_brands_per_market', ascending=False))
    
    return brand_analysis

def identify_basic_economy_candidates(df, brand_analysis):
    """
    Identify Basic Economy candidates using multiple criteria
    
    Args:
        df (pd.DataFrame): Cleaned flight data
        brand_analysis (pd.DataFrame): Brand analysis results
        
    Returns:
        tuple: (basic_economy_candidates, brand_metrics)
    """
    print("\n" + "="*60)
    print("BASIC ECONOMY IDENTIFICATION")
    print("="*60)
    
    if df.empty or brand_analysis.empty:
        print("❌ No data for Basic Economy identification")
        return pd.DataFrame(), pd.DataFrame()
    
    # Create comprehensive analysis for each airline-market-brand combination
    brand_metrics = df.groupby(['carrier', 'market', 'primary_fare_family']).agg({
        'price_inc': ['count', 'mean', 'min', 'max', 'std'],
        'refundable': 'mean',
        'change_fee': ['mean', 'max'],
        'days_to_departure': 'mean',
        'outbound_booking_class': lambda x: len(set(x))  # number of booking classes
    }).reset_index()
    
    # Flatten column names
    brand_metrics.columns = ['carrier', 'market', 'fare_family', 'record_count', 
                           'avg_price', 'min_price', 'max_price', 'price_std',
                           'refundable_pct', 'avg_change_fee', 'max_change_fee',
                           'avg_days_out', 'booking_class_variety']
    
    # Calculate price rank within each airline-market combination
    brand_metrics['price_rank'] = brand_metrics.groupby(['carrier', 'market'])['avg_price'].rank(method='min')
    
    # Basic Economy scoring criteria
    def calculate_basic_economy_score(row):
        """
        Calculate Basic Economy score based on multiple factors:
        - Price rank (40% weight)
        - Refundability (20% weight) 
        - Change fees (20% weight)
        - Brand name analysis (20% weight)
        """
        score = 0
        
        # Price criteria (40% weight)
        if row['price_rank'] == 1:  # Cheapest average price
            score += 40
        elif row['price_rank'] == 2:
            score += 30
        elif row['price_rank'] <= 3:
            score += 20
        
        # Refundability (20% weight)
        if pd.notna(row['refundable_pct']):
            if row['refundable_pct'] < 0.1:  # Less than 10% refundable
                score += 20
            elif row['refundable_pct'] < 0.3:
                score += 10
        
        # Change fee criteria (20% weight)
        if pd.notna(row['avg_change_fee']):
            if row['avg_change_fee'] > 100:  # High change fees
                score += 20
            elif row['avg_change_fee'] > 50:
                score += 10
        
        # Brand name analysis (20% weight)
        fare_family_lower = str(row['fare_family']).lower()
        basic_keywords = ['basic', 'economy', 'main', 'standard', 'saver', 'light', 'essential']
        premium_keywords = ['first', 'business', 'premium', 'plus', 'comfort', 'extra', 'flex']
        
        if any(keyword in fare_family_lower for keyword in basic_keywords):
            score += 15
        if any(keyword in fare_family_lower for keyword in premium_keywords):
            score -= 15
        
        return score
    
    # Apply scoring function
    brand_metrics['basic_economy_score'] = brand_metrics.apply(calculate_basic_economy_score, axis=1)
    
    # Identify Basic Economy for each airline-market combination
    basic_economy_candidates = brand_metrics.loc[
        brand_metrics.groupby(['carrier', 'market'])['basic_economy_score'].idxmax()
    ].copy()
    
    # Add confidence level
    def assign_confidence(score):
        """Assign confidence level based on score"""
        if score >= 70:
            return 'High'
        elif score >= 50:
            return 'Medium'
        else:
            return 'Low'
    
    basic_economy_candidates['confidence'] = basic_economy_candidates['basic_economy_score'].apply(assign_confidence)
    
    print(f"\n🎯 Basic Economy Identification Results:")
    print(f"Total airline-market combinations analyzed: {len(basic_economy_candidates):,}")
    
    confidence_dist = basic_economy_candidates['confidence'].value_counts()
    for conf, count in confidence_dist.items():
        print(f"  {conf} confidence: {count:,} combinations")
    
    # Show examples by airline
    print(f"\n✈️ Basic Economy candidates by airline:")
    for carrier in basic_economy_candidates['carrier'].unique()[:8]:
        carrier_data = basic_economy_candidates[basic_economy_candidates['carrier'] == carrier]
        high_conf = carrier_data[carrier_data['confidence'] == 'High']
        if len(high_conf) > 0:
            most_common_brand = high_conf['fare_family'].mode()
            if len(most_common_brand) > 0:
                print(f"  {carrier}: {most_common_brand[0]} (in {len(high_conf)} markets)")
    
    return basic_economy_candidates, brand_metrics

def create_visualizations(df, ap_analysis, basic_economy_candidates):
    """
    Create comprehensive visualizations for the analysis
    
    Args:
        df (pd.DataFrame): Cleaned flight data
        ap_analysis (pd.DataFrame): Advance purchase analysis
        basic_economy_candidates (pd.DataFrame): Basic Economy identification results
    """
    print("\n" + "="*60)
    print("CREATING VISUALIZATIONS")
    print("="*60)
    
    if df.empty:
        print("❌ No data for visualizations")
        return
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle('Fare Brand Analysis and Basic Economy Detection - US Domestic Markets', 
                 fontsize=16, fontweight='bold')
    
    # 1. Airline distribution
    airline_counts = df['carrier'].value_counts().head(10)
    axes[0, 0].bar(airline_counts.index, airline_counts.values, color='skyblue')
    axes[0, 0].set_title('Top Airlines by Record Count')
    axes[0, 0].set_xlabel('Airline')
    axes[0, 0].set_ylabel('Number of Records')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # 2. Price distribution by airline
    top_airlines = df['carrier'].value_counts().head(6).index
    price_data = df[df['carrier'].isin(top_airlines)]
    price_data.boxplot(column='price_inc', by='carrier', ax=axes[0, 1])
    axes[0, 1].set_title('Price Distribution by Airline')
    axes[0, 1].set_xlabel('Airline')
    axes[0, 1].set_ylabel('Price (USD)')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. Advance purchase distribution
    ap_counts = df['advance_purchase_bucket'].value_counts()
    axes[0, 2].pie(ap_counts.values, labels=ap_counts.index, autopct='%1.1f%%', startangle=90)
    axes[0, 2].set_title('Distribution by Advance Purchase Window')
    
    # 4. Brand availability by advance purchase window
    if not ap_analysis.empty:
        ap_summary = ap_analysis.groupby(['advance_purchase_bucket', 'fare_family']).agg({
            'record_count': 'sum'
        }).reset_index()
        
        # Get top 5 brands for visualization
        top_brands = ap_summary.groupby('fare_family')['record_count'].sum().nlargest(5).index
        
        ap_pivot = ap_summary[ap_summary['fare_family'].isin(top_brands)].pivot(
            index='advance_purchase_bucket', columns='fare_family', values='record_count'
        ).fillna(0)
        
        ap_pivot.plot(kind='bar', ax=axes[1, 0], width=0.8)
        axes[1, 0].set_title('Brand Availability by Advance Purchase Window')
        axes[1, 0].set_xlabel('Advance Purchase Window')
        axes[1, 0].set_ylabel('Number of Records')
        axes[1, 0].tick_params(axis='x', rotation=45)
        axes[1, 0].legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # 5. Basic Economy confidence distribution
    if not basic_economy_candidates.empty:
        conf_counts = basic_economy_candidates['confidence'].value_counts()
        colors = ['#2E8B57', '#FFA500', '#DC143C']  # Green, Orange, Red
        axes[1, 1].bar(conf_counts.index, conf_counts.values, color=colors)
        axes[1, 1].set_title('Basic Economy Detection Confidence')
        axes[1, 1].set_xlabel('Confidence Level')
        axes[1, 1].set_ylabel('Number of Combinations')
        
        # Add percentage labels
        total = conf_counts.sum()
        for i, (conf, count) in enumerate(conf_counts.items()):
            pct = (count / total) * 100
            axes[1, 1].text(i, count + 5, f'{pct:.1f}%', ha='center', va='bottom')
    
    # 6. Basic Economy brands by airline
    if not basic_economy_candidates.empty:
        be_by_airline = basic_economy_candidates[basic_economy_candidates['confidence'] == 'High'].groupby('carrier')['fare_family'].agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Unknown')
        axes[1, 2].bar(range(len(be_by_airline)), be_by_airline.values, color='lightcoral')
        axes[1, 2].set_title('High-Confidence Basic Economy Brands by Airline')
        axes[1, 2].set_xlabel('Airline')
        axes[1, 2].set_ylabel('Basic Economy Brand')
        axes[1, 2].set_xticks(range(len(be_by_airline)))
        axes[1, 2].set_xticklabels(be_by_airline.index, rotation=45)
    
    plt.tight_layout()
    
    # Save the visualization
    viz_filename = f"fare_brand_analysis_visualizations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(viz_filename, dpi=300, bbox_inches='tight')
    print(f"💾 Saved visualizations to: {viz_filename}")
    
    # Create additional summary statistics visualization
    create_summary_statistics_visualization(df, ap_analysis, basic_economy_candidates)

def create_summary_statistics_visualization(df, ap_analysis, basic_economy_candidates):
    """
    Create additional summary statistics visualization
    
    Args:
        df (pd.DataFrame): Cleaned flight data
        ap_analysis (pd.DataFrame): Advance purchase analysis
        basic_economy_candidates (pd.DataFrame): Basic Economy identification results
    """
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Summary Statistics and Brand Distribution', fontsize=16, fontweight='bold')
    
    # 1. Market complexity by airline
    if not df.empty:
        market_complexity = df.groupby(['carrier', 'market'])['primary_fare_family'].nunique().reset_index()
        avg_complexity = market_complexity.groupby('carrier')['primary_fare_family'].mean().sort_values(ascending=False)
        
        axes[0, 0].bar(avg_complexity.index, avg_complexity.values, color='steelblue')
        axes[0, 0].set_title('Average Brands per Market by Airline')
        axes[0, 0].set_xlabel('Airline')
        axes[0, 0].set_ylabel('Average Number of Brands')
        axes[0, 0].tick_params(axis='x', rotation=45)
    
    # 2. Price vs Advance Purchase
    if not df.empty:
        price_ap = df.groupby('advance_purchase_bucket')['price_inc'].mean()
        axes[0, 1].bar(price_ap.index, price_ap.values, color='gold')
        axes[0, 1].set_title('Average Price by Advance Purchase Window')
        axes[0, 1].set_xlabel('Advance Purchase Window')
        axes[0, 1].set_ylabel('Average Price (USD)')
        axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. Refundability by brand type
    if not df.empty:
        refund_by_brand = df.groupby('primary_fare_family')['refundable'].mean().sort_values(ascending=False).head(10)
        axes[1, 0].barh(refund_by_brand.index, refund_by_brand.values, color='lightgreen')
        axes[1, 0].set_title('Refundability Rate by Fare Family (Top 10)')
        axes[1, 0].set_xlabel('Refundability Rate')
        axes[1, 0].set_xlim(0, 1)
    
    # 4. Basic Economy score distribution
    if not basic_economy_candidates.empty:
        axes[1, 1].hist(basic_economy_candidates['basic_economy_score'], bins=20, color='salmon', alpha=0.7)
        axes[1, 1].set_title('Basic Economy Score Distribution')
        axes[1, 1].set_xlabel('Basic Economy Score')
        axes[1, 1].set_ylabel('Frequency')
        axes[1, 1].axvline(x=70, color='red', linestyle='--', label='High Confidence Threshold')
        axes[1, 1].axvline(x=50, color='orange', linestyle='--', label='Medium Confidence Threshold')
        axes[1, 1].legend()
    
    plt.tight_layout()
    
    # Save the summary visualization
    summary_viz_filename = f"summary_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(summary_viz_filename, dpi=300, bbox_inches='tight')
    print(f"💾 Saved summary statistics to: {summary_viz_filename}")

def create_deliverable_table(basic_economy_candidates, brand_analysis):
    """
    Create the final deliverable table
    
    Args:
        basic_economy_candidates (pd.DataFrame): Basic Economy identification results
        brand_analysis (pd.DataFrame): Brand analysis results
        
    Returns:
        pd.DataFrame: Final deliverable table
    """
    print("\n" + "="*60)
    print("CREATING DELIVERABLE TABLE")
    print("="*60)
    
    if basic_economy_candidates.empty or brand_analysis.empty:
        print("❌ No data for deliverable table")
        return pd.DataFrame()
    
    # Merge brand analysis with basic economy identification
    deliverable = brand_analysis.merge(
        basic_economy_candidates[['carrier', 'market', 'fare_family', 'basic_economy_score', 'confidence']],
        left_on=['carrier', 'market'],
        right_on=['carrier', 'market'],
        how='left'
    )
    
    # Rename columns to match requirements
    deliverable_final = deliverable[['carrier', 'market', 'brand_count_str', 'fare_family', 'confidence', 'basic_economy_score']].copy()
    deliverable_final.columns = ['Airline', 'Market', 'All_Detected_Brands', 'Identified_Basic_Economy_Brand', 'Confidence_Level', 'BE_Score']
    
    # Sort by airline and market
    deliverable_final = deliverable_final.sort_values(['Airline', 'Market'])
    
    print(f"📋 Deliverable table created with {len(deliverable_final):,} rows")
    print(f"\nSample of deliverable table:")
    print(deliverable_final.head(10).to_string(index=False))
    
    # Save to CSV
    output_filename = f"basic_economy_analysis_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    deliverable_final.to_csv(output_filename, index=False)
    print(f"\n💾 Saved deliverable table to: {output_filename}")
    
    return deliverable_final

def main():
    """
    Main execution function for the fare brand analysis pipeline
    
    Returns:
        tuple: (df_analyzed, deliverable_final, basic_economy_candidates, ap_analysis)
    """
    print("=" * 80)
    print("ENHANCED FARE BRAND ANALYSIS AND BASIC ECONOMY DETECTION")
    print("=" * 80)
    
    # Setting up account info on redshift for connection
    try:
        acct_no, acct_name = rs.RedshiftAccess.get_rs_account_info()
        print(f"Account Number: {acct_no}")
        print(f"Account Name: {acct_name}")
    except Exception as e:
        print(f"Warning: Could not retrieve account info: {e}")
    
    print("\n🚀 Starting Enhanced Fare Brand Analysis and Basic Economy Detection...")
    
    try:
        # Step 1: Fetch data
        df = get_us_domestic_data()
        
        if df.empty:
            print("❌ No data to analyze")
            return None, None, None, None
        
        # Step 2: Clean and prepare data
        df_clean = clean_and_prepare_data(df)
        
        if df_clean.empty:
            print("❌ No data remaining after cleaning")
            return None, None, None, None
        
        # Step 3: Exploratory Data Analysis
        df_analyzed = perform_exploratory_data_analysis(df_clean)
        
        # Step 4: Analyze advance purchase patterns
        ap_analysis = analyze_advance_purchase_patterns(df_analyzed)
        
        # Step 5: Analyze fare brands by airline
        brand_analysis = analyze_fare_brands_by_airline(df_analyzed)
        
        # Step 6: Identify Basic Economy candidates
        basic_economy_candidates, brand_metrics = identify_basic_economy_candidates(
            df_analyzed, brand_analysis
        )
        
        # Step 7: Create visualizations
        create_visualizations(df_analyzed, ap_analysis, basic_economy_candidates)
        
        # Step 8: Create deliverable table
        deliverable_final = create_deliverable_table(basic_economy_candidates, brand_analysis)
        
        print(f"\n✅ Enhanced analysis completed successfully!")
        print(f"📄 Check the generated CSV file for detailed results.")
        print(f"📊 Check the generated PNG files for visualizations.")
        
        return df_analyzed, deliverable_final, basic_economy_candidates, ap_analysis
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None, None, None

if __name__ == "__main__":
    df, deliverable, basic_economy, ap_analysis = main() 