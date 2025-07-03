"""
Fare Brand Analysis and Basic Economy Detection for US Domestic Markets
======================================================================

This script performs comprehensive analysis of fare brands across US domestic markets
to identify Basic Economy fare structures for each airline.

Author: Data Science Team
Date: 2025-07-03
"""

import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import rs_access_v1 as rs

warnings.filterwarnings('ignore')

print("=" * 80)
print("FARE BRAND ANALYSIS AND BASIC ECONOMY DETECTION")
print("=" * 80)

# Setting up account info on redshift for connection
acct_no, acct_name = rs.RedshiftAccess.get_rs_account_info()
print(f"Account Number: {acct_no}")
print(f"Account Name: {acct_name}")

def get_us_domestic_data():
    """
    Fetch US domestic flight data from Redshift using metadata.airportlocation_extra
    """
    print("\n📊 Fetching US domestic flight data from Redshift...")
    
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
    LIMIT 50000;
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
    """
    print("\n🧹 Cleaning and preparing data...")
    
    if df.empty:
        print("❌ No data to clean")
        return df
    
    # Create copies to avoid modifying original
    df = df.copy()
    
    # Convert date columns - they're already in YYYY-MM-DD format
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
    
    print(f"✅ Cleaned data: {len(df):,} records (removed {initial_count - len(df):,} invalid records)")
    
    if len(df) > 0:
        print(f"📅 Date range: {df['outbound_departure_date'].min()} to {df['outbound_departure_date'].max()}")
        print(f"📊 Advance purchase range: {df['days_to_departure'].min()} to {df['days_to_departure'].max()} days")
        print(f"✈️ Airlines: {sorted(df['carrier'].unique())}")
        print(f"🗺️ Markets: {df['market'].nunique()}")
    
    return df

def exploratory_data_analysis(df):
    """
    Perform comprehensive EDA
    """
    print("\n" + "="*50)
    print("EXPLORATORY DATA ANALYSIS")
    print("="*50)
    
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
    
    return df

def analyze_fare_brands_by_airline(df):
    """
    Analyze fare brands for each airline and market combination
    """
    print("\n" + "="*50)
    print("FARE BRAND ANALYSIS BY AIRLINE")
    print("="*50)
    
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
    """
    print("\n" + "="*50)
    print("BASIC ECONOMY IDENTIFICATION")
    print("="*50)
    
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
    
    # Apply scoring function correctly
    brand_metrics['basic_economy_score'] = brand_metrics.apply(calculate_basic_economy_score, axis=1)
    
    # Identify Basic Economy for each airline-market combination
    basic_economy_candidates = brand_metrics.loc[
        brand_metrics.groupby(['carrier', 'market'])['basic_economy_score'].idxmax()
    ].copy()
    
    # Add confidence level
    def assign_confidence(score):
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

def create_deliverable_table(basic_economy_candidates, brand_analysis):
    """
    Create the final deliverable table
    """
    print("\n" + "="*50)
    print("CREATING DELIVERABLE TABLE")
    print("="*50)
    
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
    output_filename = f"basic_economy_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    deliverable_final.to_csv(output_filename, index=False)
    print(f"\n💾 Saved deliverable table to: {output_filename}")
    
    return deliverable_final

def main():
    """
    Main execution function
    """
    print("🚀 Starting Fare Brand Analysis and Basic Economy Detection...")
    
    try:
        # Step 1: Fetch data
        df = get_us_domestic_data()
        
        if df.empty:
            print("❌ No data to analyze")
            return None, None, None
        
        # Step 2: Clean and prepare data
        df_clean = clean_and_prepare_data(df)
        
        if df_clean.empty:
            print("❌ No data remaining after cleaning")
            return None, None, None
        
        # Step 3: Exploratory Data Analysis
        df_analyzed = exploratory_data_analysis(df_clean)
        
        # Step 4: Analyze fare brands by airline
        brand_analysis = analyze_fare_brands_by_airline(df_analyzed)
        
        # Step 5: Identify Basic Economy candidates
        basic_economy_candidates, brand_metrics = identify_basic_economy_candidates(
            df_analyzed, brand_analysis
        )
        
        # Step 6: Create deliverable table
        deliverable_final = create_deliverable_table(basic_economy_candidates, brand_analysis)
        
        print(f"\n✅ Analysis completed successfully!")
        print(f"📄 Check the generated CSV file for detailed results.")
        
        return df_analyzed, deliverable_final, basic_economy_candidates
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None, None

if __name__ == "__main__":
    df, deliverable, basic_economy = main() 