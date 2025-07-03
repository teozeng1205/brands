"""
Enhanced Fare Brand Analysis v2.0 - Key Improvements Implementation
=================================================================

This script implements the key improvements identified in the pipeline analysis:
1. Market-specific price ranking
2. Enhanced advance purchase window analysis 
3. Improved Basic Economy detection scoring
4. Comprehensive deliverable table
5. Better data coverage

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

warnings.filterwarnings('ignore')

def get_enhanced_data():
    """Fetch enhanced US domestic flight data with improved coverage"""
    print("📊 Fetching enhanced US domestic flight data...")
    
    try:
        rs.assign_connection("ds")
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Please ensure AWS credentials are configured")
        return pd.DataFrame()
    
    # Enhanced query with no record limit and multiple dates
    query = """
    SELECT 
        carrier,
        origin,
        destination,
        origin || '-' || destination as market,
        sales_date,
        observation_date,
        outbound_departure_date,
        outbound_departure_date - observation_date as days_to_departure,
        outbound_fare_family,
        outbound_fare_basis,
        outbound_booking_class,
        price_inc,
        price_exc,
        refundable,
        change_fee,
        cabin,
        search_class
    FROM common_output.common_output_format 
    WHERE sales_date BETWEEN 20250625 AND 20250629  -- Multiple days
    AND origin IN (SELECT airportcode FROM metadata.airportlocation_extra WHERE countryname = 'United States')
    AND destination IN (SELECT airportcode FROM metadata.airportlocation_extra WHERE countryname = 'United States')
    AND origin != destination
    AND carrier IS NOT NULL
    AND outbound_fare_family IS NOT NULL
    AND price_inc > 0
    AND outbound_departure_date > observation_date
    AND outbound_departure_date - observation_date <= 90
    -- Removed LIMIT for complete coverage
    """
    
    try:
        df = rs.rq(query)
        if df is not None and len(df) > 0:
            print(f"✅ Fetched {len(df):,} records")
            return df
        else:
            print("❌ No data returned")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ Query error: {e}")
        return pd.DataFrame()

def clean_and_enhance_data(df):
    """Clean and enhance data with improved features"""
    print("🧹 Cleaning and enhancing data...")
    
    if df.empty:
        return df
    
    df = df.copy()
    
    # Enhanced advance purchase buckets
    def categorize_advance_purchase(days):
        if pd.isna(days):
            return 'Unknown'
        elif days <= 7:
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
    df['primary_fare_family'] = df['outbound_fare_family'].fillna('Unknown')
    df['is_refundable'] = df['refundable'].isin(['Y', 'y', 'Yes', 'yes', True, 1])
    
    # Clean numeric fields
    df['price_inc'] = pd.to_numeric(df['price_inc'], errors='coerce')
    df['change_fee'] = pd.to_numeric(df['change_fee'], errors='coerce')
    
    # Remove invalid data
    initial_count = len(df)
    df = df.dropna(subset=['carrier', 'market', 'primary_fare_family', 'price_inc', 'days_to_departure'])
    df = df[df['price_inc'] > 0]
    df = df[df['days_to_departure'] >= 0]
    
    print(f"✅ Cleaned data: {len(df):,} records (removed {initial_count - len(df):,})")
    return df

def add_market_specific_ranking(df):
    """Add market-specific price ranking - KEY IMPROVEMENT"""
    print("📈 Adding market-specific price ranking...")
    
    if df.empty:
        return df
    
    # Market-specific ranking within each advance purchase window
    df['price_rank_in_market'] = df.groupby(['market', 'carrier', 'advance_purchase_bucket'])['price_inc'].rank(method='min', ascending=True)
    
    # Overall market ranking across all carriers
    df['price_rank_overall_market'] = df.groupby(['market', 'advance_purchase_bucket'])['price_inc'].rank(method='min', ascending=True)
    
    print(f"✅ Market-specific ranking added")
    return df

def analyze_brand_availability(df):
    """Analyze brand availability across advance purchase windows"""
    print("📊 Analyzing brand availability patterns...")
    
    if df.empty:
        return pd.DataFrame()
    
    brand_analysis = df.groupby(['carrier', 'market', 'primary_fare_family']).agg({
        'advance_purchase_bucket': lambda x: list(x.unique()),
        'days_to_departure': ['min', 'max', 'mean', 'count'],
        'price_inc': ['min', 'max', 'mean'],
        'price_rank_in_market': ['min', 'max', 'mean'],
        'is_refundable': 'mean',
        'change_fee': 'mean'
    }).reset_index()
    
    # Flatten column names
    brand_analysis.columns = ['carrier', 'market', 'primary_fare_family', 'advance_purchase_buckets',
                             'days_min', 'days_max', 'days_mean', 'observation_count',
                             'price_min', 'price_max', 'price_mean',
                             'rank_min', 'rank_max', 'rank_mean',
                             'pct_refundable', 'avg_change_fee']
    
    # Calculate availability flags
    def get_availability_flags(buckets):
        bucket_set = set(buckets)
        return {
            'available_0_7_days': '0-7 days' in bucket_set,
            'available_8_14_days': '8-14 days' in bucket_set,
            'available_15_21_days': '15-21 days' in bucket_set,
            'available_22_30_days': '22-30 days' in bucket_set,
            'available_30_plus_days': '30+ days' in bucket_set
        }
    
    availability_flags = brand_analysis['advance_purchase_buckets'].apply(get_availability_flags)
    availability_df = pd.DataFrame(availability_flags.tolist())
    brand_analysis = pd.concat([brand_analysis, availability_df], axis=1)
    
    print(f"✅ Brand availability analysis complete: {len(brand_analysis)} brand-market combinations")
    return brand_analysis

def enhanced_basic_economy_scoring(brand_analysis):
    """Enhanced Basic Economy scoring with improved methodology"""
    print("🎯 Performing enhanced Basic Economy scoring...")
    
    if brand_analysis.empty:
        return pd.DataFrame()
    
    def calculate_enhanced_score(row):
        """Enhanced scoring algorithm (0-100 points)"""
        score = 0
        
        # 1. Market-Specific Price Ranking (35% weight)
        if row['rank_min'] == 1:
            score += 35
        elif row['rank_min'] == 2:
            score += 25
        elif row['rank_min'] == 3:
            score += 15
        elif row['rank_min'] <= 5:
            score += 10
        else:
            score += 5
        
        # 2. Advance Purchase Availability (20% weight)
        ap_score = 0
        if row['available_0_7_days']:
            ap_score += 8
        if row['available_8_14_days']:
            ap_score += 6
        if row['available_15_21_days']:
            ap_score += 4
        if row['available_22_30_days']:
            ap_score += 2
        score += min(ap_score, 20)
        
        # 3. Restriction Analysis (25% weight)
        if row['pct_refundable'] < 0.05:
            score += 25
        elif row['pct_refundable'] < 0.15:
            score += 20
        elif row['pct_refundable'] < 0.30:
            score += 15
        elif row['pct_refundable'] < 0.50:
            score += 10
        else:
            score += 5
        
        # 4. Change Fee Analysis (10% weight)
        if not pd.isna(row['avg_change_fee']):
            if row['avg_change_fee'] > 150:
                score += 10
            elif row['avg_change_fee'] > 100:
                score += 8
            elif row['avg_change_fee'] > 50:
                score += 6
            else:
                score += 4
        
        # 5. Brand Name Analysis (10% weight)
        brand_name = str(row['primary_fare_family']).lower()
        basic_keywords = ['basic', 'economy', 'main', 'standard', 'saver', 'light', 'essential']
        premium_keywords = ['first', 'business', 'premium', 'plus', 'comfort', 'extra', 'flex']
        
        brand_score = 5  # neutral
        for keyword in basic_keywords:
            if keyword in brand_name:
                brand_score = 10
                break
        for keyword in premium_keywords:
            if keyword in brand_name:
                brand_score = 0
                break
        
        score += brand_score
        return min(100, max(0, score))
    
    brand_analysis['basic_economy_score'] = brand_analysis.apply(calculate_enhanced_score, axis=1)
    
    # Enhanced confidence levels
    def assign_confidence(score):
        if score >= 80:
            return 'Very High'
        elif score >= 70:
            return 'High'
        elif score >= 50:
            return 'Medium'
        elif score >= 30:
            return 'Low'
        else:
            return 'Very Low'
    
    brand_analysis['confidence_level'] = brand_analysis['basic_economy_score'].apply(assign_confidence)
    
    # Filter meaningful candidates
    meaningful_candidates = brand_analysis[brand_analysis['basic_economy_score'] >= 30].copy()
    
    print(f"✅ Enhanced scoring complete: {len(meaningful_candidates)} meaningful candidates")
    return meaningful_candidates

def create_enhanced_deliverable(candidates):
    """Create enhanced deliverable table with all required columns"""
    print("📋 Creating enhanced deliverable table...")
    
    if candidates.empty:
        return pd.DataFrame()
    
    deliverable = pd.DataFrame({
        'Airline': candidates['carrier'],
        'Source': '3Victors Common Output',
        'Market': candidates['market'],
        'All_Detected_Brands': candidates['primary_fare_family'],
        'Identified_Basic_Economy_Brands': candidates.apply(
            lambda row: row['primary_fare_family'] if row['confidence_level'] in ['Very High', 'High'] else '', axis=1
        ),
        'Confidence_Score': candidates['basic_economy_score'].round(1),
        'Confidence_Level': candidates['confidence_level'],
        'Price_Rank_in_Market': candidates['rank_min'].astype(int),
        'Avg_Price_Inclusive': candidates['price_mean'].round(2),
        'Pct_Refundable': (candidates['pct_refundable'] * 100).round(1),
        'Avg_Change_Fee': candidates['avg_change_fee'].round(2),
        'Available_0_7_Days': candidates['available_0_7_days'].map({True: 'Y', False: 'N'}),
        'Available_8_14_Days': candidates['available_8_14_days'].map({True: 'Y', False: 'N'}),
        'Available_15_21_Days': candidates['available_15_21_days'].map({True: 'Y', False: 'N'}),
        'Available_22_Plus_Days': candidates['available_22_30_days'].map({True: 'Y', False: 'N'}),
        'Analysis_Date': datetime.now().strftime('%Y-%m-%d'),
        'Methodology_Version': '2.0_Enhanced'
    })
    
    # Sort by confidence score
    deliverable = deliverable.sort_values(['Confidence_Score', 'Airline', 'Market'], ascending=[False, True, True])
    
    print(f"✅ Enhanced deliverable created: {len(deliverable)} rows")
    return deliverable

def create_visualizations(df, deliverable):
    """Create comprehensive visualizations"""
    print("📊 Creating visualizations...")
    
    if df.empty or deliverable.empty:
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Enhanced Fare Brand Analysis Dashboard v2.0', fontsize=14, fontweight='bold')
    
    # 1. Confidence Distribution
    confidence_counts = deliverable['Confidence_Level'].value_counts()
    axes[0, 0].pie(confidence_counts.values, labels=confidence_counts.index, autopct='%1.1f%%')
    axes[0, 0].set_title('Basic Economy Confidence Distribution')
    
    # 2. Airlines by Basic Economy Detection
    be_airlines = deliverable[deliverable['Confidence_Level'].isin(['Very High', 'High'])]['Airline'].value_counts().head(10)
    axes[0, 1].bar(be_airlines.index, be_airlines.values)
    axes[0, 1].set_title('Airlines by Basic Economy Detection')
    axes[0, 1].set_xlabel('Airline')
    axes[0, 1].set_ylabel('Number of Basic Economy Brands')
    
    # 3. Price vs Confidence Score
    scatter = axes[1, 0].scatter(deliverable['Avg_Price_Inclusive'], 
                               deliverable['Confidence_Score'],
                               alpha=0.6, c=deliverable['Confidence_Score'], cmap='viridis')
    axes[1, 0].set_title('Price vs Confidence Score')
    axes[1, 0].set_xlabel('Average Price ($)')
    axes[1, 0].set_ylabel('Confidence Score')
    
    # 4. Advance Purchase Availability
    availability_cols = ['Available_0_7_Days', 'Available_8_14_Days', 'Available_15_21_Days', 'Available_22_Plus_Days']
    availability_rates = [deliverable[col].value_counts().get('Y', 0) / len(deliverable) for col in availability_cols]
    axes[1, 1].bar(['0-7d', '8-14d', '15-21d', '22+d'], availability_rates)
    axes[1, 1].set_title('Brand Availability by Advance Purchase Window')
    axes[1, 1].set_ylabel('Availability Rate')
    
    plt.tight_layout()
    
    # Save visualization
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'output/enhanced_analysis_dashboard_{timestamp}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"✅ Visualizations saved to {filename}")
    plt.show()

def main():
    """Main enhanced analysis pipeline"""
    print("🚀 Enhanced Fare Brand Analysis v2.0")
    print("="*60)
    
    try:
        # Execute enhanced pipeline
        df = get_enhanced_data()
        if df.empty:
            print("❌ No data available")
            return
        
        df_clean = clean_and_enhance_data(df)
        df_ranked = add_market_specific_ranking(df_clean)
        brand_analysis = analyze_brand_availability(df_ranked)
        candidates = enhanced_basic_economy_scoring(brand_analysis)
        deliverable = create_enhanced_deliverable(candidates)
        
        # Save results
        if not deliverable.empty:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'output/enhanced_basic_economy_analysis_{timestamp}.csv'
            deliverable.to_csv(csv_filename, index=False)
            print(f"✅ Results saved to {csv_filename}")
        
        create_visualizations(df_ranked, deliverable)
        
        print("\n🎉 Enhanced Analysis Complete!")
        print("="*60)
        
        if not deliverable.empty:
            print(f"📊 Summary:")
            print(f"   Total combinations: {len(deliverable)}")
            print(f"   High confidence Basic Economy: {len(deliverable[deliverable['Confidence_Level'].isin(['Very High', 'High'])])}")
            print(f"   Airlines analyzed: {deliverable['Airline'].nunique()}")
            print(f"   Markets analyzed: {deliverable['Market'].nunique()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        try:
            rs.close_connection()
        except:
            pass

if __name__ == "__main__":
    main() 