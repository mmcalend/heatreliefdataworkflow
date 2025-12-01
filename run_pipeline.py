"""
Heat Relief Network Data Pipeline
Simple script that does everything in order
"""
import requests
import pandas as pd
import os
from datetime import datetime

# ==================== STEP 1: GET DATA FROM REDCAP ====================
def fetch_from_redcap():
    """Pull all records from REDCap"""
    print("→ Fetching data from REDCap...")
    
    response = requests.post(
        os.environ['REDCAP_API_URL'],
        data={
            'token': os.environ['REDCAP_API_TOKEN'],
            'content': 'record',
            'format': 'json',
            'type': 'flat'
        }
    )
    
    records = response.json()
    df = pd.DataFrame(records)
    print(f"  ✓ Got {len(df)} records")
    return df


# ==================== STEP 2: SEPARATE PRESEASON FROM UPDATES ====================
def split_preseason_and_updates(df):
    """Split into base records and in-season updates"""
    print("→ Separating preseason sites from updates...")
    
    # Preseason = empty string or NaN in repeat instrument field
    preseason = df[
        (df['redcap_repeat_instrument'].isna()) | 
        (df['redcap_repeat_instrument'] == '')
    ].copy()
    
    # Updates = "in_season_updates" in repeat instrument field
    updates = df[df['redcap_repeat_instrument'] == 'in_season_updates'].copy()
    
    print(f"  ✓ {len(preseason)} preseason sites, {len(updates)} updates")
    return preseason, updates


# ==================== STEP 3: CLEAN UP THE DATA ====================
def clean_data(preseason_df):
    """Convert REDCap messy format to clean CSV format"""
    print("→ Cleaning up data...")
    
    clean = pd.DataFrame()
    
    # Basic info
    clean['site_id'] = preseason_df['record_id'].astype(str)
    clean['site_name'] = preseason_df['hrs_location']
    clean['organization'] = preseason_df['hrs_org']
    clean['email'] = preseason_df['site_email']
    
    # Address
    clean['address'] = preseason_df['site_address']
    clean['city'] = preseason_df['site_city']
    clean['state'] = 'AZ'
    clean['zip'] = preseason_df['site_zip'].astype(str).str.zfill(5)
    
    # Combine into full address for geocoding
    clean['full_address'] = (
        preseason_df['site_address'] + ', ' +
        preseason_df['site_city'] + ', AZ ' +
        clean['zip']
    )
    
    # Hours - keep it simple
    clean['hours'] = ''
    for idx, row in preseason_df.iterrows():
        if row.get('same_hours_everyday'):
            clean.at[idx, 'hours'] = f"{row.get('standard_start_time', '')} - {row.get('standard_close_time', '')}"
        else:
            hours_list = []
            days = ['mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun']
            day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            
            for day, name in zip(days, day_names):
                start = row.get(f'{day}_start')
                end = row.get(f'{day}_close')
                if pd.notna(start) and pd.notna(end):
                    hours_list.append(f"{name}: {start}-{end}")
            
            clean.at[idx, 'hours'] = '; '.join(hours_list)
    
    # Services
    clean['services'] = ''
    service_fields = [col for col in preseason_df.columns if col.startswith('services___')]
    for idx, row in preseason_df.iterrows():
        services = [col.replace('services___', '').replace('_', ' ').title() 
                   for col in service_fields if row.get(col) == 1]
        clean.at[idx, 'services'] = ', '.join(services)
    
    # Status - handle multiple REDCap configurations
    print(f"  → Checking review_status values...")
    if 'review_status' in preseason_df.columns:
        unique_statuses = preseason_df['review_status'].unique()
        print(f"     REDCap sent these status values: {unique_statuses}")
        
        # Try to map them intelligently
        status_map = {}
        for val in unique_statuses:
            if pd.isna(val):
                status_map[val] = 'Pending'
            elif val in [0, '0']:
                status_map[val] = 'Pending'
            elif val in [1, '1']:
                status_map[val] = 'Accepted'  # Most common
            elif val in [2, '2']:
                status_map[val] = 'Under Review'
            elif val in [3, '3']:
                status_map[val] = 'Accepted'  # Alternative mapping
            else:
                status_map[val] = 'Pending'
        
        clean['status'] = preseason_df['review_status'].map(status_map).fillna('Pending')
        
        # Print what we mapped
        for idx, row in clean.iterrows():
            original = preseason_df.loc[idx, 'review_status']
            mapped = row['status']
            print(f"     Site {row['site_id']}: status {original} → {mapped}")
    else:
        print(f"  ⚠ No review_status field found - setting all to Accepted")
        clean['status'] = 'Accepted'
    
    # Coordinates (will be filled by geocoding)
    clean['latitude'] = None
    clean['longitude'] = None
    
    # Timestamp
    clean['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    print(f"  ✓ Cleaned {len(clean)} sites")
    accepted_count = len(clean[clean['status'] == 'Accepted'])
    print(f"  ✓ {accepted_count} sites are Accepted and will be geocoded")
    
    return clean


# ==================== STEP 4: ADD COORDINATES ====================
def geocode_addresses(df):
    """Add lat/lon coordinates using Mapbox - ONLY for newly accepted sites"""
    print("→ Geocoding addresses...")
    
    mapbox_token = os.environ.get('MAPBOX_API_TOKEN')
    if not mapbox_token:
        print("  ⚠ No Mapbox token found - skipping geocoding")
        print("     Set MAPBOX_API_TOKEN in GitHub secrets to enable")
        return df
    
    # Check if previous data exists
    previous_csv = 'data/public/sites.csv'
    previously_geocoded = {}
    
    if os.path.exists(previous_csv):
        try:
            prev_df = pd.read_csv(previous_csv)
            for _, row in prev_df.iterrows():
                if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                    previously_geocoded[str(row['site_id'])] = {
                        'lat': row['latitude'],
                        'lon': row['longitude']
                    }
            print(f"  → Found {len(previously_geocoded)} previously geocoded sites")
        except Exception as e:
            print(f"  → No previous data found (this is fine for first run)")
    
    geocoded_count = 0
    skipped_count = 0
    reused_count = 0
    
    for idx, row in df.iterrows():
        site_id = str(row['site_id'])
        
        # Check if already geocoded
        if site_id in previously_geocoded:
            df.at[idx, 'latitude'] = previously_geocoded[site_id]['lat']
            df.at[idx, 'longitude'] = previously_geocoded[site_id]['lon']
            reused_count += 1
            print(f"  ✓ Reusing coordinates for: {row['site_name']}")
            continue
        
        # Only geocode Accepted sites
        if row['status'] != 'Accepted':
            skipped_count += 1
            continue
        
        # Geocode this NEW Accepted site
        try:
            print(f"  → Geocoding NEW site: {row['site_name']}")
            response = requests.get(
                f"https://api.mapbox.com/geocoding/v5/mapbox.places/{row['full_address']}.json",
                params={'access_token': mapbox_token, 'limit': 1}
            )
            data = response.json()
            
            if data.get('features'):
                coords = data['features'][0]['geometry']['coordinates']
                df.at[idx, 'longitude'] = coords[0]
                df.at[idx, 'latitude'] = coords[1]
                geocoded_count += 1
                print(f"     ✓ Success: {coords[1]:.6f}, {coords[0]:.6f}")
            else:
                print(f"     ✗ No results found for this address")
                
        except Exception as e:
            print(f"     ✗ Error: {e}")
    
    print(f"  ✓ Geocoded {geocoded_count} NEW sites (used {geocoded_count} Mapbox credits)")
    print(f"  ✓ Reused {reused_count} existing coordinates (used 0 credits)")
    print(f"  → Skipped {skipped_count} non-Accepted sites")
    
    return df


# ==================== STEP 5: APPLY IN-SEASON UPDATES ====================
def apply_updates(clean_df, updates_df):
    """Apply in-season updates on top of preseason data"""
    
    if updates_df.empty:
        print("→ No in-season updates to apply")
        return clean_df
    
    print(f"→ Applying {len(updates_df)} in-season updates...")
    
    # Get most recent update for each site
    if 'update_date' in updates_df.columns:
        updates_df['update_date'] = pd.to_datetime(updates_df['update_date'])
    else:
        updates_df['update_date'] = datetime.now()
    
    latest_updates = updates_df.sort_values('update_date').groupby('record_id').last()
    
    update_count = 0
    for record_id, update in latest_updates.iterrows():
        mask = clean_df['site_id'] == str(record_id)
        if not mask.any():
            print(f"  ⚠ Update for site {record_id} but no matching preseason record")
            continue
        
        idx = clean_df[mask].index[0]
        
        # Update hours if provided
        if pd.notna(update.get('temp_standard_open')):
            clean_df.at[idx, 'hours'] = f"{update['temp_standard_open']} - {update.get('temp_standard_close', '')}"
            print(f"  ✓ Updated hours for: {clean_df.at[idx, 'site_name']}")
        
        clean_df.at[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        update_count += 1
    
    print(f"  ✓ Applied {update_count} updates")
    return clean_df


# ==================== STEP 6: SAVE OUTPUT ====================
def save_files(df):
    """Save CSV and create archive"""
    print("→ Saving output files...")
    
    # Create directories
    os.makedirs('data/public', exist_ok=True)
    os.makedirs('data/archives', exist_ok=True)
    
    # Save main CSV
    csv_path = 'data/public/sites.csv'
    df.to_csv(csv_path, index=False)
    print(f"  ✓ Saved {csv_path} ({len(df)} sites)")
    
    # Verify it was created
    if os.path.exists(csv_path):
        file_size = os.path.getsize(csv_path)
        print(f"     File size: {file_size} bytes")
    else:
        print(f"     ⚠ WARNING: File was not created!")
    
    # Save daily archive
    today = datetime.now().strftime('%Y-%m-%d')
    archive_path = f'data/archives/sites_{today}.csv'
    df.to_csv(archive_path, index=False)
    print(f"  ✓ Saved {archive_path}")
    
    # Create summary
    summary = f"""Heat Relief Network Data
Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Total Sites: {len(df)}
Accepted: {len(df[df['status'] == 'Accepted'])}
Pending: {len(df[df['status'] == 'Pending'])}
Under Review: {len(df[df['status'] == 'Under Review'])}

Geocoded: {df['latitude'].notna().sum()}
Missing Coordinates: {df['latitude'].isna().sum()}
"""
    
    summary_path = 'data/public/summary.txt'
    with open(summary_path, 'w') as f:
        f.write(summary)
    print(f"  ✓ Saved {summary_path}")
    
    # Print the summary to logs
    print("\n" + "="*60)
    print(summary)
    print("="*60)


# ==================== RUN EVERYTHING ====================
def main():
    """Run the complete pipeline"""
    print("\n" + "="*60)
    print("HEAT RELIEF NETWORK DATA PIPELINE")
    print("="*60 + "\n")
    
    try:
        # Step 1: Get data
        raw_data = fetch_from_redcap()
        
        # Step 2: Split preseason and updates
        preseason, updates = split_preseason_and_updates(raw_data)
        
        # Step 3: Clean data
        clean_data_df = clean_data(preseason)
        
        # Step 4: Geocode
        clean_data_df = geocode_addresses(clean_data_df)
        
        # Step 5: Apply updates
        final_data = apply_updates(clean_data_df, updates)
        
        # Step 6: Save
        save_files(final_data)
        
        print("\n" + "="*60)
        print("✓✓✓ PIPELINE COMPLETE ✓✓✓")
        print("="*60)
        print("\nFiles created:")
        print("  - data/public/sites.csv")
        print("  - data/public/summary.txt")
        print(f"  - data/archives/sites_{datetime.now().strftime('%Y-%m-%d')}.csv")
        print("\nNext: Git will commit and push these files")
        print("="*60 + "\n")
        
    except Exception as e:
        print("\n" + "="*60)
        print("✗✗✗ PIPELINE FAILED ✗✗✗")
        print("="*60)
        print(f"\nError: {e}")
        print("\nCheck the error above and try again")
        print("="*60 + "\n")
        raise


if __name__ == '__main__':
    main()
