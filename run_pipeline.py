import requests
import pandas as pd
import os
from datetime import datetime

# Import from redcap
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
    print(f"  Got {len(df)} records")
    return df


# Pre-season
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
    
    print(f"  {len(preseason)} preseason sites, {len(updates)} updates")
    return preseason, updates


# Clean
def clean_data(preseason_df):
    """Convert REDCap messy format to clean CSV format"""
    print("→ Cleaning up data...")
    
    clean = pd.DataFrame()
    
    # Basic info
    clean['site_id'] = preseason_df['record_id']
    clean['site_name'] = preseason_df['hrs_location']
    clean['organization'] = preseason_df['hrs_org']
    clean['email'] = preseason_df['site_email']
    
    # Address
    clean['address'] = preseason_df['site_address']
    clean['city'] = preseason_df['site_city']
    clean['state'] = 'AZ'  # Hardcoded for Arizona
    clean['zip'] = preseason_df['site_zip'].astype(str).str.zfill(5)
    
    # Combine into full address for geocoding
    clean['full_address'] = (
        preseason_df['site_address'] + ', ' +
        preseason_df['site_city'] + ', AZ ' +
        clean['zip']
    )
    
    # Hours 
    clean['hours'] = ''
    for idx, row in preseason_df.iterrows():
        if row.get('same_hours_everyday'):
            # Same hours every day
            clean.at[idx, 'hours'] = f"{row.get('standard_start_time', '')} - {row.get('standard_close_time', '')}"
        else:
            # Different hours each day - combine them all
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
    
    # Status
    clean['status'] = preseason_df['review_status'].map({1: 'Pending', 2: 'Under Review', 3: 'Accepted'}).fillna('Pending')
    
    # Coordinates (empty for now, will geocode next)
    clean['latitude'] = None
    clean['longitude'] = None
    
    # Track when updated
    clean['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    print(f"  Cleaned {len(clean)} sites")
    return clean


# Geocode
def geocode_addresses(df, previous_csv_path='data/public/sites.csv'):
    """
    Add lat/lon coordinates using Mapbox
    Only geocodes NEWLY ACCEPTED sites to save API credits
    """
    print("→ Geocoding addresses...")
    
    mapbox_token = os.environ.get('MAPBOX_API_TOKEN')
    if not mapbox_token:
        print("  ⚠ No Mapbox token, skipping geocoding")
        return df
    
    
    previously_geocoded = set()
    if os.path.exists(previous_csv_path):
        try:
            prev_df = pd.read_csv(previous_csv_path)
            # Track which site_ids already have coordinates
            previously_geocoded = set(
                prev_df[pd.notna(prev_df['latitude'])]['site_id'].astype(str)
            )
            print(f"  Found {len(previously_geocoded)} previously geocoded sites")
        except:
            pass  # If can't read previous file, geocode everything accepted
    
    geocoded_count = 0
    skipped_count = 0
    
    for idx, row in df.iterrows():
        site_id = str(row['site_id'])
        
        # Only geocode if site is accepted and hasnt been geocoded before
        if row['status'] != 'Accepted':
            skipped_count += 1
            continue
            
        if site_id in previously_geocoded:
            # Already geocoded, copy from previous data
            if os.path.exists(previous_csv_path):
                try:
                    prev_df = pd.read_csv(previous_csv_path)
                    prev_row = prev_df[prev_df['site_id'].astype(str) == site_id]
                    if not prev_row.empty:
                        df.at[idx, 'latitude'] = prev_row.iloc[0]['latitude']
                        df.at[idx, 'longitude'] = prev_row.iloc[0]['longitude']
                except:
                    pass
            continue
        
        # This is a NEWLY ACCEPTED site - geocode it
        try:
            print(f"  Geocoding newly accepted site: {row['site_name']}")
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
        except Exception as e:
            print(f"  Failed to geocode {row['site_name']}: {e}")
    
    print(f"  Geocoded {geocoded_count} newly accepted sites")
    print(f"  Skipped {skipped_count} pending/rejected sites (no coordinates needed)")
    return df


# In season updates
def apply_updates(clean_df, updates_df):

    
    if updates_df.empty:
       
        return clean_df
    
    # Get most recent update for each site
    updates_df['update_date'] = pd.to_datetime(updates_df.get('update_date', datetime.now()))
    latest_updates = updates_df.sort_values('update_date').groupby('record_id').last()
    
    update_count = 0
    for record_id, update in latest_updates.iterrows():
        # Find the site in clean data
        mask = clean_df['site_id'] == record_id
        if not mask.any():
            continue
        
        idx = clean_df[mask].index[0]
        
        # Update hours if provided
        if pd.notna(update.get('temp_standard_open')):
            clean_df.at[idx, 'hours'] = f"{update['temp_standard_open']} - {update.get('temp_standard_close', '')}"
        
        # Update last_updated timestamp
        clean_df.at[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        update_count += 1
    
    print(f"  Applied {update_count} updates")
    return clean_df


# Output
def save_files(df):

    
    # Save main CSV
    os.makedirs('data/public', exist_ok=True)
    df.to_csv('data/public/sites.csv', index=False)
    print(f"  Saved data/public/sites.csv ({len(df)} sites)")
    
    # Save daily archive
    today = datetime.now().strftime('%Y-%m-%d')
    os.makedirs('data/archives', exist_ok=True)
    df.to_csv(f'data/archives/sites_{today}.csv', index=False)
    print(f"  Saved daily archive")
    
    # Create summary
    summary = f"""Heat Relief Network Data
Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Sites: {len(df)}
Accepted: {len(df[df['status'] == 'Accepted'])}
Pending: {len(df[df['status'] == 'Pending'])}
"""
    with open('data/public/summary.txt', 'w') as f:
        f.write(summary)


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
    
        
    except Exception as e:
        raise


if __name__ == '__main__':
    main()
