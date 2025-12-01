"""
Heat Relief Network Data Pipeline
Simple script that does everything in order
"""
import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# ==================== CALCULATE HOLIDAYS ====================
def calculate_holidays(year=2026):
    """Calculate floating holiday dates"""
    holidays = {}
    
    # Memorial Day: Last Monday in May
    may_31 = datetime(year, 5, 31)
    days_to_subtract = (may_31.weekday() - 0) % 7
    memorial_day = may_31 - timedelta(days=days_to_subtract)
    holidays['memorial_day'] = memorial_day.strftime('%Y-%m-%d')
    
    # Juneteenth: June 19
    holidays['juneteenth'] = f'{year}-06-19'
    
    # Independence Day: July 4
    holidays['independence_day'] = f'{year}-07-04'
    
    # Labor Day: First Monday in September
    sept_1 = datetime(year, 9, 1)
    days_to_add = (0 - sept_1.weekday()) % 7
    labor_day = sept_1 + timedelta(days=days_to_add) if days_to_add > 0 else sept_1
    holidays['labor_day'] = labor_day.strftime('%Y-%m-%d')
    
    return holidays


def convert_to_12hr(time_str):
    """Convert 24hr time to 12hr format"""
    if not time_str or pd.isna(time_str):
        return ''
    try:
        time_obj = datetime.strptime(str(time_str), '%H:%M')
        return time_obj.strftime('%-I:%M %p').lower()
    except:
        return str(time_str)


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
    
    preseason = df[
        (df['redcap_repeat_instrument'].isna()) | 
        (df['redcap_repeat_instrument'] == '')
    ].copy()
    
    updates = df[df['redcap_repeat_instrument'] == 'in_season_updates'].copy()
    
    print(f"  ✓ {len(preseason)} preseason sites, {len(updates)} updates")
    return preseason, updates


# ==================== STEP 3: CLEAN UP THE DATA ====================
def clean_data(preseason_df):
    """Convert REDCap messy format to clean CSV format"""
    print("→ Cleaning up data...")
    
    clean = pd.DataFrame()
    
    # Basic info
    clean['record_id'] = preseason_df['record_id']
    clean['organization_name'] = preseason_df['hrs_org']
    clean['site_name'] = preseason_df['hrs_location']
    
    # Site type - extract just the first part before " - "
    if 'site_type' in preseason_df.columns:
        clean['site_type'] = preseason_df['site_type']
    else:
        clean['site_type'] = ''
    
    clean['contact_email'] = preseason_df['site_email']
    
    # Address
    clean['address'] = preseason_df['site_address']
    clean['city'] = preseason_df['site_city']
    clean['state'] = preseason_df['site_state']
    clean['zip_code'] = preseason_df['site_zip'].astype(str).str.zfill(5)
    
    clean['full_address'] = (
        preseason_df['site_address'] + ', ' +
        preseason_df['site_city'] + ', ' +
        preseason_df['site_state'].astype(str) + ' ' +
        clean['zip_code']
    )
    
    # Geocoding placeholders
    clean['latitude'] = None
    clean['longitude'] = None
    clean['geocoded'] = False
    
    # Day names
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Hours - detailed breakdown
    full_schedules = []
    monday_hours = []
    tuesday_hours = []
    wednesday_hours = []
    thursday_hours = []
    friday_hours = []
    saturday_hours = []
    sunday_hours = []
    days_open_list = []
    opening_times = []
    closing_times = []
    
    for idx, row in preseason_df.iterrows():
        same_hours = row.get('same_hours_everyday', False)
        
        # Get which days are open
        open_days = []
        for i, day in enumerate(day_names, start=1):
            checkbox_value = row.get(f'dow___{i}')
            if checkbox_value == 1 or checkbox_value == '1':
                open_days.append(day)
        
        days_open_list.append(', '.join(open_days))
        
        if same_hours:
            opening = str(row.get('standard_start_time', '')).strip()
            closing = str(row.get('standard_close_time', '')).strip()
            
            opening_times.append(opening)
            closing_times.append(closing)
            
            if open_days and opening and closing:
                opening_12hr = convert_to_12hr(opening)
                closing_12hr = convert_to_12hr(closing)
                full_schedules.append(f"{', '.join(open_days)}: {opening_12hr} - {closing_12hr}")
            else:
                full_schedules.append('')
            
            hours_string = f"{opening} - {closing}" if opening and closing else ""
            monday_hours.append(hours_string if 'Monday' in open_days else "")
            tuesday_hours.append(hours_string if 'Tuesday' in open_days else "")
            wednesday_hours.append(hours_string if 'Wednesday' in open_days else "")
            thursday_hours.append(hours_string if 'Thursday' in open_days else "")
            friday_hours.append(hours_string if 'Friday' in open_days else "")
            saturday_hours.append(hours_string if 'Saturday' in open_days else "")
            sunday_hours.append(hours_string if 'Sunday' in open_days else "")
        else:
            opening_times.append('')
            closing_times.append('')
            
            schedule_parts = []
            day_fields = [
                ('Monday', 'mon_start', 'mon_close'),
                ('Tuesday', 'tues_start', 'tues_close'),
                ('Wednesday', 'wed_start', 'wed_close'),
                ('Thursday', 'thurs_start', 'thurs_close'),
                ('Friday', 'fri_start', 'fri_close'),
                ('Saturday', 'sat_start', 'sat_close'),
                ('Sunday', 'sun_start', 'sun_close')
            ]
            
            day_hours = []
            for day, start_field, close_field in day_fields:
                start = row.get(start_field)
                close = row.get(close_field)
                if pd.notna(start) and pd.notna(close):
                    hours = f"{start} - {close}"
                    day_hours.append(hours)
                    start_12 = convert_to_12hr(start)
                    close_12 = convert_to_12hr(close)
                    schedule_parts.append(f"{day}: {start_12} - {close_12}")
                else:
                    day_hours.append("")
            
            monday_hours.append(day_hours[0])
            tuesday_hours.append(day_hours[1])
            wednesday_hours.append(day_hours[2])
            thursday_hours.append(day_hours[3])
            friday_hours.append(day_hours[4])
            saturday_hours.append(day_hours[5])
            sunday_hours.append(day_hours[6])
            
            full_schedules.append('; '.join(schedule_parts))
    
    clean['same_hours_everyday'] = preseason_df['same_hours_everyday'].fillna(False).astype(bool)
    clean['opening_time'] = opening_times
    clean['closing_time'] = closing_times
    clean['full_schedule'] = full_schedules
    clean['days_open'] = days_open_list
    clean['monday_hours'] = monday_hours
    clean['tuesday_hours'] = tuesday_hours
    clean['wednesday_hours'] = wednesday_hours
    clean['thursday_hours'] = thursday_hours
    clean['friday_hours'] = friday_hours
    clean['saturday_hours'] = saturday_hours
    clean['sunday_hours'] = sunday_hours
    
    # Services - individual flags AND comma-separated list
    service_fields = [col for col in preseason_df.columns if col.startswith('services___')]
    
    # Initialize service columns
    clean['has_charging'] = False
    clean['has_pet_services'] = False
    clean['has_showers'] = False
    clean['has_storage_for_belongings'] = False
    clean['has_food'] = False
    clean['has_internet'] = False
    
    service_lists = []
    for idx, row in preseason_df.iterrows():
        site_services = []
        for field in service_fields:
            if row.get(field) == 1 or row.get(field) == '1':
                # Extract service name
                service_name = field.replace('services___', '').replace('_', ' ').title()
                site_services.append(service_name)
                
                # Set individual flag
                if 'charging' in field:
                    clean.at[idx, 'has_charging'] = True
                elif 'pet' in field:
                    clean.at[idx, 'has_pet_services'] = True
                elif 'shower' in field:
                    clean.at[idx, 'has_showers'] = True
                elif 'storage' in field:
                    clean.at[idx, 'has_storage_for_belongings'] = True
                elif 'food' in field:
                    clean.at[idx, 'has_food'] = True
                elif 'internet' in field or 'wifi' in field:
                    clean.at[idx, 'has_internet'] = True
        
        service_lists.append(', '.join(site_services))
    
    clean['services_offered'] = service_lists
    
    # Closures - combine special dates and holidays
    holidays = calculate_holidays(2026)
    closure_dates = []
    
    for idx, row in preseason_df.iterrows():
        all_closures = []
        
        # Special closure dates
        for i in range(1, 11):
            date_val = row.get(f'closure_{i}')
            if pd.notna(date_val):
                all_closures.append(str(date_val))
        
        # Holiday closures (value = 2 means closed)
        if row.get('memorial_day') == 2:
            all_closures.append(holidays['memorial_day'])
        if row.get('juneteenth') == 2:
            all_closures.append(holidays['juneteenth'])
        if row.get('july_4') == 2:
            all_closures.append(holidays['independence_day'])
        if row.get('labor_day') == 2:
            all_closures.append(holidays['labor_day'])
        
        closure_dates.append(', '.join(all_closures) if all_closures else '')
    
    clean['special_closure_dates'] = closure_dates
    
    # Status
    print(f"  → Checking review_status values...")
    if 'review_status' in preseason_df.columns:
        unique_statuses = preseason_df['review_status'].unique()
        print(f"     REDCap sent: {unique_statuses}")
        
        status_map = {
            0: 'Pending', '0': 'Pending',
            1: 'Accepted', '1': 'Accepted',
            2: 'Under Review', '2': 'Under Review',
            3: 'Accepted', '3': 'Accepted'
        }
        
        clean['review_status'] = preseason_df['review_status'].map(status_map).fillna('Pending')
        
        for idx, row in clean.iterrows():
            original = preseason_df.loc[idx, 'review_status']
            mapped = row['review_status']
            print(f"     Site {row['record_id']}: {original} → {mapped}")
    else:
        clean['review_status'] = 'Accepted'
    
    # Metadata
    clean['last_updated'] = datetime.now().isoformat()
    clean['data_source'] = 'preseason'
    
    print(f"  ✓ Cleaned {len(clean)} sites")
    accepted_count = len(clean[clean['review_status'] == 'Accepted'])
    print(f"  ✓ {accepted_count} sites are Accepted")
    
    return clean


# ==================== STEP 4: GEOCODE ====================
def geocode_addresses(df):
    """Add coordinates using Mapbox"""
    print("→ Geocoding addresses...")
    
    mapbox_token = os.environ.get('MAPBOX_API_TOKEN')
    if not mapbox_token:
        print("  ⚠ No Mapbox token - skipping")
        return df
    
    previous_csv = 'data/public/sites.csv'
    previously_geocoded = {}
    
    if os.path.exists(previous_csv):
        try:
            prev_df = pd.read_csv(previous_csv)
            for _, row in prev_df.iterrows():
                if pd.notna(row.get('latitude')):
                    previously_geocoded[str(row['record_id'])] = {
                        'lat': row['latitude'],
                        'lon': row['longitude']
                    }
            print(f"  → Found {len(previously_geocoded)} previously geocoded")
        except:
            print(f"  → No previous data (first run)")
    
    geocoded_count = 0
    reused_count = 0
    skipped_count = 0
    
    for idx, row in df.iterrows():
        record_id = str(row['record_id'])
        
        if record_id in previously_geocoded:
            df.at[idx, 'latitude'] = previously_geocoded[record_id]['lat']
            df.at[idx, 'longitude'] = previously_geocoded[record_id]['lon']
            df.at[idx, 'geocoded'] = True
            reused_count += 1
            continue
        
        if row['review_status'] != 'Accepted':
            skipped_count += 1
            continue
        
        try:
            print(f"  → Geocoding: {row['site_name']}")
            response = requests.get(
                f"https://api.mapbox.com/geocoding/v5/mapbox.places/{row['full_address']}.json",
                params={'access_token': mapbox_token, 'limit': 1}
            )
            data = response.json()
            
            if data.get('features'):
                coords = data['features'][0]['geometry']['coordinates']
                df.at[idx, 'longitude'] = coords[0]
                df.at[idx, 'latitude'] = coords[1]
                df.at[idx, 'geocoded'] = True
                geocoded_count += 1
                print(f"     ✓ {coords[1]:.6f}, {coords[0]:.6f}")
        except Exception as e:
            print(f"     ✗ Error: {e}")
    
    print(f"  ✓ Geocoded {geocoded_count} new (used {geocoded_count} credits)")
    print(f"  ✓ Reused {reused_count} (used 0 credits)")
    print(f"  → Skipped {skipped_count} non-Accepted")
    
    return df


# ==================== STEP 5: APPLY UPDATES ====================
def apply_updates(clean_df, updates_df):
    """Apply in-season updates"""
    
    if updates_df.empty:
        print("→ No updates to apply")
        return clean_df
    
    print(f"→ Applying {len(updates_df)} updates...")
    
    if 'update_date' in updates_df.columns:
        updates_df['update_date'] = pd.to_datetime(updates_df['update_date'])
    else:
        updates_df['update_date'] = datetime.now()
    
    latest_updates = updates_df.sort_values('update_date').groupby('record_id').last()
    
    update_count = 0
    for record_id, update in latest_updates.iterrows():
        mask = clean_df['record_id'] == record_id
        if not mask.any():
            continue
        
        idx = clean_df[mask].index[0]
        
        if pd.notna(update.get('temp_standard_open')):
            clean_df.at[idx, 'opening_time'] = update['temp_standard_open']
            clean_df.at[idx, 'closing_time'] = update.get('temp_standard_close', '')
            print(f"  ✓ Updated: {clean_df.at[idx, 'site_name']}")
        
        clean_df.at[idx, 'last_updated'] = datetime.now().isoformat()
        clean_df.at[idx, 'data_source'] = 'in-season update'
        update_count += 1
    
    print(f"  ✓ Applied {update_count} updates")
    return clean_df


# ==================== STEP 6: SAVE ====================
def save_files(df):
    """Save output"""
    print("→ Saving files...")
    
    os.makedirs('data/public', exist_ok=True)
    os.makedirs('data/archives', exist_ok=True)
    
    csv_path = 'data/public/sites.csv'
    df.to_csv(csv_path, index=False)
    print(f"  ✓ Saved {csv_path} ({len(df)} sites, {os.path.getsize(csv_path)} bytes)")
    
    today = datetime.now().strftime('%Y-%m-%d')
    df.to_csv(f'data/archives/sites_{today}.csv', index=False)
    print(f"  ✓ Saved daily archive")
    
    summary = f"""Heat Relief Network Data
Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Total Sites: {len(df)}
Accepted: {len(df[df['review_status'] == 'Accepted'])}
Pending: {len(df[df['review_status'] == 'Pending'])}

Geocoded: {df['geocoded'].sum()}
"""
    
    with open('data/public/summary.txt', 'w') as f:
        f.write(summary)
    
    print("\n" + summary)


# ==================== MAIN ====================
def main():
    print("\n" + "="*60)
    print("HEAT RELIEF NETWORK DATA PIPELINE")
    print("="*60 + "\n")
    
    try:
        raw_data = fetch_from_redcap()
        preseason, updates = split_preseason_and_updates(raw_data)
        clean_data_df = clean_data(preseason)
        clean_data_df = geocode_addresses(clean_data_df)
        final_data = apply_updates(clean_data_df, updates)
        save_files(final_data)
        
        print("="*60)
        print("✓✓✓ COMPLETE ✓✓✓")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        raise


if __name__ == '__main__':
    main()
