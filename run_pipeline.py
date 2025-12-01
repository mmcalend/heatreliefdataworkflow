"""
Heat Relief Network Data Pipeline
Uses REDCap metadata to avoid hardcoding
"""
import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from io import StringIO

# ==================== FETCH METADATA ====================
def fetch_metadata():
    """Get REDCap data dictionary"""
    print("→ Fetching metadata from REDCap...")
    
    response = requests.post(
        os.environ['REDCAP_API_URL'],
        data={
            'token': os.environ['REDCAP_API_TOKEN'],
            'content': 'metadata',
            'format': 'csv'
        }
    )
    
    metadata = pd.read_csv(StringIO(response.text))
    print(f"  ✓ Got metadata for {len(metadata)} fields")
    return metadata


def parse_choices(metadata, field_name):
    """Parse choice list from metadata - returns {1: 'Label', 2: 'Label', ...}"""
    field_meta = metadata[metadata['field_name'] == field_name]
    
    if len(field_meta) == 0:
        return {}
    
    choices_str = field_meta.iloc[0]['select_choices_or_calculations']
    
    if not choices_str or str(choices_str) == 'nan':
        return {}
    
    choices = {}
    for choice in str(choices_str).split('|'):
        choice = choice.strip()
        if ',' in choice:
            code, label = choice.split(',', 1)
            try:
                choices[int(code.strip())] = label.strip()
            except ValueError:
                choices[code.strip()] = label.strip()
    
    return choices


def get_checkbox_fields(metadata, base_field_name):
    """Get checkbox field mappings - returns {'field___1': 'Label 1', ...}"""
    field_meta = metadata[metadata['field_name'] == base_field_name]
    
    if len(field_meta) == 0:
        return {}
    
    choices_str = field_meta.iloc[0]['select_choices_or_calculations']
    
    if not choices_str or str(choices_str) == 'nan':
        return {}
    
    checkbox_fields = {}
    for choice in str(choices_str).split('|'):
        choice = choice.strip()
        if ',' in choice:
            code, label = choice.split(',', 1)
            code = code.strip()
            label = label.strip()
            checkbox_fields[f'{base_field_name}___{code}'] = label
    
    return checkbox_fields


def build_mappings(metadata):
    """Build all field mappings from metadata"""
    print("→ Building field mappings...")
    
    mappings = {
        'state_codes': parse_choices(metadata, 'site_state'),
        'site_types': parse_choices(metadata, 'site_type'),
        'review_status': parse_choices(metadata, 'review_status'),
        'services_offered': get_checkbox_fields(metadata, 'services_offered'),
        'dow': get_checkbox_fields(metadata, 'dow')
    }
    
    print(f"  ✓ Built mappings for {len(mappings)} field types")
    return mappings


# ==================== HELPERS ====================
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


# ==================== STEP 1: GET DATA ====================
def fetch_from_redcap():
    """Pull all records from REDCap"""
    print("→ Fetching data from REDCap...")
    
    response = requests.post(
        os.environ['REDCAP_API_URL'],
        data={
            'token': os.environ['REDCAP_API_TOKEN'],
            'content': 'record',
            'format': 'csv',
            'type': 'flat',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false'
        }
    )
    
    df = pd.read_csv(StringIO(response.text))
    print(f"  ✓ Got {len(df)} records")
    return df


# ==================== STEP 2: SEPARATE ====================
def split_preseason_and_updates(df):
    """Split into base records and updates"""
    print("→ Separating preseason from updates...")
    
    preseason = df[
        (df['redcap_repeat_instrument'].isna()) | 
        (df['redcap_repeat_instrument'] == '')
    ].copy()
    
    updates = df[df['redcap_repeat_instrument'] == 'in_season_updates'].copy()
    
    print(f"  ✓ {len(preseason)} preseason, {len(updates)} updates")
    return preseason, updates


# ==================== STEP 3: CLEAN ====================
def clean_data(preseason_df, mappings):
    """Clean data using metadata mappings"""
    print("→ Cleaning data...")
    
    clean = pd.DataFrame()
    
    # Basic info
    clean['record_id'] = preseason_df['record_id']
    clean['organization_name'] = preseason_df['hrs_org']
    clean['site_name'] = preseason_df['hrs_location']
    
    # Site type - use metadata mapping then split on " - "
    site_type_full = preseason_df['site_type'].map(mappings['site_types'])
    clean['site_type'] = site_type_full.str.split(' - ').str[0]
    
    clean['contact_email'] = preseason_df['site_email']
    
    # Address - use state mapping from metadata
    clean['address'] = preseason_df['site_address']
    clean['city'] = preseason_df['site_city']
    clean['state'] = preseason_df['site_state'].astype(int).map(mappings['state_codes'])
    clean['zip_code'] = preseason_df['site_zip'].astype(int).astype(str).str.zfill(5)
    
    clean['full_address'] = (
        preseason_df['site_address'] + ', ' +
        preseason_df['site_city'] + ', ' +
        preseason_df['site_state'].astype(int).map(mappings['state_codes']) + ' ' +
        clean['zip_code']
    )
    
    # Geocoding placeholders
    clean['latitude'] = None
    clean['longitude'] = None
    clean['geocoded'] = False
    
    # Day names
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Hours - detailed processing
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
        
        # Get open days using dow checkboxes
        open_days = []
        for i, day in enumerate(day_names, start=1):
            checkbox_value = row.get(f'dow___{i}')
            if checkbox_value == 1 or checkbox_value == '1':
                open_days.append(day)
        
        days_open_list.append(', '.join(open_days))
        
        if same_hours:
            # Same hours every day
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
            
            # Populate individual day columns ONLY for open days
            hours_string = f"{opening} - {closing}" if opening and closing else ""
            monday_hours.append(hours_string if 'Monday' in open_days else "")
            tuesday_hours.append(hours_string if 'Tuesday' in open_days else "")
            wednesday_hours.append(hours_string if 'Wednesday' in open_days else "")
            thursday_hours.append(hours_string if 'Thursday' in open_days else "")
            friday_hours.append(hours_string if 'Friday' in open_days else "")
            saturday_hours.append(hours_string if 'Saturday' in open_days else "")
            sunday_hours.append(hours_string if 'Sunday' in open_days else "")
        else:
            # Different hours per day
            opening_times.append('')
            closing_times.append('')
            
            schedule_parts = []
            
            # Monday
            mon_open = row.get('mon_start')
            mon_close = row.get('mon_close')
            if pd.notna(mon_open) and pd.notna(mon_close):
                mon_hrs = f"{mon_open} - {mon_close}"
                monday_hours.append(mon_hrs)
                schedule_parts.append(f"Monday: {convert_to_12hr(mon_open)} - {convert_to_12hr(mon_close)}")
            else:
                monday_hours.append("")
            
            # Tuesday
            tue_open = row.get('tues_start')
            tue_close = row.get('tues_close')
            if pd.notna(tue_open) and pd.notna(tue_close):
                tue_hrs = f"{tue_open} - {tue_close}"
                tuesday_hours.append(tue_hrs)
                schedule_parts.append(f"Tuesday: {convert_to_12hr(tue_open)} - {convert_to_12hr(tue_close)}")
            else:
                tuesday_hours.append("")
            
            # Wednesday
            wed_open = row.get('wed_start')
            wed_close = row.get('wed_close')
            if pd.notna(wed_open) and pd.notna(wed_close):
                wed_hrs = f"{wed_open} - {wed_close}"
                wednesday_hours.append(wed_hrs)
                schedule_parts.append(f"Wednesday: {convert_to_12hr(wed_open)} - {convert_to_12hr(wed_close)}")
            else:
                wednesday_hours.append("")
            
            # Thursday
            thu_open = row.get('thurs_start')
            thu_close = row.get('thurs_close')
            if pd.notna(thu_open) and pd.notna(thu_close):
                thu_hrs = f"{thu_open} - {thu_close}"
                thursday_hours.append(thu_hrs)
                schedule_parts.append(f"Thursday: {convert_to_12hr(thu_open)} - {convert_to_12hr(thu_close)}")
            else:
                thursday_hours.append("")
            
            # Friday
            fri_open = row.get('fri_start')
            fri_close = row.get('fri_close')
            if pd.notna(fri_open) and pd.notna(fri_close):
                fri_hrs = f"{fri_open} - {fri_close}"
                friday_hours.append(fri_hrs)
                schedule_parts.append(f"Friday: {convert_to_12hr(fri_open)} - {convert_to_12hr(fri_close)}")
            else:
                friday_hours.append("")
            
            # Saturday
            sat_open = row.get('sat_start')
            sat_close = row.get('sat_close')
            if pd.notna(sat_open) and pd.notna(sat_close):
                sat_hrs = f"{sat_open} - {sat_close}"
                saturday_hours.append(sat_hrs)
                schedule_parts.append(f"Saturday: {convert_to_12hr(sat_open)} - {convert_to_12hr(sat_close)}")
            else:
                saturday_hours.append("")
            
            # Sunday
            sun_open = row.get('sun_start')
            sun_close = row.get('sun_close')
            if pd.notna(sun_open) and pd.notna(sun_close):
                sun_hrs = f"{sun_open} - {sun_close}"
                sunday_hours.append(sun_hrs)
                schedule_parts.append(f"Sunday: {convert_to_12hr(sun_open)} - {convert_to_12hr(sun_close)}")
            else:
                sunday_hours.append("")
            
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
    
    # Services - use metadata to get field names dynamically
    service_lists = []
    for idx, row in preseason_df.iterrows():
        site_services = []
        for field_name, label in mappings['services_offered'].items():
            if field_name in preseason_df.columns:
                # Create clean field name
                clean_field = 'has_' + label.lower().replace(' ', '_').replace('-', '_')
                
                # Set flag
                if clean_field not in clean.columns:
                    clean[clean_field] = False
                clean.at[idx, clean_field] = bool(row.get(field_name, 0))
                
                # Add to list if checked
                if row.get(field_name, 0) == 1:
                    site_services.append(label)
        
        service_lists.append(', '.join(site_services) if site_services else '')
    
    clean['services_offered'] = service_lists
    
    # Closures - combine special + holidays
    holidays = calculate_holidays(2026)
    closure_dates = []
    
    for _, row in preseason_df.iterrows():
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
    
    # Status - use metadata mapping
    clean['review_status'] = preseason_df['review_status'].fillna(1).astype(int).map(mappings['review_status'])
    
    print(f"  → Status mapping from metadata: {mappings['review_status']}")
    for idx, row in clean.iterrows():
        original = preseason_df.loc[idx, 'review_status']
        mapped = row['review_status']
        print(f"     Site {row['record_id']}: {original} → {mapped}")
    
    # Metadata
    clean['last_updated'] = datetime.now().isoformat()
    clean['data_source'] = 'preseason'
    
    print(f"  ✓ Cleaned {len(clean)} sites")
    accepted_count = len(clean[clean['review_status'] == 'Accepted'])
    print(f"  ✓ {accepted_count} are Accepted")
    
    return clean


# ==================== STEP 4: GEOCODE ====================
def geocode_addresses(df):
    """Geocode only NEW Accepted sites"""
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
        
        # Reuse existing coordinates
        if record_id in previously_geocoded:
            df.at[idx, 'latitude'] = previously_geocoded[record_id]['lat']
            df.at[idx, 'longitude'] = previously_geocoded[record_id]['lon']
            df.at[idx, 'geocoded'] = True
            reused_count += 1
            continue
        
        # Only geocode Accepted sites
        if row['review_status'] != 'Accepted':
            skipped_count += 1
            continue
        
        # Geocode this NEW Accepted site
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
        # Fetch metadata first (so we never hardcode field mappings)
        metadata = fetch_metadata()
        mappings = build_mappings(metadata)
        
        # Run pipeline
        raw_data = fetch_from_redcap()
        preseason, updates = split_preseason_and_updates(raw_data)
        clean_data_df = clean_data(preseason, mappings)
        clean_data_df = geocode_addresses(clean_data_df)
        final_data = apply_updates(clean_data_df, updates)
        save_files(final_data)
        
        print("="*60)
        print("✓✓✓ COMPLETE ✓✓✓")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
