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
        'services_offered_update': get_checkbox_fields(metadata, 'services_offered_update'),
        'dow': get_checkbox_fields(metadata, 'dow'),
        'dow_update': get_checkbox_fields(metadata, 'dow_update')
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
            
            opening_12 = convert_to_12hr(opening)
            closing_12 = convert_to_12hr(closing)
            
            if opening_12 and closing_12:
                schedule = f"{', '.join(open_days)}: {opening_12} - {closing_12}"
                full_schedules.append(schedule)
                
                hours_str = f"{opening} - {closing}"
                for day in open_days:
                    if day == 'Monday':
                        monday_hours.append(hours_str)
                    elif day == 'Tuesday':
                        tuesday_hours.append(hours_str)
                    elif day == 'Wednesday':
                        wednesday_hours.append(hours_str)
                    elif day == 'Thursday':
                        thursday_hours.append(hours_str)
                    elif day == 'Friday':
                        friday_hours.append(hours_str)
                    elif day == 'Saturday':
                        saturday_hours.append(hours_str)
                    elif day == 'Sunday':
                        sunday_hours.append(hours_str)
                
                for day in day_names:
                    if day not in open_days:
                        if day == 'Monday':
                            monday_hours.append('')
                        elif day == 'Tuesday':
                            tuesday_hours.append('')
                        elif day == 'Wednesday':
                            wednesday_hours.append('')
                        elif day == 'Thursday':
                            thursday_hours.append('')
                        elif day == 'Friday':
                            friday_hours.append('')
                        elif day == 'Saturday':
                            saturday_hours.append('')
                        elif day == 'Sunday':
                            sunday_hours.append('')
            else:
                full_schedules.append('')
                opening_times.append('')
                closing_times.append('')
                monday_hours.append('')
                tuesday_hours.append('')
                wednesday_hours.append('')
                thursday_hours.append('')
                friday_hours.append('')
                saturday_hours.append('')
                sunday_hours.append('')
        else:
            # Different hours per day
            opening_times.append('')
            closing_times.append('')
            
            day_schedule_parts = []
            mon_open = str(row.get('mon_start', '')).strip()
            mon_close = str(row.get('mon_close', '')).strip()
            tues_open = str(row.get('tues_start', '')).strip()
            tues_close = str(row.get('tues_close', '')).strip()
            wed_open = str(row.get('wed_start', '')).strip()
            wed_close = str(row.get('wed_close', '')).strip()
            thurs_open = str(row.get('thurs_start', '')).strip()
            thurs_close = str(row.get('thurs_close', '')).strip()
            fri_open = str(row.get('fri_start', '')).strip()
            fri_close = str(row.get('fri_close', '')).strip()
            sat_open = str(row.get('sat_start', '')).strip()
            sat_close = str(row.get('sat_close', '')).strip()
            sun_open = str(row.get('sun_start', '')).strip()
            sun_close = str(row.get('sun_close', '')).strip()
            
            if 'Monday' in open_days and mon_open and mon_close:
                day_schedule_parts.append(f"Monday: {convert_to_12hr(mon_open)} - {convert_to_12hr(mon_close)}")
                monday_hours.append(f"{mon_open} - {mon_close}")
            else:
                monday_hours.append('')
            
            if 'Tuesday' in open_days and tues_open and tues_close:
                day_schedule_parts.append(f"Tuesday: {convert_to_12hr(tues_open)} - {convert_to_12hr(tues_close)}")
                tuesday_hours.append(f"{tues_open} - {tues_close}")
            else:
                tuesday_hours.append('')
            
            if 'Wednesday' in open_days and wed_open and wed_close:
                day_schedule_parts.append(f"Wednesday: {convert_to_12hr(wed_open)} - {convert_to_12hr(wed_close)}")
                wednesday_hours.append(f"{wed_open} - {wed_close}")
            else:
                wednesday_hours.append('')
            
            if 'Thursday' in open_days and thurs_open and thurs_close:
                day_schedule_parts.append(f"Thursday: {convert_to_12hr(thurs_open)} - {convert_to_12hr(thurs_close)}")
                thursday_hours.append(f"{thurs_open} - {thurs_close}")
            else:
                thursday_hours.append('')
            
            if 'Friday' in open_days and fri_open and fri_close:
                day_schedule_parts.append(f"Friday: {convert_to_12hr(fri_open)} - {convert_to_12hr(fri_close)}")
                friday_hours.append(f"{fri_open} - {fri_close}")
            else:
                friday_hours.append('')
            
            if 'Saturday' in open_days and sat_open and sat_close:
                day_schedule_parts.append(f"Saturday: {convert_to_12hr(sat_open)} - {convert_to_12hr(sat_close)}")
                saturday_hours.append(f"{sat_open} - {sat_close}")
            else:
                saturday_hours.append('')
            
            if 'Sunday' in open_days and sun_open and sun_close:
                day_schedule_parts.append(f"Sunday: {convert_to_12hr(sun_open)} - {convert_to_12hr(sun_close)}")
                sunday_hours.append(f"{sun_open} - {sun_close}")
            else:
                sunday_hours.append('')
            
            full_schedules.append('; '.join(day_schedule_parts))
    
    clean['same_hours_everyday'] = preseason_df['same_hours_everyday']
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
    
    # Services
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
        if row.get('independence_day') == 2 or row.get('july_4') == 2:
            all_closures.append(holidays['independence_day'])
        if row.get('labor_day') == 2:
            all_closures.append(holidays['labor_day'])
        
        closure_dates.append(', '.join(all_closures) if all_closures else '')
    
    clean['special_closure_dates'] = closure_dates
    
    # Status - use metadata mapping
    clean['review_status'] = preseason_df['review_status'].fillna(1).astype(int).map(mappings['review_status'])
    
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
def apply_updates(clean_df, updates_df, mappings):
    """Apply in-season updates - replacing preseason data completely"""
    
    if updates_df.empty:
        print("→ No updates to apply")
        return clean_df
    
    print(f"→ Applying {len(updates_df)} updates...")
    
    # Get latest update per record
    if 'update_date' in updates_df.columns:
        updates_df['update_date'] = pd.to_datetime(updates_df['update_date'])
    else:
        updates_df['update_date'] = datetime.now()
    
    latest_updates = updates_df.sort_values('update_date').groupby('record_id').last()
    
    update_count = 0
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    for record_id, update in latest_updates.iterrows():
        mask = clean_df['record_id'] == record_id
        if not mask.any():
            continue
        
        idx = clean_df[mask].index[0]
        site_updated = False
        
        # Update hours if provided
        same_hours_update = update.get('same_hours_everyday_update')
        if pd.notna(same_hours_update):
            clean_df.at[idx, 'same_hours_everyday'] = same_hours_update
            
            # Get open days from update
            open_days = []
            for i, day in enumerate(day_names, start=1):
                checkbox_value = update.get(f'dow_update___{i}')
                if checkbox_value == 1 or checkbox_value == '1':
                    open_days.append(day)
            
            clean_df.at[idx, 'days_open'] = ', '.join(open_days)
            
            if same_hours_update:
                # Same hours every day
                opening = str(update.get('standard_start_time_update', '')).strip()
                closing = str(update.get('standard_close_time_update', '')).strip()
                
                clean_df.at[idx, 'opening_time'] = opening
                clean_df.at[idx, 'closing_time'] = closing
                
                opening_12 = convert_to_12hr(opening)
                closing_12 = convert_to_12hr(closing)
                
                if opening_12 and closing_12:
                    schedule = f"{', '.join(open_days)}: {opening_12} - {closing_12}"
                    clean_df.at[idx, 'full_schedule'] = schedule
                    
                    hours_str = f"{opening} - {closing}"
                    clean_df.at[idx, 'monday_hours'] = hours_str if 'Monday' in open_days else ''
                    clean_df.at[idx, 'tuesday_hours'] = hours_str if 'Tuesday' in open_days else ''
                    clean_df.at[idx, 'wednesday_hours'] = hours_str if 'Wednesday' in open_days else ''
                    clean_df.at[idx, 'thursday_hours'] = hours_str if 'Thursday' in open_days else ''
                    clean_df.at[idx, 'friday_hours'] = hours_str if 'Friday' in open_days else ''
                    clean_df.at[idx, 'saturday_hours'] = hours_str if 'Saturday' in open_days else ''
                    clean_df.at[idx, 'sunday_hours'] = hours_str if 'Sunday' in open_days else ''
                    site_updated = True
            else:
                # Different hours per day
                clean_df.at[idx, 'opening_time'] = ''
                clean_df.at[idx, 'closing_time'] = ''
                
                day_schedule_parts = []
                
                mon_open = str(update.get('mon_start_update', '')).strip()
                mon_close = str(update.get('mon_close_update', '')).strip()
                if 'Monday' in open_days and mon_open and mon_close:
                    day_schedule_parts.append(f"Monday: {convert_to_12hr(mon_open)} - {convert_to_12hr(mon_close)}")
                    clean_df.at[idx, 'monday_hours'] = f"{mon_open} - {mon_close}"
                else:
                    clean_df.at[idx, 'monday_hours'] = ''
                
                tues_open = str(update.get('tues_start_update', '')).strip()
                tues_close = str(update.get('tues_close_update', '')).strip()
                if 'Tuesday' in open_days and tues_open and tues_close:
                    day_schedule_parts.append(f"Tuesday: {convert_to_12hr(tues_open)} - {convert_to_12hr(tues_close)}")
                    clean_df.at[idx, 'tuesday_hours'] = f"{tues_open} - {tues_close}"
                else:
                    clean_df.at[idx, 'tuesday_hours'] = ''
                
                wed_open = str(update.get('wed_start_update', '')).strip()
                wed_close = str(update.get('wed_close_update', '')).strip()
                if 'Wednesday' in open_days and wed_open and wed_close:
                    day_schedule_parts.append(f"Wednesday: {convert_to_12hr(wed_open)} - {convert_to_12hr(wed_close)}")
                    clean_df.at[idx, 'wednesday_hours'] = f"{wed_open} - {wed_close}"
                else:
                    clean_df.at[idx, 'wednesday_hours'] = ''
                
                thurs_open = str(update.get('thurs_start_update', '')).strip()
                thurs_close = str(update.get('thurs_close_update', '')).strip()
                if 'Thursday' in open_days and thurs_open and thurs_close:
                    day_schedule_parts.append(f"Thursday: {convert_to_12hr(thurs_open)} - {convert_to_12hr(thurs_close)}")
                    clean_df.at[idx, 'thursday_hours'] = f"{thurs_open} - {thurs_close}"
                else:
                    clean_df.at[idx, 'thursday_hours'] = ''
                
                fri_open = str(update.get('fri_start_update', '')).strip()
                fri_close = str(update.get('fri_close_update', '')).strip()
                if 'Friday' in open_days and fri_open and fri_close:
                    day_schedule_parts.append(f"Friday: {convert_to_12hr(fri_open)} - {convert_to_12hr(fri_close)}")
                    clean_df.at[idx, 'friday_hours'] = f"{fri_open} - {fri_close}"
                else:
                    clean_df.at[idx, 'friday_hours'] = ''
                
                sat_open = str(update.get('sat_start_update', '')).strip()
                sat_close = str(update.get('sat_close_update', '')).strip()
                if 'Saturday' in open_days and sat_open and sat_close:
                    day_schedule_parts.append(f"Saturday: {convert_to_12hr(sat_open)} - {convert_to_12hr(sat_close)}")
                    clean_df.at[idx, 'saturday_hours'] = f"{sat_open} - {sat_close}"
                else:
                    clean_df.at[idx, 'saturday_hours'] = ''
                
                sun_open = str(update.get('sun_start_update', '')).strip()
                sun_close = str(update.get('sun_close_update', '')).strip()
                if 'Sunday' in open_days and sun_open and sun_close:
                    day_schedule_parts.append(f"Sunday: {convert_to_12hr(sun_open)} - {convert_to_12hr(sun_close)}")
                    clean_df.at[idx, 'sunday_hours'] = f"{sun_open} - {sun_close}"
                else:
                    clean_df.at[idx, 'sunday_hours'] = ''
                
                clean_df.at[idx, 'full_schedule'] = '; '.join(day_schedule_parts)
                site_updated = True
        
        # Update services if provided
        site_services = []
        for field_name, label in mappings['services_offered_update'].items():
            if field_name in updates_df.columns:
                clean_field = 'has_' + label.lower().replace(' ', '_').replace('-', '_')
                
                if update.get(field_name, 0) == 1:
                    site_services.append(label)
                    clean_df.at[idx, clean_field] = True
                    site_updated = True
                else:
                    clean_df.at[idx, clean_field] = False
        
        if site_services:
            clean_df.at[idx, 'services_offered'] = ', '.join(site_services)
        
        # Update closures if provided
        if pd.notna(update.get('other_closures_updates')):
            holidays = calculate_holidays(2026)
            all_closures = []
            
            for i in range(1, 11):
                date_val = update.get(f'closure_{i}_update')
                if pd.notna(date_val):
                    all_closures.append(str(date_val))
            
            clean_df.at[idx, 'special_closure_dates'] = ', '.join(all_closures) if all_closures else ''
            site_updated = True
        
        if site_updated:
            clean_df.at[idx, 'last_updated'] = datetime.now().isoformat()
            clean_df.at[idx, 'data_source'] = 'in-season update'
            update_count += 1
            print(f"  ✓ Updated: {clean_df.at[idx, 'site_name']}")
    
    print(f"  ✓ Applied {update_count} updates")
    return clean_df


# ==================== STEP 6: FILTER ====================
def filter_accepted_only(df):
    """Keep only Accepted sites for public display"""
    print("→ Filtering to Accepted sites only...")
    
    before_count = len(df)
    df_filtered = df[df['review_status'] == 'Accepted'].copy()
    after_count = len(df_filtered)
    
    print(f"  ✓ Filtered from {before_count} to {after_count} sites")
    return df_filtered


# ==================== STEP 7: SAVE ====================
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
        final_data = apply_updates(clean_data_df, updates, mappings)
        final_data = filter_accepted_only(final_data)
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
