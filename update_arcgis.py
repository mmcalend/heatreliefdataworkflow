"""Update ArcGIS Online Feature Layer"""
import pandas as pd
import requests
import json
import os

def get_token():
    """Login to ArcGIS Organization"""
    print("→ Logging into ArcGIS Online...")
    
    # For organizational accounts, use your org's portal URL
    # Format: https://YOURORG.maps.arcgis.com/sharing/rest/generateToken
    # If you're not sure, it's usually just www.arcgis.com for hosted orgs
    
    username = os.environ['ARCGIS_USERNAME']
    
    # Try organizational login first
    response = requests.post(
        'https://www.arcgis.com/sharing/rest/generateToken',
        data={
            'username': username,
            'password': os.environ['ARCGIS_PASSWORD'],
            'referer': 'https://www.arcgis.com',
            'f': 'json'
        }
    )
    
    result = response.json()
    
    # Check if we got a token
    if 'token' in result:
        print("  ✓ Logged in")
        return result['token']
    
    # If login failed, check if it's because of org account
    if 'error' in result:
        error_msg = result['error'].get('message', '')
        print(f"  ⚠ Login issue: {error_msg}")
        
        # If it mentions signing in through organization
        if 'organization' in error_msg.lower():
            print("  → Trying organization-specific login...")
            
            # You'll need to set ARCGIS_ORG_URL as a secret
            org_url = os.environ.get('ARCGIS_ORG_URL', 'https://www.arcgis.com')
            
            response = requests.post(
                f'{org_url}/sharing/rest/generateToken',
                data={
                    'username': username,
                    'password': os.environ['ARCGIS_PASSWORD'],
                    'referer': org_url,
                    'f': 'json'
                }
            )
            
            result = response.json()
            if 'token' in result:
                print("  ✓ Logged in via organization")
                return result['token']
    
    raise Exception(f"Login failed: {result}")


def csv_to_features():
    """Convert CSV to ArcGIS features"""
    print("→ Loading CSV...")
    
    df = pd.read_csv('data/public/sites.csv')
    
    # Only include geocoded sites
    df = df[df['geocoded'] == True]
    
    features = []
    for _, row in df.iterrows():
        feature = {
            'geometry': {
                'x': float(row['longitude']),
                'y': float(row['latitude']),
                'spatialReference': {'wkid': 4326}
            },
            'attributes': {
                'record_id': int(row['record_id']),
                'organization_name': str(row.get('organization_name', '')),
                'site_name': str(row['site_name']),
                'site_type': str(row.get('site_type', '')),
                'contact_email': str(row.get('contact_email', '')),
                'address': str(row.get('address', '')),
                'city': str(row.get('city', '')),
                'state': str(row.get('state', '')),
                'zip_code': int(row.get('zip_code', 0)) if pd.notna(row.get('zip_code')) else 0,
                'full_address': str(row.get('full_address', '')),
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'geocoded': str(row.get('geocoded', False)),
                'same_hours_everyday': str(row.get('same_hours_everyday', False)),
                'opening_time': str(row.get('opening_time', '')),
                'closing_time': str(row.get('closing_time', '')),
                'full_schedule': str(row.get('full_schedule', '')),
                'days_open': str(row.get('days_open', '')),
                'monday_hours': str(row.get('monday_hours', '')),
                'tuesday_hours': str(row.get('tuesday_hours', '')),
                'wednesday_hours': str(row.get('wednesday_hours', '')),
                'thursday_hours': str(row.get('thursday_hours', '')),
                'friday_hours': str(row.get('friday_hours', '')),
                'saturday_hours': str(row.get('saturday_hours', '')),
                'sunday_hours': str(row.get('sunday_hours', '')),
                'has_charging': str(row.get('has_charging', False)),
                'has_pet_services': str(row.get('has_pet_services', False)),
                'has_showers': str(row.get('has_showers', False)),
                'has_storage_for_belongings': str(row.get('has_storage_for_belongings', False)),
                'has_food': str(row.get('has_food', False)),
                'has_internet': str(row.get('has_internet', False)),
                'services_offered': str(row.get('services_offered', '')),
                'special_closure_dates': str(row.get('special_closure_dates', '')),
                'review_status': str(row.get('review_status', '')),
                'last_updated': str(row.get('last_updated', '')),
                'data_source': str(row.get('data_source', ''))
            }
        }
        features.append(feature)
    
    print(f"  ✓ Converted {len(features)} sites")
    return features


def update_layer(token, layer_url, features):
    """Replace all features in ArcGIS layer"""
    print("→ Updating ArcGIS layer...")
    
    # Delete all existing
    delete_response = requests.post(
        f"{layer_url}/deleteFeatures",
        data={
            'where': '1=1',
            'f': 'json',
            'token': token
        }
    )
    print("  ✓ Cleared old data")
    
    # Add new features (in batches if needed)
    batch_size = 1000
    total_added = 0
    
    for i in range(0, len(features), batch_size):
        batch = features[i:i+batch_size]
        
        add_response = requests.post(
            f"{layer_url}/addFeatures",
            data={
                'features': json.dumps(batch),
                'f': 'json',
                'token': token
            }
        )
        
        result = add_response.json()
        if 'addResults' in result:
            success_count = sum(1 for r in result['addResults'] if r.get('success'))
            total_added += success_count
            print(f"  ✓ Added batch: {success_count} sites")
        else:
            print(f"  ⚠ Error in batch: {result}")
    
    print(f"  ✓ Total added: {total_added} sites to ArcGIS")


def main():
    print("\n" + "="*60)
    print("UPDATING ARCGIS ONLINE")
    print("="*60 + "\n")
    
    try:
        # Check if configured
        if not os.environ.get('ARCGIS_USERNAME'):
            print("⚠ ArcGIS not configured - skipping")
            return
        
        token = get_token()
        layer_url = os.environ['ARCGIS_LAYER_URL']
        features = csv_to_features()
        update_layer(token, layer_url, features)
        
        print("\n" + "="*60)
        print("✓ ARCGIS UPDATED")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        # Don't fail the whole workflow
        print("\nContinuing anyway...")


if __name__ == '__main__':
    main()
