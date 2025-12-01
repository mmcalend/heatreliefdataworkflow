import pandas as pd
import requests
import json
import os

# arcgis login
def login_to_arcgis():
    
    response = requests.post(
        'https://www.arcgis.com/sharing/rest/generateToken',
        data={
            'username': os.environ['ARCGIS_USERNAME'],
            'password': os.environ['ARCGIS_PASSWORD'],
            'referer': 'https://www.arcgis.com',
            'f': 'json'
        }
    )
    
    result = response.json()
    if 'token' in result:
        print("  ✓ Logged in")
        return result['token']
    else:
        raise Exception(f"Login failed: {result}")


# reformat
def csv_to_features(csv_path):
    """Convert CSV to ArcGIS feature format"""
    print("→ Converting data to ArcGIS format...")
    
    df = pd.read_csv(csv_path)
    
    features = []
    for _, row in df.iterrows():
        # Skip sites without coordinates
        if pd.isna(row['latitude']) or pd.isna(row['longitude']):
            continue
        
        # Create a feature (point on map with data)
        feature = {
            'geometry': {
                'x': float(row['longitude']),
                'y': float(row['latitude']),
                'spatialReference': {'wkid': 4326}  # Standard GPS coordinates
            },
            'attributes': {
                'site_id': int(row['site_id']),
                'site_name': str(row['site_name']),
                'organization': str(row.get('organization', '')),
                'address': str(row.get('address', '')),
                'city': str(row.get('city', '')),
                'state': str(row.get('state', '')),
                'zip': str(row.get('zip', '')),
                'hours': str(row.get('hours', '')),
                'services': str(row.get('services', '')),
                'email': str(row.get('email', '')),
                'status': str(row.get('status', '')),
                'last_updated': str(row.get('last_updated', ''))
            }
        }
        features.append(feature)
    
    print(f"  Converted {len(features)} sites")
    return features


# Update layer
def update_feature_layer(token, layer_url, features):
    """Replace all features in the ArcGIS layer with new data"""
    print("→ Updating ArcGIS feature layer...")
    
    # Step 1: Delete all existing features
    delete_response = requests.post(
        f"{layer_url}/deleteFeatures",
        data={
            'where': '1=1',  # Delete everything
            'f': 'json',
            'token': token
        }
    )
    print("  Cleared old data")
    
    # Step 2: Add new features
    add_response = requests.post(
        f"{layer_url}/addFeatures",
        data={
            'features': json.dumps(features),
            'f': 'json',
            'token': token
        }
    )
    
    result = add_response.json()
    if 'addResults' in result:
        success_count = sum(1 for r in result['addResults'] if r.get('success'))
        print(f"  ✓ Added {success_count} sites")
    else:
        print(f"  ⚠ Response: {result}")


# Run
def main():
    """Update ArcGIS Online with latest data"""
    print("\n" + "="*60)
    print("UPDATING ARCGIS ONLINE")
    print("="*60 + "\n")
    
    try:
        # Check if configured
        if not os.environ.get('ARCGIS_USERNAME'):
            print("⚠ ArcGIS not configured, skipping")
            print("  To enable: Add ARCGIS_USERNAME, ARCGIS_PASSWORD,")
            print("  and ARCGIS_LAYER_URL to GitHub secrets")
            return
        
        # Get credentials
        token = login_to_arcgis()
        layer_url = os.environ['ARCGIS_LAYER_URL']
        
        # Convert CSV to features
        features = csv_to_features('data/public/sites.csv')
        
        # Update layer
        update_feature_layer(token, layer_url, features)
        
        print("\n" + "="*60)
        print("✓ ARCGIS UPDATED")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        # Don't fail the whole pipeline if ArcGIS fails
        print("Continuing anyway...")


if __name__ == '__main__':
    main()
