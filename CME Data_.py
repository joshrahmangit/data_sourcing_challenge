import requests
import json

# Define the base URL and parameters for the NASA API
base_url = "https://api.nasa.gov/DONKI/"
specifier = "CME"
startDate = "2013-05-01"
endDate = "2024-05-01"

# Use your NASA API key
NASA_API_KEY = "ycFxg72rpUh6xbeexzPSG1nGu4fnvoWMPjWQgToF"

# Construct the query URL
query_url_cme = f"{base_url}{specifier}?startDate={startDate}&endDate={endDate}&api_key={NASA_API_KEY}"

# Debug: Print the constructed URL
print("Constructed URL:", query_url_cme)

# Fetch the CME data
try:
    print("Attempting to fetch CME data...")
    cme_response = requests.get(query_url_cme)

    # Check if the response was successful
    if cme_response.status_code == 200:
        print("CME data retrieved successfully!")
        cme_json = cme_response.json()
        # Debug: Print the number of entries retrieved
        print(f"Number of CME entries retrieved: {len(cme_json)}")
        # Preview the first three results
        print("Previewing first three entries:")
        print(json.dumps(cme_json[:3], indent=4))
    else:
        print(f"Failed to retrieve CME data. Status code: {cme_response.status_code}")
except Exception as e:
    print(f"An error occurred: {e}")