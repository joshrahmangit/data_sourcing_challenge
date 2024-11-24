import requests
import json
import pandas as pd

# Define the base URL and parameters for the NASA API
base_url = "https://api.nasa.gov/DONKI/"
startDate = "2013-05-01"
endDate = "2024-05-01"
NASA_API_KEY = "ycFxg72rpUh6xbeexzPSG1nGu4fnvoWMPjWQgToF"

# Define parameters for GST data
specifier_gst = "GST"

# Construct the query URL for GST data
query_url_gst = f"{base_url}{specifier_gst}?startDate={startDate}&endDate={endDate}&api_key={NASA_API_KEY}"

# Fetch GST data
try:
    print("Attempting to fetch GST data...")
    gst_response = requests.get(query_url_gst)

    # Check if the response was successful
    if gst_response.status_code == 200:
        print("GST data retrieved successfully!")
        gst_json = gst_response.json()

        # Debug: Preview the first three entries in JSON format
        print("Previewing first three entries in GST JSON response:")
        print(json.dumps(gst_json[:3], indent=4))

        # Convert GST JSON to DataFrame
        print("Converting GST JSON to Pandas DataFrame...")
        gst_df = pd.DataFrame(gst_json)

        # Retain only the relevant columns
        print("Filtering required columns: 'activityID', 'startTime', 'linkedEvents'...")
        gst_df = gst_df[['activityID', 'startTime', 'linkedEvents']]

        # Preview the filtered DataFrame
        print("Filtered GST DataFrame preview:")
        print(gst_df.head())

        # Save the filtered DataFrame to a CSV file
        gst_df.to_csv("filtered_gst_data.csv", index=False)
        print("Filtered GST data saved to 'filtered_gst_data.csv'.")

    else:
        print(f"Failed to retrieve GST data. Status code: {gst_response.status_code}")
        print("Response content:", gst_response.text)
except Exception as e:
    print(f"An error occurred while fetching GST data: {e}")