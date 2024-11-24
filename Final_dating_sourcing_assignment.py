import requests
import pandas as pd
import json

# Set up the API URL and parameters
base_url = "https://api.nasa.gov/DONKI/"
NASA_API_KEY = "ycFxg72rpUh6xbeexzPSG1nGu4fnvoWMPjWQgToF"

# Specify the date range for the data
startDate = "2013-05-01"
endDate = "2024-05-01"

# Part 1: Request CME data from the NASA API
specifier_cme = "CME"
query_url_cme = f"{base_url}{specifier_cme}?startDate={startDate}&endDate={endDate}&api_key={NASA_API_KEY}"

# Attempt to fetch CME data
try:
    print("Attempting to fetch CME data...")
    cme_response = requests.get(query_url_cme)
    if cme_response.status_code == 200:
        print("CME data retrieved successfully!")
        cme_json = cme_response.json()  # Store the JSON data
    else:
        print(f"Failed to retrieve CME data. Status code: {cme_response.status_code}")
except Exception as e:
    print(f"An error occurred while fetching CME data: {e}")

# Preview the first three entries in the CME JSON response
if cme_response.status_code == 200:
    print("Previewing first three entries in CME JSON response:")
    print(json.dumps(cme_json[:3], indent=4))

# Convert cme_json to a Pandas DataFrame and keep only relevant columns
cme_df = pd.DataFrame(cme_json)
cme_df = cme_df[['activityID', 'startTime', 'linkedEvents']]
cme_df = cme_df[cme_df['linkedEvents'].notnull()]

# Initialize an empty list to store the expanded rows
expanded_rows = []

# Iterate over each index in the CME DataFrame
for i in cme_df.index:
    # Extract values for 'activityID', 'startTime', and 'linkedEvents'
    activityID = cme_df.loc[i, 'activityID']
    startTime = cme_df.loc[i, 'startTime']
    linkedEvents = cme_df.loc[i, 'linkedEvents']
    
    # Inner loop: Iterate over each event in 'linkedEvents'
    for event in linkedEvents:
        expanded_rows.append({
            'cmeID': activityID,
            'startTime_CME': startTime,
            'GST_ActivityID': event.get('activityID', None)
        })

# Create a new DataFrame from the expanded rows
expanded_cme_df = pd.DataFrame(expanded_rows)

# Convert 'GST_ActivityID' column to string format
expanded_cme_df['GST_ActivityID'] = expanded_cme_df['GST_ActivityID'].astype(str)

# Convert 'startTime' to datetime format and rename to 'startTime_CME'
expanded_cme_df['startTime_CME'] = pd.to_datetime(expanded_cme_df['startTime_CME'])

# Rename 'activityID' to 'cmeID'
expanded_cme_df.rename(columns={'cmeID': 'activityID'}, inplace=True)

# Filter rows where 'GST_ActivityID' contains 'GST'
expanded_cme_df = expanded_cme_df[expanded_cme_df['GST_ActivityID'].str.contains('GST', na=False)]

# Part 2: Request GST data from the NASA API
specifier_gst = "GST"
query_url_gst = f"{base_url}{specifier_gst}?startDate={startDate}&endDate={endDate}&api_key={NASA_API_KEY}"

# Attempt to fetch GST data
try:
    print("Attempting to fetch GST data...")
    gst_response = requests.get(query_url_gst)
    if gst_response.status_code == 200:
        print("GST data retrieved successfully!")
        gst_json = gst_response.json()  # Store the JSON data
    else:
        print(f"Failed to retrieve GST data. Status code: {gst_response.status_code}")
except Exception as e:
    print(f"An error occurred while fetching GST data: {e}")

# Preview the first three entries in the GST JSON response
if gst_response.status_code == 200:
    print("Previewing first three entries in GST JSON response:")
    print(json.dumps(gst_json[:3], indent=4))

# Convert gst_json to a Pandas DataFrame and keep only relevant columns
gst_df = pd.DataFrame(gst_json)
gst_df = gst_df[['gstID', 'startTime', 'linkedEvents']]
gst_df = gst_df[gst_df['linkedEvents'].notnull()]

# Expand 'linkedEvents' and reset the index
gst_df['linkedEvents'] = gst_df['linkedEvents'].apply(lambda x: x if isinstance(x, list) else [])
gst_df = gst_df.explode('linkedEvents', ignore_index=True)

# Drop rows with missing values in 'linkedEvents' column
gst_df.dropna(subset=['linkedEvents'], inplace=True)

# Apply extract_activityID_from_dict to 'linkedEvents' column and create 'CME_ActivityID' column
gst_df['CME_ActivityID'] = gst_df['linkedEvents'].apply(lambda x: extract_activityID_from_dict(x))

# Convert 'CME_ActivityID' column to string format
gst_df['CME_ActivityID'] = gst_df['CME_ActivityID'].astype(str)

# Convert 'startTime' to datetime format and rename to 'startTime_GST'
gst_df['startTime_GST'] = pd.to_datetime(gst_df['startTime'])
gst_df.rename(columns={'startTime': 'startTime_GST'}, inplace=True)

# Filter rows where 'CME_ActivityID' contains 'CME'
gst_df = gst_df[gst_df['CME_ActivityID'].str.contains('CME', na=False)]

# Part 3: Merge CME and GST DataFrames
merged_df = pd.merge(
    expanded_cme_df,       # CME DataFrame
    gst_df,                # GST DataFrame
    left_on=['GST_ActivityID', 'activityID'],  # Columns from CME DataFrame
    right_on=['gstID', 'CME_ActivityID'], # Columns from GST DataFrame
    how='inner'            # Inner join ensures only matching rows are kept
)

# Verify the merge operation
print(f"Number of rows in CME DataFrame: {expanded_cme_df.shape[0]}")
print(f"Number of rows in GST DataFrame: {gst_df.shape[0]}")
print(f"Number of rows in merged DataFrame: {merged_df.shape[0]}")

# Calculate the time difference between 'startTime_GST' and 'startTime_CME'
merged_df['timeDiff'] = (merged_df['startTime_GST'] - merged_df['startTime_CME']).dt.total_seconds() / 3600.0  # Time difference in hours

# Use describe() to compute the mean and median of 'timeDiff'
time_diff_stats = merged_df['timeDiff'].describe()

# Extract and print the mean and median
mean_time_diff = time_diff_stats['mean']
median_time_diff = merged_df['timeDiff'].median()

print(f"Mean time difference: {mean_time_diff:.2f} hours")
print(f"Median time difference: {median_time_diff:.2f} hours")

# Export the merged DataFrame to a CSV file without the index
output_file = 'merged_data.csv'
merged_df.to_csv(output_file, index=False)

# Confirm the file has been saved
print(f"DataFrame successfully exported to {output_file}")
