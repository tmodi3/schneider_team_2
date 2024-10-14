import pandas as pd

# first read csv file
load_posting = pd.read_csv('data/load_posting.csv')
load_stop = pd.read_csv('data/load_stop.csv')

# join them by LOAD_ID
merged_data = pd.merge(load_posting, load_stop, on='LOAD_ID')

# if we only query "NEW " in POSTING_STATUS
filtered_data = merged_data[merged_data['POSTING_STATUS'] == 'NEW']

# order them by LOAD_ID and STOP_SEQUENCE
sorted_data = filtered_data.sort_values(by=['LOAD_ID', 'STOP_SEQUENCE'])

print(sorted_data.head())


# filter 
def search_freight(origin=None, destination=None, date_range=None):
    # read data
    load_posting = pd.read_csv('data/load_posting.csv')
    load_stop = pd.read_csv('data/load_stop.csv')

    # merge
    merged_data = pd.merge(load_posting, load_stop, on='LOAD_ID')

    # filter criteria
    if origin:
        merged_data = merged_data[merged_data['CITY'] == origin]
    if destination:
        merged_data = merged_data[merged_data['CITY'] == destination]
    if date_range:
        start_date, end_date = date_range
        merged_data = merged_data[(merged_data['APPOINTMENT_FROM'] >= start_date) & (merged_data['APPOINTMENT_TO'] <= end_date)]

    return merged_data

# test
result = search_freight(origin='Atlanta')
print(result)
