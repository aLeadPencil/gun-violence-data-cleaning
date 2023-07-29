import pandas as pd
import numpy as np
import calendar

columns_to_drop = [
    'incident_id',
    'address',
    'incident_url',
    'source_url', 
    'incident_url_fields_missing',
    'participant_name', 
    'sources', 
    'congressional_district', 
    'state_house_district',
    'state_senate_district', 
    'notes', 
    'gun_stolen',
    'latitude',
    'longitude',
    'incident_characteristics',
    'location_description',
    'participant_relationship',
    'participant_age_group'
]


column_dtypes = {
    'date': object,
    'state': 'string',
    'city_or_county': 'category',
    'n_killed': 'int16',
    'n_injured': 'int16',
    'gun_type': object,
    'n_guns_involved': object,
    'participant_age': object,
    'participant_gender': object,
    'participant_status': object,
    'participant_type': object,
    'state_code': 'category',
    'weekday': 'category',
    'month': 'category',
    'year': 'int16'
}


us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}


gun_map = {
    'Handgun': 'Handgun',
    '25 Auto': 'Handgun',
    '45 Auto': 'Handgun',
    '44 Mag': 'Handgun',
    '357 Mag': 'Handgun',
    '9mm': 'Handgun',
    '32 Auto': 'Handgun',
    '38 Spl': 'Handgun',
    '40 SW': 'Handgun',
    '10mm': 'Handgun',
    '380 Auto': 'Handgun',
    '7.62 [AK-47]': 'Rifle',
    '22 LR': 'Rifle',
    '30-30 Win': 'Rifle',
    'Rifle': 'Rifle',
    '223 Rem [AR-15]': 'Rifle',
    '300 Win': 'Rifle',
    '308 Win': 'Rifle',
    '30-06 Spr': 'Rifle',
    'Shotgun': 'Shotgun',
    '28 gauge': 'Shotgun',
    '16 gauge': 'Shotgun',
    '20 gauge': 'Shotgun',
    '12 gauge': 'Shotgun',
    '410 gauge': 'Shotgun',
    'Other': 'Other'
}


weekday_map = {
    0: 'Mon',
    1: 'Tue',
    2: 'Wed',
    3: 'Thu',
    4: 'Fri',
    5: 'Sat',
    6: 'Sun'
}


def original_data_reader():
    """
    Read and combine all portions of the original data
    Filters years between 2014-2017
    """

    original_data_1 = pd.read_csv('data/original_data/original_data_1.csv', dtype = column_dtypes)
    original_data_2 = pd.read_csv('data/original_data/original_data_2.csv', dtype = column_dtypes)
    original_data_3 = pd.read_csv('data/original_data/original_data_3.csv', dtype = column_dtypes)
    original_data_4 = pd.read_csv('data/original_data/original_data_4.csv', dtype = column_dtypes)

    data = pd.concat([original_data_1, original_data_2, original_data_3, original_data_4], ignore_index = True)

    data = data.drop(columns_to_drop, 1)
    data = data[(data['date'] >= '2014-01-01') & (data['date'] < '2018-01-01')]
    data = data.reset_index(drop = True)

    return data


def data_feature_engineering(data):
    """
    Add features to the data for later use
    state_code, weekday, month, year
    """

    data['state_code'] = data['state'].map(us_state_abbrev)

    data['weekday'] = pd.to_datetime(data['date']).dt.weekday
    data['weekday'] = data['weekday'].map(weekday_map)

    month_dict = dict(enumerate(calendar.month_abbr))
    data['month'] = pd.to_datetime(data['date']).dt.month
    data['month'] = data['month'].map(month_dict)

    data['year'] = pd.to_datetime(data['date']).dt.year


    return data


def row_cleaner(row):
    """
    Clean a string value to split based on '::' and '||'
    Compiles and returns list of second elements for each split row entry

    Parameters:
    -----------
    row: str

    Returns
    -----------
    cleaned_row: list

    """
    
    if row is not np.nan:
        cleaned_row = []
        row = row.replace('||', '|')
        row = row.replace('::', ':')
        row = row.split('|')
        row = [i.split(':') for i in row]
    
        for idx, _ in enumerate(row):
            cleaned_row.append(row[idx][-1])
            
    else: 
        cleaned_row = np.nan
    
    return cleaned_row


def column_cleaner(column):
    """
    Apply row_cleaner function a provided column from a dataframe
    Compiles each cleaned row and returns a series for the cleaned column

    Parameters:
    -----------
    column: df series

    Returns:
    cleaned_column: df series
    """
    
    cleaned_column = []
    
    for idx, _ in enumerate(column):
        cleaned_row = row_cleaner(column[idx])
        cleaned_column.append(cleaned_row)
        
    cleaned_column = pd.Series(cleaned_column)
    
    return cleaned_column


def final_column_cleaning(data):
    """
    Apply df_cleaner() function to certain columns in the dataframe
    Returns a cleaned dataframe

    Parameters:
    -----------
    data: dataframe

    Returns
    -----------
    data: dataframe

    """

    data['participant_age'] = column_cleaner(data['participant_age'])
    data['participant_status'] = column_cleaner(data['participant_status'])
    data['participant_type'] = column_cleaner(data['participant_type'])
    data['gun_type'] = column_cleaner(data['gun_type'])
    data['participant_gender'] = column_cleaner(data['participant_gender'])
    
    return data

def data_save(data):
    """
    Save cleaned data into 2 separate csv files

    Parameters:
    -----------
    data: dataframe

    Returns
    -----------
    None
    """

    cleaned_data_1 = data[:112798]
    cleaned_data_2 = data[112798:]

    cleaned_data_1.to_csv('data/cleaned_data/cleaned_data_1.csv', index = False)
    cleaned_data_2.to_csv('data/cleaned_data/cleaned_data_2.csv', index = False)

    return None


def string_to_list(row_value):
    """
    Convert a string representation of a list to an actual list
    
    Parameters:
    -----------
    row: str

    Returns
    -----------
    cleaned_row: list
    """
    
    if row_value is np.nan:
        return np.nan

    else:
        try:
            return list(map(int, row_value.replace("'", '').strip('][]').split(', ')))
        except:
            return row_value.replace("'", '').strip('][]').split(', ')


if __name__ == '__main__':
    print('This is the data cleaning functions file')