import pandas as pd
import numpy as np

from data_cleaning_functions import gun_map, column_dtypes

# Functions to read data #
def string_to_list(row_value):
    '''
    Check if a value is a string representation of a list and return a list object instead of the string representation of it

    Parameters
    ----------
        row_value: row of a column

    Returns
    ----------
        row_value: null value or a list object
    '''

    if row_value is np.nan:
        return np.nan
    
    elif type(row_value) is str:
        row_value = eval(row_value)

    return row_value


def cleaned_data_reader():
    '''
    Read in data and convert necessary columns to correctly interpret lists

    Paramaters
    ----------
        None
    Returns
    ----------
        data: dataframe
    '''

    cleaned_data_1 = pd.read_csv('./data/cleaned_data/cleaned_data_1.csv', dtype = column_dtypes)
    cleaned_data_2 = pd.read_csv('./data/cleaned_data/cleaned_data_2.csv', dtype = column_dtypes)
    data = pd.concat([cleaned_data_1, cleaned_data_2], ignore_index = True)

    data['participant_age'] = data['participant_age'].apply(lambda x: string_to_list(x))
    data['participant_status'] = data['participant_status'].apply(lambda x: string_to_list(x))
    data['participant_type'] = data['participant_type'].apply(lambda x: string_to_list(x))
    data['gun_type'] = data['gun_type'].apply(lambda x: string_to_list(x))
    data['participant_gender'] = data['participant_gender'].apply(lambda x: string_to_list(x))

    return data



# Functions to transform data #
def age_distribution_df_generator(df):
    '''
    Create a dataframe to identify particpant/suspect/victim ages and their counts
    Iterate through the dataframe to append to a master list of ages and participant types

    Parameters
    ----------
        df: dataframe
    Returns
    ----------
        data: dataframe
    '''

    # Compile lists of ages for each participant type #
    ages = df['participant_age'].tolist()
    types = df['participant_type'].tolist()
    age_list = []
    
    for idx, _ in enumerate(ages):
        if isinstance(ages[idx], list):
            for idx2, _ in enumerate(ages[idx]):
                age_list.append(ages[idx][idx2])
           
    age_list = list(map(int, age_list))    
    unique_ages = list(sorted(set(age_list)))
    ages_count = [0] * len(unique_ages)
    
    for idx, _ in enumerate(age_list):
        for idx2, _ in enumerate(unique_ages):
            if unique_ages[idx2] == age_list[idx]:
                ages_count[idx2] += 1
                
    victim_age_list = []
    suspect_age_list = []

    for idx, _ in enumerate(ages):
        if ages[idx] is not np.nan:
            for idx2, _ in enumerate(ages[idx]):
                if 'Victim' in types[idx][idx2] and ages[idx][idx2] is not np.nan:
                    victim_age_list.append(ages[idx][idx2])

                elif 'Suspect' in types[idx][idx2] and ages[idx][idx2] is not np.nan:
                    suspect_age_list.append(ages[idx][idx2])

    victim_age_list = list(map(int, victim_age_list))
    victim_unique_ages = list(sorted(set(victim_age_list)))
    victim_ages_count = [0] * len(victim_unique_ages)

    suspect_age_list = list(map(int, suspect_age_list))
    suspect_unique_ages = list(sorted(set(suspect_age_list)))
    suspect_ages_count = [0] * len(suspect_unique_ages)
    
    for idx, _ in enumerate(victim_age_list):
        for idx2, _ in enumerate(victim_unique_ages):
            if victim_unique_ages[idx2] == victim_age_list[idx]:
                victim_ages_count[idx2] += 1

    for idx, _ in enumerate(suspect_age_list):
        for idx2, _ in enumerate(suspect_unique_ages):
            if suspect_unique_ages[idx2] == suspect_age_list[idx]:
                suspect_ages_count[idx2] += 1
    

    # Create dataframes for each participant type and merges them on their 'age' columns #
    all_ages_df = pd.DataFrame(
        {
            'Age': unique_ages,
            'Age_Counts': ages_count,
        }
    )

    victim_ages_df = pd.DataFrame(
        {
            'Victim_Age': victim_unique_ages,
            'Victim_Age_Counts': victim_ages_count
        }
    )

    suspect_ages_df = pd.DataFrame(
        {
            'Suspect_Age': suspect_unique_ages,
            'Suspect_Age_Counts': suspect_ages_count
        }
    )

    age_distribution_df = all_ages_df.merge(
        victim_ages_df,
        how = 'outer',
        left_on = 'Age',
        right_on = 'Victim_Age'
    )

    age_distribution_df = age_distribution_df.merge(
        suspect_ages_df,
        how = 'outer',
        left_on = 'Age',
        right_on = 'Suspect_Age'
    )

    age_distribution_df['Victim_Age'] = age_distribution_df['Victim_Age'].fillna(0)
    age_distribution_df['Suspect_Age'] = age_distribution_df['Suspect_Age'].fillna(0)

    age_distribution_df['Victim_Age'] = age_distribution_df['Victim_Age'].astype(int)
    age_distribution_df['Suspect_Age'] = age_distribution_df['Suspect_Age'].astype(int)

    return age_distribution_df


def gun_type_df_generator(data):
    '''
    Create a dataframe to identify gun types and their counts
    Iterate through the dataframe to append to a master list of gun types

    Parameters
    ----------
        df: dataframe
    Returns
    ----------
        data: dataframe
    '''
    
    gun_list = []
    gun_types = data['gun_type'].tolist()

    for idx, _ in enumerate(gun_types):
        if gun_types[idx] is not np.nan:
            for idx2, _ in enumerate(gun_types[idx]):
                if gun_types[idx][idx2] is not np.nan:
                    gun_list.append(gun_types[idx][idx2])
                    
    unique_guns = list(set(gun_list))
    gun_counts = [0] * len(unique_guns)

    for idx, _ in enumerate(gun_list):
        for idx2, _ in enumerate(unique_guns):
            if unique_guns[idx2] == gun_list[idx]:
                gun_counts[idx2] += 1

    temp_gun_type = pd.DataFrame(
        {
            'gun_list': gun_list
        }
    )

    temp_gun_type['gun_list'] = temp_gun_type['gun_list'].map(gun_map)
    gun_type_cleaned_labels = temp_gun_type['gun_list'].value_counts().index.tolist()
    gun_type_cleaned_counts = temp_gun_type['gun_list'].value_counts()
    
    gun_type_df = pd.DataFrame(
        gun_type_cleaned_counts
    ).reset_index().rename(columns = {'index': 'gun_type', 'gun_list': 'gun_type_counts'})
    
    return gun_type_df


def gun_count_df_generator(data):
    '''
    Create a dataframe to identify gun counts per incident and how many times each one occurs
    Iterate through the dataframe to append to a master list of gun counts

    Parameters
    ----------
        df: dataframe
    Returns
    ----------
        gun_count_df: dataframe
    '''

    data_n_guns_drop = data[['n_guns_involved']].dropna().reset_index(drop = True)
    data_n_guns_drop = data_n_guns_drop['n_guns_involved'].tolist()
    data_n_guns_drop = list(map(float, data_n_guns_drop))

    for idx, _ in enumerate(data_n_guns_drop):
        if data_n_guns_drop[idx] >= 5:
            data_n_guns_drop[idx] = '5+'


    n_guns = pd.DataFrame(
        {
            'n_guns_involved': data_n_guns_drop
        }
    )

    n_guns_labels = [1, 2, 3, 4, '5+']
    n_guns_counts = n_guns.value_counts()[n_guns_labels].tolist()
    
    gun_count_df = pd.DataFrame(
        {
            'num_of_guns': n_guns_labels,
            'counts': n_guns_counts
        }
    )
    
    gun_count_df['num_of_guns'] = gun_count_df['num_of_guns'].astype('str')
    
    return gun_count_df


def suspect_gender_df_generator(data):
    '''
    Create a dataframe to identify suspect genders and their counts

    Parameters
    ----------
        df: dataframe
    Returns
    ----------
        suspect_gender_df: dataframe
    '''
    genders = data['participant_gender'].tolist()
    types = data['participant_type'].tolist()
    suspect_gender_list = []

    for idx, _ in enumerate(genders):
        if isinstance(genders[idx], list):
            for idx2, _ in enumerate(genders[idx]):
                try:
                    if 'Suspect' in types[idx][idx2]:
                        suspect_gender_list.append(genders[idx][idx2])
                except:
                    pass
                    
    gender_labels = ['Male', 'Female']
    suspect_gender_counts = [suspect_gender_list.count('Male'), suspect_gender_list.count('Female')]
    
    suspect_gender_df = pd.DataFrame(
        {
            'gender': gender_labels,
            'gender_counts': suspect_gender_counts
        }
    )
    
    return suspect_gender_df


def victim_gender_df_generator(data):
    '''
    Create a dataframe to identify victim genders and their counts

    Parameters
    ----------
        df: dataframe
    Returns
    ----------
        victim_gender_df: dataframe
    '''

    genders = data['participant_gender'].tolist()
    types = data['participant_type'].tolist()
    victim_gender_list = []

    for idx, _ in enumerate(genders):
        if isinstance(genders[idx], list):
            for idx2, _ in enumerate(genders[idx]):
                try:
                    if 'Victim' in types[idx][idx2]:
                        victim_gender_list.append(genders[idx][idx2])
                except:
                    pass
                
    gender_labels = ['Male', 'Female']
    victim_gender_counts = [victim_gender_list.count('Male'), victim_gender_list.count('Female')]

    victim_gender_df = pd.DataFrame(
        {
            'gender': gender_labels,
            'gender_counts': victim_gender_counts
        }
    )
    
    return victim_gender_df