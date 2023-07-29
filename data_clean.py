from data_cleaning_functions import original_data_reader, data_feature_engineering, final_column_cleaning, data_save

def data_clean():
    """
    Clean original data and save the results for later use
    """

    # Preliminary Data Cleaning
    data = original_data_reader()


    # Create features in data for future use
    data = data_feature_engineering(data)


    # Clean specific data columns
    data = final_column_cleaning(data)


    # Save cleaned data files
    data_save(data)

    return None


if __name__ == "__main__":
    data_clean()