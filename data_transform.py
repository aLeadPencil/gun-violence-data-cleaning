from data_transform_functions import *

def data_transform():
    df = cleaned_data_reader()

    age_distribution_df = age_distribution_df_generator(df)
    gun_type_df = gun_type_df_generator(df)
    gun_count_df = gun_count_df_generator(df)
    suspect_gender_df = suspect_gender_df_generator(df)
    victim_gender_df = victim_gender_df_generator(df)

    age_distribution_df.to_csv('data/data_outputs/age_distribution_df.csv', header=True, index=False)
    gun_type_df.to_csv('data/data_outputs/gun_type_df.csv', header=True, index=False)
    gun_count_df.to_csv('data/data_outputs/gun_count_df.csv', header=True, index=False)
    suspect_gender_df.to_csv('data/data_outputs/suspect_gender_df.csv', header=True, index=False)
    victim_gender_df.to_csv('data/data_outputs/victim_gender_df.csv', header=True, index=False)

    return None

if __name__ == "__main__":
    data_transform()