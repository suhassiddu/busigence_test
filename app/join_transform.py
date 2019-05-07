import pandas as pd

def transform_sort(df, column_name, sorting_type_value):
    if sorting_type_value == False:
        return df.sort_values(column_name)
    else:
        return df.sort_values(column_name, ascending=False)

def transform_join(df_a, df_b, column_name_a, column_name_b, join_type):
    # valid = df_b.columns.difference(df_a.columns)
    return pd.merge(
           df_a,
           df_b,
           left_on = column_name_a,
           right_on = column_name_b,
           how = join_type)
