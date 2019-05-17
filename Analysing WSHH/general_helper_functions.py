import io


def write_df_infos_to_file(input_dataframe, file_directory):
    '''
    Writes infos like output from .info(), all dataframe columns and describe() to a
    text file.
    :param input_dataframe: A  pandas dataframe that infos should be retrieved from
    :param file_directory: The name of the file that should be generated
    :return:
    '''
    buffer = io.StringIO()
    input_dataframe.info(buf=buffer)
    s = buffer.getvalue()
    list_of_columns = list(input_dataframe.columns.values)
    s2 = input_dataframe.describe().to_string()

    with open(file_directory, "w", encoding="utf-8") as f:  # doctest: +SKIP
        f.write("Info Function:\n\n")
        f.write(s)
        f.write("\n\n" + "#" * 100)
        f.write("\nColumns:\n")
        line_iterator = 0
        for column in list_of_columns:
            if line_iterator % 4 == 0:
                f.write("\n")
            f.write(column + ", ")
            line_iterator += 1
        f.write("\n\n" + "#" * 100)
        f.write("\nDescribe Function:\n\n")
        f.write(s2)
        f.write("\n\nEnd of File")


def create_small_subframe(df, number_of_rows=3000):
    '''
    Creates a smaller sample dataframe for testing purposes
    and returns it
    :param df: The dataframe to create a subframe from
    :return:
    '''
    df_small = df.iloc[:number_of_rows].copy()
    df_small.to_csv("data_small.csv")
    return df_small


def seperate_a_target_column(dataframe, name_of_column):
    '''
    Seperates a column from a dataframe and makes a new dataframe from it
    :param dataframe: Pandas Dataframe
    :param name_of_column: What column should be seperated
    :return:
    '''
    features = dataframe.copy().drop([name_of_column], axis=1)
    seperated = dataframe[[name_of_column]].copy()
    return features, seperated
