import sys
import pandas as pd
import numpy as np

'''
This function prepares the resulting dataframe depending on the order of the arguments csv_reader_1 and csv_reader_2.
'''


def create_data_frame(csv_reader_1, csv_reader_2):
    result = csv_reader_1.copy()
    data_to_add = []
    for column in csv_reader_2.columns:
        if column not in result.columns:
            result[column] = np.nan
            data_to_add.append(column)

    return result, data_to_add


'''
The "join" function joins two dataframes.
'''


def join(csv_reader, result, data_to_add, column_name):
    idx = 0
    for column in data_to_add:
        for row in csv_reader[column]:
            for i in result.index[result[column_name] == csv_reader[column_name][idx]].tolist():
                result.at[i, column] = row
            idx += 1

    return result


'''
This function is used with inner join and looks for common rows for a given column.
'''


def common_rows(csv_reader, result, column_name):
    for row in result[column_name]:
        if row not in csv_reader.values:
            result = result[result[column_name] != row]
    for row in csv_reader[column_name]:
        if row not in result.values:
            result = result[result[column_name] != row]

    return result


def run(argv):
    csv_reader_1 = read_input_file(argv[0])
    csv_reader_2 = read_input_file(argv[1])

    input_handler(csv_reader_1, csv_reader_2, argv)

    if len(argv) == 4 and argv[3] == "inner":
        result, data_to_add = create_data_frame(csv_reader_1, csv_reader_2)
        result = common_rows(csv_reader_2, result, argv[2])
    elif len(argv) == 4 and argv[3] == "right":
        result, data_to_add = create_data_frame(csv_reader_2, csv_reader_1)
    else:
        result, data_to_add = create_data_frame(csv_reader_1, csv_reader_2)

    result = join(csv_reader_2, result, data_to_add, argv[2]).reset_index(drop=True)

    result.to_csv("result.csv")
    print(result)

    return result


def read_input_file(file):
    try:
        csv_reader = pd.read_csv(file)
        return csv_reader
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file} was not found")
    except PermissionError:
        raise PermissionError(f"No permission to access the file {file}")


def input_handler(csv_reader_1, csv_reader_2, argv):
    types = ["left", "right", "inner"]

    if len(argv) > 4 or len(argv) < 3:
        raise Exception("Number of arguments is wrong")
    if argv[2] not in csv_reader_1 or argv[2] not in csv_reader_2:
        raise Exception("Column does not exist")
    if len(argv) == 4 and argv[3] not in types:
        raise Exception("Wrong type")


if __name__ == "__main__":
    run(sys.argv[1:])
