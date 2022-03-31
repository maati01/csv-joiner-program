import unittest
import join


class TestAddFishToAquarium(unittest.TestCase):
    def test_join(self):
        files = ["test1.csv", "test2.csv"]
        column_names = ["Username", "Identifier", "First name"]
        types = ["left", "right", "inner"]
        csv_reader_1 = join.read_input_file("test1.csv")
        csv_reader_2 = join.read_input_file("test2.csv")

        test = csv_reader_1.merge(csv_reader_2, on=column_names[0], suffixes=('', '__2'), how=types[2])
        test.columns = test.columns.str.replace('__2', '')
        test = test.loc[:, ~test.columns.duplicated(keep='first')]

        self.assertTrue(join.run([files[0], files[1], column_names[0], types[2]]).equals(test.reset_index(drop=True)))

        test = csv_reader_1.merge(csv_reader_2, on=column_names[2], suffixes=('', '__2'), how=types[0])
        test.columns = test.columns.str.replace('__2', '')
        test = test.loc[:, ~test.columns.duplicated(keep='first')]

        self.assertTrue(join.run([files[0], files[1], column_names[2], types[0]]).equals(test.reset_index(drop=True)))
