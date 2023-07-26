import logging

class Validation:
    def validate_csv_data(csvfilepath):
        csv_line_count= Validation.csv_line_count(csvfilepath)
        assert csv_line_count != 0, "No data is populated in csv"

    def csv_line_count(csvfilepath):
        return sum(1 for line in open(csvfilepath))

    def validate_true(value,error_msg):
        assert value == True, error_msg
