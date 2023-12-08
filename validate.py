from main import GreatExpectationsManager
suite_name = "my_suite_v1"
input_data_path = "data.csv"

ge_manager = GreatExpectationsManager(suite_name)
validation_result = ge_manager.validate(input_data_path)
print(validation_result)