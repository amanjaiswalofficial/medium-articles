"""Pre-requisite: great_expectations init"""

from main import ExpectationConfig, GreatExpectationsManager

config_object = ExpectationConfig("./expectations.json")
expectations_list = config_object.get_expectations_list()

ge_manager = GreatExpectationsManager('my_suite_v1')
ge_manager.generate_suite(expectations_list)
ge_manager.generate_and_open_data_docs()