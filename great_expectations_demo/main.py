import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult

import json
from typing import List

class ExpectationConfig:
    def __init__(self, path) -> None:
        with open(path, "r") as file:
            self.config = json.load(file)

    def get_expectations_list(self) -> List[ExpectationConfiguration]:
        expec_list = []
        for item in self.config:
            expec_list.append(ExpectationConfiguration(**{
                "expectation_type": item["name"],
                "kwargs": {
                    "column": item["column"],
                    **item["extra_args"]
                }
            }))
            
        return expec_list
        

class GreatExpectationsManager:
    def __init__(self, suite_name) -> None:
        self.context = gx.get_context()
        self.suite_name = suite_name

    def generate_suite(self, expectations_list) -> None:
        self.suite = self.context.create_expectation_suite(
            expectation_suite_name=self.suite_name,
            overwrite_existing=True
        )
        for expectation_conf in expectations_list:
            self.suite.add_expectation(expectation_configuration=expectation_conf)
        
        save_path = self.context.save_expectation_suite(expectation_suite=self.suite)
        save_path = save_path.replace("\\", "/")
        print("Suite created successfully as:",save_path)

    def generate_and_open_data_docs(self) -> None:
        suite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=self.suite_name)
        self.context.build_data_docs(resource_identifiers=[suite_identifier])
        self.context.open_data_docs(resource_identifier=suite_identifier)

    def validate(self, input_dataset_path) -> ExpectationSuiteValidationResult:
        suite = self.context.get_expectation_suite(self.suite_name)
        validator = self.context.sources.pandas_default.read_csv(input_dataset_path)
        validation_result = validator.validate(expectation_suite=suite)
        return validation_result

