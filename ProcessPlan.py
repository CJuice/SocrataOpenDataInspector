"""
What does/should this do?
    COMPLETE:
    it inventories any socrata dataset whose api url it is given
    the inventory is of every field in the dataset
    null and empty values are counted for each and every field
    a csv report is generated for every dataset
    the report contains the dataset name, total # of records, the name of and null/empty count and percent for each field
    an overview summary report is generated
    the overview report contains the dataset name, link to dataset report csv, total # of records, total # null values
        that value as a percent of the total # of values in the dataset
    Handles non-uniform behavior of socrata and datasets. This requires custom handling of unique situations, built as discovered/encountered.

    PENDING:
    it only processes datasets that have been processed more recently than the last run
    it reads the last date processed information from the api metadata for the dataset

"""
# TODO: Handling datasets without a transmitted header/field list?? Confir with Pat.
# TODO: only process datasets processed since last run of this script (assuming this is regularly scheduled)
# TODO: compare the results of a round of evaluation against previous rounds to see change in the datasets

# IMPORTS
import urllib2
import re
import json
import os
# import types
from time import sleep
from datetime import date
import time

start = time.time() # TESTING

# VARIABLES (alphabetic)
DATA_FRESHNESS_REPORT_API_ID = ("t8k3-edvn",)
dataset_exceptions_startswith = ("Maryland Statewide Vehicle Crashes")
datasets_with_too_many_fields = set()
LIMIT_MAX_AND_OFFSET = (20000,)
OVERVIEW_STATS_FILE_NAME = ("_OVERVIEW_STATS",)
PROBLEM_DATASETS_FILE_NAME = ("_PROBLEM_DATASETS",)
ROOT_URL_FOR_CSV_OUTPUT = (r"E:\DoIT_OpenDataInspection_Project\TESTING_OUTPUT_CSVs",)
ROOT_URL_FOR_DATASET_ACCESS = (r"https://data.maryland.gov/resource/",)
TESTING_PERFORMANCE_SUMMARY_FILE_PATH = (r"E:\DoIT_OpenDataInspection_Project\TESTING_OUTPUT_CSVs\_script_performance_summary.txt",) # TESTING

# FUNCTIONS (alphabetic)
def build_csv_file_name_with_date(today_date_string, filename):
    return "{}_{}.csv".format(today_date_string, filename)

def build_dataset_url(url_root, api_id, limit_amount, offset, total_count):
    # if the record count exceeds the initial limit then the url must include offset parameter
    if total_count >= LIMIT_MAX_AND_OFFSET[0]:
        return "{}{}.json?$limit={}&$offset={}".format(url_root, api_id, limit_amount, offset)
    else:
        return "{}{}.json?$limit={}".format(url_root, api_id, limit_amount)

def build_datasets_inventory(dataset_url):
    datasets_dictionary = {}
    url = dataset_url
    req = urllib2.Request(url)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, "reason"):
            print("build_datasets_inventory(): Failed to reach a server. Reason: {}".format(e.reason))
        elif hasattr(e, "code"):
            print("build_datasets_inventory(): The server couldn't fulfill the request. Error Code: {}".format(e.code))
        exit()
    else:
        html = response.read()
        json_objects = json.loads(html)
        for record_obj in json_objects:
            dataset_name = record_obj["dataset_name"]
            api_id = record_obj["link"]
            datasets_dictionary[dataset_name] = os.path.basename(api_id)
    return datasets_dictionary

def build_today_date_string():
    return "{:%Y%m%d}".format(date.today())

def calculate_percent_null_for_dataset(null_count_total, total_records_processed, number_of_fields_in_dataset):
    if number_of_fields_in_dataset is None:
        return 0
    else:
        total_number_of_values_in_dataset = float(total_records_processed*number_of_fields_in_dataset)
        if total_number_of_values_in_dataset == 0:
            return 0
        else:
            return (float(null_count_total/total_number_of_values_in_dataset)*100)

def calculate_total_number_of_empty_values_per_dataset(null_counts_list):
    return sum(null_counts_list)

def handle_illegal_characters_in_string(string_with_illegals, spaces_allowed=False):
    if spaces_allowed:
        re_string = "[a-zA-Z0-9 ]"
    else:
        re_string = "[a-zA-Z0-9]"
    strings_list = re.findall(re_string,string_with_illegals)
    concatenated = ""
    for item in strings_list:
        if len(item) > 0:
            concatenated = concatenated + item
    return concatenated

def inspect_record_for_null_values(field_null_count_dict, record_dictionary):
    # In the response from a request to Socrata, only the fields with non-null/empty values appear to be included
    record_dictionary_fields = record_dictionary.keys()
    for field_name in field_null_count_dict.keys():
        if field_name in record_dictionary_fields:
            # If we rely on Socrata to filter out null values and not return a field if it is null then we don't
            #   need to check the data and can simply look at the included field names. The code in this "if" statement
            #   checked the data values for null but doesn't seem necessary given Socrata appears to
            #   prefilter null/empty data. Leaving the code until we are confident in this assumption.

            # data_value_of_focus = None
            # try:
            #     data_value_of_focus = record_dictionary[field_name]
            #
            #     # Handle dictionaries (location_1), ints, lists, that cause errors when encoding
            #     if isinstance(data_value_of_focus, types.StringType):
            #         data_value_of_focus = data_value_of_focus.encode("utf8")
            #     elif isinstance(data_value_of_focus, types.DictType):
            #         data_value_of_focus = "value is a dictionary"
            #     elif isinstance(data_value_of_focus, types.IntType):
            #         data_value_of_focus = str(data_value_of_focus)
            #     else:
            #         data_value_of_focus = data_value_of_focus.encode("utf8")
            # except UnicodeEncodeError as e:
            #     print(e)
            # except AttributeError as e:
            #     # print("AttributeError: key={}, key type={}\n\t{}".format(field_name, type(field_name),
            #     #                                                          record_dictionary))
            #     print(e)
            #
            # if data_value_of_focus == None or data_value_of_focus.strip() == "" or len(data_value_of_focus) == 0:
            #     field_null_count_dict[field_name] += 1
            # else:
            #     pass
            pass
        else:
            # It appears Socrata does not send empty fields so absence will be presumed to indicate empty/null values
            field_null_count_dict[field_name] += 1
    return

def write_dataset_results_to_csv(dataset_name, root_file_destination_location, filename, dataset_inspection_results, total_records):
    file_path = os.path.join(root_file_destination_location, filename)
    if os.path.exists(root_file_destination_location):
        with open(file_path, 'w') as file_handler:
            file_handler.write("{}\n".format(dataset_name))
            file_handler.write("Total Number of Records,{}\n".format(total_records))
            file_handler.write("Field Name, Null Count, Percent\n")
            for key, value in dataset_inspection_results.items():
                percent = 0
                if total_records > 0:
                    percent = (value / float(total_records))*100
                file_handler.write("{},{},{:6.2f}\n".format(key, value, percent))
    else:
        print("Directory DNE: {}".format(root_file_destination_location))
        exit()
    return

def write_overview_stats_to_csv(root_file_destination_location, filename, dataset_name, dataset_csv_file_path, total_number_of_dataset_records, total_number_of_null_fields=0, percent_null=0):
    file_path = os.path.join(root_file_destination_location, filename)
    if os.path.exists(root_file_destination_location):
        if not os.path.exists(file_path):
            with open(file_path, "w") as file_handler:
                file_handler.write("DATASET NAME,FILE PATH,TOTAL RECORD COUNT,TOTAL NULL VALUE COUNT,PERCENT NULL\n")
        if os.path.exists(file_path):
            with open(file_path, 'a') as file_handler:
                file_handler.write("{},{},{},{},{:6.2f}\n".format(dataset_name, dataset_csv_file_path, total_number_of_dataset_records, total_number_of_null_fields,percent_null))
    else:
        print("Directory DNE: {}".format(root_file_destination_location))
        exit()
    return

def write_problematic_datasets_to_csv(root_file_destination_location, filename, dataset_name, message, resource=None):
    file_path = os.path.join(root_file_destination_location, filename)
    if os.path.exists(root_file_destination_location):
        if not os.path.exists(file_path):
            with open(file_path, "w") as file_handler:
                file_handler.write("DATASET NAME,PROBLEM MESSAGE,RESOURCE\n")
        if os.path.exists(file_path):
            with open(file_path, 'a') as file_handler:
                file_handler.write("{},{},{}\n".format(dataset_name, message, resource))
    else:
        print("Directory DNE: {}".format(root_file_destination_location))
        exit()
    return

#TESTING
def write_script_performance_summary(start_time, number_of_datasets_in_data_freshness_report, dataset_counter, valid_nulls_dataset_counter, valid_no_null_dataset_counter, problem_dataset_counter):
    with open(TESTING_PERFORMANCE_SUMMARY_FILE_PATH[0],'w') as scriptperformancesummaryhandler:
        scriptperformancesummaryhandler.write("{}\n".format(build_today_date_string()))
        scriptperformancesummaryhandler.write("Number of datasets in freshness report: {}\n".format(number_of_datasets_in_data_freshness_report))
        scriptperformancesummaryhandler.write("Total datasets processed: {}\n".format(dataset_counter))
        scriptperformancesummaryhandler.write("Valid datasets with nulls (csv generated) count: {}\n".format(valid_nulls_dataset_counter))
        scriptperformancesummaryhandler.write("Valid datasets without nulls (no csv generated) count: {}\n".format(valid_no_null_dataset_counter))
        scriptperformancesummaryhandler.write("Problematic datasets count: {}\n".format(problem_dataset_counter))
        time_took = time.time() - start_time
        scriptperformancesummaryhandler.write("Took: {:4.2}m".format(time_took/60))

# FUNCTIONALITY
def main():

    # Initiate csv report files
    problem_datasets_csv_filename = build_csv_file_name_with_date(
        today_date_string=build_today_date_string(),
        filename=PROBLEM_DATASETS_FILE_NAME[0])
    overview_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                          filename=OVERVIEW_STATS_FILE_NAME[0])

    # Need an inventory of all Maryland Socrata datasets; will gather from the data freshness report.
    data_freshness_url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS[0],
                                           api_id=DATA_FRESHNESS_REPORT_API_ID[0],
                                           limit_amount=LIMIT_MAX_AND_OFFSET[0],
                                           offset=0,
                                           total_count=0)
    dict_of_socrata_dataset_IDs = build_datasets_inventory(dataset_url=data_freshness_url)
    number_of_datasets_in_data_freshness_report = len(dict_of_socrata_dataset_IDs)

    # Variables for next lower scope (alphabetic)
    dataset_counter = 0
    problem_dataset_counter = 0
    valid_nulls_dataset_counter = 0
    valid_no_null_dataset_counter = 0

    # Need to inventory field names of every dataset and tally null/empty values
    for dataset_name, dataset_api_id in dict_of_socrata_dataset_IDs.items():
        dataset_counter += 1

        # Handle occasional error when writing unicode to string using format. sometimes "-" was problematic
        dataset_name = dataset_name.encode("utf8")
        dataset_api_id = dataset_api_id.encode("utf8")
        print("{}: {} ............. {}".format(dataset_counter, dataset_name.upper(), dataset_api_id))

        dataset_name_with_spaces_but_no_illegal = handle_illegal_characters_in_string(
            string_with_illegals=dataset_name,
            spaces_allowed=True)

        # Variables for next lower scope (alphabetic)
        field_headers = None
        more_records_exist_than_response_limit_allows = True
        is_problem = False
        null_count_for_each_field_dict = {}
        number_of_columns_in_dataset = None
        offset = 0
        problem_message = None
        problem_resource = None
        socrata_url_response = None
        total_record_count = 0

        # Some datasets will have more records than are returned in a single response; varies with the limit_max value
        while more_records_exist_than_response_limit_allows:

            # Maryland Statewide Vehicle Crashes are excel files, not Socrata records, but they will return empty json objects endlessly
            if dataset_name.startswith("Maryland Statewide Vehicle Crashes"):
                problem_message = "Intentionally skipped. Dataset was an excel file as of 20180409. Call to Socrata endlessly returns empty json objects."
                is_problem = True
                break

            cycle_record_count = 0
            url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS[0],
                                    api_id=dataset_api_id,
                                    limit_amount=LIMIT_MAX_AND_OFFSET[0],
                                    offset=offset,
                                    total_count=total_record_count)
            print(url)

            req = urllib2.Request(url)

            try:
                socrata_url_response = urllib2.urlopen(req)
            except urllib2.URLError as e:
                problem_resource = url
                is_problem = True
                if hasattr(e, "reason"):
                    problem_message = "Failed to reach a server. Reason: {}".format(e.reason)
                    break
                elif hasattr(e, "code"):
                    problem_message = "The server couldn't fulfill the request. Error Code: {}".format(e.code)
                    break
            else:
                try:
                    # For datasets with a lot of fields it looks like Socrata doesn't return the
                    #   field headers in the response.info() so the X-SODA2-Fields key DNE.
                    dataset_fields_string = socrata_url_response.info()["X-SODA2-Fields"]
                except KeyError as e:
                    problem_message = "Too many fields. Socrata suppressed X-SODA2-FIELDS value in response."
                    problem_resource = url
                    is_problem = True
                    break
                field_headers = re.findall("[a-zA-Z0-9_]+", dataset_fields_string)

            # Need a dictionary of headers to store null count
            for header in field_headers:
                null_count_for_each_field_dict[header] = 0

            if number_of_columns_in_dataset == None:
                number_of_columns_in_dataset = len(field_headers)

            response_string = socrata_url_response.read()
            json_objects = json.loads(response_string)
            # Some datasets are html or other but socrata returns an empty object rather than a json object with
            #   reason or code. These datasets are then not recognized as problematic and throw off the tracking counts.
            if len(json_objects) == 0:
                problem_message = "Response json object was empty"
                problem_resource = url
                is_problem = True
                break

            for record_obj in json_objects:
                inspect_record_for_null_values(field_null_count_dict=null_count_for_each_field_dict,
                                               record_dictionary=record_obj)
                cycle_record_count += 1
                total_record_count += 1

            # Any cycle_record_count that equals the max limit indicates another request is needed
            if cycle_record_count == LIMIT_MAX_AND_OFFSET[0]:
                sleep(0.3)
                offset = cycle_record_count + offset
            else:
                more_records_exist_than_response_limit_allows = False

        # Output the results, to a stand alone csv for each dataset containing null values,
        #   to a csv of problematic datasets, and to the overview for all datasets.
        total_number_of_null_values = calculate_total_number_of_empty_values_per_dataset(
            null_count_for_each_field_dict.values())
        percent_of_dataset_are_null_values = calculate_percent_null_for_dataset(
            null_count_total=total_number_of_null_values,
            total_records_processed=total_record_count,
            number_of_fields_in_dataset=number_of_columns_in_dataset)
        if is_problem:
            problem_dataset_counter += 1
            write_problematic_datasets_to_csv(root_file_destination_location=ROOT_URL_FOR_CSV_OUTPUT[0],
                                              filename=problem_datasets_csv_filename,
                                              dataset_name=dataset_name_with_spaces_but_no_illegal,
                                              message=problem_message,
                                              resource=problem_resource)
        elif total_number_of_null_values > 0:
            valid_nulls_dataset_counter += 1

            # Write each datasets stats to its own csv
            dataset_name_no_spaces_no_illegal = handle_illegal_characters_in_string(string_with_illegals=dataset_name)
            dataset_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                                 filename=dataset_name_no_spaces_no_illegal)
            dataset_csv_file_path = os.path.join(ROOT_URL_FOR_CSV_OUTPUT[0],dataset_csv_filename)
            write_dataset_results_to_csv(dataset_name=dataset_name_with_spaces_but_no_illegal,
                                         root_file_destination_location=ROOT_URL_FOR_CSV_OUTPUT[0],
                                         filename=dataset_csv_filename,
                                         dataset_inspection_results=null_count_for_each_field_dict,
                                         total_records=total_record_count)

            # Append the overview stats for each dataset to the overview stats csv
            write_overview_stats_to_csv(root_file_destination_location=ROOT_URL_FOR_CSV_OUTPUT[0],
                                        filename=overview_csv_filename,
                                        dataset_name=dataset_name_with_spaces_but_no_illegal,
                                        dataset_csv_file_path=dataset_csv_file_path,
                                        total_number_of_dataset_records=total_record_count,
                                        total_number_of_null_fields=total_number_of_null_values,
                                        percent_null=percent_of_dataset_are_null_values)
        else:
            valid_no_null_dataset_counter += 1

            # Append the overview stats for each dataset to the overview stats csv
            write_overview_stats_to_csv(root_file_destination_location=ROOT_URL_FOR_CSV_OUTPUT[0],
                                        filename=overview_csv_filename,
                                        dataset_name=dataset_name_with_spaces_but_no_illegal,
                                        dataset_csv_file_path=None,
                                        total_number_of_dataset_records=total_record_count,
                                        total_number_of_null_fields=total_number_of_null_values,
                                        percent_null=percent_of_dataset_are_null_values)


    #TESTING
    write_script_performance_summary(start, number_of_datasets_in_data_freshness_report, dataset_counter, valid_nulls_dataset_counter, valid_no_null_dataset_counter, problem_dataset_counter)

if __name__ == "__main__":
    main()
else:
    print("Not __main__")
