"""
What does this do?
    it inventories any dataset whose api url it is given
    the inventory is of every field in the dataset
    null and empty values are counted for each and every field
    a csv report is generated for every dataset
    the report contains the date processed, # of fields, total # of records, a null/empty count and percent for each field
    it only processes datasets that have been processed more recently than the last run
    it reads this from the api metadata for the dataset

"""
# TODO: Add timing functionality
# TODO: Must handle large datasets that exceed the limit request
# TODO: Develop report writing functionality. Will be applied to every dataset.
# TODO: add multithreading/workers for speed

# IMPORTS
import urllib2
import re
import json
import os
import types
from time import sleep
from datetime import date

# VARIABLES
DATA_FRESHNESS_REPORT_API_ID = ("t8k3-edvn",)
root_url_for_dataset_access = r"https://data.maryland.gov/resource/"
ROOT_URL_FOR_CSV_OUTPUT = r"E:\DoIT_OpenDataInspection_Project\TESTING_OUTPUT_CSVs"
LIMIT_MAX_AND_OFFSET = (20000,)
dataset_exceptions_startswith = ("Maryland Statewide Vehicle Crashes")


# root_url_for_metadata_access = r"https://data.maryland.gov/api/views/metadata/v1"
# dict_of_Socrata_Dataset_IDs = {}

# FUNCTIONS
#TODO: should I have default values for offset and total_count ?
def build_dataset_url(url_root, api_id, limit_amount=1000, offset=0, total_count=0):
    # if the record count exceeds the initial limit then the url must include offset parameter
    if total_count >= LIMIT_MAX_AND_OFFSET[0]:
        return "{}{}.json?$limit={}&$offset={}".format(url_root, api_id, limit_amount, offset)
    else:
        return "{}{}.json?$limit={}".format(url_root, api_id, limit_amount)

def build_datasets_inventory(dataset_url):
    dict = {}
    url = dataset_url
    req = urllib2.Request(url)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, "reason"):
            print("Failed to reach a server. Reason: {}".format(e.reason))
        elif hasattr(e, "code"):
            print("The server couldn't fulfill the request. Error Code: {}".format(e.code))
    else:
        html = response.read()
        json_objects = json.loads(html)
        for record_obj in json_objects:
            dataset_name = record_obj["dataset_name"]
            api_id = record_obj["link"]
            dict[dataset_name] = os.path.basename(api_id)
    return dict

def inspect_record_for_null_values(field_null_count_dict, record_dictionary):
    record_keys = record_dictionary.keys()
    for null_counter_key in field_null_count_dict.keys():
        if null_counter_key in record_keys:
            value_of_focus = None
            try:
                value_of_focus = record_dictionary[null_counter_key]

                # Handle dictionaries (location_1), ints, lists
                if isinstance(value_of_focus, types.StringType):
                    value_of_focus = value_of_focus.encode("utf8")
                elif isinstance(value_of_focus, types.DictType):
                    value_of_focus = "value is a dictionary"
                elif isinstance(value_of_focus, types.IntType):
                    value_of_focus = str(value_of_focus)
                    # .encode("utf8")
                else:
                    value_of_focus = value_of_focus.encode("utf8")
            except UnicodeEncodeError as e:
                print(e)
            except AttributeError as e:
                print("AttributeError: key={}, key type={}\n\t{}".format(null_counter_key, type(null_counter_key),
                                                                         record_dictionary))
                print(e)
            # value_of_focus = str(value_of_focus)
            if value_of_focus == None or value_of_focus.strip() == "" or len(value_of_focus) == 0:
                field_null_count_dict[null_counter_key] += 1
            else:
                pass
        else:
            # It appears Socrata does not send empty fields so absence will be presumed to indicate empty/null values
            field_null_count_dict[null_counter_key] += 1
    return

def write_results_to_csv(root_file_destination_location, dataset_name, dataset_inspection_results, total_records):
    today_date_string = "{:%Y%m%d}".format(date.today())
    dataset_name = dataset_name.replace(" ", "_")
    file_name_string = "{}_{}".format(today_date_string, dataset_name)
    file_path = os.path.join(root_file_destination_location, file_name_string)
    if os.path.exists(root_file_destination_location):
        with open(file_path, 'w') as file_handler:
            file_handler.write("{}\n".format(dataset_name))
            file_handler.write("Total Number of Records,{}\n".format(total_records))
            for key, value in dataset_inspection_results.items():
                percent = 0
                if total_records > 0:
                    percent = value / total_records
                file_handler.write("{},{},{}\n".format(key, value, percent))
    else:
        print("Directory DNE: {}".format(root_file_destination_location))
        exit()
    return


# FUNCTIONALITY
def main():
    # Need an inventory of all Maryland Socrata datasets; will gather from the data freshness report.
    data_freshness_url = build_dataset_url(url_root=root_url_for_dataset_access,
                                           api_id=DATA_FRESHNESS_REPORT_API_ID[0])
    dict_of_Socrata_Dataset_IDs = build_datasets_inventory(dataset_url=data_freshness_url)

    # Need to inventory field names of every dataset and tally null/empty values
    for dataset_name, dataset_api_id in dict_of_Socrata_Dataset_IDs.items():
        # print("\n{}".format(dataset_name))
        print(dataset_name.upper())

        # Maryland Statewide Vehicle Crashes are excel files, not Socrata records
        if dataset_name.startswith("Maryland Statewide Vehicle Crashes"):
            print("\tINTENTIONALLY SKIPPED")
            continue

        field_headers = None
        null_count_for_each_field_dict = {}
        socrata_url_response = None
        total_count = 0
        more_records_exist_than_response_limit_allows = True
        offset = 0
        no_null_empty_flag = True

        # Some datasets will have more records than are returned in a single response; varies with the limit_max value
        while more_records_exist_than_response_limit_allows:
            cycle_count = 0

            # print("cycle start: {}".format(cycle_count))
            # print("Offset: {},  Total Count: {}".format(offset, total_count))
            url = build_dataset_url(url_root=root_url_for_dataset_access,
                                    api_id=dataset_api_id,
                                    limit_amount=LIMIT_MAX_AND_OFFSET[0],
                                    offset=offset,
                                    total_count=total_count)
            # print(url)
            req = urllib2.Request(url)

            try:
                socrata_url_response = urllib2.urlopen(req)
            except urllib2.URLError as e:
                no_null_empty_flag = False
                if hasattr(e, "reason"):
                    print("Failed to reach a server. Reason: {}".format(e.reason))
                    break
                elif hasattr(e, "code"):
                    print("The server couldn't fulfill the request. Error Code: {}".format(e.code))
                    break
            else:
                try:
                    # For datasets with a ton of fields it looks like Socrata doesn't return the
                    #   field headers in the response.info() so the X-SODA2-Fields key DNE.
                    dataset_fields_string = socrata_url_response.info()["X-SODA2-Fields"]
                    field_headers = re.findall("[a-zA-Z0-9_]+", dataset_fields_string)
                except KeyError as e:
                    print("\tToo many fields. Socrata suppressed X-SODA2-FIELDS value in response")
                    no_null_empty_flag = False
                    break

            if field_headers == None or len(field_headers) == 0:
                # Datasets like Real Property don't have fields in the response.info() so can't be processed this way
                print("\t\tNo Fields")
                no_null_empty_flag = False
                break
            else:
                # Need a dictionary of headers to store count
                for header in field_headers:
                    null_count_for_each_field_dict[header] = 0

            html = socrata_url_response.read()
            json_objects = json.loads(html)
            # print("totalA: {}".format(total_count))
            for record_obj in json_objects:
                inspect_record_for_null_values(field_null_count_dict=null_count_for_each_field_dict,
                                               record_dictionary=record_obj)
                cycle_count += 1
                total_count += 1
            # print("totalB: {}".format(total_count))
            # print("\tCycle Count: {},  MAX: {}".format(cycle_count, LIMIT_MAX_AND_OFFSET[0]))

            # Any cycle_count that equals the max limit indicates another request is needed
            if cycle_count == LIMIT_MAX_AND_OFFSET[0]:
                # print("\tCycle Count: {} was equal to MAX (total={})".format(cycle_count, total_count))
                sleep(0.3)
                # print("offset before: {}".format(offset))
                offset = cycle_count + offset
                # print("offset after: {}".format(offset))

            else:
                more_records_exist_than_response_limit_allows = False
        # write_results_to_csv(root_file_destination_location=ROOT_URL_FOR_CSV_OUTPUT,
        #                      dataset_name=dataset_name,
        #                      dataset_inspection_results=null_count_for_each_field_dict,
        #                      total_records=total_count)
        print("\tTotal records processed: {}".format(total_count))  # TESTING



        # Is the null counter working? Isolate non-zero counts and print        TESTING

        for value in null_count_for_each_field_dict.values():
            if value > 0:
                # print("\t{}".format(null_count_for_each_field_dict))
                no_null_empty_flag = False
                break
            else:
                pass
        if no_null_empty_flag:
            print("\tNo Null/Empty Values Detected")

        # Limit amount processed        TESTING
        # counter += 1
        # if counter > 10:
        #     exit()


if __name__ == "__main__":
    main()
else:
    print("Not __main__")
