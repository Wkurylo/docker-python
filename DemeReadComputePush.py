#####################################################################################################
#########                                                                                   #########
#########       * GET HTTP request : WebID/ time frame/ spacing                             #########
#########       * return Pandas DataFrame                                                   #########
#########       * Fuel consumption formula applied to Pandas DataFrame                      #########
#########       * Encode Pandas DataFrame values into List                                  #########
#########       * PUSH HTTP - calculated data to: WebId / time frame / spacing              #########
#########       * Data Object format "28 Jan 2022 08:20:10 GMT"                             #########
#########                                                                                   #########
#####################################################################################################

from asyncio.log import logger
import threading, time
from datetime import datetime, timedelta
import json
import DemeFuelCalculationFunctions as pi
import logging
from logging.handlers import RotatingFileHandler
import os
import time
import datetime

# #                  # #
#  Logger and Handler  #
# #                  # #
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not os.path.exists('logs'):
    os.mkdir('logs')
log_handler = RotatingFileHandler(
    filename='logs/app.log',
    maxBytes=10*1024*1024,
    backupCount=5
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

# #                                                             # #
#  Select Tag name interested in and get WebID of this Attribute  #
#          Returns: Tag_WebID in dict and in string               #
# #                                                             # #

# READ DATA
data_spacing_sec = 20
tag_to_read_data_from = ['H037.Dredge.HopperLoad.Displacement.Mass']
tag_webid_read_data_from =  pi.get_multi_tag_webid(tag_to_read_data_from)
webid_to_read_string = tag_webid_read_data_from["H037.Dredge.HopperLoad.Displacement.Mass"]
# OVERWRITE DATA
tag_to_overwrite = ['00TEST-PIwebAPI.POST.NUMBER.Value']
tag_webid_to_overwrite =  pi.get_multi_tag_webid(tag_to_overwrite)
webid_to_overwrite_string = tag_webid_to_overwrite["00TEST-PIwebAPI.POST.NUMBER.Value"]

logger.info("WebId to get data : {}, WebId to overwrite data : {}".format(webid_to_read_string, webid_to_overwrite_string))

def main_function():
    while True:
        data_query_start,data_query_end = get_last_recorded_value()
        # #                                                                                    # #
        #             Get the List of interpolated values Timestamp x value                      #
        #          Returns: List  Timestamp x  00TEST-PIwebAPI.POST.NUMBER.Value                 #
        # #                                                                                    # #
        data_received = get_interpolated_timestapms_values(data_query_start,data_query_end)
        logger.info('\r\nData Received:\r\n{}'.format(data_received))

        values_list,timestamps_list = data_computation(data_received)
        logger.info('\r\nSending Data:\r\nWebId:{}'.format(webid_to_overwrite_string))
        data_sent = pi.write_on_tag(webid = webid_to_overwrite_string, values = values_list, timestamps = timestamps_list, start= None, end= None, spacing = None)
        time.sleep(10)

# #                                             # #
#  Write test data to endpoint - testing purpose  #
#               To be delated                     #
# #                                             # # 
#data_sent = pi.write_on_tag(webid = webid_string, values = [777], timestamps = ["2 Feb 2022 09:20:33 GMT"], start= None, end= None,spacing = None)
#logger.info("Dummy data sent to : {}".format(webid_string))

# #                                        # #
#  Return the last recorded value Timestamp  #
#        SOURCE: webid_to_read_string        #
# #                                        # #
def get_last_recorded_value():
    parameters = {"webid[]": webid_to_read_string,
            "time": "*",
            "timezone": "UTC",
            "retrievalMode" : "Before",
            "selectedfields": "Items.Items.Timestamp;Items.Items.Value",
            }
    last_recorded_json = pi.pi_request_recorded(parameters= parameters, search_url= 'data')
    #data_query_start = last_recorded_json["Items"]
    last_recorded_str = json.dumps(last_recorded_json)
    last_recorded_dict = json.loads(last_recorded_str)
    last_timestamp_as_string = str(last_recorded_dict['Items'][0]['Items'][0]['Timestamp'])

    logger.info("Last recorded value Timestamp : {}".format(last_timestamp_as_string))
    # #                                                          # #
    #  Timestamp into DataTime Object and saved as data_query_end  #
    #          Returns: data_guery_end and data_query_start        #
    # #                                                          # #
    # Taking last recorded data Timestamp and changing into Datatime object
    # Last recorded Timestamp is going to be used as "data_query_end" 
    last_timestamp_as_datetime_object = datetime.datetime.strptime(last_timestamp_as_string[:-2], "%Y-%m-%dT%H:%M:%S.%f")
    data_query_end = last_timestamp_as_datetime_object.strftime("%d %b %Y %H:%M:%S GMT")
    #Query time specification
    start_search_timestamp = last_timestamp_as_datetime_object - timedelta(hours=5, minutes=00)
    data_query_start = start_search_timestamp.strftime("%d %b %Y %H:%M:%S GMT")

    logger.info('\r\nStart: {}\r\nEnd: {}\r\nSpacing[sec]: {}'.format(data_query_start, data_query_end, data_spacing_sec))

    return(data_query_start,data_query_end)

# #                                        # #
#  Return interpolated values within Time    #
# #                                        # #
def get_interpolated_timestapms_values(data_query_start, data_query_end):

    start_dt = pi.resolve_datetime(data_query_start)
    end_dt = pi.resolve_datetime(data_query_end)
    duration = int( (end_dt - start_dt).total_seconds() )
    max_query_points = 1.5e5
    maxtime = int(max_query_points * data_spacing_sec)
    queries = ((duration-1)//maxtime + 1)
    list = []
    for i in range( 1, queries + 1 ):
        list.append(get_interpolated_timestamps_and_value(
            webid_to_read_string,
            start_dt, 
            end_dt,
            str(data_spacing_sec)+'s'
            )
        )
    return list

def get_interpolated_timestamps_and_value(tags, start, end, interval):

    parameters = {"webid[]": tags,
                 "starttime": start,
                 "endtime": end,
                 "interval": interval,
                 "timezone": "UTC",
                 "selectedfields": "Items.Items.Timestamp;Items.Items.Value",
                 }
    data = pi.pi_request_interpolated_ts_values(parameters= parameters, search_url= 'data')
    return(data)

# #                                         # #
#       Stefanos formula                      #
#     Push Data to Web_ID (List of values)    #
# #                                         # #
def data_computation(data_received):
    logger.info('Stefanos formula')
    timestamps_list = []
    for i in range(len(data_received)):
        for entry in data_received[0]['Items'][0]['Items']:
            timestamps_list.append(entry['Timestamp'])
    values_list =[]
    for i in range(len(data_received)):
        for entry in data_received[0]['Items'][0]['Items']:
            values_list.append(entry['Value']- 5000)
    #calculated = [{'Timestamp': entry['Timestamp'], 'Value': entry['Value'] + 500} for entry in data_received[0]['Items'][0]['Items']]
    return(values_list,timestamps_list)


if __name__ == '__main__':
    while True:
        thread = threading.Thread(target=main_function, daemon=True)
        thread.start()  
        time.sleep(10)




# #                                                     # #
#     Clean data of selected Web-ID within timestamp      #
# #                                                     # #
#https://pivision.deme.com/piwebapi/help/controllers/stream/actions/updatevaluesdef 

# Get all data (without spacing) - RECORDED ! and set value as 0. Than clean
# TODO

# def clean_data():
#     overwrite_value = 0
#     status_code = pi.pi_clear_data(webid_to_overwrite_string, overwrite_value, timestamps_list)
#     return status_code
#status_code = clean_data()
#print(status_code)



