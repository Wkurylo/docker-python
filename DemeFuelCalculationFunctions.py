import configparser
from sqlite3 import Timestamp
import requests
import urllib3
import datetime
import pandas as pd
import numpy as np
import time
from math import ceil
from dateutil import parser
from dateutil.tz import tzutc
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# Read config configfile.ini for Moxa // configfilelocalhost.ini for localhost
config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")
deme_pi_basic_auth = config_obj["Test"]
#Set the parameters for MQTT Broker
deme_username = deme_pi_basic_auth["username"]
deme_password = deme_pi_basic_auth["password"]

def _get_session(user= (deme_username, deme_password), headers= {}):

    """Create a requests session and return it

    Args:
        user (tuple of 2 strings, optional): username and password pair. Default is a generic user.

        headers (dict, optional): additional headers for session. Defaults to {}.

    Returns:
        requests.session: the initialised session
    """    
    req_headers = {'Content-Type': 'application/json','X-Requested-With': 'message/http'}
    req_headers.update(headers)
    session = requests.Session()
    session.auth = user
    session.headers.update(req_headers)
    return session

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def all_attributes(vessel, scope= 'database', mode = 1):

    """ Function to get all existing attributes for a given vessel (string variable)
        Arguments: vessel, string and compulsory, 
                   scope, a string: 'database' (default) looks in the database, 'all_af' looks in all AF's
                   mode = 1 is verbose, silent otherwise
        Returns a dataframe with the properties of all attributes, named in the column "TagName"
    """
    # # # # # # # #  --  Scope selection --  # # # # # # # # # # # # # # # 
    #     'scope' : "PI:PIDB.DEME.COM",                   # Database
    #     'scope' : "AF:\\\PIAF.DEME.COM\\OPERATIONS",    # The one to be used eventually in the future
    #     'scope' : "AF:\\\PIAF.DEME.COM\\PiAf",          # The most complete currently (October 2019)
    scope_dict = {'database': "PI:PIDB.DEME.COM", 'all_af': None}
    field_dict = {'database': "name;uom;WebId", 'all_af': 'name;attributes;WebId'}
    parameters = {
        'q' : vessel + '*',
        'fields' : field_dict[scope],
        'scope' : scope_dict[scope],
        'count' : 1000
    }
    data, status = pi_request(parameters= parameters, search_url= 'query')
    if mode == 1:
        print("Get status: ", status)
    if status != 200:
        print("Could not get attributes")
        return
    # 
    dataf = resolve_pages(data)
    if scope == 'all_af':
        dataAtrName = dataf.loc[dataf.Attributes.notnull()].drop(['Score'], axis=1)
        dataAtr = dataAtrName.reset_index(drop=True)
        All_Attributes = []
        for attribute in dataAtr['Attributes']:
            All_Attributes.extend(attribute)
        result = pd.DataFrame(All_Attributes)
        result.rename(columns={"Value": "TagName"}, inplace=True)
    elif scope == 'database':
        result = dataf.rename(columns={"Name": "TagName"})
    if mode == 1:
        print("Found a grand total of {} attributes".format(len(result)))
    return result

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def resolve_pages(data):
    dataf = pd.DataFrame(data['Items'])
    pages = 1
    if "TotalHits" in data:
        pages = ceil(data["TotalHits"] / 1000)
    # If there is more than 1 page, call up the data from the other ones and add the entries to the dataf
    for i in range(1, pages) :
        next = data["Links"]["Next"]
        data, status = pi_request(parameters= {}, search_url= next)
        if status != 200:
            print("Could not get attributes")
            return
        dataf = dataf.append(pd.DataFrame(data['Items']), ignore_index = True, sort=True)
    return dataf

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 


def pi_request(parameters, search_url):
    """ Function to make requests to the PI web API
        Arguments: parameters, a dictionary specifying the request, 
                   search_url, a string: 'data' to ask for a stream of interpolated data, 'query' to look for a tag
        Returns the response of the request
    """
    url_dict = {'data': "https://pivision.deme.com/piwebapi/streamsets/interpolated", 'query': "https://pivision.deme.com/piwebapi/search/query"}
    if search_url in url_dict:
        search_url = url_dict[search_url]
    # 
    session = _get_session()
    response = session.get(search_url, params=parameters, verify=False)
    return response.json(), response.status_code

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def get_multi_tag_webid(queries, scope= 'database'):
    """ Function to search for attribute names containing all strings given as substrings
        Arguments: queries, compulsory list of strings of keywords separated by spaces or dots
                   NOTE 1: first keyword in each query MUST be the vessel code! 
                   NOTE 2: CASE DEPENDENT!!
                   scope, string: 'database' (default) looks in the database, 'all_af' looks in all AF's
        example:   get_tag_webid(['H037 Pump SB', 'H037 Pump PS']) searches for names related to 
                   the pumps of the Congo River (still too many results in this example!)
        Returns:   dictionary {attribute : webID} which can be used in streamData
    """
    if not type(queries) == type([]):
        print('Please type in something to search for')
        return
    # 
    result = {}
    for query in queries:
        answer = get_tag_webid(query = query, scope= 'database', show_webids= 1)
        result[query] = answer[query]
    return result

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def get_tag_webid(query = '', scope= 'database', show_webids= 0):
    """ Function to search for attribute names containing all strings given as substrings
        Arguments: query, compulsory string of keywords separated by spaces or dots
                   NOTE 1: first keyword in query MUST be the vessel code! 
                   NOTE 2: CASE DEPENDENT!!
                   scope, string: 'database' (default) looks in the database, 'all_af' looks in all AF's
                   show_webids, numeric: 0 show no Web ID's (default to improve readability), 1 show Web ID's
        example:   get_tag_webid('H037 Pump SB') searches for names related to 
                   the SB pump of the Congo River (still too many results in this example!)
        Returns:   if no query given, it reverts to all_attributes and returns a pandas dataframe
                   if there are queries and show_webids = 0, it returns a list of attribute names
                   if there are queries and show_webids = 1, it returns a dictionary {attribute : webID} which can be used in streamData
    """
    if len(query)==0:
        print('Please type in something to search for')
        return 
    queries = split_str(query, (" ", "."))
    vessel_code = queries.pop(0)
    vessel_attributes = all_attributes(vessel_code, scope, mode = 0)
    # 
    if len(queries) == 0:
        if len(vessel_attributes)==0: 
            print('Searched for all attributes including {}, found nothing.'.format(vessel_code))
            return
        if show_webids:
            return dict(vessel_attributes[['TagName','WebId']].to_numpy())
        else:
            return (vessel_attributes[['TagName']].to_numpy()).flatten()
    else:
#         queries = split_str(queries, (" ", "."))
        if vessel_code in queries: queries.remove(vessel_code)
        mask = np.array([(vessel_attributes['TagName'].str.find(query) >= 0) for query in queries])
        if all( ['WebId' in vessel_attributes.keys(), show_webids] ):
            return dict(vessel_attributes[mask.all(axis=0)][['TagName','WebId']].to_numpy())
        else:
#             print(vessel_attributes[mask.all(axis=0)].keys())
            return (vessel_attributes[mask.all(axis=0)][['TagName']].to_numpy()).flatten()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def split_str(string, delimiters, maxsplit=0):
    """ Utility function to split a string into parts, 
        separated by a list of delimiters
    """
    import re
    regexPattern = '|'.join(map(re.escape, delimiters))
    return re.split(regexPattern, string, maxsplit)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def get_data_for(webids, start, end, spacing=0, total_points=0, max_query_points = 1.5e5):
    """ Wrapper function to get the data stream for webids
        Arguments: webids is a compulsory dictionary of webids {"columnname" : "query"}
                   start, end, strings specifying the times, compulsory
                   choose either
                             spacing, integer number of seconds bewteen each data point
                          or
                             total_points, integer number of total data points
                   max_query_points, integer, number of data points per query to PI, 150.000 is the hard upper limit
        Returns:   a pandas dataframe with the data stream(s)
    """
    if (spacing==0) and (total_points==0):
        print('No time spacing or the total number of points were specified')
        return
    if spacing and total_points:
        print('Either a time spacing or the total number of points can be specified, not both')
        return
    # Parse the start + end and compute total time in secs
    start_dt = resolve_datetime(start)
    end_dt = resolve_datetime(end)
    print(start_dt)
    print(end_dt)
    duration = int( (end_dt - start_dt).total_seconds() )
    # compute the spacing if not given
    if total_points:
        spacing = duration//(total_points-1)
    # specify the time length for each call
    maxtime = int(max_query_points * spacing)
    # maxtime = 130   # dummy used for testing!
    frames = []
    queries = ((duration-1)//maxtime + 1)
    do_batch = ((duration-1)/maxtime)*len(webids) < 0.85
    # 
    for i in range( 1, queries + 1 ):
        start_bin = start_dt + datetime.timedelta(0,(i-1)*maxtime)
        end_bin = min(start_bin + datetime.timedelta(0,maxtime - spacing), end_dt)
        print('fetching data between {} and {}'.format(start_bin.strftime("%d %b %Y %H:%M:%S.%f"), end_bin.strftime("%d %b %Y %H:%M:%S.%f"))  )
        frames.append(stream_data(webids, 
                                  start_bin.strftime("%d %b %Y %H:%M:%S.%f"), 
                                  end_bin.strftime("%d %b %Y %H:%M:%S.%f"), 
                                  str(spacing)+'s', do_batch = do_batch
                                  )
                      )
    return frames

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def stream_data(tags, start, end, interval, do_batch=False):
    """ Function to get data between start and end with certain interval for a dictionary of webids {"columnname" : "query"}
        Returns a data frame with specified column names
    """
    result = pd.DataFrame()
    if do_batch:
        temp = stream_tag(list(tags.values()), start, end, interval)    #Uses helper function
        temp.columns = list(tags.keys())
        result = result.join(temp, how="outer")
        return result
    # 
    for tag_key, tag_value in tags.items():
        temp = stream_tag([tag_value], start, end, interval)    #Uses helper function
        temp.columns = [tag_key]
        result = result.join(temp, how="outer")
    return result

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def stream_tag(tag, start, end, interval):
    """ Function to get data between start and end with certain interval for a webid
        Returns a single-column dataframe
        Version updated to exclude the 'Annotated' field in the response
    """
    result = pd.DataFrame({'Value': [] })
    timer = time.time()
    # Default maxPoints = 21600
    maxPoints = 20
    sortOrder = "Ascending"
    parameters = {"webid[]": tag,
                 "starttime": start,
                 "endtime": end,
                 "interval": interval,
                 "timezone": "UTC",
                 "maxCount" : maxPoints,
                 "sortOrder": sortOrder,
                 "selectedfields": "Items.Items.Timestamp;Items.Items.Value",
                 }
    timed_out = True
    timed_out_cnt = 0
    max_time_outs = 2
    # TODO check whether timing out code really improves performance (avoiding running into error)
    while timed_out & (timed_out_cnt < max_time_outs):
        data, status = pi_request(parameters= parameters, search_url= 'data')
        # result = pd.DataFrame(data['Items'][0]['Items'])
        list_data = [pd.DataFrame(datum['Items']).set_index('Timestamp').rename(columns={"Value": i}) for i, datum in enumerate(data['Items'])]
        result = pd.concat(list_data, axis=1)
        # 
        if any( [result.empty, status != 200] ):
            print("Warning: No data returned")
            return result
        # result.set_index("Timestamp", inplace=True)
        exec_time = time.time() - timer
        # 
        if len(result)==1:
            if result['Errors'].values[0][0]['Message'][0] == "[-10722] PINET: Timeout on PI RPC or System Call.":
                timed_out_cnt += 1
                if (timed_out_cnt < max_time_outs):
                    time.sleep(3)
                    print('Fetching request timed out, trying again.')
                else:
                    print("Warning: Stream Tag timed out, run stream_tag({}, {}, {}, {}) again to obtain missing data".format(tag, start, end, interval))
                    return result
        else:
            timed_out =False
    print("> Data query HTTP response: %d | Data size: %s | Execution Time: %d s" %(status, str(result.shape), exec_time))
    return result

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def pi_request_recorded(parameters, search_url):
    """ Function to make requests to the PI web API
        Arguments: parameters, a dictionary specifying the request, 
                   search_url, a string: 'data' to ask for a stream of recorded data, 'query' to look for a tag
        Returns the response of the request
    """
    url_dict = {'data': "https://pivision.deme.com/piwebapi/streamsets/recordedattimes", 'query': "https://pivision.deme.com/piwebapi/search/query"}
    if search_url in url_dict:
        search_url = url_dict[search_url]
    # 
    session = _get_session()
    response = session.get(search_url, params=parameters, verify=False)
    return response.json()
    #return response.json(), response.status_code

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def pi_request_interpolated_ts_values(parameters, search_url):
    """ Function to make requests to the PI web API
        Arguments: parameters, a dictionary specifying the request, 
                   search_url, a string: 'data' to ask for a stream of recorded data, 'query' to look for a tag
        Returns the response of the request
    """
    url_dict = {'data': "https://pivision.deme.com/piwebapi/streamsets/interpolated", 'query': "https://pivision.deme.com/piwebapi/search/query"}
    if search_url in url_dict:
        search_url = url_dict[search_url]
    # 
    session = _get_session()
    response = session.get(search_url, params=parameters, verify=False)
    return response.json()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def pi_write(webid, data):
    url = f"https://pivision.deme.com/piwebapi/streams/{webid}/recorded"
    # 
    session = _get_session()
    response = session.post(url=url, json=data, verify=False)
    return response.status_code

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def pi_clear_data(webid, value, timestamps):
    url = f"https://pivision.deme.com/piwebapi/streams/{webid}/recorded"
    # 
    session = _get_session()

    data = [ {'Timestamp': timestamp, 'Value': value } for timestamp in timestamps ]

    response = session.post(url=url, json=data, verify=False)
    return response.status_code

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def pi_request(parameters, search_url):
    """ Function to make requests to the PI web API
        Arguments: parameters, a dictionary specifying the request, 
                   search_url, a string: 'data' to ask for a stream of interpolated data, 'query' to look for a tag
        Returns the response of the request
    """
    url_dict = {'data': "https://pivision.deme.com/piwebapi/streamsets/interpolated", 'query': "https://pivision.deme.com/piwebapi/search/query"}
    if search_url in url_dict:
        search_url = url_dict[search_url]
    # 
    session = _get_session()
    response = session.get(search_url, params=parameters, verify=False)
    return response.json(), response.status_code

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def write_on_tag(webid=None, values=None, timestamps=None, start=None, end=None, spacing=None): 
    """Write a list of values to timestamps either passed explicitly or as start, end and spacing data.

    Args:
        webid (str, optional): webID of the tag to write to. Pass None for debugging -- writes nothing. Defaults to None.
        
        values (list, optional): list of values to write to tag. Defaults to None.
        
        timestamps (list of strings, optional): explicit timestamps where the values should be written -- must have same len as values, must be None if start/end/spacing are passed. Defaults to None.
        
        start (str, optional): starting time for inserting the values -- must be None if timestamps are passed. Defaults to None.
        
        end (str, optional): end time for inserting the values -- must be None if timestamps are passed. Defaults to None.
        
        spacing (int, optional): spacing between values in seconds (tested for integer) -- must be None if timestamps are passed. Defaults to None.

    Returns:
        [type]: [description]
    """    
    if timestamps is None:
        assert all(v is not None for v in [start, end, spacing]) , 'Must provide start, end and spacing if explicit timestamps are not passed!'
    else: 
        assert len(timestamps) == len(values) , 'Number of values must match number of timestamps!'
        
    if any(v is not None for v in [start, end, spacing]):
        assert timestamps is None , 'Cannot pass start, end or spacing if explicit timestamps are passed!'
        assert all(v is not None for v in [start, end, spacing]) , 'Must provide of start, end and spacing!'
        # 
        timestamps = _split_duration(start, end, spacing)
    # 
    assert len(timestamps) == len(values) , f'Number of values must match number of time steps including start and end! {len(values)} passed for {len(timestamps)} time points'
    # 
    data = [ {'Timestamp': timestamp, 'Value': value} for timestamp, value in zip(timestamps, values) ]
    # 
    status_code = None
    if isinstance(webid, str):
        status_code = pi_write(webid, data)
    return status_code

def _split_duration(start, end, spacing):
    # Parse the start + end and compute total time in secs
    start_dt = resolve_datetime(start)
    end_dt = resolve_datetime(end)
    duration = int( (end_dt - start_dt).total_seconds() )
    steps = int(duration/spacing)
    timestamps = [start_dt + datetime.timedelta(seconds=i*spacing) for i in range(steps) ] + [end_dt]
    return [timestamp.isoformat() for timestamp in timestamps]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def resolve_datetime(in_time=''):
    """Check if time is given as string or datetime object and return a datetime object
    
    Keyword Arguments:
        in_time {str} -- input, can be a datetime object (returned intact), or '', '*' to get current datetime (default: {''})
    
    Raises:
        Exception: input type not string or datetime
    
    Returns:
        datetime object -- if input is a datetime : the input, if input is a string date : its parsed vesion
         if input is '' or '*' : current datetime
    """
    if in_time == '*':
        in_time = ''
    if in_time:
        if isinstance(in_time, str):
            out_time = parser.parse(in_time)
        elif isinstance(in_time, datetime.datetime):
            out_time = in_time
        else:
            raise Exception('Expected string or datetime object as input, {} is neither'.format(in_time))
    else:
        out_time = datetime.datetime.now()
    
    # local_timezone_correction = datetime.timedelta(hours=-1)
    # belgium_out_time = out_time + local_timezone_correction
    return out_time



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # #     Unused functions
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 