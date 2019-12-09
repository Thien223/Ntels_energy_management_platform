import argparse
from utils.hyperparams import hparams as config
from utils.utils import *

# y = sub_data
# plt.plot(sub_data) 2.8
def detect_peak_values(y, lag=None, threshold=None, influence=0.0):
    #### detect peak values
    ##
    ### lag: number of  base data points. the next datapoint will be compared to this base datapoints to detect peak
    if lag is None:
        lag = int(len(y) * 0.15) ### 15% data points of total will be use as base data points
    if threshold is None:
        threshold=5 ### when the |current data point - mean| exceeds threshold * standard_deviation, then it will be classify as peak
    signals = np.zeros(len(y))
    filteredY = np.array(y)
    avgFilter = [0]*len(y)
    stdFilter = [0]*len(y)
    avgFilter[lag - 1] = np.mean(y[0:lag])
    stdFilter[lag - 1] = np.std(y[0:lag])
    for i in range(lag, len(y)):
        ### determine peak
        if abs(y[i] - avgFilter[i-1]) > threshold * stdFilter [i-1]: ## <<--- peak
            if y[i] > avgFilter[i-1]:
                signals[i] = 1 ## if y above threshold
            else:
                signals[i] = -1 ## if y under threshold
            ## update moving average
            filteredY[i] = influence * y[i] + (1 - influence) * filteredY[i-1]
            avgFilter[i] = np.mean(filteredY[(i-lag+1):i+1])
            stdFilter[i] = np.std(filteredY[(i-lag+1):i+1])
        else:                                                       ##<<--- not peak
            signals[i] = 0
            ### update moving average
            filteredY[i] = y[i]
            avgFilter[i] = np.mean(filteredY[(i-lag+1):i+1])
            stdFilter[i] = np.std(filteredY[(i-lag+1):i+1])
    # for i,s in zip(y, signals):
    #     if s==1:
    #         print(i)
    return np.asarray(signals), np.asarray(avgFilter), np.asarray(stdFilter)
    # return peak_ids, mean, std


def statistical_analysis(data, peak_indexes, energy_col_idx=0):
    info = {}
    time_index = data.index
    info['start_time'] = str(time_index[0])
    info['end_time'] = str(time_index[-1])
    ## energy data (default index is 0)
    y = data[data.columns[energy_col_idx]]
    ### store peak value and time index into info dictionary
    #### remember that this peak is calculated by comparing current value with [lag_number] of previous datapoints <~~~~~ it can be not highest.
    for energy_value,peak_index,idx in zip(y, peak_indexes, time_index ):
        if peak_index==1:
            info['peak_{}'.format(idx)] = energy_value
    info['average'] = np.mean(y)
    info['standard_deviation'] = np.std(y)
    for idx, value in enumerate(y):
        if value == np.min(y):
            info['lowest'] = np.min(y)
            info['lowest_time'] = str(time_index[idx])
        if value == np.max(y):
            info['highest'] = np.max(y)
            info['highest_time'] = str(time_index[idx])
    return info





### export analyzed info to json format. (with same start and end time)
## if the start and end time is different from other data file, run this separately
def analyzed_filename_to_json(data_filenames, start_time=None, end_time=None,freq = 'H', resample_interval='H', formula='sum', lag = 48, threshold=2.5, influence=0.5):
    ####### analysis all building
    '''
        export analyzed data in data filenames info to json
        :param data_lists: energy dataframe
        :param building_list: buildings corressponding to dataframe_list
        :param lag: number of base data point (will be used to determine next point is peak or not)
        :param threshold: standard deviation multiply (data point value - mean > [threshold * std] --> peak).
        :param influence: stationary param, if data is highly in incresing trend --> 1, else choose between [0,1]
        :return: analyzed dictionary
        '''
    all_building_info = {}
    for data_file in data_filenames:
        print(data_file)
        link = data_file
        data, building = loadData(path=link, idx_col=0,freq='H',  formula='sum')
        sub_data = get_data_by_time(data, start_time, end_time)
        # Settings: peak value detecting params
        # Run algo with settings from above
        if sub_data is not None:
            peak_indexes, _, _ = detect_peak_values(y=sub_data[sub_data.columns[0]], lag=lag, threshold=threshold, influence = influence)
            ### store analyzed data to dictionary
            statistical_analysis_info = statistical_analysis(sub_data, peak_indexes)
            ### prepare filename and export
            filename = building + '_' + str(sub_data.index[0].date()) + '-' + str(sub_data.index[-1].date())
            filename = filename + '.json'
            dict_to_json(folder='analyzed', filename=filename,
                                     dict=statistical_analysis_info)
            all_building_info[building] = statistical_analysis_info
    print('Exported results to analyzed folder..')
    return all_building_info


### export analyzed info to json format.
## if the start and end time is different from other data file, run this separately
def analyzed_data_to_json(data_lists, building_list, lag = 48, threshold=2.5, influence=0.5):
    '''
    export analyzed data info to json
    :param data_lists: energy dataframe
    :param building_list: buildings corressponding to dataframe_list
    :param lag: number of base data point (will be used to determine next point is peak or not)
    :param threshold: standard deviation multiply (data point value > [threshold * std] --> peak).
    :param influence: stationary param, if data is highly in incresing trend --> 1, else choose between [0,1]
    :return: analyzed dictionary
    '''
    ####### analysis all building
    all_building_info = {}
    for data, building in zip(data_lists, building_list):
        if data is not None:
            energy_col=0 ### default energy column is first column
            peak_indexes, _, _ = detect_peak_values(y=data[data.columns[energy_col]], lag=lag, threshold=threshold, influence = influence)
            ### store analyzed data to dictionary
            statistical_analysis_info = statistical_analysis(data, peak_indexes)
            ### prepare filename and export
            filename = building + '_' + str(data.index[0].date()) + '-' + str(data.index[-1].date())
            filename = filename + '.json'
            dict_to_json(folder='statistical_analyzed', filename=filename,
                                     dict=statistical_analysis_info)
            all_building_info[building] = statistical_analysis_info
            return all_building_info
        else:
            return (print('{} data is empty. Existing...'.format(building)))





def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputs', default='', help='input data')
    parser.add_argument('--folder', default='', help='input data folder')
    parser.add_argument('--start_time', default='', help='time to start analyzing')
    parser.add_argument('--end_time', default='', help='time to end analyzing')
    parser.add_argument('--buildings', default='', help='buildings to analyze, will be all building by default')
    parser.add_argument('--hparams', default='',
                        help='Hyperparameter overrides as a comma-separated list of name=value pairs')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_arguments()
    modified_hparams = config.parse(args.hparams)

    if args.folder == '':
        raise Exception('Data folder is required..')
    ### all csv file name from folder:
    data_filenames = get_data_filenames_from_folder(args.folder)

    ## get building from --buildings parameters
    buildings = args.buildings.strip().split(',')
    if len(buildings) >= 1:
        ## keep only buildings that are in --building parameter.
        data_filenames = [filename for filename in data_filenames if any(xs in filename for xs in buildings)]


    # start_time, end_time
    start_time=end_time=None
    if args.start_time != '':
        try:
            if args.start_time=='today':
                start_time, end_time = get_time_by_keyword(args.start_time)
            else:
                start_time = pd.to_datetime(args.start_time)
        except NameError:
            start_time, end_time = get_time_by_keyword(args.start_time)
        except AssertionError:
            raise AssertionError(
                'time parameter must be in date time format, or keywords such as \'today, this month, last week...\'')
    if args.end_time != '':
            #### get time range
            try:
                end_time = pd.to_datetime(args.end_time)
            except NameError:
                start_time, end_time = get_time_by_keyword(args.end_time)
            except AssertionError:
                raise AssertionError(
                    'time parameter must be in date time format, or keywords such as \'today, this month, last week...\'')

    print('start time: {}'.format(start_time))
    print('end time: {}'.format(end_time))
    ################################################
    #
    # ##### change this function to get data from database instead
    data_list = []
    building_list = []
    for filename in data_filenames:
        data, building = loadData(path=filename, idx_col=0,freq='H',  formula='sum')
        data_list.append(data)
        building_list.append(building)
    ## get only a subset of data based on start and end time
    data_list = [get_data_by_time(data_list[i], start_time=start_time, end_time=end_time) for i in range(len(data_list))]
    ### analyze data file and export result to json format. Choose lag and threshold adaptable to data
    analyzed_info_buildings = analyzed_filename_to_json(data_filenames=data_filenames, start_time=start_time, end_time=end_time, lag=6, threshold=2.8, influence=0.5)
    ### print result
    for key, value in analyzed_info_buildings.items():
        print('==={}==='.format(key))
        print('{:30} {:20}'.format('==key==', '==value=='))
        for k, v in value.items():
            print('{:30} : {:20}'.format(k,v))
        print('\n')


