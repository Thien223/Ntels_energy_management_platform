from utils.utils import *
from utils.hyperparams import hparams as config
from scripts.pattern_clustering import daily_energy_consumption_pattern_clustering, \
    one_time_pattern_clustering
import copy
import datetime
from statistical_analysis.energy_data_analysis import detect_peak_values




def filter_noise(data):
    ### power data
    y = data[data.columns[0]]
    ## outline index storing list
    outline_index = []
    ## detect outline
    signal, _, _ = detect_peak_values(y=y, lag=8, threshold=10, influence=0.5)
    ### extract outline position (index)
    for i, (id, sig) in zip(data[data.columns[0]], enumerate(signal)):
        if sig == 1:
            outline_index.append(id)
    ### remove outline from data
    non_outline_data = data.drop(data.index[outline_index])
    # get the mean of the non-outline data
    replace_value = np.mean(non_outline_data[non_outline_data.columns[0]])
    ## replace outline with above mean value
    data.iloc[outline_index, :] = replace_value
    return data


def building_energy_contract_optimization(building_id, contract_start_time='2019-03-04 00:00:00',
                                          contract_end_time='2020-03-04 23:45:00', alpha=0.8, freq='15T',
                                          safe_ratio=1.5):
    ## make sure start and end time is datetime format
    try:
        contract_start_time = pd.to_datetime(contract_start_time)
        contract_end_time = pd.to_datetime(contract_end_time)
    except ValueError:
        raise ValueError('Contract start and end time must be in datetime format..')

    ##### optimized peak = (alpha * last_year_peak + (1-alpha) * next_year_predicted_peak) * safe_ratio <--- need last month peak and history peak #####
    ######### get last month consumption data
    temp_config = copy.deepcopy(config)
    ## get end_time as yesterday
    start_time, end_time = get_time_by_keyword('last month', freq='15T')
    # start_time = start_time.replace(year=end_time.year-1)
    ## format to YYYYMMDDHHMMSS
    end_ = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
    start_ = str(start_time).replace('-', '').replace(' ', '').replace(':', '')
    ### edit config
    temp_config.start_time = temp_config.start_time + start_
    temp_config.end_time = temp_config.end_time + end_
    ### get historycal energy consumption
    last_month_data, _ = load_peak_data_from_HTTPS_API(temp_config, building_ids=[building_id])
    ### reindex to fit pattern length
    loop = 3
    while True:
        try:
            ### resample to fill missing data
            last_month_data = last_month_data[0].resample('15T').sum()
            last_month_data = last_month_data.reindex(
                pd.date_range(last_month_data.index[0], last_month_data.index[-1], freq='15T'))
            break
        except IndexError as e:
            if loop >0:
                loop-=1
                print('Error: {}, retrying.. {}'.format(e,loop))
                pass
            else:
                ## if there are no last_month_data -->last_month_data[0] will return IndexError Exception. --> contract power will be set to 0
                return 0, 0.8, 0, 0, 1.5  ####contract_peak, alpha, old_peak, predicted_peak, safe_ratio
    ##get pattern of each day of last month
    last_month_patterns = []
    dates_ = [i.date() for i in last_month_data.index]
    dates_ = sorted(set(dates_))
    for date in dates_:
        if len(last_month_data[str(date)]) == 96:  ### 15minutes frequence means 1 day has 96 data points
            last_month_patterns.append(last_month_data[str(date)]['Power'])
    ## calculate average
    last_month_average_pattern = [float(sum(col)) / len(col) for col in zip(*list(last_month_patterns))]
    ## reset config
    temp_config = copy.deepcopy(config)
    ### get all history consumption data of nearest 1 year
    ### because the prediction API use 24 days data to predict next 7 days consumption, so we need get data from 24 days before contract
    start_time = contract_start_time.replace(year=contract_start_time.year - 1) - datetime.timedelta(days=24)
    end_time = contract_end_time.replace(year=contract_end_time.year - 1)
    ## reformat to YYYYMMDDHHMMSS
    end_ = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
    start_ = str(start_time).replace('-', '').replace(' ', '').replace(':', '')
    ### edit config
    temp_config.start_time = temp_config.start_time + start_
    temp_config.end_time = temp_config.end_time + end_
    ### get all consumption data
    loop = 3
    while True:
        try:
            history_data, _ = load_peak_data_from_HTTPS_API(temp_config, building_ids=[building_id])
            ### resample to fill missing data
            building_data = history_data[0].resample('15T').sum()
            ### reindex to fit pattern length
            building_data = building_data.reindex(
                pd.date_range(building_data.index[0], building_data.index[-1], freq='15T'))
            ## filter noise from building data
            building_data = filter_noise(data=building_data)
            ### clustering energy pattern by months
            daily_centroids, assignment, days = daily_energy_consumption_pattern_clustering(building_data, [building_id],
                                                                                            num_clusters=3)
            ### clustering last_month average pattern to determine similar patterns
            _, cluster_idx = one_time_pattern_clustering(pd.DataFrame(last_month_average_pattern, columns=['Power']),
                                                         daily_centroids, by_day=True)
            break
        except IndexError as e:
            if loop >0:
                loop-=1
                print('Error: {}, retrying.. {}'.format(e,loop))
                pass
            else:
                # if there are not history_data --> history_data[0] will return IndexError Exception
                return 0, 0.8, 0, 0, 1.5  ####contract_peak, alpha, old_peak, predicted_peak, safe_ratio

        except Exception as e:
            if loop >0:
                loop-=1
                print('Error: {}, retrying.. {}'.format(e,loop))
                pass
            else:
                # if length of last_month_pattern does not match length of daily_centroids
                return 0, 0.8, 0, 0, 1.5  ####contract_peak, alpha, old_peak, predicted_peak, safe_ratio
    ## get last_month average pattern's similar pattern (by cluster_idx)
    similar_patterns = []
    for idx in assignment[cluster_idx]:
        similar_patterns.append(building_data[str(days[idx])]['Power'])
    ### remove pattern that has missing data (length < 96)
    max_length = np.max([len(item) for item in similar_patterns])
    to_remove_idxs = []
    for idx, item in enumerate(similar_patterns):
        if len(item) < max_length:
            to_remove_idxs.append(idx)
    for i in sorted(to_remove_idxs, reverse=True):
        similar_patterns.pop(i)
    ### calculate similar pattern average
    similar_average_pattern = [float(sum(col)) / len(col) for col in zip(*list(similar_patterns))]
    ### calculate residual pattern (last month - similar)
    residual_pattern = (np.array(last_month_average_pattern) - np.array(similar_average_pattern))
    ### residual pattern's mean and std
    residual_mean = np.mean(residual_pattern)
    residual_std = np.std(residual_pattern)
    ### Gaussian distribution from residual mean and std:
    residual_gauss = np.random.normal(residual_mean, residual_std, size=len(residual_pattern))
    ### index of history data
    history_index = pd.date_range(start_time, end_time, freq='15T')
    old_dates = sorted(set([str(idx.date()) for idx in history_index]))
    ## index of contract data (future)
    contract_index = pd.date_range(contract_start_time, contract_end_time, freq='15T')
    predicted_building_energy = copy.deepcopy(building_data)
    ## alter data by adding gaussian residual
    for old_date in old_dates:
        try:
            predicted_building_energy.loc[old_date]['Power'] = np.array(building_data[old_date]['Power']) + np.array(
                residual_gauss)
        except ValueError as e:
            ### if building data does not has energy consumption data for current date. bypass it and move to next date
            pass
        except KeyError as e:
            ### if current date is not exist in building data
            pass
    ######update predicted building energy by consumption prediction algorithm here
    #### waiting for energy prediction API
    #########################################
    #### get temperature data and add to dataframe as column
    #### temperature API only support hourly data, so we reindex data from 15 minutes frequency to hour frequency
    temp_df = copy.deepcopy(building_data)
    new_data = add_hourly_temp_column_from_csv(temp_df)
    ### add weekday index column
    new_data_ = add_weekday_column(new_data)
    ### create dataframe's columns [date,weekday, hour, minute, temperature, energy] as prediction API's input
    dates = [str(index.date()) for index in new_data_.index]
    weekdays = list(new_data_['weekday_index'].astype(str))
    hours = [str(index.hour) for index in new_data_.index]
    minutes = [str(index.minute) for index in new_data_.index]
    temperatures = list(new_data_['Temperature'].astype(str))
    energys = list(predicted_building_energy['Power'])
    ### append columns to dataframe (for easier manipulating latter)
    predicted_building_energy_ = pd.DataFrame(
        {'dates': dates, 'weekdays': weekdays, 'hours': hours, 'minutes': minutes, 'temperatures': temperatures,
         'energys': energys})
    ### split data into small piece (2304 points) and pass to API
    window_size = 2304  ### 24 days
    start_index = 0
    next_index = 672  ### 7 days
    predicted = pd.DataFrame(columns=['Power'])
    ### get predicted energy from API (next year consumption) note that we keep index as this year, and will change latter
    for i in range(int(len(predicted_building_energy_) / next_index)):
        window = predicted_building_energy_[
                 start_index:start_index + window_size]  ### get data from start index to 24days after
        window_ = [list(window.loc[j].astype(str)) for j in window.index]  ### convert to list
        if len(window_) == window_size:  ### make sure API input meet the requirement
            predicted_window = get_predicted_consumption_from_API(input=window_,
                                                                  building_id=building_id)  # get predicted value
            predicted = predicted.combine_first(predicted_window)  ### append to predicted dataframe
        start_index = start_index + next_index  ## move index to next part
    ### convert index to date time type
    predicted.index = pd.to_datetime(predicted.index)
    ### change index to next year index
    predicted.index = [idx.replace(year=idx.year + 1) for idx in predicted.index]
    ### match index with contract index (remove redundant parts as well as leave missing pass as NaN
    predicted_ = pd.DataFrame(predicted, index=contract_index)
    ## calculate contract peak
    predicted_peak = np.max(predicted_.resample('H').sum())
    old_peak = np.max(building_data)
    # building_data.idxmax()
    if float(predicted_peak) == 0:
        contract_peak = old_peak * safe_ratio
    elif float(old_peak) == 0:
        contract_peak = predicted_peak * safe_ratio
    else:
        contract_peak = ((alpha * old_peak) + ((1 - alpha) * predicted_peak)) * safe_ratio
    return float(contract_peak), float(alpha), float(old_peak), float(predicted_peak), float(safe_ratio)


def energy_contract_optimization():
    import time
    print('Run at: {}'.format(time.ctime()))
    print('runing energy contract optimization..')
    ### get buildings list from database
    building_list = get_building_ids(config)
    ## create a list to hold building peak value
    ## contract's start and end time (from today to 1 year latter)
    contract_start_time, _ = get_time_by_keyword('yesterday')
    contract_end_time = contract_start_time.replace(year=contract_start_time.year + 1, hour=23, minute=45, second=0, microsecond=0)
    ### database connection
    db = connect_to_data_base(config)
    cursor = db.cursor()

    # building_list=['B0001','B0007','B0031','B0033','B0043','B0044','B0054','B0055','B0057','B0059','B0061','B0062','B0068','B0069','B0072','B0073','B0074','B0075','B0076','B0077','B0078','B0079','B0080','B0081','B0082','B0083','B0084','B0085','B0086','B0087','B0089','B0091','B0092','B0093','B0094','B0095','B0096','B0097','B0098','B0099','B0101','B0115']

    # building_list=['B0035']
    ## loop over building list and calculate contract optimizied peak values
    for building_id in building_list:
        retry_times = 30
        stop = False
        while not stop:
            try:
                ####### if building energy consumption does not exist???? how to deal???
                contract_peak, alpha, old_peak, predicted_peak, safe_ratio = building_energy_contract_optimization(
                    building_id,
                    contract_start_time,
                    contract_end_time,
                    freq='H',
                    alpha=0.8,
                    safe_ratio=1.5)
                comment = '\n&middot; 과거 최대피크(P) : {} kW' \
                          '\n\n&middot; 예측 최대피크(F) : {} kW' \
                          '\n\n&middot; 과거피크 적용 가중치(&alpha;) : {}' \
                          '\n\n&middot; 예측피크 적용 가중치(&beta;) : {}' \
                          '\n\n&middot; 산출식' \
                          '\n\n\t\t( &alpha; &middot; P + &beta; &middot; F ) &middot; 1.5 = 최적계약전력\n\n'.format(
                    round(old_peak, 2), round(predicted_peak, 2), round(alpha, 2), round(1 - alpha, 2))
                insert_query = 'insert into nisbcp.l_opti_ai (' \
                               'bld_id,' \
                               'elec_cost,' \
                               'opti_contract,' \
                               'opti_cost,' \
                               'opti_save,' \
                               'ai_co2, ' \
                               'ai_peak, ' \
                               'ai_cost, ' \
                               'how_comment, ' \
                               'prev_peak, ' \
                               'forw_peak, ' \
                               'prev_peak_weight, ' \
                               'forw_peak_weight, ' \
                               'coef) values (\'' + \
                               str(building_id) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               comment + '\',\'' + \
                               str(round(old_peak, 2)) + '\',\'' + \
                               str(round(predicted_peak, 2)) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\',\'' + \
                               str(0) + '\')'
                update_query = 'update ' + \
                               'prev_peak=\'' + str(round(old_peak, 2)) + '\',' + \
                               'forw_peak=\'' + str(round(predicted_peak, 2)) + '\';'
                query = insert_query + ' on duplicate key ' + update_query
                try:
                    cursor.execute(query)
                    db.commit()
                    print("Updated optimal contract (l_opti_ai) table  successfully for building {}..".format(
                        building_id))
                    stop = True
                except:
                    print('There is problem, could not execute the query..')
                    print('SQL query: {}'.format(query))
                    db.rollback()
            except Exception as e:
                print('Error "{}" occurred, retry after 10 minutes..'.format(e))
                time.sleep(600)
                if retry_times > 0:
                    retry_times -= 1
                    pass
                else:
                    print('Too many times error occurred, exit..')
                    break
    ## close database connection
    cursor.close()
    db.close()


if __name__ == '__main__':
    energy_contract_optimization()