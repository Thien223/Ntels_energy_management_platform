import argparse
import os, math
import copy
import time
from utils.hyperparams import hparams as config
from utils.utils import *


os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"  ### disable warning messages



###https://github.com/alexminnaar/time-series-classification-and-clustering/blob/master/Time%20Series%20Classification%20and%20Clustering.ipynb


def dynamic_time_wraping_distance(serie_1, serie_2, window=10):
    ### calculate distance between 2 series
    DTW = {}
    w = max(window, abs(len(serie_1) - len(serie_2)))

    for i in range(-1, len(serie_1)):
        for j in range(-1, len(serie_2)):
            DTW[(i, j)] = float("inf")
    DTW[(-1, -1)] = 0

    for i in range(len(serie_1)):
        for j in range(max(0, i - w), min(len(serie_2), i + w)):
            dist = (serie_1[i] - serie_2[j]) ** 2
            DTW[(i, j)] = dist + min(DTW[(i - 1, j)], DTW[(i, j - 1)], DTW[(i - 1, j - 1)])
    return math.sqrt(DTW[len(serie_1) - 1, len(serie_2) - 1])


def lb_keogh_distance(s1, s2, r):
    ### calculate distance between 2 series, the faster method
    """
    use to check lower bound and upper bound between 2 time series before calculating dynamic time wrapper.
    ---> save DTW calculating time
    :param s1:
    :param s2:
    :param r:
    :return:
    """
    LB_sum = 0
    for ind, i in enumerate(s1):
        lower_bound = min(s2[(ind - r if ind - r >= 0 else 0):(ind + r)])
        upper_bound = max(s2[(ind - r if ind - r >= 0 else 0):(ind + r)])
        if i > upper_bound:
            LB_sum = LB_sum + (i - upper_bound) ** 2
        elif i < lower_bound:
            LB_sum = LB_sum + (i - lower_bound) ** 2
    return math.sqrt(LB_sum)

def k_means_clust(series, centroids=None, num_clusters=3, num_iter=100, w=5):
    ### cluster a set of series to [num_clusters] cluster
    import random
    centroids = random.sample(list(series), num_clusters) if centroids is None else centroids
    assignments = None
    for n in range(num_iter):
        print("Training centroids - step {} / {}".format(n, num_iter))
        assignments = {}
        # assign data points to clusters
        for idx, serie in enumerate(series):
            min_distance = float("inf")  ### create a infinity number and assign to min
            closest_centroid_idx = None
            for centroid_idx, centroid_serie in enumerate(centroids):
                if lb_keogh_distance(serie, centroid_serie, w) < min_distance:
                    current_serie_distance_from_centroid = dynamic_time_wraping_distance(serie, centroid_serie, w)
                    if current_serie_distance_from_centroid < min_distance:
                        min_distance = current_serie_distance_from_centroid
                        closest_centroid_idx = centroid_idx
            #### add to assignments
            if closest_centroid_idx is not None:  ##### exclude empty series
                assignments.setdefault(closest_centroid_idx, [])
                assignments[closest_centroid_idx].append(idx)
            else:
                assignments.setdefault(closest_centroid_idx, [])
        # recalculate centroids of clusters
        for key in assignments:
            clust_sum = 0
            for k in assignments[key]:
                clust_sum = clust_sum + series[k]
            centroids[key] = [m / len(assignments[key]) for m in clust_sum]
    print("Training done..")
    return centroids, assignments


def one_time_k_means_clust(serie, centroids, w=5):
    ### cluster a set of series to [num_clusters] cluster
    # assign data points to clusters
    closest_centroid_idx = None
    min_distance = float("inf")
    for idx, centroid in enumerate(centroids):
        distance = dynamic_time_wraping_distance(serie, centroid, w)
        if distance < min_distance:
            min_distance = distance
            closest_centroid_idx = idx
    centroids[closest_centroid_idx] = (serie + centroids[closest_centroid_idx]) / 2 ####??????
    return centroids, closest_centroid_idx




##### need recheck
def monthly_energy_consumption_pattern_clustering(building_data, building_id, num_clusters=3):
    """
    clustering a building data by date
    :param building_data: data to be clustered
    :param building_id: building id
    :param num_clusters: number of cluster
    :return: cluster"s centroids and corresponding data serie by date
    """

    ### sub segment data by date####
    dates = [date.date() for date in building_data.index]
    dates = [date.replace(day=1) for date in dates]
    dates = sorted(set(dates))
    ### remove date part from date string (keep only month and year)
    months= [str(date)[:-3] for date in dates]


    data_list = [building_data[str(month)] for month in months]

    ### todo: if filling missing data is not allowed
    ### remove data that contain missing data from data_list (as  well as building id from building_list)
    idxs=[]
    for idx, building_data_ in enumerate(data_list):
        if building_data_.isnull().values.any():
            idxs.append(idx)
    for i in sorted(idxs, reverse=True):
        data_list.pop(i)
    # todo: filling missing data is allowed or not?
    ### if filling missing data is allowed
    # data_list = [data.resample(args.freq).interpolate() for data in data_list]  ## fill missing data

    ### convert data to series
    series = []
    for df, month in zip(data_list, months):
        series.append(list(df["Power"]))
    ### normalizing
    ## extract statistical params
    mean = np.mean([np.mean(serie) for serie in series])
    std = np.mean([np.std(serie) for serie in series])
    norm_series = []
    ## get normalizied series
    max_length = 0
    for serie in series:
        if len(serie) > max_length:
            max_length = len(serie)
    #### remove series that has different length
    idxs=[]
    for serie, month in zip(series, months):
        if len(serie) == max_length and std != 0:
            norm_serie = (serie - mean) / std
            norm_series.append(norm_serie)
        else:
            # print('not max length: {}'.format(month))
            idxs.append(month)
    for i in sorted(idxs, reverse=True):
        months.remove(i)

    try:
        #### clustering (centroids hold the center serie, assignments hold serie ids, belong to each cluster.
        centroids, assignments = k_means_clust(norm_series, centroids=None, num_clusters=num_clusters, num_iter=100, w=30)

        ### PLOT RESULTS
        #### save figure
        ### save ploting images (we remove add building name to figure for clearly viewing)
        print("Saving images..")
        # Turn interactive plotting off
        import matplotlib
        matplotlib.use("Agg")  ##
        import matplotlib.pyplot as plt
        plt.ioff()  ## turn plotting off
        os.makedirs("images", exist_ok=True)
        for i in range(num_clusters):
            ## images saving path
            image_path = os.path.join("images", "cluster_{}_{}.png".format(i, building_id))
            ## create image
            plt.figure()
            ## plot centroid serie as a black line
            ## plot series that belong to current cluster
            try:
                for id in assignments[i]:
                    plt.plot(list(norm_series[id]))
            except:
                pass
                    ##plt.plot(list(series[id]), label=building) ## add building name to figure
            plt.plot(centroids[i], label="cluster-{}".format(i), color="black")
            ## save image
            plt.title("Monthly pattern clustering - {}".format(building_id))
            plt.savefig(image_path, format="png")
            plt.legend()
            plt.close()
        print("Clusters image saved in images folder..")
        centroids = [(np.asarray(centroid) * std) + mean for centroid in
                     centroids]  # de normalizing centroid to real energy scale
        return centroids, assignments, months
    except ValueError:
        print('Not enough data to form a pattern..')
        return




def daily_energy_consumption_pattern_clustering(building_data, building_id, num_clusters=3):
    """
    clustering a building data by date
    :param building_data: data to be clustered
    :param building_id: building id
    :param num_clusters: number of cluster
    :return: cluster"s centroids and corresponding data serie by date
    """
        #### sub segment data by date####
    dates = [day.date() for day in building_data.index]
    dates = sorted(set(dates))
    data_list = [building_data[str(date)] for date in dates]

    ### todo: if filling missing data is not allowed
    ### remove data that contain missing data from data_list (as  well as building id from building_list)
    idxs=[]
    for idx, building_data in enumerate(data_list):
        if building_data.isnull().values.any():
            idxs.append(idx)
    for i in sorted(idxs, reverse=True):
        data_list.pop(i)
    # todo: filling missing data is allowed or not?
    ### if filling missing data is allowed
    # data_list = [data.resample(args.freq).interpolate() for data in data_list]  ## fill missing data


    ### convert data to series
    series = []
    for df, date in zip(data_list, dates):
        series.append(list(df[df.columns[0]]))
    ### normalizing
    ## extract statistical params
    mean = np.mean([np.mean(serie) for serie in series])
    std = np.mean([np.std(serie) for serie in series])
    norm_series = []
    ## get normalizied series
    max_length = 0
    for serie in series:
        if len(serie) > max_length:
            max_length = len(serie)
    #### remove series that has different length
    for serie, date in zip(series, dates):
        if len(serie) == max_length and std != 0:
            norm_serie = (serie - mean) / std
            norm_series.append(norm_serie)
        else:
            dates.remove(date)
    try:
        #### clustering (centroids hold the center serie, assignments hold serie ids, belong to each cluster.
        centroids, assignments = k_means_clust(norm_series, centroids=None, num_clusters=num_clusters, num_iter=100, w=30)

        # ### PLOT RESULTS
        # #### save figure
        # ### save ploting images (we remove add building name to figure for clearly viewing)
        # print("Saving images..")
        # # Turn interactive plotting off
        # import matplotlib
        # matplotlib.use("Agg")  ##
        # import matplotlib.pyplot as plt
        # plt.ioff()  ## turn plotting off
        # os.makedirs("images", exist_ok=True)
        # for i in range(num_clusters):
        #     ## images saving path
        #     image_path = os.path.join("images", "cluster_{}_{}.png".format(i, building_id))
        #     ## create image
        #     plt.figure()
        #     ## plot centroid serie as a black line
        #     ## plot series that belong to current cluster
        #     try:
        #         for id in assignments[i]:
        #             plt.plot(list(norm_series[id]))
        #     except:
        #         pass
        #             ##plt.plot(list(series[id]), label=building) ## add building name to figure
        #     plt.plot(centroids[i], label="cluster-{}".format(i), color="black")
        #     ## save image
        #     plt.title("Daily pattern clustering - {}".format(building_id))
        #     plt.savefig(image_path, format="png")
        #     plt.legend()
        #     plt.close()
        # print("Clusters image saved in images folder..")
        centroids = [(np.asarray(centroid) * std) + mean for centroid in
                     centroids]  # de normalizing centroid to energy
        return centroids, assignments, dates
    except ValueError:
        print('Not enough data to form a pattern..')
        return


def one_time_pattern_clustering(one_pattern_data, centroids, by_day=True):
    """
    clustering a building data by date
    :param one_day_data: data to be clustered
    :param centroids: exist cluster"s centroids
    :param num_clusters: number of cluster
    :param data_offset: cutout the first parts of data  to save training time (by date)
    :return: cluster"s centroids and corresponding data serie by date
    """
    assert len(one_pattern_data) == len(centroids[0]), 'Length of patterns do not match the old ones in database..'  ## length of data and centroid must be matched
    # get date of data
    ### convert data to series
    serie = list(one_pattern_data[one_pattern_data.columns[0]])
    ## get mean and std for normalizing
    mean = np.mean(centroids[0])
    std = np.mean(centroids[0])
    ### normalizing
    ## extract statistical params
    norm_serie = (serie - mean) / std
    centroids = [(centroid - mean) / std for centroid in centroids]  ## also normalize centroid
    ### assign serie to centroid
    new_centroids, cluster_idx = one_time_k_means_clust(norm_serie, centroids=centroids, w=30)
    new_centroids = [(np.asarray(centroid) * std) + mean for centroid in
                     centroids]  ### denormalizing centroid to energy
    # ### PLOT RESULTS
    try:
        if by_day:
            date = str(one_pattern_data.index[0].date())
            return new_centroids, cluster_idx, date
        else:
            date = str(one_pattern_data.index[0].date())
            month = str(date)[:-3]
            return new_centroids, cluster_idx, month
    except AttributeError:
        return new_centroids, cluster_idx



def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fresh_run', type=bool, default=False, help='Set this to False to do a fresh training')
    parser.add_argument("--freq", default="H", help="data frequency")  ## could be "H", "D", or "15T" ("15m")
    parser.add_argument("--num_clusters", default="3", help="number of clusters")
    parser.add_argument("--hparams", default="",
                        help="Hyperparameter overrides as a comma-separated list of name=value pairs")
    args = parser.parse_args()
    return args





def get_cluster_dicts_from_database(building_id):
    db = connect_to_data_base(config)
    cursor = db.cursor()
    select_query = 'SELECT * FROM nisbcp.l_ptrn_info_daily where bld_id=\'' + building_id + '\';'
    cursor.execute(select_query)
    data=cursor.fetchall()
    cursor.close()
    db.close()
    ### extract data from query
    data=data[0]
    clusters_peak_value=[data[1],data[2],data[3]]
    clusters_peak_time=[data[4],data[5],data[6]]
    clusters_temp_average=[str(data[7]),str(data[8]),str(data[9])]
    clusters_humi_average=[str(data[10]),str(data[11]),str(data[12])]
    str_clusters_dict = [data[13],data[14],data[15]]
    str_clusters_dict=[str_cluster_dict.replace('"','\'') for str_cluster_dict in str_clusters_dict]
    cluster_dicts = [eval(str_cluster_dict) for str_cluster_dict in str_clusters_dict]
    return clusters_peak_value, clusters_peak_time, clusters_temp_average, clusters_humi_average, cluster_dicts



def ptn_clustering(args):
    print('Run at: {}'.format(time.ctime()))
    stop = False
    retry_times = 300

    while not stop:
        try:
            if args.fresh_run:
                insert_flag=False
                first_time_config = copy.deepcopy(config)
                print("Runing clustering on all days as the first time")
                ## for the first time, run on data from 2018-02-31 to yesterday
                start_time='2017-05-30 00:00:00'
                # end_time='20181026000000'
                ## get end_time as yesterday
                _, end_time= get_time_by_keyword('yesterday')
                ## format to YYYYMMDDHHMMSS
                end_ = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
                start_ = str(start_time).replace('-', '').replace(' ', '').replace(':', '')

                first_time_config.start_time = first_time_config.start_time + start_
                first_time_config.end_time = first_time_config.end_time + end_
                # load weather data
                weather_data = load_weather_data_from_database(first_time_config, str(pd.to_datetime(start_time)), str(pd.to_datetime(end_time)))
                ##load energy data
                data_list, building_list = load_energy_data_from_HTTPS_API(first_time_config)

                ### reindex data to [freq] interval
                data_list = [data.reindex(pd.date_range(start_time, end_time, freq=args.freq)) for data in data_list]
                ### add cluster all data. Run this for only first time
                #### connect to database
                db = connect_to_data_base(first_time_config)
                cursor = db.cursor()
                ### run clustering algorithm on each building data
                for building_data, building_id in zip(data_list, building_list):
                    clusters_peak_value=[]
                    clusters_peak_time=[]
                    clusters_humi_average=[]
                    clusters_temp_average=[]
                    str_clusters_dict = []
                    try:
                        centroids, assignments, dates = daily_energy_consumption_pattern_clustering(
                            building_data=building_data,
                            building_id=building_id,
                            num_clusters=int(args.num_clusters))
                        insert_flag=True
                        cluster_dict = {}
                        cluster_dict["building_id"] = building_id
                        for id, centroid in enumerate(centroids):
                            cluster_dict["cluster_id"] = id
                            ## denormalize centroid by calculating  average of all days
                            cluster_dict["energy"] = list(centroids[id])

                            try:
                                cluster_dict["dates"] = [str(dates[i]) for i in assignments[id]]
                            except:
                                pass
                            ## get cluster max value and its index
                            idx, peak_value = max(enumerate(centroid), key=(lambda x: x[1]), default=-1)
                            if args.freq in ['15m', '15T']:
                                hour = int(idx / 4)  ## for 15m interval, 1 hour has 4 data points
                                minute = (idx % 4) * 15
                                peak_time = str(hour) + ':' + str(minute) + ':00'  ## format: HH:MM:SS
                            elif args.freq == 'H':
                                hour = idx
                                peak_time = str(hour) + ':' + '00:00'
                            else:
                                peak_time = ''
                            ## add peak value and time to list
                            clusters_peak_time.append(peak_time)
                            clusters_peak_value.append(peak_value)
                            cluster_temp_average = []
                            cluster_humi_average = []
                            # ### get weather info from weather data
                            idxs = []
                            for date_id, date in enumerate(cluster_dict['dates']):
                                if len(weather_data[str(date)]) > 0:
                                    #### calculate average temperature and humidity data each day of cluster's dates
                                    cluster_temp_average.append(np.average(weather_data[str(date)]['Temperature']))
                                    cluster_humi_average.append(np.average(weather_data[str(date)]['Humidity']))
                                else:
                                    ## delete date that do not have weather data
                                    idxs.append(date_id)
                                for date_list_id in sorted(idxs, reverse=True):
                                    cluster_dict['dates'].pop(date_list_id)

                            ## calculate average all days of cluster's dates
                            clusters_temp_average.append(np.average(cluster_temp_average))
                            clusters_humi_average.append(np.average(cluster_humi_average))
                            str_cluster_dict = str(cluster_dict)
                            str_cluster_dict = str_cluster_dict.replace('\'', '"')
                            str_clusters_dict.append(str_cluster_dict)
                    except:
                        insert_flag=False
                    #### insert to database
                    if insert_flag:
                        ## prepare query to update to database
                        insert_query = 'insert into nisbcp.l_ptrn_info_daily (bld_id,ptrn1_peak,ptrn2_peak,ptrn3_peak,ptrn1_time,ptrn2_time,ptrn3_time,ptrn1_temp,ptrn2_temp,ptrn3_temp,ptrn1_humi,ptrn2_humi,ptrn3_humi,ptrn1_json,ptrn2_json,ptrn3_json) values (\''+ \
                                       building_id + '\',\'' + \
                                       str(clusters_peak_value[0]) + '\',\'' + \
                                       str(clusters_peak_value[1]) + '\',\'' + \
                                       str(clusters_peak_value[2]) + '\',\'' + \
                                       str(clusters_peak_time[0]) + '\',\'' + \
                                       str(clusters_peak_time[1]) + '\',\'' + \
                                       str(clusters_peak_time[2]) + '\',\'' + \
                                       str(clusters_temp_average[0]) + '\',\'' + \
                                       str(clusters_temp_average[1]) + '\',\'' + \
                                       str(clusters_temp_average[2]) + '\',\'' + \
                                       str(clusters_humi_average[0]) + '\',\'' + \
                                       str(clusters_humi_average[1]) + '\',\'' + \
                                       str(clusters_humi_average[2]) + '\',\'' + \
                                       str(str_clusters_dict[0]) + '\',\'' + \
                                       str(str_clusters_dict[1]) + '\',\'' + \
                                       str(str_clusters_dict[2]) + '\')'
                        update_query = 'update ' \
                                       'ptrn1_peak=\'' + str(clusters_peak_value[0]) + '\',' + \
                                       'ptrn2_peak=\'' + str(clusters_peak_value[1]) + '\',' + \
                                       'ptrn3_peak=\'' + str(clusters_peak_value[2]) + '\',' + \
                                       'ptrn1_time=\'' + str(clusters_peak_time[0]) + '\',' + \
                                       'ptrn2_time=\'' + str(clusters_peak_time[1]) + '\',' + \
                                       'ptrn3_time=\'' + str(clusters_peak_time[2]) + '\',' + \
                                       'ptrn1_temp=\'' + str(clusters_temp_average[0]) + '\',' + \
                                       'ptrn2_temp=\'' + str(clusters_temp_average[1]) + '\',' + \
                                       'ptrn3_temp=\'' + str(clusters_temp_average[2]) + '\',' + \
                                       'ptrn1_humi=\'' + str(clusters_humi_average[0]) + '\',' + \
                                       'ptrn2_humi=\'' + str(clusters_humi_average[1]) + '\',' + \
                                       'ptrn3_humi=\'' + str(clusters_humi_average[2]) + '\',' + \
                                       'ptrn1_json=\'' + str(str_clusters_dict[0]) + '\',' + \
                                       'ptrn2_json=\'' + str(str_clusters_dict[1]) + '\',' + \
                                       'ptrn3_json=\'' + str(str_clusters_dict[2]) + '\';'
                        query = insert_query + ' on duplicate key ' + update_query
                        ### run query
                        try:
                            cursor.execute(query)
                            db.commit()
                            print('successfully inserted pattern to database for building {}..'.format(building_id))
                        except:
                            print('There is problem, could not execute the query for building: {}..'.format(building_id))
                            print('SQL query: {}'.format(query))
                            db.rollback()
                    else:
                        print('Building {} data from {} to {} is too bad to update..'.format(building_id, start_time, end_time))
                cursor.close()
                db.close()
                # ## set fresh run as false. next time will be in update mode
                args.fresh_run = False
            ### if not first time runing (update mode)
            else:
                update_flag=False
                update_config = copy.deepcopy(config)
                print("Runing clustering on yesterday data only".format(False))
                start_time, end_time = get_time_by_keyword("yesterday")
                start = str(start_time).replace('-','').replace(' ','').replace(':','')
                end = str(end_time).replace('-','').replace(' ','').replace(':','')
                update_config.start_time = update_config.start_time + start
                update_config.end_time = update_config.end_time + end
                # load weather data
                weather_data = load_weather_data_from_database(update_config, str(pd.to_datetime(start_time)), str(pd.to_datetime(end_time)))
                ##load energy data
                data_list, building_list = load_energy_data_from_HTTPS_API(update_config)
                #data_list = [data.reindex(pd.date_range(data.index[0],data.index[-1], freq='H')) for data in data_list] ### do not fill missing data
                data_list = [data.reindex(pd.date_range(start_time,end_time, freq=args.freq)) for data in data_list] #args.freq ### fill missing data
                #### connect to database
                db = connect_to_data_base(update_config)
                cursor = db.cursor()

            ## clustering 1 day data for each building (clustering only new data to save time, run everyday)\
                for building_data, building_id in zip(data_list, building_list):
                    clusters_peak_value = []
                    clusters_peak_time = []
                    clusters_humi_average = []
                    clusters_temp_average = []
                    str_clusters_dict = []
                    try:
                        ### if this building is in l_ptn_info_daily data table: get info from database
                        clusters_peak_value, clusters_peak_time, clusters_temp_average, clusters_humi_average, cluster_dicts = get_cluster_dicts_from_database(building_id)

                        centroids = [cluster_dict['energy'] for cluster_dict in cluster_dicts]


                        if building_data.isnull().values.any()==False:
                            new_centroids, new_centroid_idx, date = one_time_pattern_clustering(building_data, centroids)
                            ### prepare new data to update into database
                            for id, new_centroid in enumerate(
                                    new_centroids):  ## id is the id of cluster that has changed (just need update only this one)
                                if sum(new_centroid) != sum(centroids[id]):
                                    ### determine peak time and value
                                    idx, peak_value = max(enumerate(new_centroid), key=(lambda x: x[1]), default=-1)
                                    if args.freq in ['15m', '15T']:
                                        hour = int(idx / 4)  ## for 15m interval, 1 hour has 4 data points
                                        minute = (idx % 4) * 15
                                        peak_time = str(hour) + ':' + str(minute) + ':00'  ## format: HH:MM:SS
                                    elif args.freq == 'H':
                                        hour = idx
                                        peak_time = str(hour) + ':' + '00:00'
                                    else:
                                        peak_time = ''
                                    clusters_peak_value[id] = peak_value
                                    clusters_peak_time[id] = peak_time
                                    try:
                                    ### update temperature and humidity averages
                                        if len(weather_data[str(date)]) > 0:
                                            yesterday_temp = np.average(weather_data[str(date)]['Temperature'])
                                            yesterday_humi = np.average(weather_data[str(date)]['Humidity'])
                                            ### recalculate temperature and humidity average
                                            clusters_temp_average[id] = ((len(cluster_dicts[id]['dates']) * np.float(
                                                clusters_temp_average[id])) + yesterday_temp) / (len(cluster_dicts[id]['dates']) + 1)
                                            clusters_humi_average[id] = ((len(cluster_dicts[id]['dates']) * np.float(
                                                clusters_humi_average[id])) + yesterday_humi) / (len(cluster_dicts[id]['dates']) + 1)
                                    except KeyError as e:
                                        print("The weather info is missing the date: {}, check t_weather_data table.. pattern's weather info will not be updated..".format(e))
                                        # pass
                                    ## add date
                                    cluster_dicts[id]['dates'].append(date)
                                    ## add new pattern data
                                    cluster_dicts[id]['energy'] = list(new_centroid)
                            # convert to string and replace single quote by double quote
                        str_clusters_dict = [str(cluster_dict) for cluster_dict in cluster_dicts]
                        str_clusters_dict = [str_cluster_dict.replace('\'', '"') for str_cluster_dict in str_clusters_dict]
                        update_flag = True
                    except IndexError: ## #### if the data was not inserted to ptn_info_daily table. insert new.
                        print('{} building pattern is not in database. Adding new record..'.format(building_id))
                        ### if this date data has missing records --> bypass
                        if building_data.isnull().values.any()==False:
                            import datetime
                            insert_config = copy.deepcopy(config)
                            _, end_time = get_time_by_keyword("yesterday")
                            start_time = end_time - datetime.timedelta(days=12)
                            start = str(start_time).replace('-', '').replace(' ', '').replace(':', '')
                            end = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
                            insert_config.start_time = insert_config.start_time + start
                            insert_config.end_time = insert_config.end_time + end
                            # load weather data
                            weather_data = load_weather_data_from_database(insert_config, str(pd.to_datetime(start_time)),
                                                                           str(pd.to_datetime(end_time)))
                            ##load energy data
                            data_list, _ = load_energy_data_from_HTTPS_API(insert_config, building_ids=[building_id])
                            data_list = [data.reindex(pd.date_range(start_time, end_time, freq='H')) for data in
                                         data_list]  # args.freq ### fill missing data

                            centroids, assignments, dates = daily_energy_consumption_pattern_clustering(
                                building_data=data_list[0], ## because there is only 1 building data
                                building_id=building_id,
                                num_clusters=int(args.num_clusters))
                            cluster_dict = {}
                            cluster_dict["building_id"] = building_id
                            for id, centroid in enumerate(centroids):
                                cluster_dict["cluster_id"] = id
                                ## denormalize centroid by calculating  average of all days
                                cluster_dict["energy"] = list(centroids[id])
                                try:
                                    cluster_dict["dates"] = [str(dates[i]) for i in assignments[id]]
                                except:
                                    pass
                                ## get cluster max value and its index
                                idx, peak_value = max(enumerate(centroid), key=(lambda x: x[1]), default=-1)
                                if args.freq in ['15m', '15T']:
                                    hour = int(idx / 4)  ## for 15m interval, 1 hour has 4 data points
                                    minute = (idx % 4) * 15
                                    peak_time = str(hour) + ':' + str(minute) + ':00'  ## format: HH:MM:SS
                                elif args.freq == 'H':
                                    hour = idx
                                    peak_time = str(hour) + ':' + '00:00'
                                else:
                                    peak_time = ''
                                ## add peak value and time to list
                                clusters_peak_time.append(peak_time)
                                clusters_peak_value.append(peak_value)
                                cluster_temp_average = []
                                cluster_humi_average = []
                                # ### get weather info from weather data
                                idxs=[]
                                for date_id, date in enumerate(cluster_dict['dates']):
                                    if len(weather_data[str(date)]) > 0:
                                        #### calculate average temperature and humidity data each day of cluster's dates
                                        cluster_temp_average.append(np.average(weather_data[str(date)]['Temperature']))
                                        cluster_humi_average.append(np.average(weather_data[str(date)]['Humidity']))
                                    else:
                                        ## delete date that do not have weather data
                                        idxs.append(date_id)
                                for date_list_id in sorted(idxs, reverse=True):
                                    cluster_dict['dates'].pop(date_list_id)
                                ## calculate average all days of cluster's dates
                                clusters_temp_average.append(np.average(cluster_temp_average))
                                clusters_humi_average.append(np.average(cluster_humi_average))
                                str_cluster_dict = str(cluster_dict)

                                str_cluster_dict = str_cluster_dict.replace('\'', '"')
                                str_clusters_dict.append(str_cluster_dict)
                            update_flag=True
                        else:
                            update_flag=False
                        # raise IndexError('The will be updated building ID data is not exist in database, run with --fresh_run=True parameter first..')
                    # #database query
                    finally:

                        if update_flag:
                            insert_query = 'insert into nisbcp.l_ptrn_info_daily (bld_id,ptrn1_peak,ptrn2_peak,ptrn3_peak,ptrn1_time,ptrn2_time,ptrn3_time,ptrn1_temp,ptrn2_temp,ptrn3_temp,ptrn1_humi,ptrn2_humi,ptrn3_humi,ptrn1_json,ptrn2_json,ptrn3_json) values (\'' + \
                                           building_id + '\',\'' + \
                                           str(clusters_peak_value[0]) + '\',\'' + \
                                           str(clusters_peak_value[1]) + '\',\'' + \
                                           str(clusters_peak_value[2]) + '\',\'' + \
                                           str(clusters_peak_time[0]) + '\',\'' + \
                                           str(clusters_peak_time[1]) + '\',\'' + \
                                           str(clusters_peak_time[2]) + '\',\'' + \
                                           str(clusters_temp_average[0]) + '\',\'' + \
                                           str(clusters_temp_average[1]) + '\',\'' + \
                                           str(clusters_temp_average[2]) + '\',\'' + \
                                           str(clusters_humi_average[0]) + '\',\'' + \
                                           str(clusters_humi_average[1]) + '\',\'' + \
                                           str(clusters_humi_average[2]) + '\',\'' + \
                                           str(str_clusters_dict[0]) + '\',\'' + \
                                           str(str_clusters_dict[1]) + '\',\'' + \
                                           str(str_clusters_dict[2]) + '\')'
                            update_query = 'update ' \
                                           'ptrn1_peak=\'' + str(clusters_peak_value[0]) + '\',' + \
                                           'ptrn2_peak=\'' + str(clusters_peak_value[1]) + '\',' + \
                                           'ptrn3_peak=\'' + str(clusters_peak_value[2]) + '\',' + \
                                           'ptrn1_time=\'' + str(clusters_peak_time[0]) + '\',' + \
                                           'ptrn2_time=\'' + str(clusters_peak_time[1]) + '\',' + \
                                           'ptrn3_time=\'' + str(clusters_peak_time[2]) + '\',' + \
                                           'ptrn1_temp=\'' + str(clusters_temp_average[0]) + '\',' + \
                                           'ptrn2_temp=\'' + str(clusters_temp_average[1]) + '\',' + \
                                           'ptrn3_temp=\'' + str(clusters_temp_average[2]) + '\',' + \
                                           'ptrn1_humi=\'' + str(clusters_humi_average[0]) + '\',' + \
                                           'ptrn2_humi=\'' + str(clusters_humi_average[1]) + '\',' + \
                                           'ptrn3_humi=\'' + str(clusters_humi_average[2]) + '\',' + \
                                           'ptrn1_json=\'' + str(str_clusters_dict[0]) + '\',' + \
                                           'ptrn2_json=\'' + str(str_clusters_dict[1]) + '\',' + \
                                           'ptrn3_json=\'' + str(str_clusters_dict[2]) + '\';'
                            query = insert_query + ' on duplicate key ' + update_query
                            ### run query
                            try:
                                cursor.execute(query)
                                db.commit()
                                print("Building {} updated pattern successfully..".format(building_id))
                            except:
                                print('There is problem, could not execute the query for building: {}..'.format(building_id))
                                print('SQL query: {}'.format(query))
                                db.rollback()
                        else:
                            print('Building {} data from {} to {} is too bad to update.. '.format(building_id, start_time, end_time))
                cursor.close()
                db.close()
            stop=True
        except Exception as e:
            print('Error "{}" occurred, retry after 1 minutes'.format(e))
            stop=False
            time.sleep(300)
            if retry_times>0:
                retry_times -=1
                pass
            else:
                print('Too many time error occurred, exit..')
                break


#          except
if __name__ == "__main__":
    args = get_arguments()
    ptn_clustering(args)
#     try:
#         num_clusters = int(args.num_clusters)  ### make sure user pass an integer cluster number
#     except:
#         raise ValueError("Number of cluster must be an integer..")
#     assert args.freq.strip() in ["15T", "15m", "H", "D"]
#     # ptn_clustering(args)
#     schedule.every().day.at('00:01').do(ptn_clustering,args)  ### scheduling runing at 00:01 everyday. to disable console output using command: nohup python pattern.clustering.py &
#     while True:
#         schedule.run_pending()
