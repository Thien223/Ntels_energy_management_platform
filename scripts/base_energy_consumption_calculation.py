from utils.utils import *
import copy
from utils.hyperparams import hparams as config
import time
from dateutil.relativedelta import relativedelta

config_=config

def get_predicted_energy_consumption_from_API(config_, building_ids=None):
    ### get energy data using web API
    login_sess = login(config_)
    host = config_.ntels_host
    url = host + "/NISBCP/urbanmap/energy/getBuildingIFTrend.ajax?"
    pad = "&"
    start_time = config_.start_time
    end_time = config_.end_time
    period = "period=15m"
    data_list = []
    building_list = []
    if building_ids is None:  ### get data of all building
        # db = connect_to_data_base(config_)
        # cursor = db.cursor()
        # query = 'SELECT bld_id FROM nisbcp.t_buildings;'
        # cursor.execute(query)
        # building_ids = cursor.fetchall()
        # cursor.close()
        # db.close()
        ## getbuilding list
        building_ids = get_building_ids(config)
        building_ids = ["bid=" + bld_id for bld_id in building_ids]
        ##initialize data_list and building_list to hold data
    else:  ### get data of buildings that are in the list
        building_ids = ["bid=" + bld_id.strip() for bld_id in building_ids]

    for idx, building_id in enumerate(building_ids):
        url = url + start_time + pad + end_time + pad + building_id + pad + period
        # url='https://nisbcp.ntels.com:18080/NISBCP/urbanmap/energy/getBuildingIFTrend.ajax?startDate=20171107230000&endDate=20181107230000&bid=B0036&period=15m'
        json = get_data_from_HTTPS_request(url, login_sess)
        ##reset url
        url = host + "/NISBCP/urbanmap/energy/getBuildingIFTrend.ajax?"

        #### API data has a field to determind whether return data is empty or not.
        ## real data is stored in 'list' field
        dates = []
        predicted_energy_values = []
        ## if there are any data
        if len(json) > 0:
            for log in json:
                dates.append(log["logdate"])
                predicted_energy_values.append(log["usage"])
            dates = pd.to_datetime(dates)
            data = pd.DataFrame(predicted_energy_values, index=dates, columns=["Power"])
            data_list.append(data)
            building_list.append(building_id)
        else:
            print('Building {} does not have predicted energy consumption data from {} to {}'.format(building_id, start_time, end_time))
    return data_list, building_list


def base_energy_calculation():
    print('Run at: {}'.format(time.ctime()))
    print('runing base energy calculation for nearest 5 day..')
    retry_times = 30
    stop = False
    while not stop:
        try:
            base_energy_config = copy.deepcopy(config)
            ## for the first time, run on data from 2018-02-31 to tomorow
            ## get end_time as tomorow
            _, end_time= get_time_by_keyword('today', freq='15T')
            _, today = get_time_by_keyword('today', freq='15T')
            yesterday, _ = get_time_by_keyword('yesterday', freq='15T')
            # ### get data from 5 days before)
            start_time = end_time.replace(year = end_time.year-1,hour=0, minute=0, second=0)
            end_time = end_time.replace(hour=23, minute=45, second=0)



            ## get data from nearest 1 year
            # start_time = end_time.replace(year=end_time.year-1, hour=0, minute=0, second=0)
            ## format to YYYYMMDDHHMMSS
            end_ = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
            start_ = str(start_time).replace('-', '').replace(' ', '').replace(':', '')
            base_energy_config.start_time = base_energy_config.start_time + start_
            base_energy_config.end_time = base_energy_config.end_time + end_
            # load weather data


            ### past temperature
            temp = pd.read_csv('temperature_bk.csv')
            temp[temp.columns[0]] = pd.to_datetime(temp[temp.columns[0]])
            temp.set_index(temp.columns[0], inplace=True)
            temp = temp.resample('D').mean()



            # weather_data = load_weather_data_from_database(base_energy_config, str(start_time), str(end_time))
            # ### reindex to make sure there are not missing data records
            # weather_data = weather_data.reindex(pd.date_range(weather_data.index[0], weather_data.index[-1], freq='H'))
            # ## fill missing data with appropriate value
            # weather_data = weather_data.resample('H').interpolate()
            ##load energy data
            data_list_, building_list = load_energy_data_from_HTTPS_API(base_energy_config)
            # data_list__ = [data.resample('D').sum() for data in data_list_]
            data_list__ = [data.resample('D').sum() for data in data_list_]
            data_list = [data.reindex(pd.date_range(str(start_time), str(today), freq='D'))for data in data_list__]


            ###data_list = [data.resample('D').sum() for data in data_list_]
            # ### fill missing data
            # for id,building_data in enumerate(data_list):
            #     b_mean = np.mean(data_list[id].loc[building_data['Power'] > 0])
            #     b_std = np.std(data_list[id].loc[building_data['Power'] > 0])
            #     gauss = np.random.normal(b_mean, b_std, size=len(data_list[id]))
            #     gauss = np.abs(gauss)
            #     gauss = pd.DataFrame(gauss, columns=['Power'], index=data_list[id].index)
            #     for row in data_list[id].itertuples():
            #         data_list[id].loc[str(row[0])]['Power'] = gauss.loc[str(row[0])]['Power'] if float(data_list[id].loc[str(row[0])]['Power'])== 0 or np.isnan(data_list[id].loc[str(row[0])]['Power']) else data_list[id].loc[str(row[0])]['Power']
            w_data = temp
            w_data = w_data.dropna()
            w_data = w_data.astype(int)

            db = connect_to_data_base(base_energy_config)
            cursor = db.cursor()
            for building_data, building_id in zip(data_list, building_list):
                # if building_id=='B0004':
                #     break
                ### get predicted energy consuption from API
                try:
                    ### if curreent building has predicted energy consumption data
                    buildings_predicted_consumption, _ = get_predicted_energy_consumption_from_API(
                        base_energy_config, building_ids=[building_id])
                    building_predicted_consumption = buildings_predicted_consumption[0].resample('D').sum()
                except IndexError:
                    ## if current building does not have predicted energy data, then create an empty dataframe
                    building_predicted_consumption = pd.DataFrame(columns=['Power'])
                ### create empty base_energy dataframe for each building
                base_energy_data = pd.DataFrame(columns=['Power'])
                ## also, create performace ratio dataframe to store 이행률
                performance_ratio = pd.DataFrame(columns=['Power'])
                base_energy_data = base_energy_data.reindex(pd.date_range(start_time, end_time, freq='D'))
                performance_ratio = performance_ratio.reindex(pd.date_range(start_time, end_time, freq='D'))
                ### loop over each row and fill base energy value
                for d in base_energy_data.index:
                    #date='2019-01-13'
                    date = str(d.date())
                    month = d.month
                    ### get previous month
                    month_before = (d - relativedelta(months=1)).month
                    ## get next month
                    month_after= (d + relativedelta(months=1)).month
                    try:
                        ### get nearest 3 months
                        temp_w_data = w_data.loc[(w_data.index.month == month) | (w_data.index.month == month_before) | (w_data.index.month == month_after)]
                        temp_w_data = temp_w_data.loc[:yesterday.date()] ## keep only data until yesterday

                        date_temp = int(w_data.loc[date])
                        ### get days that have same temperature as current date
                        same_temp_days = temp_w_data.loc[temp_w_data['Temperature'] == date_temp]
                        # print(same_temp_days)
                        ## add days to list
                        days = [str(i.date()) for i in same_temp_days.index]
                        ## get energy consumption of above days
                        # building_data['2017-11-28']
                        energy_consumption = [float(building_data.loc[day]) for day in days]
                        ### remove missing data records
                        energy_consumption = [item for item in energy_consumption if str(item) !='nan']

                        ## mean
                        base_energy = np.mean(energy_consumption) if len(energy_consumption)!= 0 else 0  ## when the energy consumption of building on day is NaN (missing data) base energy will be NaN --> result 0
                        ### add to base energy dataframe
                        base_energy_data.loc[date] = base_energy
                        # ## calculate performance ratio

                        ### when the Exception occur (when the date is not in building_predicted_consumption) --> performance_ratio will be NaN --> will be filled as 0 latter
                        ## only consider building that has predicted consumption greater than base consumption
                        if float(building_predicted_consumption.loc[date] - base_energy_data.loc[date]) > 0:
                            ratio = float((building_predicted_consumption.loc[date] - building_data.loc[date]) / (building_predicted_consumption.loc[date] - base_energy_data.loc[date]))
                            ## add to ratio dataframe
                            if ratio >= 1:
                                performance_ratio.loc[date] = 100
                            elif ratio > 0 and ratio < 1:
                                performance_ratio.loc[date] = round(ratio *100,2)
                            else:
                                performance_ratio.loc[date] =0 ### if real consumption is greater than base consumption --> performance ratio is 0
                        else:
                            performance_ratio.loc[date] = 0
                    except Exception as e:
                        # print(e)
                        ### when the month is not in w_data, by pass.
                        ## when the day is not in building data, by pass
                        pass
                ## create a dataframe holds building information (consumption, base_consumption, predicted_consumption, performance ratio)
                df = pd.concat([building_data,base_energy_data,building_predicted_consumption,performance_ratio],axis=1)
                df = df.fillna(0) ### convert all null value to 0
                print(df.iloc[-2:,])
                # break
                for row in df[-2:].itertuples(): ## update only nearest 1 records base energy
                # for row in df.itertuples(): ## update all (1 year) base energy
                    insert_query = 'insert into nisbcp.l_pdr_daily (bld_id,dr_date, dr_base, ai_elec,dr_rate) values (\'' + \
                                   str(building_id) + '\',\'' + \
                                   str(row[0].date()) + '\',\'' + \
                                   str(round(row[2],2)) + '\',\'' + \
                                   str(round(row[3],2)) + '\',\'' + \
                                   str(round(row[4],2)) + '\')'
                    update_query = 'update ' \
                                   'dr_base=\'' + str(round(row[2],2)) + '\',' + \
                                   'ai_elec=\'' + str(round(row[3],2)) + '\',' + \
                                   'dr_rate=\'' + str(round(row[4],2))  + '\';'
                    query = insert_query + ' on duplicate key ' + update_query
                    try:
                        cursor.execute(query)
                        db.commit()
                        print("Updated l_pdr_daily table for {} building successfully..".format(building_id))
                    except:
                        print('There is problem, could not execute the query..')
                        print('SQL query: {}'.format(query))
                        db.rollback()
            cursor.close()
            db.close()
            stop=True
        except Exception as e:
            print('Error "{}" occurred, retry after 5 minutes'.format(e))
            stop=False
            time.sleep(300)
            if retry_times>0:
                retry_times -=1
                pass
            else:
                print('Too many time error occurred, exit..')
                break




if __name__=='__main__':
    base_energy_calculation()
    update_temperature_from_kma_API_to_csv()