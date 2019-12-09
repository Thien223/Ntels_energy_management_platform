import time
from dateutil.relativedelta import relativedelta
from utils.utils import *
from utils.hyperparams import hparams as config
import copy

def update_dust_data_to_database():
    stop = False
    print('Run at: {}'.format(time.ctime()))
    print('updating dust data from other source to database..')
    while not stop:
        try:
            dust_data = get_dust_data_from_other_source()
            db = connect_to_data_base(config)
            cursor = db.cursor()
            for row in dust_data.itertuples():
                query = 'insert into nisbcp.l_weather_pm (pmdt, pm10) values (\'' + str(row[0]) + '\',\'' + str(
                    row[1]) + '\') on duplicate key update pm10 =\'' + str(row[1]) + '\';'

                try:
                    cursor.execute(query)
                    db.commit()
                    print("Successully updated dust data..")
                except:
                    print('There is problem, could not update dust data table on {}..'.format(row[0]))
                    print('Data: {}'.format(dust_data))
                    print('SQL query: {}'.format(query))
                    db.rollback()
            cursor.close()
            db.close()
            stop=True
        except Exception as e:
            print('Error {} occurred, retry after 1 minutes'.format(e))
            stop=False
            time.sleep(60)
            pass

def correlation_analysis():
    print('Run at: {}'.format(time.ctime()))

    print('runing correllation analysis on nearest 30 days..')
    correlation_analysis_config = copy.deepcopy(config)
    ## for the first time, run on data from 2018-02-31 to yesterday
    ## get end_time as yesterday

    _,end_time= get_time_by_keyword('yesterday', freq='D')

    start_time = end_time - relativedelta(months=1)
    # start_time = end_time.replace(month=end_time.month-1 , day=end_time.day)

    ## format to YYYYMMDDHHMMSS
    end_ = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
    start_ = str(start_time).replace('-', '').replace(' ', '').replace(':', '')

    correlation_analysis_config.start_time = correlation_analysis_config.start_time + start_
    correlation_analysis_config.end_time = correlation_analysis_config.end_time + end_
    # load weather data
    weather_data = load_weather_data_from_database(correlation_analysis_config, str(start_time), str(end_time))
    weather_data = weather_data.resample('D').mean()

    ## load dust_data from database
    dust_data = get_dust_data_from_database(correlation_analysis_config, str(start_time), str(end_time))
    dust_data = dust_data.resample('D').mean()

    weather_data = pd.concat([weather_data,dust_data], axis=1, sort=False).dropna()
    ##load energy data
    data_list, building_list = load_energy_data_from_HTTPS_API(correlation_analysis_config)
    data_list = [data.resample('D').mean() for data in data_list]

    buildings_correlation_info=[]
    db = connect_to_data_base(correlation_analysis_config)
    cursor = db.cursor()
    for data, building_id in zip(data_list, building_list):
        building_correlation_info=[]
        data = pd.concat([data,weather_data], axis=1)
        data = data.dropna()
        corr_pearson = data.corr(method='pearson')
        corr_spearman = data.corr(method='spearman')
        corr_kendall = data.corr(method='kendall')
        correlation_dict_temp = {}
        correlation_dict_humi = {}
        correlation_dict_dust = {}

        correlation_dict_temp['building_id'] = building_id
        correlation_dict_temp['power_temp']={}
        correlation_dict_temp['power_temp']['score'] = corr_pearson.loc['Power',:]['Temperature']
        correlation_dict_temp['power_temp']['plot_data'] = [[np.float(data['Power'][i]),np.float(data['Temperature'][i])] for i in range(len(data))]
        building_correlation_info.append(correlation_dict_temp)
        correlation_dict_humi['building_id'] = building_id
        correlation_dict_humi['power_humi']={}
        correlation_dict_humi['power_humi']['score'] = corr_pearson.loc['Power', :]['Humidity']
        correlation_dict_humi['power_humi']['plot_data'] = [[np.float(data['Power'][i]), np.float(data['Humidity'][i])] for i in range(len(data))]
        building_correlation_info.append(correlation_dict_humi)
        correlation_dict_dust['building_id'] = building_id
        correlation_dict_dust['power_dust']={}
        correlation_dict_dust['power_dust']['score'] = corr_pearson.loc['Power', :]['Dust']
        correlation_dict_dust['power_dust']['plot_data'] = [[np.float(data['Power'][i]), np.float(data['Dust'][i])] for i in range(len(data))]
        building_correlation_info.append(correlation_dict_humi)

        buildings_correlation_info.append(building_correlation_info)

        ## set correlation level
        if correlation_dict_temp['power_temp']['score'] ==-1:
            correlation_temp_level = 'Perfect negative'
        elif correlation_dict_temp['power_temp']['score'] > -1 and correlation_dict_temp['power_temp']['score'] < -0.75:
            correlation_temp_level = 'Strong negative'
        elif correlation_dict_temp['power_temp']['score'] >= -0.75 and correlation_dict_temp['power_temp']['score'] < -0.5:
            correlation_temp_level = 'Moderate negative'
        elif correlation_dict_temp['power_temp']['score'] >= -0.5 and correlation_dict_temp['power_temp']['score'] < -0.25:
            correlation_temp_level = 'Weak negative'
        elif correlation_dict_temp['power_temp']['score'] >= -0.25 and correlation_dict_temp['power_temp']['score'] < 0.25:
            correlation_temp_level = 'No linear association'
        elif correlation_dict_temp['power_temp']['score'] >= 0.25 and correlation_dict_temp['power_temp']['score'] < 0.5:
            correlation_temp_level = 'Weak positive'
        elif correlation_dict_temp['power_temp']['score'] >= 0.5 and correlation_dict_temp['power_temp']['score'] < 0.75:
            correlation_temp_level = 'Moderate positive'
        elif correlation_dict_temp['power_temp']['score'] >= 0.75 and correlation_dict_temp['power_temp']['score'] < 1:
            correlation_temp_level = 'Strong positive'
        else:
            correlation_temp_level = 'Perfect positive'


        if correlation_dict_humi['power_humi']['score'] ==-1:
            correlation_humi_level = 'Perfect negative'
        elif correlation_dict_humi['power_humi']['score'] > -1 and correlation_dict_humi['power_humi']['score'] < -0.75:
            correlation_humi_level = 'Strong negative'
        elif correlation_dict_humi['power_humi']['score'] >= -0.75 and correlation_dict_humi['power_humi']['score'] < -0.5:
            correlation_humi_level = 'Moderate negative'
        elif correlation_dict_humi['power_humi']['score'] >= -0.5 and correlation_dict_humi['power_humi']['score'] < -0.25:
            correlation_humi_level = 'Weak negative'
        elif correlation_dict_humi['power_humi']['score'] >= -0.25 and correlation_dict_humi['power_humi']['score'] < 0.25:
            correlation_humi_level = 'No linear association'
        elif correlation_dict_humi['power_humi']['score'] >= 0.25 and correlation_dict_humi['power_humi']['score'] < 0.5:
            correlation_humi_level = 'Weak positive'
        elif correlation_dict_humi['power_humi']['score'] >= 0.5 and correlation_dict_humi['power_humi']['score'] < 0.75:
            correlation_humi_level = 'Moderate positive'
        elif correlation_dict_humi['power_humi']['score'] >= 0.75 and correlation_dict_humi['power_humi']['score'] < 1:
            correlation_humi_level = 'Strong positive'
        else:
            correlation_humi_level = 'Perfect positive'


        if correlation_dict_dust['power_dust']['score'] ==-1:
            correlation_dust_level = 'Perfect negative'
        elif correlation_dict_dust['power_dust']['score'] > -1 and correlation_dict_dust['power_dust']['score'] < -0.75:
            correlation_dust_level = 'Strong negative'
        elif correlation_dict_dust['power_dust']['score'] >= -0.75 and correlation_dict_dust['power_dust']['score'] < -0.5:
            correlation_dust_level = 'Moderate negative'
        elif correlation_dict_dust['power_dust']['score'] >= -0.5 and correlation_dict_dust['power_dust']['score'] < -0.25:
            correlation_dust_level = 'Weak negative'
        elif correlation_dict_dust['power_dust']['score'] >= -0.25 and correlation_dict_dust['power_dust']['score'] < 0.25:
            correlation_dust_level = 'No linear association'
        elif correlation_dict_dust['power_dust']['score'] >= 0.25 and correlation_dict_dust['power_dust']['score'] < 0.5:
            correlation_dust_level = 'Weak positive'
        elif correlation_dict_dust['power_dust']['score'] >= 0.5 and correlation_dict_dust['power_dust']['score'] < 0.75:
            correlation_dust_level = 'Moderate positive'
        elif correlation_dict_dust['power_dust']['score'] >= 0.75 and correlation_dict_dust['power_dust']['score'] < 1:
            correlation_dust_level = 'Strong positive'
        else:
            correlation_dust_level = 'Perfect positive'

        str_correlation_dict_temp = str(correlation_dict_temp).replace('\'','"')
        str_correlation_dict_humi = str(correlation_dict_humi).replace('\'','"')
        str_correlation_dict_dust = str(correlation_dict_dust).replace('\'','"')

        insert_query = 'insert into nisbcp.l_anal_info_env (bld_id, anal1_env, anal2_env, anal3_env, anal1_r, anal2_r, anal3_r, anal1_value, anal2_value, anal3_value, anal1_json, anal2_json, anal3_json) values (\'' + \
                       building_id + '\',\'온도\',\'습도\',\'미세먼지\',\'' + \
                       str(round(correlation_dict_temp['power_temp']['score'],2)) + '\',\'' + \
                       str(round(correlation_dict_humi['power_humi']['score'],2)) + '\',\'' + \
                       str(round(correlation_dict_dust['power_dust']['score'],2)) + '\',\'' + \
                       correlation_temp_level+'\',\''+correlation_humi_level+'\',\''+correlation_dust_level+'\',\'' + \
                       str(str_correlation_dict_temp) + '\',\'' + \
                       str(str_correlation_dict_humi) + '\',\'' + \
                       str(str_correlation_dict_dust) + '\')'
        update_query = 'update ' \
                       'anal1_env=\'온도\',' + \
                       'anal2_env=\'습도\',' + \
                       'anal3_env=\'미세먼지\',' + \
                       'anal1_r=\'' + str(round(correlation_dict_temp['power_temp']['score'],2)) + '\',' + \
                       'anal2_r=\'' + str(round(correlation_dict_humi['power_humi']['score'],2)) + '\',' + \
                       'anal3_r=\'' + str(round(correlation_dict_dust['power_dust']['score'],2)) + '\',' + \
                       'anal1_value=\''+correlation_temp_level+'\',' + \
                       'anal2_value=\''+correlation_humi_level+'\',' + \
                       'anal3_value=\''+correlation_dust_level+'\',' + \
                       'anal1_json=\'' + str(str_correlation_dict_temp) + '\',' + \
                       'anal2_json=\'' + str(str_correlation_dict_humi) + '\',' + \
                       'anal3_json=\'' + str(str_correlation_dict_dust) + '\';'
        query = insert_query + ' on duplicate key ' + update_query
        ### run query
        try:
            cursor.execute(query)
            db.commit()
            print("Building {} updated correlation analysis successfully..".format(building_id))
        except:
            print('There is problem, could not execute the query for building: {}..'.format(building_id))
            print('SQL query: {}'.format(query))
            db.rollback()
    cursor.close()
    db.close()
