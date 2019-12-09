from utils.utils import *
from utils.hyperparams import hparams as config
import copy


def building_monthly_energy_consumption_calculation():
    import time
    print('Run at: {}'.format(time.ctime()))

    print('Calculating monthly energy consumption for each building')
    retry_times=30
    stop = False
    while not stop:
        try:
            building_ids = get_building_ids(config)
            temp_config = copy.deepcopy(config)
            start_time, end_time = get_time_by_keyword(keyword='this month', freq='15m')
            start_time = end_time.replace(year=end_time.year - 2, hour=0, minute=0, second=0)
            ## format time to YYYYMMDDHHMMSS
            start = str(start_time).replace('-', '').replace(' ', '').replace(':', '')
            end = str(end_time).replace('-', '').replace(' ', '').replace(':', '')
            ## update time (to get data) to config
            temp_config.start_time = temp_config.start_time + start
            temp_config.end_time = temp_config.end_time + end
            ## load energy data of building in building_ids list
            data_list, building_list = load_energy_data_from_HTTPS_API(config = temp_config, building_ids = building_ids)
            db = connect_to_data_base(config)
            cursor = db.cursor()
            for building_data, building_id in zip(data_list, building_list):
                monthly_building_data = building_data.resample('M').sum()
                monthly_building_data.index = [pd.to_datetime(idx).replace(day=1) for idx in monthly_building_data.index]
                ### loop throush each row and insert to database
                for row in monthly_building_data.itertuples():
                    insert_query = 'insert into nisbcp.l_elec_month (bld_id,elec_month,elec_use) values (\'' + \
                                   str(building_id) + '\',\'' + \
                                   str(row[0].date()) + '\',\'' + \
                                   str(round(row[1],2)) + '\')'
                    update_query = 'update ' \
                                   'elec_month=\'' + str(row[0].date()) + '\',' + \
                                   'elec_use=\'' + str(round(row[1],2)) + '\';'
                    query = insert_query + ' on duplicate key ' + update_query
                    try:
                        cursor.execute(query)
                        db.commit()
                        print("Monthly consumption table updated successfully for building {}..".format(building_id))
                    except:
                        print('There is problem, could not execute the query..')
                        print('SQL query: {}'.format(query))
                        db.rollback()
            cursor.close()
            db.close()
            stop=True
        except Exception as e:
            print('Error "{}" occurred, retry after 10 minutes'.format(e))
            stop=False
            time.sleep(600)
            if retry_times>0:
                retry_times -=1
                pass
            else:
                print('Too many time error occurred, exit..')
                break


#
# def building_monthly_energy_consumption_calculation():
#
#     #### get base energy consumption from database
#     db = connect_to_data_base(config)
#     cursor = db.cursor()
#     query = 'SELECT bld_id,dr_date,dr_base from nisbcp.l_pdr_daily;'
#     try:
#         cursor.execute(query)
#         data = cursor.fetchall()
#         rows = [list(row) for row in data]  ## read row from query
#         ## create dataframe and put data into
#         base_energy_data = pd.DataFrame(rows, columns=['Building_id','Time', 'Base Energy'])
#         ## set index
#         base_energy_data['Time'] = pd.to_datetime(base_energy_data['Time'])
#         base_energy_data.set_index(['Building_id','Time'], inplace=True)
#         ## close database connection
#         cursor.close()
#         db.close()
#     except Exception as e:
#         cursor.close()
#         db.close()
#         print(e)
#         raise ConnectionError('Could not execute the query, connection to database..')
#
#
#     building_ids = get_building_ids(config)
#
#     #### get  month, year value to check time before calculating
#     today = datetime.date.today()
#     this_month = today.month
#     this_year = today.year
#     last_year = this_year - 1
#
#     building = {}
#     for building_id in building_ids:
#         try:
#             building_base_energy = base_energy_data.loc[building_id]
#             months_sum = []
#             for month in range(1, 13):
#                 if month <= this_month:
#                     year = this_year
#                 else:
#                     year=last_year
#                 sum = np.sum(building_base_energy.loc['{}-{}'.format(str(year),month)])
#                 months_sum.append(float(sum))
#             building[building_id] = months_sum
#         except KeyError as e:
#             print(e)
#             pass
#
#
#
#     db = connect_to_data_base(config)
#     cursor = db.cursor()
#     for key, val in building.items():
#         for monthly_consumption, month in zip(val, range(1,13)):
#             day = datetime.datetime(year=int(last_year), month=month, day=1).date()
#             insert_query = 'insert into nisbcp.l_elec_month (bld_id,elec_month,elec_use) values (\'' + \
#                            str(key) + '\',\'' + \
#                            str(day) + '\',\'' + \
#                            str(monthly_consumption) + '\')'
#             update_query = 'update ' \
#                            'elec_month=\'' + str(day) + '\',' + \
#                            'elec_use=\'' + str(monthly_consumption) + '\';'
#             query = insert_query + ' on duplicate key ' + update_query
#             try:
#                 cursor.execute(query)
#                 db.commit()
#                 print("Monthly consumption table updated successfully for building {}..".format(key))
#             except Exception as e:
#                 print(e)
#                 print('There is problem, could not execute the query..')
#                 print('SQL query: {}'.format(query))
#                 db.rollback()
#     cursor.close()
#     db.close()




if __name__=='__main__':
    building_monthly_energy_consumption_calculation()