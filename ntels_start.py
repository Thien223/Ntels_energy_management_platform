from scripts.pattern_clustering import ptn_clustering, get_arguments
from scripts.base_energy_consumption_calculation import base_energy_calculation
from scripts.building_monthly_energy_consumption import building_monthly_energy_consumption_calculation
from statistical_analysis.energy_temp_humi_correlation_analysis import correlation_analysis, \
    update_dust_data_to_database
from utils.utils import update_temperature_from_kma_API_to_csv
from scripts.energy_contract_optimization import energy_contract_optimization
from apscheduler.schedulers.background import BackgroundScheduler
import time
from datetime import datetime
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

if __name__ == "__main__":
    args = get_arguments()
    try:
        num_clusters = int(args.num_clusters)  ### make sure user pass an integer cluster number
    except:
        raise ValueError("Number of cluster must be an integer..")
    assert args.freq.strip() in ["15T", "15m", "H", "D"]

    scheduler = BackgroundScheduler()

    ### everyday at 00:05
    scheduler.add_job(func=ptn_clustering, args=[args], trigger='interval', days=1, next_run_time=datetime(year=2018, month=11, day=26, hour=0, minute=5), max_instances=10)

    ### everyday at 00:05
    scheduler.add_job(func=update_temperature_from_kma_API_to_csv, trigger='interval', days=1, next_run_time=datetime(year=2018, month=11, day=26, hour=0, minute=6), max_instances=10)

    ### everyday at 00:10
    scheduler.add_job(func=correlation_analysis, trigger='interval', days=1, next_run_time=datetime(year=2018, month=11, day=26, hour=0, minute=10), max_instances=10)

    ### every hour at 00:15
    scheduler.add_job(func=update_dust_data_to_database, trigger='interval', hours=1, next_run_time=datetime(year=2018, month=11, day=26, hour=0, minute=15), max_instances=10)

    ### everyday at 17:20
    scheduler.add_job(func=base_energy_calculation, trigger='interval', days=1, next_run_time=datetime(year=2018, month=11, day=26, hour=0, minute=20), max_instances=10)

    ### everyday month at 00:25
    scheduler.add_job(func=energy_contract_optimization, trigger='cron', month='*', next_run_time=datetime(year=2018, month=12, day=1, hour=0, minute=25), max_instances=10)

    ### everyday at 00:30
    scheduler.add_job(func=building_monthly_energy_consumption_calculation, trigger='interval', days=1, next_run_time=datetime(year=2018, month=11, day=26, hour=0, minute=30), max_instances=10)

    scheduler.print_jobs()
    scheduler.start()

    while True:
        time.sleep(1)


    # schedule.every().day.at('00:01').do(ptn_clustering,
    #                                     args)  ### scheduling runing at 00:01 everyday. to disable console output using command: nohup python pattern.clustering.py &
    # time.sleep(1)
    # schedule.every().day.at('01:05').do(correlation_analysis)
    # time.sleep(1)
    # schedule.every().hour.at('02:10').do(update_dust_data_to_database)
    # time.sleep(1)
    # schedule.every().day.at('17:05').do(base_energy_calculation)
    # time.sleep(1)
    # schedule.every(31).days.at('04:20').do(energy_contract_optimization)
    # time.sleep(1)
    # schedule.every().day.at('03:25').do(building_monthly_energy_consumption_calculation)
    # while True:
    #     try:
    #         schedule.run_pending()
    #     except:
    #         pass
#### parallel runing

# import threading
# import time
# import schedule
#
#
# def job():
#     print("I'm running on thread %s" % threading.current_thread())
#
#
# def run_threaded(job_func):
#     job_thread = threading.Thread(target=job_func)
#     job_thread.start()
#
#
# schedule.every(10).seconds.do(run_threaded, job)
# schedule.every(10).seconds.do(run_threaded, job)
# schedule.every(10).seconds.do(run_threaded, job)
# schedule.every(10).seconds.do(run_threaded, job)
# schedule.every(10).seconds.do(run_threaded, job)
#
# while 1:
#     schedule.run_pending()
#     time.sleep(1)
