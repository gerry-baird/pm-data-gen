import csv
from datetime import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import yaml
from yaml.loader import SafeLoader
import logging
import sys

# Setup Logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
file_handler = logging.FileHandler('data-gen.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)


config_filename = "config.yaml"

# Load the config
try:
    with open(config_filename, 'r') as config_file:
        config = yaml.load(config_file, Loader=SafeLoader)
except FileNotFoundError as e:
    msg = "Cannot find config file : config.yaml or the config file specified in arguments"
    print(msg)
    logger.error(msg)
    sys.exit("Aborting.")

PROCESS_VARIANTS = config['process-variants']
START_DATE_TIME = config['start-date-time']
INSTANCE_OFFSET_HOURS = config['instance-offset-hours']

class Task:
    def __init__(self, task_id, task_name, start):
        self.task_id = task_id
        self.task_name = task_name
        self.start = start


def build_instance_task_list(instance_id, process_variant, start_date_time, task_df):
    instance_task_list = []
    rnd = np.random.default_rng()  # a random number generator

    # This code generates a random duration by leveraging the numpy lognormal function that gives a random
    # number drawn from a log normal distribution. Look up a picture of log normal distributions and you'll see 
    # why this is useful for generating random durations based on a mean and std deviation.
    # most samples are near the average with a long tail stretching towards infinity.

    # process_variant is a series
    for task_type in process_variant:
        # get the avg and max durations from the task_df dataframe using task_type as the key
        avg_dur = task_df.loc[task_type, 'Avg']
        max_dur = task_df.loc[task_type, 'Max']

        sigma = (max_dur - avg_dur) / max_dur  # std dev
        log_mean = np.log(avg_dur)  # can't pass the mean duration into lognormal until it has been logged itself 
        delta = rnd.lognormal(log_mean, sigma)  # get a random sample from a log normal distribution with a std dev

        task = Task(instance_id, task_type, start_date_time)
        instance_task_list.append(task)

        # increment the start time by the delta so that the next task start after this one
        start_date_time = start_date_time + timedelta(hours=delta)

    return instance_task_list


def main():

    task_list = []    
    #load task durations
    task_df = pd.read_csv('task_duration_config.csv')
    task_df.set_index('Task', inplace=True)
    start_date_time = START_DATE_TIME

    print(task_df)
    instance_counter = 1
    
    for variant in PROCESS_VARIANTS:
        qty = variant["qty"]
        print(f"Process Variant : {variant}, QTY : {qty}")

        for x in range(0, qty):
            tasks = build_instance_task_list(instance_counter, variant["tasks"], start_date_time, task_df)
            for task in tasks:
                task_list.append(task)

            # Increment the start time by 24 hour
            start_date_time = start_date_time + timedelta(hours=INSTANCE_OFFSET_HOURS)
            # Increment the process_id
            instance_counter = instance_counter +1

    filename = 'task_list.csv'
    try:
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write headers
            writer.writerow(["task_id", "task_name", "start"])
            for task in task_list:
                writer.writerow([task.task_id, task.task_name, task.start])
    except BaseException as e:
        print('BaseException:', filename)
    else:
        print('Data has been generated successfully !')


if __name__ == "__main__":
    main()
