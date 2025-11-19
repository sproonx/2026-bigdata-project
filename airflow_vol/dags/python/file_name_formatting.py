import os
import re
import logging

def get_yyyyMM_new_format(parent_dir_path: str, **kwargs):
    """
    Return a sorted list of YYYYMM from files like 'YYYYMM-hubway-tripdata.csv'.
    """

    results = []
    if not os.path.isdir(parent_dir_path):
        return results

    for fname in os.listdir(parent_dir_path):
        m = re.match(r"(\d{6})-hubway-tripdata\.csv$", fname)
        if m:
            results.append(str(m.group(1)))

    return sorted(results)


def get_yyyy_old_format(parent_dir_path: str, **kwargs):
    """
    Return a sorted list of years (YYYY) from old-format files like 'hubway_Trips_YYYY.csv'.
    """
    results = []
    if not os.path.isdir(parent_dir_path):
        return results

    for fname in os.listdir(parent_dir_path):
            m = re.match(r"hubway_Trips_(\d{4})\.csv$", fname)
            if m:
                results.append(str(m.group(1)))

    return sorted(results)


def get_yyyyMM_old_format(parent_dir_path: str, **kwargs):
    """
    Return a sorted list of YYYYMM from files old-format files like 'hubway_Trips_YYYY.csv'.
    As they only contain YYYY, all months are added for each year.
    """
    years = get_yyyy_old_format(parent_dir_path=parent_dir_path)
    results = []
    for year in years:
        for month in range(1, 13):
            results.append(f"{year}{month:02d}")

    return sorted(results)

def get_mv_import_raw_pairs(parent_dir_path: str, **kwargs):
    """
    Return a list of (source, destination) HDFS paths for moving files from import to raw directory.
    """
    pairs = []
    
    yyyyMM_list = get_yyyyMM_new_format(parent_dir_path=parent_dir_path)
    for yyyyMM in yyyyMM_list:
        source = f"{parent_dir_path}/{yyyyMM}-hubway-tripdata.csv"
        destination = f"/data/bikesharing/raw/{yyyyMM}/data.csv"
        pairs.append((source, destination))


    yyyy_list = get_yyyy_old_format(parent_dir_path=parent_dir_path)
    for yyyy in yyyy_list:
        source = f"{parent_dir_path}/hubway_Trips_{yyyy}.csv"
        destination = f"/data/bikesharing/raw/{yyyy}/data.csv"
        pairs.append((source, destination))

    return pairs