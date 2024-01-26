import azure.functions as func
import datetime
import json
import logging
import os
import sys
import time
import json
from datetime import datetime
import pandas as pd
import pyodbc
from azure.identity import DefaultAzureCredential
import pysftp
from io import BytesIO
from dotenv import load_dotenv
import numpy as np

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False
)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")
    load_dotenv()
    # Get the environment variables

    df_server1 = pullData(
        os.getenv("FTP_HOST_1"),
        os.getenv("FTP_USER_1"),
        os.getenv("FTP_PASS_1"),
        1,
    )

    df_server2 = pullData(
        os.getenv("FTP_HOST_2"),
        os.getenv("FTP_USER_2"),
        os.getenv("FTP_PASS_2"),
        2,
    )
    df_server3 = pullData(
        os.getenv("FTP_HOST_3"),
        os.getenv("FTP_USER_3"),
        os.getenv("FTP_PASS_3"),
        3,
    )

    df_server1 = cleanData(df_server1)
    df_server2 = cleanData(df_server2)
    df_server3 = cleanData(df_server3)
    dataval = CheckResData(df_server1, df_server2, df_server3)

    if dataval is True:
        logging.info("Data checkes passed")
    elif dataval is False:
        logging.error("Data checks failed")
        return
    else:
        df_server1, df_server2, df_server3 = dataval
        logging.warning("Data checks passed after correction")

    master_df = processData(df_server1, df_server2, df_server3)


def processData(df_server1, df_server2, df_server3):
    # Create the master dataframe
    master_df = pd.DataFrame()

    # Copy the 'Time' column from the first server's dataframe
    master_df["Time"] = df_server1["Time"]

    # Create submeters by combining values from different meters
    master_df["1st_Floor"] = (
        df_server1["Meter_01_Watt(avg)"] + df_server3["Meter_01_Watt(avg)"]
    )
    master_df["2nd_Floor"] = (
        df_server1["Meter_03_Watt(avg)"]
        + df_server1["Meter_05_Watt(avg)"]
        + df_server1["Meter_10_Watt(avg)"]
        + df_server3["Meter_02_Watt(avg)"]
    )
    master_df["3rd_Floor"] = (
        df_server1["Meter_04_Watt(avg)"]
        + df_server1["Meter_07_Watt(avg)"]
        + df_server1["Meter_09_Watt(avg)"]
        + df_server3["Meter_04_Watt(avg)"]
    )
    master_df["4th_Floor"] = (
        df_server1["Meter_06_Watt(avg)"]
        + df_server1["Meter_08_Watt(avg)"]
        + df_server1["Meter_13_Watt(avg)"]
        + df_server3["Meter_03_Watt(avg)"]
    )
    master_df["Utilities"] = (
        df_server1["Meter_11_Watt(avg)"]
        + df_server1["Meter_12_Watt(avg)"]
        + df_server2["Meter_02_Watt(avg)"]
        + df_server2["Meter_03_Watt(avg)"]
        + df_server2["Meter_05_Watt(avg)"]
        + df_server3["Meter_05_Watt(avg)"]
        + df_server3["Meter_06_Watt(avg)"]
    )

    master_df["Total"] = (
        master_df["1st_Floor"]
        + master_df["2nd_Floor"]
        + master_df["3rd_Floor"]
        + master_df["4th_Floor"]
        + master_df["Utilities"]
    )

    master_df["1st_Floor_Kwh"] = master_df["1st_Floor"] * 5 / 60 / 1000
    master_df["2nd_Floor_Kwh"] = master_df["2nd_Floor"] * 5 / 60 / 1000
    master_df["3rd_Floor_Kwh"] = master_df["3rd_Floor"] * 5 / 60 / 1000
    master_df["4th_Floor_Kwh"] = master_df["4th_Floor"] * 5 / 60 / 1000
    master_df["Utilities_Kwh"] = master_df["Utilities"] * 5 / 60 / 1000
    master_df["Total_Kwh"] = master_df["Total"] * 5 / 60 / 1000

    # Print the master dataframe
    logging.info(master_df)
    return master_df


def cleanData(df):
    # Delete any columns that have no data
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")

    # Correct duplicate time stamps
    df["Time"] = pd.to_datetime(df["Time"])
    df = df.sort_values("Time")

    # Check if there are any duplicate time stamps
    duplicates = df[df.duplicated(["Time"], keep=False)]

    # If there are duplicates, correct them
    if not duplicates.empty:
        logging.warning("Duplicate time stamps found, correcting...")
        # Get the unique time stamps
        unique_times = df["Time"].unique()

        # Create a new dataframe with the unique time stamps
        new_df = pd.DataFrame(index=unique_times)

        # Merge the new dataframe with the old one, this will correct the duplicate time stamps
        df = new_df.merge(df, left_index=True, right_on="Time", how="left")

    return df


def CheckResData(df_server1, df_server2, df_server3):
    # test 1 - check if all dataframes are the same length
    if len(df_server1) != len(df_server2) or len(df_server1) != len(df_server3):
        print("Dataframes are not the same length attempting to fix... - check logs")
        # Get the latest time that is common to all three dataframes
        latest_common_time = min(
            df_server1.index.max(), df_server2.index.max(), df_server3.index.max()
        )

        # trim the dataframes to the latest common time
        df_server1 = df_server1[df_server1.index <= latest_common_time]
        df_server2 = df_server2[df_server2.index <= latest_common_time]
        df_server3 = df_server3[df_server3.index <= latest_common_time]

        # Check if the dataframes are the same length now
        if len(df_server1) != len(df_server2) or len(df_server1) != len(df_server3):
            print("Dataframes are still not the same length, exiting...")
            logging.error("Dataframes are still not the same length, exiting...")
            logging.error("Length of df_server1: " + str(len(df_server1)))
            logging.error("Length of df_server2: " + str(len(df_server2)))
            logging.error("Length of df_server3: " + str(len(df_server3)))
            return False
        else:
            return df_server1, df_server2, df_server3

    # test 2 - check if all dataframes have the same time stamps
    if not df_server1.index.equals(df_server2.index) or not df_server1.index.equals(
        df_server3.index
    ):
        logging.warning("Dataframes do not have the same time stamps")
        return False
    print("All dataframes have length " + str(len(df_server1)))
    logging.info("All dataframes have length " + str(len(df_server1)))
    print(
        "All dataframes start at "
        + str(df_server1.index[0])
        + " and end at "
        + str(df_server1.index[-1])
    )
    return True


def pullData(FTP_HOST, FTP_USER, FTP_PASS, SERVER_NUM):
    now = datetime.now()
    Fdate = now.strftime("%Y%m%d")

    logging.info(
        "[DOWNLOAD_INFO]  Attempting to download" + str(SERVER_NUM) + " at " + str(now)
    )
    print(
        "[DOWNLOAD_INFO]  Attempting to download" + str(SERVER_NUM) + " at " + str(now)
    )

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(
        host=FTP_HOST, username=FTP_USER, password=FTP_PASS, cnopts=cnopts, port=2222
    ) as sftp:
        sftp.chdir("trend")
        with BytesIO() as fl:
            sftp.getfo("Trend_Virtual_Meter_Watt_" + Fdate + ".csv", fl)
            fl.seek(0)
            df = pd.read_csv(
                fl,
                header=0,
            )
            print("[DOWNLOAD_INFO]  Download successful")
            logging.info("[DOWNLOAD_INFO]  Download successful")

    return df
