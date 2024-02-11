# This file contains the functions used to process energy data from
# Eaton Power Xpert Gateway servers and create a master dataframe
# that contains the total energy consumption for the entire building
# as well as the energy consumption for each floor and the utilities
# for each server.


# Import the required libraries
import datetime
import logging
import os
import sys
import time
import json
from datetime import datetime
from datetime import timedelta
import pandas as pd
import pyodbc, struct
from azure.identity import DefaultAzureCredential
import pysftp
from io import BytesIO
from dotenv import load_dotenv
import pytz

# *****************************************************************************
# The following functions are used to process energy data from the servers
# *****************************************************************************


# Function: processData
# ---------------------
# Parameters:
#   3 dataframes - df_server1, df_server2, df_server3
# Returns:
#   master_df - a dataframe that contains the total energy consumption for the entire
#   building as well as the energy consumption for each floor and the utilities
# ---------------------
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


# Function: cleanData
# ---------------------
# This function cleans the data by removing any columns that have no data,
# correcting duplicate time stamps, converting all negative values to positive,
# and rounding all values to 2 decimal places.
#
# Parameters:
#   df - a dataframe
# Returns:
#   df - a cleaned dataframe
# ---------------------
def cleanData(df):
    # Delete any columns that have no data
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")

    # Correct duplicate time stamps
    df["Time"] = pd.to_datetime(df["Date"] + " " + df["Time"])
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

    # Convert all negative values to positive
    df = df.abs()

    # Round all values to 2 decimal places
    df = df.round(2)

    return df


# Function: CheckResData
# ---------------------
# This function checks if the dataframes are the same length and have the same time stamps.
# If they are not the same length, the function will attempt to fix the issue by trimming the
# dataframes to the latest common time. If the dataframes are still not the same length, the
# function will return False.
#
# Parameters:
#   3 dataframes - df_server1, df_server2, df_server3
# Returns:
#   True if the dataframes are the same length and have the same time stamps, False otherwise
# ---------------------
def CheckResData(df_server1, df_server2, df_server3):
    # test 1 - check if all dataframes are the same length
    if len(df_server1) != len(df_server2) or len(df_server1) != len(df_server3):
        print("Dataframes are not the same length attempting to fix...")
        logging.warning("Dataframes are not the same length attempting to fix...")
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
            logging.warning("Fixed! Dataframes are now the same length")
            print("Fixed! Dataframes are now the same length")
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


# *****************************************************************************
# The following functions are used to retrieve data from,
# and connect to, the Eaton Power Xpert Gateway servers
# *****************************************************************************


# Function: pullData
# ---------------------
# This function connects to the Eaton Power Xpert Gateway servers and downloads the
# energy data for the CURRENT DAY. The function then returns the data as a dataframe.
#
# Parameters:
#   FTP_HOST - the IP address of the server
#   FTP_USER - the username
#   FTP_PASS - the password
#   SERVER_NUM - the server number (1, 2, or 3)
# Returns:
#   df - a dataframe containing the energy data
# ---------------------
def pullData(FTP_HOST, FTP_USER, FTP_PASS, SERVER_NUM):
    my_tz = pytz.timezone("US/Pacific")
    now = datetime.now(my_tz)
    Fdate = now.strftime("%Y%m%d")
    logging.info(
        "[DOWNLOAD_INFO]  Attempting to download from server:"
        + str(SERVER_NUM)
        + " at "
        + str(now)
    )
    print(
        "[DOWNLOAD_INFO]  Attempting to download from server:"
        + str(SERVER_NUM)
        + " at "
        + str(now)
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


# Function: get_data_from_range
# ---------------------
# This function connects to the Eaton Power Xpert Gateway servers and downloads the
# energy data for the given range. The function then returns the data as a dataframe.
#
# Parameters:
#   FTP_HOST - the IP address of the server
#   FTP_USER - the username
#   FTP_PASS - the password
#   SERVER_NUM - the server number (1, 2, or 3)
#   start - the start date in the format "YYYY-MM-DD"
#   end - the end date in the format "YYYY-MM-DD"


def get_data_from_range(FTP_HOST, FTP_USER, FTP_PASS, start, end):
    my_tz = pytz.timezone("US/Pacific")
    now = datetime.now(my_tz)
    Fdate = now.strftime("%Y%m%d")
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    days_between = (end_date - start_date).days

    if days_between < 0:
        logging.error("Start date is after end date")
        return None
    elif Fdate < start:
        logging.error("Start date is in the future")
        return None
    elif Fdate < end:
        logging.error("End date is in the future")
        return None
    elif days_between > 60:
        logging.error("Date range is too large")
        return None

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    master_df = pd.DataFrame()
    with pysftp.Connection(
        host=FTP_HOST, username=FTP_USER, password=FTP_PASS, cnopts=cnopts, port=2222
    ) as sftp:
        sftp.chdir("trend")
        print("Attempting to download data from range:" + start + " to " + end)
        for i in range(days_between + 1):
            date = start_date + timedelta(days=i)
            Fdate = date.strftime("%Y%m%d")
            with BytesIO() as fl:
                print("Downloading file for:" + Fdate)
                sftp.getfo("Trend_Virtual_Meter_Watt_" + Fdate + ".csv", fl)
                fl.seek(0)
                df = pd.read_csv(
                    fl,
                    header=0,
                )
                master_df = pd.concat([master_df, df])
        print("Successfully downloaded data from range")
        print("there are:" + str(len(master_df)) + " rows")
    return master_df


# *****************************************************************************
# The following functions are used to connect to the SQL database
# *****************************************************************************

# Function: uploadData
# ---------------------
# This function uploads the master dataframe to the SQL database.
#
# Parameters:
#   master_df - a dataframe containing the energy data
# Returns:
#   None
# ---------------------


def uploadData(master_df, table):
    DB_Last_time = get_last_time()
    Server_Last_time = master_df["Time"].iloc[-1]
    load_dotenv()
    row = master_df.iloc[-1]
    print("DB_Last: " + str(DB_Last_time))
    print("Server_Last: " + str(Server_Last_time))

    if DB_Last_time == None or DB_Last_time < Server_Last_time:
        print("Uploading new data to DB")
        with get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"INSERT INTO {table} (dateTime, First_Floor, Second_Floor, Third_Floor, Fourth_Floor, Utilities, TOTAL, First_Floor_Kwh, Second_Floor_Kwh, Third_Floor_Kwh, Fourth_Floor_Kwh, Utilities_Kwh, TOTAL_Kwh) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row["Time"],
                    row["1st_Floor"],
                    row["2nd_Floor"],
                    row["3rd_Floor"],
                    row["4th_Floor"],
                    row["Utilities"],
                    row["Total"],
                    row["1st_Floor_Kwh"],
                    row["2nd_Floor_Kwh"],
                    row["3rd_Floor_Kwh"],
                    row["4th_Floor_Kwh"],
                    row["Utilities_Kwh"],
                    row["Total_Kwh"],
                )
            except Exception as e:
                print("Error executing SQL statement: {}".format(e))
    elif DB_Last_time == Server_Last_time:
        print("No new data to upload")

    else:
        print("Server data is older than DB data, exiting...")


# Function: get_last_time
# ---------------------
# This function gets the last time from the SQL database.
#
# Parameters:
#   None
# Returns:
#   The last time from the SQL database
def get_last_time():
    with get_conn() as conn:
        cursor = conn.cursor()
        load_dotenv()
        table = os.getenv("SQL_TABLE")
        logging.info("Getting last time from DB")
        print("Getting last time from DB")
        cursor.execute(f"SELECT TOP 1 * FROM {table} ORDER BY dateTime DESC")
        rows = cursor.fetchall()
        if len(rows) == 0:
            logging.warning("database is empty")
            return None
        return rows[0][1]


# Function: get_conn
# ---------------------
# This function connects to the SQL database.
#
# Parameters:
#   None
# Returns:
#   A connection to the SQL database
# ---------------------
def get_conn():
    print("Getting connection to database")
    load_dotenv()
    connection_string = os.getenv("SQL_CONNECTION_STRING")
    # credential = DefaultAzureCredential()

    # Get an access token for the Azure SQL Database
    # token_bytes = credential.get_token("https://database.windows.net/").token.encode(
    #   "UTF-16-LE"
    # )

    # token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    # SQL_COPT_SS_ACCESS_TOKEN = 1256
    # Connect to the Azure SQL Database
    conn = pyodbc.connect(connection_string)
    print("Connected to database")
    return conn
