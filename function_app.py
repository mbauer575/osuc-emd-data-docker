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

    print(df_server1)


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
            df = pd.read_csv(fl)
            print("[DOWNLOAD_INFO]  Download successful")

    return df
