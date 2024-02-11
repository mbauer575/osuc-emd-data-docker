from datapross import *
from dotenv import load_dotenv


def main():
    load_dotenv()
    # Get the environment variables
    table = os.getenv("TABLE_NAME")
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

    uploadData(master_df, table)


main()
