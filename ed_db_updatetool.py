# This program is used to manually update the database.
# It will be used to add new data to the database or to update existing data.
# The program will be run from the command line.
# The program will prompt the user for the time period of the data to be updated.
# The program will prompt the user for the to be added to a table or to create a new table for the data.
# The program will then update the database with the new data.
# The program will then display a message to the user indicating that the update was successful.
# The program will then terminate.


# import required libraries
# from dotenv import load_dotenv
# import all functions from datapross.py file
from datapross import *
import os
import logging
import sys
import argparse


def main():
    # Get the environment variables
    # load_dotenv()
    table = os.getenv("TABLE_NAME")

    # ask the user what they want to do
    while True:
        print("What would you like to do?")
        print("1. Add new data to the database")
        print("2. Update existing data in the database")
        print("3. Create a new table for the data")
        print("4. Quit")
        choice = input("Enter your choice: ")

        if choice == "1":
            # Add new data to the database
            add_new_data()
            break
        elif choice == "2":
            # Update existing data in the database
            break
        elif choice == "3":
            # Create a new table for the data
            break
        elif choice == "4":
            sys.exit()
        else:
            print("Invalid choice. Please try again.")


def add_new_data():
    # Get the time period of the data to be updated
    start_date = input("Enter the start date (YYYY-MM-DD): ")
    end_date = input("Enter the end date (YYYY-MM-DD): ")

    # Get the data from the servers
    df_server1 = get_data_from_range(
        os.getenv("FTP_HOST_1"),
        os.getenv("FTP_USER_1"),
        os.getenv("FTP_PASS_1"),
        start_date,
        end_date,
    )

    # df_server2 = get_data_from_range(
    #     os.getenv("FTP_HOST_2"),
    #     os.getenv("FTP_USER_2"),
    #     os.getenv("FTP_PASS_2"),
    #     start_date,
    #     end_date,
    # )
    # df_server3 = get_data_from_range(
    #     os.getenv("FTP_HOST_3"),
    #     os.getenv("FTP_USER_3"),
    #     os.getenv("FTP_PASS_3"),
    #     start_date,
    #     end_date,
    # )

    # Save df_server1 to a .csv file
    df_server1.to_csv("df_server1.csv", index=False)


main()
