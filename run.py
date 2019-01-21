# -*- coding: utf-8 -*-
"""run.py: Get data from MariaDB and send to AWS S3.
author: Willian Eduardo Becker
date: 09-08-2018
"""
import threading
import sys
import csv
import os
import boto3
import MySQLdb

# MariaDB parameters
db_host = "YOUR_MARIADB_HOST"
db_user = "YOUR_MARIADB_USER"
db_password = "YOUR_MARIADB_PASSWORD"
db_name = "YOUR_MARIADB_DATABASE"
db_table = "YOUR_MARIADB_TABLE"

# AWS parameters
aws_access_key_id = "YOUR_AWS_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_AWS_SECRET_ACCESS_KEY"
aws_bucket = "YOUR_AWS_BUCKET"
aws_path = "YOUR_AWS_PATH"
output_folder = "/YOUR/TEMPORARY/FOLDER/"


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            sys.stdout.write(
                "\r%s --> %s bytes transferred" % (
                    self._filename, self._seen_so_far))
            sys.stdout.flush()


def get_table_data(cursor, first_value, last_value):
    # Receive a DB cursor, first and last ids and return chunck data
    cursor.execute("SELECT * FROM "+db_table+" WHERE ID BETWEEN " +
                   str(first_value)+" AND "+str(last_value)+";")

    return cursor.fetchall()


def get_table_count(cursor,):
    # Receive a DB cursor and return the row count of specific table
    cursor.execute("SELECT COUNT(*) FROM "+db_table+";")

    return cursor.fetchall()


def get_table_column_names(cursor):
    # Receive a DB cursor and return column names of specific table
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"
        + db_table+"';")

    return cursor.fetchall()


db = MySQLdb.connect(host=db_host,
                     user=db_user,
                     passwd=db_password,
                     db=db_name)
cur = db.cursor()

column_names = get_table_column_names(cur)

# values for chunking process
min_id = 1
max_id = get_table_count(cur)
threshold = 1000000

s3client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

for i, value in enumerate(range(min_id, max_id, threshold)):
    table_data = None
    output_filename = None

    if i == 0:
        first_value = 0
        last_value = threshold

    elif ((max_id - threshold) < value):
        first_value = value
        last_value = max_id

    else:
        first_value = value
        last_value = value + (threshold-1)

    table_data = get_table_data(cur, first_value, last_value)
    output_filename = output_folder+db_table+'_PART_'+str(i)+'.csv'

    with open(output_filename, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        for line in table_data:
            writer.writerow(line)

    key = aws_path+db_table+"_PART_"+str(i)+".csv"
    s3client.upload_file(output_filename, aws_bucket, key,
                         Callback=ProgressPercentage(output_filename))
    os.remove(db_table+"_PART_"+str(i)+".csv")
