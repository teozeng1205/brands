# usage : assign_connection("demo")
# usage : assign_connection("monitoring")
# usage : assign_connection("dev")
# usage : assign_connection("ds")
# usage : assign_connection("Monitoring")
# usage : assign_connection("Analytics")
# usage : assign_connection("Core")

import json
import logging
import sys

import boto3
import pandas as pd
import psycopg2 as pg
from botocore.exceptions import ClientError

print("rs_access_admin.py is being imported !!")
logger = logging.getLogger(__name__)


class RedshiftAccess:
    _instance = None

    def __new__(cls, database_name):
        if cls._instance is None:
            cls._instance = super(RedshiftAccess, cls).__new__(cls)
            cls._instance._initialize(database_name)
        return cls._instance

    def _initialize(self, database_name):
        print("Initializing RedshiftAccess..")
        if database_name == "demo":
            secret_name = "demo-cluster-user_code"
        elif database_name == "monitoring":
            secret_name = "prod/monitoring/redshift/admin/george"
        elif database_name == "dev":
            secret_name = "prod/monitoring/redshift/admin/george"
        elif database_name == "ds":
            secret_name = "prod/monitoring/redshift/admin/george"
        elif database_name == "Monitoring":
            secret_name = "Redshift/monitoring/user_code"
        elif database_name == "Analytics":
            secret_name = "Redshift/analytics/user_code"
        elif database_name == "Core":
            secret_name = "Redshift/core/user_code"
        else:
            print("Database name not recognized")
            raise Exception("Database name not recognized")

        self.secret_name = secret_name
        self.region_name = "us-east-1"
        self.dbparam = get_secret_str(secret_name, self.region_name)
        _db_secret = self.dbparam
        db_secret = _db_secret[0]

        print("Assigning connection..")
        print(
            f"Host: {db_secret['host']}, DB: {db_secret['dbname']}, User: {db_secret['username']}, Port: {db_secret['port']}"
        )
        con = pg.connect(
            host=db_secret["host"],
            dbname=db_secret["dbname"],
            port=db_secret["port"],
            user=db_secret["username"],
            password=db_secret["password"],
        )
        self.connection = con

    def close_connection(self):
        self.connection.close()

    @staticmethod
    def get_rs_account_info():
        sts_client = boto3.client("sts", region_name="us-east-1")
        try:
            caller_identity = sts_client.get_caller_identity()
            account_no = caller_identity["Account"]
            account_arn = caller_identity["Arn"]

            # print(f"AWS Account ID: {account_no}")
            # print(f"AWS Account ARN: {account_arn}")
            if account_no == "590183652635":
                account_name = "3vdev"
            elif account_no == "539247469204":
                account_name = "3vprod"
            elif account_no == "891377228241":
                account_name = "3vdevds"

            # print(f"Account Name: {account_name}")
            return account_no, account_name
        except ClientError as e:
            print(f"Error retrieving account information: {e}")
            raise e


def get_secret_str(secret_name, region_name):
    print("Getting secret..")
    client = boto3.client("secretsmanager", region_name=region_name)
    print("Secret_name: ", secret_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        account_no, account_name = RedshiftAccess.get_rs_account_info()
        dbparam = json.loads(secret)
        return dbparam, account_no, account_name
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        print(sys.exc_info())
        raise e


def assign_connection(database_name):
    RedshiftAccess._instance = None  # Force reset the connection
    RedshiftAccess(database_name)  # Reinitialize connection


def close_connection():
    if RedshiftAccess._instance is not None:
        print("Closing Redshift connection...")
        RedshiftAccess._instance.connection.close()
        RedshiftAccess._instance = None  # Ensure a fresh connection is made later


def rq(qq):
    df = None
    colnames = None
    try:
        cursor = RedshiftAccess._instance.connection.cursor()
        cursor.execute(qq)  # execute our Query
        colnames = [desc[0] for desc in cursor.description]  # get headers
        records = cursor.fetchall()  # retrieve the records from the database
        df = pd.DataFrame(records, columns=colnames)  # create pd df
    except pg.DatabaseError as error:
        print(error)
    cursor.close()
    return df


def action_rs(qq):
    t = "N/a"
    try:
        cursor = RedshiftAccess._instance.connection.cursor()
        cursor.execute(qq)  # execute our Query
        RedshiftAccess._instance.connection.commit()
        t = "action_rs action is complete"
    except pg.DatabaseError as error:
        print(error)
        t = "there was an error: " + str(error)
    cursor.close()
    return t


def alter_rs(qq):
    t = "N/a"
    try:
        # con.set_isolation_level('ISOLATION_LEVEL_AUTOCOMMIT')
        cursor = RedshiftAccess._instance.connection.cursor()
        cursor.execute(qq)  # execute our Query
        RedshiftAccess._instance.connection.commit()
        t = "alter_rs action is complete"
    except pg.DatabaseError as error:
        print(error)
        t = "there was an error: " + str(error)
    return t
