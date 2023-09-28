'''
Script that tests the insert speed of multiple types of databases
Such as MySQL/MariaDB, MS SQL, PostgreSQL, etc.
Author Siggi Bjarnason Copyright 2023

Uses the following packages, which CheckDependency will try to install if missing
pip install pymysql
pip install pyodbc
pip install psycopg2
pip install wonderwords

'''

import random
import uuid
import subprocess
import sys
import os
import time
import platform
import wonderwords


def FetchEnv(strVarName):
    """
    Function that fetches the specified content of specified environment variable,
    converting nonetype to empty string.
    Parameters:
      strVarName: The name of the environment variable to be fetched
    Returns:
      The content of the environment or empty string
    """

    if os.getenv(strVarName) != "" and os.getenv(strVarName) is not None:
        return os.getenv(strVarName)
    else:
        return ""


def CheckDependency(Module):
    """
    Function that installs missing depedencies
    Parameters:
      Module : The name of the module that should be installed
    Returns:
      dictionary object without output from the installation.
        if the module needed to be installed
          code: Return code from the installation
          stdout: output from the installation
          stderr: errors from the installation
          args: list object with the arguments used during installation
          success: true/false boolean indicating success.
        if module was already installed so no action was taken
          code: -5
          stdout: Simple String: {module} version {x.y.z} already installed
          stderr: Nonetype
          args: module name as passed in
          success: True as a boolean
    """
    dictComponents = {}
    dictReturn = {}
    strModule = Module
    lstOutput = subprocess.run(
        [sys.executable, "-m", "pip", "list"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lstLines = lstOutput.stdout.decode("utf-8").splitlines()
    for strLine in lstLines:
        lstParts = strLine.split()
        dictComponents[lstParts[0].lower()] = lstParts[1]
    if strModule.lower() not in dictComponents:
        lstOutput = subprocess.run(
            [sys.executable, "-m", "pip", "install", strModule], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        dictReturn["code"] = lstOutput.returncode
        dictReturn["stdout"] = lstOutput.stdout.decode("utf-8")
        dictReturn["stderr"] = lstOutput.stderr.decode("utf-8")
        dictReturn["args"] = lstOutput.args
        if lstOutput.returncode == 0:
            dictReturn["success"] = True
        else:
            dictReturn["success"] = False
        return dictReturn
    else:
        dictReturn["code"] = -5
        dictReturn["stdout"] = "{} version {} already installed".format(
            strModule, dictComponents[strModule.lower()])
        dictReturn["stderr"] = None
        dictReturn["args"] = strModule
        dictReturn["success"] = True
        return dictReturn


def Conn(*, DBType, Server, Port, DBUser="", DBPWD="", Database=""):
    """
    Function that handles establishing a connection to a specified database
    imports the right module depending on database type
    Parameters:
      DBType : The type of database server to connect to
                  Supported server types are sqlite, mssql, mysql and postgres
      Server : Hostname for the database server
      Port   : The Port number the database server is listening on
      DBUser : Database username
      DBPWD  : Password for the database user
      Database  : The name of the database to use
    Returns:
      Connection object to be used by query function, or an error string
    """
    strDBType = DBType
    strServer = Server
    strDBUser = DBUser
    strDBPWD = DBPWD
    strInitialDB = Database
    strPort = Port

    if strServer == "":
        return "Servername can't be empty"

    try:
        if strDBType == "sqlite":
            import sqlite3
            strVault = strServer
            strVault = strVault.replace("\\", "/")
            if strVault[-1:] == "/":
                strVault = strVault[:-1]
            if strVault[-3:] != ".db":
                strVault += ".db"
            lstPath = os.path.split(strVault)
            if not os.path.exists(lstPath[0]):
                os.makedirs(lstPath[0])
            return sqlite3.connect(strVault)
    except dboErr as err:
        return ("SQLite Connection failure {}".format(err))

    try:
        if strDBType == "mssql":
            if not CheckDependency("pyodbc")["success"]:
                return "failed to install pyodbc. Please pip install pyodbc before using MS SQL option."
            import pyodbc as dbo
            if strDBUser == "":
                strConnect = (" DRIVER={{ODBC Driver 17 for SQL Server}};"
                              " SERVER={};"
                              " DATABASE={};"
                              " Trusted_Connection=yes;".format(strServer, strInitialDB))
            else:
                strConnect = (" DRIVER={{ODBC Driver 17 for SQL Server}};"
                              " SERVER={};"
                              " DATABASE={};"
                              " UID={};"
                              " PWD={};".format(strServer, strInitialDB, strDBUser, strDBPWD))
            return dbo.connect(strConnect)
        elif strDBType == "mysql":
            if not CheckDependency("pymysql")["success"]:
                return "failed to install pymysql. Please pip install pymysql before using mySQL option."
            import pymysql as dbo
            from pymysql import err as dboErr
            return dbo.connect(host=strServer, user=strDBUser, password=strDBPWD, db=strInitialDB)
        elif strDBType == "postgres":
            if not CheckDependency("psycopg2-binary")["success"]:
                return "failed to install psycopg2-binary. Please pip install psycopg2-binary before using PostgreSQL option."
            import psycopg2 as dbo
            return dbo.connect(host=strServer, user=strDBUser, port=strPort, password=strDBPWD, database=strInitialDB)
        else:
            return ("Unknown database type: {}".format(strDBType))
    except Exception as err:
        return ("Error: unable to connect: {}".format(err))


def Query(*, SQL, dbConn):
    """
    Function that handles executing a SQL query using a predefined connection object
    imports the right module depending on database type
    Parameters:
      SQL    : The query to be executed
      dbConn : The connection object to use
    Returns:
      NoneType for queries other than select, DBCursor object with the results from the select query
      or error message as a string
    """
    strSQL = SQL
    try:
        dbCursor = dbConn.cursor()
        dbCursor.execute(strSQL)
        if strSQL[:6].lower() != "select":
            dbConn.commit()
            return None
        else:
            return dbCursor
    except Exception as err:
        return "Failed to execute query: {}\n{}\nLength of SQL statement {}\n".format(err, strSQL[:255], len(strSQL))


def isInt(CheckValue):
    """
    Function checks if a value is an integer
    Parameters:
      CheckValue: String to be evaluated
    Returns:
      true/false
    """
    if isinstance(CheckValue, int):
        return True
    elif isinstance(CheckValue, str):
        if CheckValue.isnumeric():
            return True
        else:
            return False
    else:
        return False


def main():
    global strScriptName
    global strScriptHost
    global dbConn

    strScriptName = os.path.basename(sys.argv[0])
    strRealPath = os.path.realpath(sys.argv[0])
    strVersion = "{0}.{1}.{2}".format(
        sys.version_info[0], sys.version_info[1], sys.version_info[2])

    strScriptHost = platform.node().upper()

    print("This is a script to test insert speed of databases. This is running under Python Version {}".format(strVersion))
    print("Running from: {}".format(strRealPath))
    now = time.asctime()
    print("The time now is {}".format(now))

    strServer = FetchEnv("HOST")
    strInitialDB = FetchEnv("DB")
    strDBPWD = FetchEnv("DBPWD")
    strDBUser = FetchEnv("DBUSSER")
    strDBType = FetchEnv("DBTYPE")
    iIterations = FetchEnv("ITERATIONS")
    strPortNum = FetchEnv("PORT")

    if isInt(iIterations):
        iIterations = int(iIterations)
    else:
        print("Invalid interation value of {}".format(iIterations))
        sys.exit(1)

    dbConn = ""
    dbCursor = None
    print("establishing a connection to {} on {}".format(strDBType, strServer))
    dbConn = Conn(DBType=strDBType, Server=strServer, Port=strPortNum,
                  DBUser=strDBUser, DBPWD=strDBPWD, Database=strInitialDB)
    if isinstance(dbConn, str):
        print("Connection failed: {}".format(dbConn))
        sys.exit(1)

    strSQL = "create table if not exists perf(teststr varchar, uuid uuid);"
    dbCursor = Query(SQL=strSQL, dbConn=dbConn)
    if isinstance(dbCursor, str):
        print("Results is only the following string: {}".format(dbCursor))

    objRanWord = wonderwords.RandomWord()

    tStart = time.time()
    now = time.asctime()
    print("Starting the test at {}".format(now))

    for i in range(iIterations):
        print("On iteration {} of {}".format(i, iIterations), end="\r")
        strDataInsert = "INSERT INTO perf (teststr,uuid) VALUES('{}', '{}');".format(
            " ".join(objRanWord.random_words(random.randint(3, 15))), uuid.uuid4())
        dbCursor = Query(SQL=strDataInsert, dbConn=dbConn)
        if isinstance(dbCursor, str):
            print("Results is only the following string: {}".format(dbCursor))
            sys.exit(8)
        # else:
        #  print("Query complete.")

    tStop = time.time()
    iElapseSec = tStop - tStart
    iMin, iSec = divmod(iElapseSec, 60)
    iHours, iMin = divmod(iMin, 60)

    now = time.asctime()
    print("Completed at {}".format(now))
    print("Took {0:.2f} seconds to complete, which is {1} hours, {2} minutes and {3:.2f} seconds.".format(
        iElapseSec, iHours, iMin, iSec))
    print("{} completed successfully on {}".format(
        strScriptName, strScriptHost))


if __name__ == '__main__':
    main()
