"""
Util module contains utilities functions that helps to parse database audit files (AUD). 


Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240510
"""

import re


def getAuditedActivity(line):
    """
    Determines the audited activity mentioned in the given line.

    Args:
        line (str): The line containing the SQL statement.

    Returns:
        str: The audited activity if found ('Insert', 'Update', 'Delete', 'Truncate', 'Drop').
             If no audited activity is found, an empty string is returned.

    Examples:
        >>> getAuditedActivity("INSERT INTO table_name VALUES (value1, value2)")
        'Insert'
        >>> getAuditedActivity("UPDATE table_name SET column_name = value WHERE condition")
        'Update'
        >>> getAuditedActivity("DELETE FROM table_name WHERE condition")
        'Delete'
        >>> getAuditedActivity("TRUNCATE TABLE table_name")
        'Truncate'
        >>> getAuditedActivity("DROP TABLE table_name")
        'Drop'
        >>> getAuditedActivity("SELECT * FROM table_name")
        ''
    """
    line = line.lower()
    activity = ''
    if 'insert ' in line:
        activity = 'Insert'
    elif 'update ' in line:
        activity = 'Update'
    elif 'delete ' in line:
        activity = 'Delete'
    elif 'truncate ' in line:
        activity = 'Truncate'
    elif 'drop ' in line:
        activity = 'Drop'
    return activity


def getNextData(line, toGetNextWordFrom):
    """
    Retrieves the next word after the specified word in the given line.

    Args:
        line (str): The input line to search for the next word.
        toGetNextWordFrom (str): The word to search for in the line.

    Returns:
        str: The next word after the specified word in the line.

    Examples:
        >>> getNextData("The quick brown fox jumps over the lazy dog", "quick")
        'brown'
        >>> getNextData("The quick brown fox jumps over the lazy dog", "lazy")
        'dog'
        >>> getNextData("The quick brown fox jumps over the lazy dog", "jumps")
        'over'
    """    
    try:
        result = re.split(toGetNextWordFrom, line, flags=re.IGNORECASE)[1].split(" ")[0]
    except Exception as e:
        result=''    
    return result

def getDataBetween(line, initialStr, endStr, toGetNextWordFrom=""):
    """
    Extracts data between two specified strings in the given line.

    Args:
        line (str): The input line containing the data.
        initialStr (str): The initial string marking the start of the data.
        endStr (str): The end string marking the end of the data.
        toGetNextWordFrom (str, optional): The word to search for in the extracted data.

    Returns:
        str: The data between the initial and end strings.

    Examples:
        >>> getDataBetween("The quick brown fox jumps over the lazy dog", "quick", "dog")
        'brown fox jumps over the lazy'
        >>> getDataBetween("The quick brown fox jumps over the lazy dog", "quick", "lazy", "jumps")
        'over the'
        >>> getDataBetween("The quick brown fox jumps over the lazy dog", "quick", "fox", "brown")
        'jumps'
    """   
    try: 
        result1 = re.split(initialStr, line, flags=re.IGNORECASE)[1]
        result = re.split(endStr,result1, flags=re.IGNORECASE)[0]
        if result == result1:
            # it means there is no endStr
            result = ''
        else:
            if toGetNextWordFrom != "":
                result = result.split(toGetNextWordFrom)[1]
            result = result.replace('"', '').strip()
    except Exception as e:
        result=''       
    return result

def getObjectNameFromQuery(query, act):
    """
    Extracts the object name from the given SQL query based on the specified action.

    Args:
        query (str): The SQL query from which to extract the object name.
        act (str): The action to determine how to extract the object name ('Insert', 'Update', 'Delete', 'Truncate', 'Drop').

    Returns:
        str: The name of the object extracted from the query.

    Examples:
        >>> getObjectNameFromQuery("INSERT INTO BILLDB.RB_RECHARGE (RECHARGE_LOG_ID, RECHARGE_CODE, RECHARGE_AMT) VALUES (138300010024049163, 'TOPUP', 10000)", "Insert")
        'BILLDB.RB_RECHARGE'
        >>> getObjectNameFromQuery("UPDATE BILLDB.RB_RECHARGE SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163", "Update")
        'BILLDB.RB_RECHARGE'
        >>> getObjectNameFromQuery("DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste'", "Delete")
        'Customers'
        >>> getObjectNameFromQuery("TRUNCATE TABLE table_name", "Truncate")
        'table_name'
        >>> getObjectNameFromQuery("DROP TABLE Customers", "Drop")
        'Customers'
    """    
    result = ""
    sqlKeyWord = ""
    #INSERT INTO BILLDB.RB_RECHARGE (RECHARGE_LOG_ID, RECHARGE_CODE, RECHARGE_AMT, ...
    if 'Insert' in act:
        result = getNextData(query, "INTO ")
        if result == '':
            result = getDataBetween(query, "GRANT INSERT ", " TO")
    elif 'Update' in act:
        # UPDATE BILLDB.RB_RECHARGE SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163
        result = getDataBetween(query, "UPDATE ", " SET")
        if result == '':
            result = getDataBetween(query, "GRANT UPDATE ", " TO")
    elif 'Delete' in act:
        # 'DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste'
        result = getNextData(query, "FROM ")
        if result == '':
            result = getDataBetween(query, "GRANT DELETE ", " TO")
    elif 'Truncate' in act:
        #TRUNCATE TABLE table_name;
        result = getNextData(query, "TABLE ")        
    elif 'Drop' in act:
        if "DROP TABLE IF EXISTS" in query.upper():
            #DROP TABLE IF EXISTS table_name; 
            result = getNextData(query, "EXISTS ")
        if 'DROP TABLE' in query.upper():
            #DROP TABLE Customers
            result = getNextData(query, "TABLE ")
        elif 'ALTER TABLE ONLY' in query.upper():
            #ALTER TABLE [ ONLY ] table DROP CONSTRAINT constraint { RESTRICT | CASCADE }            
            result = getDataBetween(query, "ONLY ", " DROP")
        elif 'ALTER TABLE' in query.upper():
            #ALTER TABLE Customers DROP COLUMN ContactName;    
            result = getDataBetween(query, "TABLE ", " DROP")
        elif 'DROP DATABASE' in query.upper():
            #DROP DATABASE testDB
            result = getNextData(query, "DATABASE ")
        elif 'DROP SCHEMA IF EXISTS' in query.upper():
            #DROP SCHEMA [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]
            result = getNextData(query, "EXISTS ")            
        elif 'DROP SCHEMA' in query.upper():
            result = getNextData(query, "SCHEMA ")     
        elif 'DROP INDEX' in query.upper():
            #DROP INDEX [ CONCURRENTLY ] [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]
            result = query.replace("DROP INDEX ","").replace("CONCURRENTLY ", "").replace("IF EXISTS ","").strip()
            result = result.split(" ")[0]
        elif 'DROP USER' in query.upper():
            result = getNextData(query, "USER ")

    #
    return result.strip()
