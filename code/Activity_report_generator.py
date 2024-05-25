"""
Module: Activity_report_generator

Activity report generator module contains functions to check database audited activities described in database audit files (AUD). 

The database audited activity are: Insert, Update, Delete, Truncate and Drop

The main function is checkForAuditedActivities. This function parse all AUD files in a directory (parameter local_dir_audit_files_by_db defined in config.properties file) to search for audited activities.
All data from audited activities founded is saved in a report file named as activity_report-{logFileDateStr}.txt
logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

A content example of audit file (.aud) is (without hyphens):
------------------------------------------------------------------------------------------------------------------------------------------------------------
UTC-4:00 2024-04-17 03:00:02.926
LENGTH: "231"
SESSIONID:[3] "977" STMTID:[1] "0" USER:[4] "USR1" HOST:[11] "11.12.1.123" ACTION:[21] "PREP_EXEC[AUTOCOMMIT]" RETURNCODE:[8] "GS-00000" SQLTEXT:[79] " 
            
            truncate table SOME_TABLE_BYPASS01
        
        "

UTC-4:00 2024-04-17 03:00:04.278
LENGTH: "228"
SESSIONID:[3] "977" STMTID:[1] "0" USER:[4] "USR1" HOST:[11] "11.12.1.123" ACTION:[21] "PREP_EXEC[AUTOCOMMIT]" RETURNCODE:[8] "GS-00000" SQLTEXT:[76] " 
             alter table SOME_TABLE_RE  drop partition _SYS_P4460
        "

UTC-4:00 2024-04-17 06:33:48.564
LENGTH: "234"
SESSIONID:[2] "75" STMTID:[1] "0" USER:[15] "LONG_USER_NAME1" HOST:[9] "127.0.0.1" ACTION:[7] "PREPARE" RETURNCODE:[8] "GS-01001" SQLTEXT:[89] "UPDATE SCHEM1.SOME_TABLE1 SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163"

UTC-4:00 2024-04-17 06:34:52.057
LENGTH: "1990"
SESSIONID:[2] "75" STMTID:[1] "0" USER:[15] "LONG_USER_NAME1" HOST:[9] "127.0.0.1" ACTION:[7] "EXECUTE" RETURNCODE:[8] "GS-00000" SQLTEXT:[1843] "INSERT INTO SCHEM1.SOME_TABLE1 ...

UTC-4:00 2024-04-27 03:00:04.278
LENGTH: "257"
SESSIONID:[3] "977" STMTID:[1] "0" USER:[9] "GONZALESV" HOST:[11] "11.12.1.123" ACTION:[21] "PREP_EXEC[AUTOCOMMIT]" RETURNCODE:[8] "GS-00000" SQLTEXT:[97] " 
             alter table LONG_SCHEMA_DWH1.SOME_TABLE_OFF1 drop partition PRT_D_20240222
        "

UTC-4:00 2024-04-27 03:00:04.278
LENGTH: "214"
SESSIONID:[3] "977" STMTID:[1] "0" USER:[9] "GONZALESV" HOST:[11] "11.12.1.123" ACTION:[21] "PREP_EXEC[AUTOCOMMIT]" RETURNCODE:[8] "GS-00000" SQLTEXT:[55] "truncate table LONG_SCHEMA_NA1.LONG_TABLE_NAME_SCHEMAN1"
		
------------------------------------------------------------------------------------------------------------------------------------------------------------


An example of report file with all data from each audited activity is written as result of checkForAuditedActivities execution (without hyphens):
------------------------------------------------------------------------------------------------------------------------------------------------------------
27/04/2024 07:54:50 PM	================================Started================================
27/04/2024 07:54:50 PM	Fecha y Hora        	Usuario de BD	Hostname	Linea	Actividad	Schema	Table	Query	Archivo
27/04/2024 07:54:50 PM	2024-04-17 03:00:02.926	USR1	11.12.1.123	11	Truncate	USR1	SOME_TABLE_BYPASS01	truncate table SOME_TABLE_BYPASS01	PATH/AUD/FILES/file.aud/datos_prueba.aud
27/04/2024 07:54:50 PM	2024-04-17 03:00:04.278	USR1	11.12.1.123	19	Drop	USR1	SOME_TABLE_RE	alter table SOME_TABLE_RE  drop partition _SYS_P4460	PATH/AUD/FILES/file.aud/datos_prueba.aud
27/04/2024 07:54:50 PM	2024-04-17 06:34:52.057	LONG_USER_NAME1	127.0.0.1	29	Insert	SCHEM1	SOME_TABLE1	INSERT INTO SCHEM1.SOME_TABLE1 ...	PATH/AUD/FILES/file.aud/datos_prueba.aud
27/04/2024 07:54:50 PM	2024-04-27 03:00:04.278	GONZALESV	11.12.1.123	33	Drop	LONG_SCHEMA_DWH1	SOME_TABLE_OFF1	alter table LONG_SCHEMA_DWH1.SOME_TABLE_OFF1 drop partition PRT_D_20240222	PATH/AUD/FILES/file.aud/datos_prueba.aud
27/04/2024 07:54:50 PM	2024-04-27 03:00:04.278	GONZALESV	11.12.1.123	39	Truncate	LONG_SCHEMA_NA1	LONG_TABLE_NAME_SCHEMAN1	truncate table LONG_SCHEMA_NA1.LONG_TABLE_NAME_SCHEMAN1	PATH/AUD/FILES/file.aud/datos_prueba.aud
------------------------------------------------------------------------------------------------------------------------------------------------------------

Imports:
    - os
    - logging
    - configparser
    - re
    - datetime
    - Util_string.getAuditedActivity
    - Util_string.getDataBetween
    - Util_string.getObjectNameFromQuery
    - Util_files.writeScriptsChecksumInLog
    - Util_files.read_config
    - traceback
    - glob
    - sys

Functions:
    1. getUserDB(line)
    2. getHost(line)
    3. getTable(objectName)
    4. getSchema(objectName, userDB)
    5. getDate(dateLine)
    6. getQuery(line)
    7. checkForAuditedActivities(dir, month_str='', exceptfiles='', add_summary_report='0')
    8. activity_report_generator(month_str)
    9. main()

Usage Examples:
    1. Checking for audited activities in a directory:
    >>> from database_audit_etl import checkForAuditedActivities
    >>> checkForAuditedActivities("/path/to/directory")
    'OK'

    2. Generating an activity report for a specific month:
    >>> from database_audit_etl import activity_report_generator
    >>> activity_report_generator("202405")
    'OK'

Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240512
"""

import os
import logging, configparser
import re
import datetime
from Util_string import getAuditedActivity, getDataBetween, getObjectNameFromQuery
from Util_files import writeScriptsChecksumInLog, read_config
import traceback
import glob
import sys

def getUserDB(line):
    # SESSIONID:[2] "75" STMTID:[1] "0" USER:[15] "LONG_USER_NAME1" HOST:[9] "127.0.0.1" ACTION:[7] "PREPARE" 
    return getDataBetween(line, "USER:", "HOST:", "]")

def getHost(line):
    return getDataBetween(line, "HOST:", "ACTION:", "]")

def getTable(objectName):
    result = objectName
    n = objectName.find('.') 
    if n >= 0:
        result = result.split(".")[1].split(" ")[0].strip()
    return result

def getSchema(objectName, userDB):
    # UPDATE SCHEM1.SOME_TABLE1 SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163
    result = objectName
    n = result.find('.') 
    if n >= 0:
        result = result.split(".")[0].split(' ')[0].strip()
    else:
        result = userDB
    return result

def getDate(dateLine):
    result = dateLine.split(":00 ")[1].strip()
    return result

def getQuery(line):
    """
    Extracts SQL statement from a string like this
        SESSIONID:[2] "75" STMTID:[1] "0" USER:[15] "LONG_USER_NAME1" HOST:[9] "127.0.0.1" ACTION:[7] "PREPARE" RETURNCODE:[8] "GS-01001" SQLTEXT:[89] "UPDATE SCHEM1.SOME_TABLE1 SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163"

    Args:
        line (str): The line containing the SQL statement.

    Examples:
        >>> getQuery(line)
        UPDATE SCHEM1.SOME_TABLE1 SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163
    """        
    query = ''
    n = line.find('SQLTEXT:') 
    if n >= 0:
        querytmp= line.split("SQLTEXT:",1)[1] #[89] "UPDATE SCHEM1.SOME_TABLE1 SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163"
        cantidadCar = querytmp.split("[")[1].split("]")[0] #89
        cantidadCar = int(cantidadCar) + 2
        query = querytmp[-cantidadCar:] # "UPDATE SCHEM1.SOME_TABLE1 SET RECHARGE_AMT=10000 WHERE RECHARGE_LOG_ID=138300010024049163"
        if "[" in query:
            query = "null|"+str(cantidadCar)
        else:
            query = query+"|"+str(cantidadCar)
    else:
        query =  'null|0'	
    return query


def checkForAuditedActivities(dir, month_str='',exceptfiles='', add_summary_report='0'):
    """
    Reads all AUD files in a directory to search for audited activities.
    All data from audited activities founded is saved in logging file that was defined by invoker
    
    Args:
        dir (str): The directory path containing the files to analyze.

    Result:
        In console, for each AUD file analized where audited activities were found a message like this is showed
        Existen 5 actividades auditadas en el archivo PATH/AUD/FILES/file.aud/datos_prueba.aud
        On the other hand, all data from each audited activity founded is saved in logging file that was defined by invoker in this format
        time + "\t" + userDB+ "\t" + host + "\t" + str(queryLineNumber) + "\t" + auditedActivity + "\t" + schema + "\t" + table + "\t" + query + "\t" + file

    Examples:
        1.- In console
        >>> checkForAuditedActivities("/path/to/directory")
        Existen 5 actividades auditadas en el archivo PATH/AUD/FILES/file.aud/datos_prueba.aud

        2.- In logging file all data from each audited activity is written 
        27/04/2024 07:54:50 PM	================================Started================================
        27/04/2024 07:54:50 PM	Fecha y Hora        	Usuario de BD	Hostname	Linea	Actividad	Schema	Table	Query	Archivo
        27/04/2024 07:54:50 PM	2024-04-17 03:00:02.926	USR1	11.12.1.123	11	Truncate	USR1	SOME_TABLE_BYPASS01	truncate table SOME_TABLE_BYPASS01	PATH/AUD/FILES/file.aud/datos_prueba.aud
        27/04/2024 07:54:50 PM	2024-04-17 03:00:04.278	USR1	11.12.1.123	19	Drop	USR1	SOME_TABLE_RE	alter table SOME_TABLE_RE  drop partition _SYS_P4460	PATH/AUD/FILES/file.aud/datos_prueba.aud
        27/04/2024 07:54:50 PM	2024-04-17 06:34:52.057	LONG_USER_NAME1	127.0.0.1	29	Insert	SCHEM1	SOME_TABLE1	INSERT INTO SCHEM1.SOME_TABLE1 ...	PATH/AUD/FILES/file.aud/datos_prueba.aud
        27/04/2024 07:54:50 PM	2024-04-27 03:00:04.278	GONZALESV	11.12.1.123	33	Drop	LONG_SCHEMA_DWH1	SOME_TABLE_OFF1	alter table LONG_SCHEMA_DWH1.SOME_TABLE_OFF1 drop partition PRT_D_20240222	PATH/AUD/FILES/file.aud/datos_prueba.aud
        27/04/2024 07:54:50 PM	2024-04-27 03:00:04.278	GONZALESV	11.12.1.123	39	Truncate	LONG_SCHEMA_NA1	LONG_TABLE_NAME_SCHEMAN1	truncate table LONG_SCHEMA_NA1.LONG_TABLE_NAME_SCHEMAN1	PATH/AUD/FILES/file.aud/datos_prueba.aud
    """
    #title in log file
    logging.info("Fecha y Hora        " + "\tUsuario de BD" + "\tHostname" + "\tLinea" + "\tActividad" + "\tSchema" + "\tTable" + "\tQuery" + "\tArchivo")
    result = 'OK'   
    current_filename = ''
    current_line_number = 0
    current_line = ''
    audited_activities_counter = 0
    summary_report = [] 
    try:
        for name in sorted(glob.glob('**', recursive=True, root_dir=dir)):
            
        #for root, dirs, ficheros in os.walk(dir):
            #for name in dirs:
            #      print("Directorio", os.path.join(root, name))
            #for name in sorted(ficheros):
                if  not re.search(f'.*{exceptfiles}.*', name) and re.search(f'.*{month_str}.*\\.aud', name):
                    #file = os.path.join(root, name)
                    current_filename = name
                    file = os.path.join(dir, name)
                    with open(file) as myfile:
                        n = 0
                        audited_activities_counter = 0
                        host = ''
                        userDB = ''
                        time = ''
                        query = ''
                        schema = ''
                        table = ''
                        queryLine = ''
                        queryLineNumber = 0
                        restarVars = False
                        queryEnVariasLineas = False
                        for line in myfile:
                            n = n + 1
                            current_line_number = n
                            current_line = line
                            if 'UTC-4:' in line:
                                fecha = line
                            elif 'LENGTH:' in line:
                                longitud = line
                            elif 'RETURNCODE:[8] "GS-00000"' in line and ('PREP_EXEC' in line or 'EXECUTE' in line):
                            #elif  ('PREPARE' in line or 'PREP_EXEC' in line or 'EXECUTE' in line):
                                queryLine = line
                                queryLineNumber = n
                                query = getQuery(line)
                                queryCantidadCaracteres = query.split("|")[1]
                                query = query.split("|")[0]
                                queryEnVariasLineas = (query == 'null') 
                                if queryEnVariasLineas:
                                    query = ''
                            elif queryEnVariasLineas:
                                query = query + line
                                if '"' in query:
                                    queryEnVariasLineas = False
                            elif line.strip() == '':
                                #Reset variables
                                restarVars = True
                            # If audited activity is founded then logging all related data
                            if not queryEnVariasLineas and query != '':
                                query = query.replace('"', '').strip()
                                auditedActivity = getAuditedActivity(query)
                                if auditedActivity != "":
                                    audited_activities_counter = audited_activities_counter + 1
                                    userDB = getUserDB(queryLine)
                                    host = getHost(queryLine)
                                    objectName = getObjectNameFromQuery(query, auditedActivity)
                                    schema = getSchema(objectName, userDB)
                                    table = getTable(objectName)
                                    time = getDate(fecha)
                                    logging.info( time + "\t" + userDB+ "\t" + host + "\t" + str(queryLineNumber) + "\t" + auditedActivity + "\t" + schema + "\t" + table + "\t" + query + "\t" + file )
                                #Reset variables
                                restarVars = True
                            #
                            if restarVars:
                                host = ''
                                userDB = ''
                                time = ''
                                query = ''
                                schema = ''
                                table = ''
                                queryLine = ''
                                queryLineNumber = 0
                                restarVars = False
                                queryEnVariasLineas = False
                        #if audited_activities_counter > 0:
                        #    print("Existen", audited_activities_counter, "actividades auditadas en el archivo", file)   
                    sumary_report_file = [audited_activities_counter, current_line_number, current_filename]
                    summary_report.append(sumary_report_file)
        if add_summary_report == '1':
            logging.info("================ Summary report ===================")
            logging.info("Nro archivo \t Nro actividades \t Nro lineas revisadas \t Archivo")
            counter = 0
            for summary in summary_report:
                counter = counter +1
                logging.info(str('{:0=4}'.format(counter)) + "\t" + str('{:0=3}'.format(summary[0])) + "\t" + str('{:0=6}'.format(summary[1])) + "\t" + str(summary[2]))
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        logging.error(f'Error ocurred when processing file {current_filename} in line {current_line_number}:')
        logging.error(f'{current_line}')
        result = 'ERROR'                 
    return result 

def activity_report_generator(month_str):
    #
    config = read_config(f'{os.path.dirname(os.path.abspath(__file__))}\\config.properties')
    #
    local_server = config['LOCAL_SERVER']
    local_dir_reports = local_server['local_dir_reports']
    localDirOrganizedByDB = local_server['local_dir_audit_files_by_db']
    local_dir_scripts = local_server['local_dir_scripts']
    #
    activity_report_config = config['ACTIVITY_REPORT']
    add_summary_report = activity_report_config['add_summary_report']
    #
    logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    log_filename = f'{local_dir_reports}/activity_report_{logFileDateStr}.txt'
    logging_defined_before = logging.getLogger().hasHandlers() 
    if not logging_defined_before:
        logging.basicConfig(filename= f'{log_filename}', level=logging.INFO, format='%(asctime)s\t%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')    
    # Monthly report
    #month_str = '202405'    
    #
    logging.info(f'Script running: {os.path.basename(__file__)}')    
    logging.info(f'Month parameter : {month_str}') 
    #
    result = 'OK'
    logging.info('======================Chesksum script files==========================')
    if result == 'OK': 
        result = writeScriptsChecksumInLog(local_dir_scripts)
    #
    logging.info('================Start auditing database activities ===================') 

    if result == 'OK':
        if month_str.strip() != '' : 
            logging.info(f'Activity report using audit files from month: {month_str}') 
            logging.info(f'Source directory: {localDirOrganizedByDB}')        
            result = checkForAuditedActivities(localDirOrganizedByDB, month_str, '\\.tar\\.gz', add_summary_report )
        else:
            logging.info(f'Month to process must be sent as parameter using format YYYYMM')
            result = 'ERROR'
    return result

def main():
    #Call example: python.exe Activity_report_generator.py "202405"
    month_str = ''
    for i, arg in enumerate(sys.argv[1:], start=1):
        #  202405
        if i==1:
            month_str = arg           
    result = activity_report_generator(month_str)    
    print (result)

if __name__ == '__main__':
    main()