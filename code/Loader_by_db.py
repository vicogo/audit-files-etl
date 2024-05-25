"""
Module: loader_by_db

A module for organizing audit files by database and loading them into the destination directory.

Source and destiny directory are defined in config.properties like next example:

    [LOCAL_SERVER]
    # Local directories
    local_dir_audit_files = /home/arcsight/auditCBS
    local_dir_audit_files_by_db =  /home/arcsight/auditCBS_Processed

In source directory (local_dir_audit_files) is expected subdirectories like this
    202405
        billdb-1-1-m-0
        billdb-1-1-m-1
        billsharedb-1-1-m-0
        billsharedb-1-1-m-1
        bmpdb-1-1-m-0
        bmpdb-1-1-m-1
        edrdb-1-1-m-0
        edrdb-1-1-m-1
        meddb-1-1-m-0
        meddb-1-1-m-1
        usrdb-1-1-m-0
        usrdb-1-1-m-1
        uvcdb-1-1-m-0
        uvcdb-1-1-m-1

The result in destiny directory (local_dir_audit_files_by_db) there are sub-directories named same as databases:
    billsharedb
    bmpdb
    edrdb
    meddb
    usrdb
    uvcdb

Imports:
    - os
    - logging
    - traceback
    - glob
    - re
    - shutil
    - datetime
    - Util_files.read_config
    - Util_files.deleteDirContent
    - Util_files.writeScriptsChecksumInLog
    - Util_files.getFilesQuantityInDir
    - sys

Functions:
    1. organizeAuditFilesbyDB(sourceDir, destinyDir, fileDateStr, subdirs)
    2. loader_by_db(force_fileDateStr='')

Usage Examples:
    1. Organize audit files by database and load them into the destination directory:
    >>> from loader_by_db import organizeAuditFilesbyDB
    >>> organizeAuditFilesbyDB("/source/dir", "/destination/dir", "20240504", "subdir1\nsubdir2")
    'OK'

    2. Run the loader_by_db module:
    >>> from loader_by_db import loader_by_db
    >>> loader_by_db("20240503")
    
    
Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240510
"""

import os
import logging
import traceback
import glob
import re
import shutil
import datetime
from Util_files import read_config, deleteDirContent, writeScriptsChecksumInLog, getFilesQuantityInDir
import sys

def organizeAuditFilesbyDB(sourceDir, destinyDir, fileDateStr, subdirs):
    logging.info(f'Start organizing AUD files by Database')
    result = 'OK'    
    try:
        fileCounter = 0    
        for filename in glob.glob('**', recursive=True, root_dir=sourceDir):
            sourcePath = os.path.join(sourceDir, filename)
            simpleFilename = os.path.basename(filename)
            subdir = os.path.dirname(filename)
            dbName = subdir.split('-')[0]
            destPath = f'{destinyDir}/{dbName}/{simpleFilename}'
            if os.path.isfile(sourcePath) and re.search(f'.*{fileDateStr}.*\\.aud', filename) :
                fileCounter = fileCounter + 1
                destFilename = f'{dbName}/{simpleFilename}'
                destDir = os.path.dirname(destPath)
                if not os.path.exists(destDir):
                    os.makedirs(destDir, exist_ok=True)
                    logging.info(f'Directory {destDir} created')
                shutil.copy(sourcePath, destPath)
                logging.info(f'{fileCounter}: {destFilename}')
        logging.info(f'Total files organized: {fileCounter}')
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        result = 'ERROR'                 
    return result

def loader_by_db(force_fileDateStr=''):
    #
    config = read_config(f'{os.path.dirname(os.path.abspath(__file__))}\\config.properties')
    #
    cbs_config = config['CBS_SERVER']
    subdirs = cbs_config['cbs_sub_dir_list_audit_files']
    #
    extractionConfig = config['EXTRACTION']
    old_files_in_days_to_be_extracted = int (extractionConfig['old_files_in_days_to_be_extracted'])
    delete_destiny_dir_content = extractionConfig['delete_destiny_dir_content']
    #
    local_server = config['LOCAL_SERVER']
    destinyDir = local_server['local_dir_audit_files']
    localDirOrganizedByDB = local_server['local_dir_audit_files_by_db']
    local_dir_logs = local_server['local_dir_logs']
    local_dir_scripts = local_server['local_dir_scripts']
    #
    logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    local_dir_logs = f'{local_dir_logs}/loader_by_db_{logFileDateStr}.log'
    logging_defined_before = logging.getLogger().hasHandlers() 
    if not logging_defined_before:
        logging.basicConfig(filename= f'{local_dir_logs}', level=logging.INFO, format='%(asctime)s\t%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    #
    if logging_defined_before:
        logging.info('=====================================================================')
    logging.info(f'Script running: {os.path.basename(__file__)}')
    logging.info(f'''Day parameter: {force_fileDateStr if force_fileDateStr !='' else 'It will be calculated internally'}''')
    #
    result = 'OK'
    if result == 'OK' and not logging_defined_before: 
        logging.info('======================Chesksum script files==========================')
        result = writeScriptsChecksumInLog(local_dir_scripts)
    #
    fileDate = datetime.datetime.now() - datetime.timedelta(days=old_files_in_days_to_be_extracted)
    fileDateStr = fileDate.strftime('%Y%m%d')
    #fileDateStr = '20240504'
    if force_fileDateStr != '':
        fileDateStr = force_fileDateStr    
    #        
    logging.info('==========================Organize by DB=============================') 
    logging.info(f'''Day: {fileDateStr} {'(Calculated internally)' if force_fileDateStr =='' else ''} ''')
    #    
    logging.info(f'Source directory: {destinyDir}/{fileDateStr}')         
    logging.info(f'Destiny directory: {localDirOrganizedByDB}')
    exceptfiles='\\.tar\\.gz'
    file_quantity_in_dir = getFilesQuantityInDir(localDirOrganizedByDB, fileDateStr, exceptfiles)    
    if result == 'OK' and delete_destiny_dir_content == '1' and file_quantity_in_dir > 0:
        result = deleteDirContent(localDirOrganizedByDB, fileDateStr, exceptfiles, justSubdirs=subdirs)    

    if result == 'OK':
       result = organizeAuditFilesbyDB(f'{destinyDir}/{fileDateStr}', localDirOrganizedByDB, fileDateStr, subdirs)
    #
    return result

def main():
    #Call example: python.exe Loader_by_db.py "20240503"
    force_fileDateStr = ''
    for i, arg in enumerate(sys.argv[1:], start=1):
        #  20240503
        if i==1:
            force_fileDateStr = arg       
    result = loader_by_db(force_fileDateStr)
    print(result)

if __name__ == '__main__':
    main()