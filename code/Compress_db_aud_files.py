"""
Module: Compress_db_aud_files

A module for compressing audit files based on configuration settings into config.properties file.

Source *.aud files to be compressed is defined by local_dir_audit_files_by_db configuration parameter.

A result of this task is a file named as {month_str}-db-aud-files.tar.gz, with mont_str=%Y%m, for example:
    202405-db-aud-files.tar.gz

This compressed file example contains all may/2024 audit files and is saved into same local_dir_audit_files_by_db directory

Details of this task are logged into compress_db_aud_files-{logFileDateStr}.log file
With logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

Imports:
    - os
    - logging
    - datetime
    - Util_files.read_config
    - Util_files.writeScriptsChecksumInLog
    - Util_files.compressFiles
    - sys

Functions:
    1. compress_db_aud_files(force_month_str='')
        Compresses audit files based on configuration settings and optional force_month_str.

Usage Examples:
    1. Compress audit files based on default settings:
    >>> from compress_db_aud_files import compress_aud_files
    >>> compress_aud_files()
    'OK'

    2. Compress audit files for a specific month:
    >>> compress_db_aud_files("202405")
    'OK'

    3. Run the compress_aud_files module:
    >>> from compress_db_aud_files import main
    >>> main()
    OK

Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240510
"""

import os
import logging
import datetime
from Util_files import read_config, writeScriptsChecksumInLog, compressFiles
import sys

def compress_db_aud_files(force_month_str):
    #
    config = read_config(f'{os.path.dirname(os.path.abspath(__file__))}\\config.properties')
    #
    cbs_config = config['CBS_SERVER']
    subdirs = cbs_config['cbs_sub_dir_list_audit_files']
    #
    extractionConfig = config['EXTRACTION']
    old_files_in_days_to_be_extracted = int (extractionConfig['old_files_in_days_to_be_extracted'])
    #
    compress_config = config['COMPRESS']
    compress_aud_files_organized_by_db_n_months_after = int (compress_config['compress_aud_files_organized_by_db_n_months_after'])
    #
    local_server = config['LOCAL_SERVER']
    #destinyDir = local_server['local_dir_audit_files']
    localDirOrganizedByDB = local_server['local_dir_audit_files_by_db']
    local_dir_logs = local_server['local_dir_logs']
    local_dir_scripts = local_server['local_dir_scripts']
    #
    fileDate = datetime.datetime.now() - datetime.timedelta(days=old_files_in_days_to_be_extracted)
    #
    logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    log_filename = f'{local_dir_logs}/compress_db_aud_files_{logFileDateStr}.log'
    logging_defined_before = logging.getLogger().hasHandlers() 
    if not logging_defined_before:
        logging.basicConfig(filename= f'{log_filename}', level=logging.INFO, format='%(asctime)s\t%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    #
    if logging_defined_before:
        logging.info('=====================================================================')
    logging.info(f'Script running: {os.path.basename(__file__)}')
    logging.info(f"Month parameter: {force_month_str if force_month_str !='' else 'It will be calculated internally'}")
    #
    result = 'OK'
    if result == 'OK' and not logging_defined_before: 
        logging.info('======================Chesksum script files==========================')        
        result = writeScriptsChecksumInLog(local_dir_scripts)
    #        
    logging.info('===============Compress AUD files organized by DB ===================')
    #Compress AUD Files organized by DB
    if result == 'OK':
        fileMonthStr = fileDate.strftime('%Y%m')
        #fileMonthStr = '202406'
        month_str = int(fileMonthStr) - compress_aud_files_organized_by_db_n_months_after
        if force_month_str != '':
            month_str = force_month_str
        logging.info(f"Compress files by month: {month_str} {'(Calculated internally)' if force_month_str =='' else ''}")
        output_filename = f'{localDirOrganizedByDB}/{month_str}_db_aud_files.tar.gz'
        result = compressFiles(localDirOrganizedByDB, output_filename, month_str, subdirs)
    #
    return result


def main():
    #Call example: python.exe Compress_db_aud_files.py "202405"
    force_month_str = ''
    for i, arg in enumerate(sys.argv[1:], start=1):
        #  202405
        if i==1:
            force_month_str = arg 
    result = compress_db_aud_files(force_month_str)
    print(result)

if __name__ == '__main__':
    main()