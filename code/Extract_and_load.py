"""
Module: Extract_and_load

This module provides functionalities to extract, load, compress, and send audit database files.

Usage:

    - To run the main function with optional arguments:
        $ python.exe Extract_and_load.py "extract_date" "compress_aud_month" "compress_db_aud_month"

    - Extract_and_load.py script accepts three optional arguments:
        1. "extract_date": Date in the format "YYYYMMDD" for data extraction.
        2. "compress_aud_month": Month in the format "YYYYMM" for compressing audit files.
        3. "compress_db_aud_month": Month in the format "YYYYMM" for compressing database audit files.
    
    - As result a string 'OK' or 'ERROR' is written into console
    
    - More processing details are in  extract_and_load-{logFileDateStr}.log file into logs directory (local_dir_logs parameter in config.properties) . 
      logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
      For example: extract_and_load-20240510145357630753.log

Example, Script Call from Python:

    >>> from Extract_and_load import extract_and_load
    >>> extract_and_load("20240509", "202403", "202404")
    OK

Example 1, Script Call from console: 

    $ python.exe Extract_and_load.py "20240509" "202403" "202404"
    OK

Example 2, Script Call from console: Day and month will be calculated by module according [EXTRACTION] and [COMPRESS] configuration section defined in config.properties

    $ python.exe Extract_and_load.py
    OK

Dependencies:

    - Extractor module: Provides functionality for data extraction.
    - Loader_by_db module: Provides functionality for loading data into local_dir_audit_files_by_db directory.
    - Compress_aud_files module: Provides functionality for compressing audit files to save tar files into local_dir_audit_files directory.
    - Compress_db_aud_files module: Provides functionality for compressing database audit files to save tar files into local_dir_audit_files_by_db directory.
    - Util_files module: Provides utility functions for reading configuration and writing checksum in logs.

Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240512    
"""
from Extractor import extractor
from Loader_by_db import loader_by_db
from Compress_aud_files import compress_aud_files
from Compress_db_aud_files import compress_db_aud_files 
from Util_files import read_config, writeScriptsChecksumInLog
import datetime
import logging
import traceback
import os
import sys
#
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def send_mail(send_from, send_to, subject, text, files=None,server=""):  
	msg = MIMEMultipart()
	msg['From'] = send_from
	msg['To'] = send_to
	msg['Date'] = formatdate(localtime=True)
	msg['Subject'] = subject
	msg.attach(MIMEText(text))
	for f in files:
		with open(f, "rb") as fil:
			part = MIMEApplication(fil.read(),Name=basename(f))
		# After the file is closed
		part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
		msg.attach(part)
	smtp = smtplib.SMTP(server)
	smtp.sendmail(send_from, send_to, msg.as_string())
	smtp.close()

def prepare_and_send_mail(config,result_acumulator,log_filename):
    result = 'OK'
    try:
        mail_config= config['MAIL']
        mailFrom = mail_config['mailFrom']
        mailTo = mail_config['mailTo']
        mailSubject = mail_config['mailSubject']
        mailBody = result_acumulator
        mailServer = mail_config['mailServer']
        mailPort = mail_config['mailPort']

        send_mail(mailFrom, mailTo, mailSubject, mailBody, [log_filename], mailServer)
        logging.info('Email sent')        
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        result = 'ERROR'         
        logging.error('Error triying sent and email')
    return result   

def extract_and_load(extract_date='', compress_aud_month='', compress_db_aud_month='' ):
    """
    Function: extract_and_load

    Extracts, loads, compresses, and sends audit database files.

    Args:
        extract_date (str): Date in the format "YYYYMMDD" for data extraction. Defaults to ''.
        compress_aud_month (str): Month in the format "YYYYMM" for compressing audit files. Defaults to ''.
        compress_db_aud_month (str): Month in the format "YYYYMM" for compressing database audit files. Defaults to ''.

    Returns:
        str: Execution result ('OK' or 'ERROR').

    Example:
        >>> from your_module import extract_and_load
        >>> extract_and_load("20240509", "202403", "202404")
        'OK'
    """    
    config = read_config(f'{os.path.dirname(os.path.abspath(__file__))}\\config.properties')
    #
    local_server = config['LOCAL_SERVER']
    local_dir_logs = local_server['local_dir_logs']   
    local_dir_scripts = local_server['local_dir_scripts'] 
    #
    logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    log_filename = f'{local_dir_logs}/extract_and_load_{logFileDateStr}.log'
    logging_defined_before = logging.getLogger().hasHandlers() 
    if not logging_defined_before:
        logging.basicConfig(filename= f'{log_filename}', level=logging.INFO, format='%(asctime)s\t%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')         
    #
    logging.info(f'Script running: {os.path.basename(__file__)}')
    logging.info(f'''extract_date parameter: {extract_date if extract_date !='' else 'It will be calculated internally'}''')
    logging.info(f'''compress_aud_month parameter: {compress_aud_month if compress_aud_month !='' else 'It will be calculated internally'}''')
    logging.info(f'''compress_db_aud_month parameter: {compress_db_aud_month if compress_db_aud_month !='' else 'It will be calculated internally'}''')
    #
    result = 'OK'
    result_acumulator = f'Execution result:\n\n'
    logging.info('======================Chesksum script files==========================')
    if result == 'OK' and not logging_defined_before: 
        result = writeScriptsChecksumInLog(local_dir_scripts)
        result_acumulator = result_acumulator + f'\t writeScriptsChecksumInLog(..): {result}\n'  

    if result == 'OK': 
        result = extractor(extract_date)
        result_acumulator = result_acumulator + f'\t extractor(): {result}\n' 
    #
    if result == 'OK':
        result = loader_by_db(extract_date)
        result_acumulator = result_acumulator + f'\t loader_by_db(): {result}\n'
    #

    if result == 'OK':
        result = compress_aud_files(compress_aud_month)
        result_acumulator = result_acumulator + f'\t compress_aud_files():{result}\n'
    #
    if result == 'OK':
        result = compress_db_aud_files(compress_db_aud_month)
        result_acumulator = result_acumulator + f'\t compress_db_aud_files():{result}\n'

    result = prepare_and_send_mail(config,result_acumulator, log_filename)
    return result

def main():
    #Call example: python.exe Extract_and_load.py "20240509" "202403" "202404" 
    extract_date = ''
    compress_aud_month = ''
    compress_db_aud_month = ''
    for i, arg in enumerate(sys.argv[1:], start=1):
        #  20240509
        if i==1:
            extract_date = arg 
        #  202403    
        if i==2:
            compress_aud_month = arg    
        #  202404     
        if i==3:
            compress_db_aud_month = arg     
    result = extract_and_load(extract_date, compress_aud_month, compress_db_aud_month)
    print (result)

if __name__ == '__main__':
    main()