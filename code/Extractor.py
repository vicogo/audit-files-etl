"""
Module: Extractor

A module for extracting audit files (*.aud) from external server, copying them locally, and generating checksums.

Background:
	Original database audit files: AUD files
	Files are in one directory in some server, for each CBS node
	
Algorithm:
    Calculate audit file date to extract files
    If exists, delete files in local directory: aud-extract
    Copy audit files from external server, put this files in local directory: aud-extract
    Calculate checksum files from directory- aud-extract

Additional details:
    In remote server in a directory defined (see config.properties) there is a main directory that contains sub-directories like this:
    
    auditlogs
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
    
    In each directory there are audit files named as like next example:
        zengine_20240503134936581.aud
        zengine_20240504012422459.aud
        ..
    
    The result of extraction will be a new directory (named using format YYYYMMDD) created into a base directory (see config.properties). 
    This new directory will have all files copied from one day
      
    auditCBS
        20240503
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
        20240504
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
        ..          

config.properties
    In this file there is a section for remote server like next example:

    [CBS_SERVER]
    host = 10.24.0.30
    port = 22
    user = auditfiles
    password = XXXYYZZZ
    cbs_base_dir_audit_files = /auditlogs
    cbs_sub_dir_list_audit_files = 
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
    
    As well, in this file there is a section for local server like next example

    [LOCAL_SERVER]
    # Local directories
    local_dir_audit_files = /home/arcsight/auditCBS
    local_dir_audit_files_by_db =  /home/arcsight/auditCBS_Processed
    local_dir_scripts =   /root/Scripts
    local_dir_logs =   /root/Scripts/logs
    local_dir_reports = /root/Scripts/reports    

    
Imports:
    - shutil
    - os
    - logging
    - datetime
    - glob
    - re
    - SCPClient (from scp)
    - traceback
    - hashlib
    - Util_files.deleteDirContent
    - Util_files.createSSHClient
    - Util_files.read_config
    - Util_files.createAudFilesInLocalDir
    - Util_files.writeScriptsChecksumInLog
    - Util_files.getFilesQuantityInRemoteDir
    - Util_files.getParamikoSSHCLient
    - Util_files.getFilesQuantityInDir
    - configparser
    - sys

Functions:
    1. copyAudFilesFromExternalServer(sourceDir, destinyDir, fileDateStr, subdirs, server='localhost', port='', user='', password='')
    2. checksumFiles(destinyDir, fileDateStr, generate_checksum_files='0', generate_chesksum_log='1')
    3. extractor(force_fileDateStr='')

Usage Examples:
    1. Copy audit files from an external server to a local directory:
    >>> from aud_file_extractor import copyAudFilesFromExternalServer
    >>> copyAudFilesFromExternalServer("/source/dir", "/destination/dir", "202405", "subdir1\nsubdir2", "server", "port", "user", "password")
    'OK'

    2. Calculate checksums for audit files in a directory:
    >>> from aud_file_extractor import checksumFiles
    >>> checksumFiles("/destination/dir", "202405", generate_checksum_files='1', generate_chesksum_log='1')
    'OK'

    3. Extract audit files from an external server and generate checksums:
    >>> from aud_file_extractor import extractor
    >>> extractor("202405")
    'OK'



Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240512
"""
import shutil
import os
import logging
import datetime
import glob
import re
from scp import SCPClient
import traceback
import hashlib
from Util_files import deleteDirContent, createSSHClient, read_config, createAudFilesInLocalDir, writeScriptsChecksumInLog, getFilesQuantityInRemoteDir, getParamikoSSHCLient, getFilesQuantityInDir
import configparser
import sys

def copyAudFilesFromExternalServer(sourceDir,destinyDir, fileDateStr, subdirs, server='localhost', port='', user='', password='' ):
    result = 'OK'
    logging.info(f'Start copying files from server {server}, directory {sourceDir} to local directory {destinyDir}/{fileDateStr}')
    fileCounter = 0
    try:    
        if server == 'localhost':
            for filename in glob.glob('**', recursive=True, root_dir=sourceDir):
                destPath = os.path.join(destinyDir, filename)
                sourcePath = os.path.join(sourceDir, filename)
                if os.path.isfile(sourcePath) and re.search(f'.*{fileDateStr}.*\\.aud', filename):
                    fileCounter = fileCounter + 1
                    destDir = os.path.join(destinyDir, os.path.dirname(filename))
                    if not os.path.exists(destDir):
                        os.makedirs(destDir, exist_ok=True)
                        logging.info(f'Directory {destDir} created')
                    shutil.copy(sourcePath, destPath)
                    logging.info(f'{fileCounter}: {filename}')
        else:
            ssh = createSSHClient(server, port, user, password)
            paramikoSSHCLient =  getParamikoSSHCLient(server, port, user, password)
            with SCPClient(ssh.get_transport(), sanitize=lambda x: x) as scp:
                for subdir in  subdirs.split('\n'):
                    if subdir != '':
                        #If directory does not exists then it will be created
                        destinyPathDate= f'{destinyDir}/{fileDateStr}/{subdir}'
                        if not os.path.exists(destinyPathDate):
                            createAudFilesInLocalDir (destinyPathDate)
                        file_counter_destiny_directory = len([name for name in os.listdir(destinyPathDate) if os.path.isfile(os.path.join(destinyPathDate, name))])                            
                        if file_counter_destiny_directory ==0:
                            #file_quantity_in_source_dir = getFilesQuantityInRemoteDir(server, port, user, password, f'{sourceDir}/{subdir}', fileDateStr)
                            file_quantity_in_source_dir = getFilesQuantityInRemoteDir(paramikoSSHCLient, f'{sourceDir}/{subdir}', fileDateStr)
                            logging.info (f'File quantity in source directory {subdir}: {file_quantity_in_source_dir}')
                            #This validation is neccesary to avoid error in scp.get command
                            if file_quantity_in_source_dir > 0:
                                scp.get(remote_path=f'{sourceDir}/{subdir}/*{fileDateStr}*.aud', local_path=f'{destinyPathDate}/.')                            
                                for filename in os.listdir(destinyPathDate):
                                    if os.path.isfile(os.path.join(destinyPathDate, filename)):
                                        fileCounter = fileCounter + 1
                                        logging.info(f'{fileCounter}: {filename}')
                                fileCounter = fileCounter - file_counter_destiny_directory 
                        else:
                          logging.info(f'The directory {fileDateStr}/{subdir} has {file_counter_destiny_directory} files. Copy cannot by realized.')
            ssh.close()
            paramikoSSHCLient.close()
        logging.info(f'Total files copied: {fileCounter}')
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'server={server}, port={port}')
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        result = 'ERROR'
    return result

def checksumFiles(destinyDir, fileDateStr, generate_checksum_files='0', generate_chesksum_log='1'):
    result = 'OK'
    fileCounter = 0
    logging.info(f'Start calculating checksum files from {destinyDir}')    
    if not os.path.exists(destinyDir):
        logging.error(destinyDir+" directory is not existing");
        result = 'ERROR'
    else:
        try:
            #for filename in os.listdir(destinyDir):
            for filename in glob.glob('**', recursive=True, root_dir=destinyDir):
                filePath = os.path.join(destinyDir, filename)
                if not filename.endswith('~') and os.path.isfile(filePath) and re.search(f'.*{fileDateStr}.*\\.aud', filePath):
                    fileCounter = fileCounter +1
                    #fnaav = os.path.join(destinyDir, filename);
                    #fd = open(fnaav, 'rb');
                    fd = open(filePath, 'rb')
                    data = fd.read()
                    fd.close()
                    checksumFile = hashlib.md5(data).hexdigest()
                    #crear archivo checksum
                    if generate_checksum_files == '1':
                        with open(f'{filePath}.cheksum', "w") as text_file:
                            text_file.write("%s" % checksumFile)
                    #
                    if generate_chesksum_log == '1':
                        logging.info(f'{fileCounter}: {filename} - {checksumFile}')
        except Exception as e:
            logging.error('Exception occurred:' )
            logging.error(f'{e}')
            logging.error(f'Traceback: {traceback.format_exc()}')
            result = 'ERROR'
    logging.info(f'Total checksum files calculated: {fileCounter}')
    return result

def extractor(force_fileDateStr=''):
    # 
    config = read_config(f'{os.path.dirname(os.path.abspath(__file__))}\\config.properties')
    #
    cbs_config = config['CBS_SERVER']
    server = cbs_config['host'] 
    port = cbs_config['port']
    user = cbs_config['user']
    password = cbs_config['password']
    sourceDir = cbs_config['cbs_base_dir_audit_files']
    subdirs = cbs_config['cbs_sub_dir_list_audit_files']
    #
    local_server = config['LOCAL_SERVER']
    destinyDir = local_server['local_dir_audit_files']
    #localDirOrganizedByDB = local_server['local_dir_audit_files_by_db']
    local_dir_scripts = local_server['local_dir_scripts']
    local_dir_logs = local_server['local_dir_logs']
    #
    extractionConfig = config['EXTRACTION']
    old_files_in_days_to_be_extracted = int (extractionConfig['old_files_in_days_to_be_extracted'])
    delete_destiny_dir_content = extractionConfig['delete_destiny_dir_content']
    generate_checksum_files = extractionConfig['generate_checksum_files']
    generate_chesksum_log = extractionConfig['generate_chesksum_log']
    #
    fileDate = datetime.datetime.now() - datetime.timedelta(days=old_files_in_days_to_be_extracted)
    fileDateStr = fileDate.strftime('%Y%m%d')
    #fileDateStr = '20240417'
    if force_fileDateStr != '':
        fileDateStr = force_fileDateStr
    #
    logFileDateStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    local_dir_logs = f'{local_dir_logs}/extractor_{logFileDateStr}.log'
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

    logging.info('=========================Start extraction============================')
    logging.info(f'''Extraction day: {fileDateStr} {'(Calculated internally)' if force_fileDateStr =='' else ''}''')
    #    
    exceptfiles='\\.tar\\.gz'
    file_quantity_in_dir = getFilesQuantityInDir(f'{destinyDir}/{fileDateStr}', fileDateStr, exceptfiles)      
    if result == 'OK' and delete_destiny_dir_content == '1' and file_quantity_in_dir > 0:
        result = deleteDirContent(f'{destinyDir}/{fileDateStr}', fileDateStr,exceptfiles)

    if result == 'OK':
        #result = extractor (sourceDir, destinyDir, fileDateStr, subdirs, server, port, user, password)
        result = copyAudFilesFromExternalServer(sourceDir,destinyDir, fileDateStr, subdirs, server, port, user, password)

    if result == 'OK' and (generate_checksum_files == '1' or generate_chesksum_log =='1'):
        result = checksumFiles(destinyDir, fileDateStr, generate_checksum_files, generate_chesksum_log)
    #
    return result

def main():
    #Call example: python.exe Extractor.py "20240417"
    force_fileDateStr = ''
    for i, arg in enumerate(sys.argv[1:], start=1):
        #  20240417
        if i==1:
            force_fileDateStr = arg     
    result = extractor(force_fileDateStr) 
    print (result)

if __name__ == '__main__':
    main()