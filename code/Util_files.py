"""
Util module contains utilities functions that helps to manage files and directories. 


Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240511
"""

import os
import logging
import shutil
import paramiko
import glob
import configparser
import traceback
import hashlib
import re
import tarfile
import datetime

def deleteAudFilesInLocalDir(destinyDir, fileDate):
    result = 'OK'
    try:
        # Check if the directory exists before attempting to delete it
        if os.path.exists(destinyDir):
            shutil.rmtree(destinyDir)
            logging.info(f"The directory {destinyDir} has been deleted.")
        else:
            logging.info(f"The directory {destinyDir} does not exist.")
    except Exception as e:
        logging.error('Error: '+ e)
        result = 'ERROR'
    return result

def createAudFilesInLocalDir (destinyDir):
    result = 'OK'
    try:
        if not os.path.exists(destinyDir):
            # Create the directory
            os.makedirs(destinyDir)
            logging.info('The directory '+ destinyDir +' created successfully!')
        else:
            logging.info('The directory '+ destinyDir +' already exists!!')
    except Exception as e:
        logging.error('Error: '+ e)
        result = 'ERROR'
    return result

def deleteDirContent(destinyDir, fileDateStr, exceptfiles='', justSubdirs=''):
    result = 'OK'
    logging.info(f'Start deleting files from directory {destinyDir}')
    fileCounter = 0
    for filename in glob.glob('**', recursive=True, root_dir=destinyDir):
        filePath = os.path.join(destinyDir, filename)
        try:
            if os.path.exists(filePath):
                if os.path.isfile(filePath) and re.search(f'''.*{fileDateStr}.*\\.aud''', filePath) and not re.search(f'''.*{exceptfiles}.*''', filePath):
                    os.remove(filePath)
                    fileCounter = fileCounter +1
                    logging.info(f'{fileCounter}: {filename} deleted (file)')
                elif os.path.islink(filePath):
                    os.unlink(filePath)
                    fileCounter = fileCounter +1
                    logging.info(f'{fileCounter}: {filename} deleted (link)')
                elif os.path.isdir(filePath) and re.search(f'.*{justSubdirs}.*', filePath):
                    shutil.rmtree(filePath)
                    fileCounter = fileCounter +1
                    logging.info(f'{fileCounter}: {filename} deleted (directory)')
                else:
                    logging.info(f'{fileCounter}: {filename} NOT deleted') 

        except Exception as e:
            logging.error('Exception occurred. Failed to delete %s. Reason: %s' % (filePath, e) )
            logging.error(f'Traceback: {traceback.format_exc()}')
            result = 'ERROR'   
    logging.info(f'Total files/directories deleted: {fileCounter}')
    return  result


def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client

def read_config(file_path):
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(file_path)
    return config

def getChecksumFile(filename):
    with open(filename, 'rb') as fd:
        #fd = open(filename, 'rb');                    
        data = fd.read();
        #fd.close();
    checksumFile = hashlib.md5(data).hexdigest();
    return checksumFile

def writeScriptsChecksumInLog(local_dir_scripts):
    result = 'OK'
    try:
        checksumConfig = getChecksumFile(f'{local_dir_scripts}/config.properties')
        checksumExtract_and_load = getChecksumFile(f'{local_dir_scripts}/Extract_and_load.py')
        checksumCompress_aud_files = getChecksumFile(f'{local_dir_scripts}/Compress_aud_files.py')
        checksumCompress_db_aud_files = getChecksumFile(f'{local_dir_scripts}/Compress_db_aud_files.py')
        checksumExtractor = getChecksumFile(f'{local_dir_scripts}/Extractor.py')
        checksumLoad = getChecksumFile(f'{local_dir_scripts}/Loader_by_db.py')
        checkUtil = getChecksumFile(f'{local_dir_scripts}/Util_string.py')
        checkUtilFiles = getChecksumFile(f'{local_dir_scripts}/Util_files.py')
        checkTransform = getChecksumFile(f'{local_dir_scripts}/Activity_report_generator.py')
        logging.info(f'1: {local_dir_scripts}/config.properties - {checksumConfig}')
        logging.info(f'2: {local_dir_scripts}/Extract_and_load.py - {checksumExtract_and_load}')
        logging.info(f'3: {local_dir_scripts}/Extractor.py - {checksumExtractor}')
        logging.info(f'4: {local_dir_scripts}/Loader_by_db.py - {checksumLoad}')
        logging.info(f'5: {local_dir_scripts}/Activity_report_generator.py - {checkTransform}')     
        logging.info(f'6: {local_dir_scripts}/Compress_aud_files.py - {checksumCompress_aud_files}')
        logging.info(f'7: {local_dir_scripts}/Compress_db_aud_files.py - {checksumCompress_db_aud_files}')
        logging.info(f'8: {local_dir_scripts}/Util_string.py - {checkUtil}')
        logging.info(f'9: {local_dir_scripts}/Util_files.py - {checkUtilFiles}') 
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        result = 'ERROR'
    return result

def createTarfile(output_filename, sourceDir, fileDateStr, subdirs, extension_file = 'aud'):
    result = 'OK'    
    logging.info(f'Start compressing files from directory {sourceDir}')
    try:
        if not os.path.exists(output_filename):
            fileCounter = 0  
            logging.info(f'Compressed file created: {output_filename}') 
            with tarfile.open(output_filename, "w:gz") as tar:
                for filename in glob.glob('**', recursive=True, root_dir=sourceDir):
                    sourcePath = os.path.join(sourceDir, filename)
                    if os.path.isfile(sourcePath) and re.search(f'.*{fileDateStr}.*\\.{extension_file}', filename):
                        arcname = os.path.relpath(sourcePath, sourceDir)
                        tar.add(sourcePath, arcname=arcname)     
                        fileCounter = fileCounter + 1
                        logging.info(f'{fileCounter}: {filename}') 
            logging.info(f'Total files compressed: {fileCounter}') 
            if fileCounter == 0:
                os.remove(output_filename) 
                logging.info(f'Zero (0) bytes compressed file deleted: {output_filename} ') 
        else:
            logging.info(f'Compressed file exists: {output_filename} ')  
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        result = 'ERROR'                 
    return result                      

def compressFiles(fileDir, output_filename, compressedFileDateStr, subdirs, extension_file = 'aud'):
    result = 'OK'    
    try:
        result = createTarfile(output_filename, fileDir, compressedFileDateStr, subdirs, extension_file)
        #Delete files AUD files except TAR.GZ file
        if result == 'OK':
            if os.path.exists(output_filename):
                file_stats = os.stat(output_filename)
                file_size = file_stats.st_size
                current_date = datetime.datetime.now().strftime("%Y%m%d")
                file_date = datetime.datetime.fromtimestamp(file_stats.st_birthtime).strftime("%Y%m%d")
                logging.error(f'current_date:{current_date}; file_date: {file_date}' )
                exceptfiles='\\.tar\\.gz'
                file_quantity_in_dir = getFilesQuantityInDir(fileDir, compressedFileDateStr, exceptfiles)
                if file_size > 0 and current_date == file_date and file_quantity_in_dir > 0: 
                    result = deleteDirContent(fileDir, compressedFileDateStr, exceptfiles,justSubdirs=compressedFileDateStr)   
    except Exception as e:
        logging.error('Exception occurred:' )
        logging.error(f'{e}')
        logging.error(f'Traceback: {traceback.format_exc()}')
        result = 'ERROR'                 
    return result   

def getParamikoSSHCLient(host, port, username, password):
    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password)  
    return client

def getFilesQuantityInRemoteDir(SSHClient, dir, fileDateStr):
    counter = 0
    with SSHClient.open_sftp() as sftp:
        files = sftp.listdir(dir)
        for i, file in enumerate(files):
            if file and fileDateStr in file:
                counter = counter + 1                    
    return counter

def getFilesQuantityInDir(dir, fileDateStr, exceptfiles):
    counter = 0
    for filename in glob.glob('**', recursive=True, root_dir=dir):
        if  re.search(f'.*{fileDateStr}.*\\.aud', filename) and not re.search(f'.*{exceptfiles}.*', filename):
            counter = counter +1
    return counter
"""
def getFilesQuantityInRemoteDir(host, port, username, password, dir, fileDateStr):
    counter = 0
    with paramiko.SSHClient() as client:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, port, username, password)  
        with client.open_sftp() as sftp:
            files = sftp.listdir(dir)
            for i, file in enumerate(files):
                if file and fileDateStr in file:
                    counter = counter + 1                    
    return counter
"""    