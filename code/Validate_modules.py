"""
Validation of necesary modules for correct script execution in all Python ETL files. 

Author: Victor Hugo Gonzales Alvarez
email: gonzalesv@tigo.net.bo, vicogo@gmail.com
Last update: 20240511
"""

modules = ['os', 'logging', 'configparser', 're', 'datetime', 'traceback', 'glob', 'sys', 'shutil', 'hashlib','smtplib', 'paramiko','tarfile','base64'
           ,'from Util_string import getAuditedActivity, getDataBetween, getObjectNameFromQuery'
           ,'from scp import SCPClient'
           ,'from Util_files import deleteDirContent, createSSHClient, read_config, createAudFilesInLocalDir, writeScriptsChecksumInLog, getFilesQuantityInRemoteDir, getParamikoSSHCLient, getFilesQuantityInDir, compressFiles'
           ,'from Extractor import extractor'
           ,'from Loader_by_db import loader_by_db'
           ,'from Compress_aud_files import compress_aud_files'
           ,'from os.path import basename'
           ,'from email.mime.application import MIMEApplication'
           ,'from email.mime.multipart import MIMEMultipart'
           ,'from email.mime.text import MIMEText'
           ,'from email2.utils import COMMASPACE, formatdate'
           ]


for type in ['ok', 'nok']:
    print(f'\n{type.upper()} modules:')
    i = 0
    for module in modules:
        try:
            command  = f'''{'' if module.startswith('from') else 'import' } {module}'''
            exec (command.strip())
            if type == 'ok':
                i = i+1
                print (f'{i}. Yes module named {module}')
        except ImportError as e:
            if type == 'nok':
                i = i+1
                print (f'{i}. {e}')
                