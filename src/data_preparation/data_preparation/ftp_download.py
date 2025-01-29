from ftplib import FTP, all_errors
import os
from io import StringIO

def getFTPTestFile(save_directory):
# Ensure the save directory exists
    os.makedirs(save_directory, exist_ok=True)

    # FTP server details
    ftp_address = '10.147.229.180'
    username = 'Default User'
    password = 'robotics'
    directory = '/hd0a/IRB4400_44-51346/HOME/LOGS'
    
    # Connect to the FTP server
    ftp = FTP(ftp_address, encoding='utf-8')
    ftp.login(username, password)
    
    # Change to the desired directory
    ftp.cwd(directory)
    
    # Retrieve the file and save it to the specified directory
    local_file_path = os.path.join(save_directory, 'ProgramExecution')
    with open(local_file_path, 'wb') as fp:
        ftp.retrbinary('RETR ProgramExecution', fp.write)
    
    # Close the FTP connection
    ftp.quit()
    
def changeFTPValue(value):
    # Create a text file named SystemVariables with the value 1 inside
    file_name = 'SystemVariables.txt'
    with open(file_name, 'w') as file:
        file.write(value)
    
    # FTP server details
    ftp_address = '10.147.229.180'
    username = 'Default User'
    password = 'robotics'
    directory = '/hd0a/IRB4400_44-51346/HOME/'
    
    # Connect to the FTP server
    ftp = FTP(ftp_address, encoding='latin-1')
    ftp.login(username, password)
    
    # Change to the desired directory
    ftp.cwd(directory)
    
    # Open the file in binary read mode and upload it to the FTP server
    with open(file_name, 'rb') as file:
        ftp.storbinary(f'STOR {file_name}', file)
    
    # Close the FTP connection
    ftp.quit()

# Function for sending files via FTP
def sendRandomTrajectoriesFTP(trajectories_directory):
    try:
        # FTP server details
        ftp_address = '10.147.229.180'
        username = 'Default User'
        password = 'robotics'
        directory = '/hd0a/IRB4400_44-51346/HOME/RANDOM'

        # Connect to the FTP server
        ftp = FTP(ftp_address, encoding='latin-1')
        ftp.login(username, password)
        
        # Change to the desired directory on the FTP server
        ftp.cwd(directory)
        
        # Lists all files in the local directory
        files = [os.path.join(trajectories_directory, f) for f in os.listdir(trajectories_directory) if os.path.isfile(os.path.join(trajectories_directory, f))]
        
        # Send each file
        for filename in files:
            with open(filename, 'rb') as file:
                ftp.storbinary(f'STOR {os.path.basename(filename)}', file)
                print(f'File {filename} successfully sent via FTP.')
        
        # Closes the FTP connection
        ftp.quit()
        print('All files have been sent successfully.')
    
    except all_errors as e:
        print(f'Error sending files via FTP: {e}')



if __name__ == "__main__":
    changeFTPValue()