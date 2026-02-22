import psycopg2
import sys
from psycopg2 import sql
from datetime import datetime
import ast
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.by import By
from selenium import webdriver
import time
import threading
import pandas as pd
import json
import uuid
import base64
import shutil
import re
import requests
from azure.storage.blob import BlobServiceClient,ContentSettings
import numpy as np
import os
import configparser
import string
import random
from cryptography.fernet import Fernet
import concurrent.futures
from selenium.webdriver.support.ui import Select
import os
import time
import errno
import socket
import requests
from selenium.webdriver.chrome.service import Service
import html
from pathlib import Path
import boto3
import glob
from selenium.webdriver.common.keys import Keys
import os
from pypdf import PdfWriter, PdfReader

current_directory = os.getcwd()
config_file_path = os.path.join(current_directory, 'Config/path_details_UAT30.ini')
# config_file_path = os.path.join(current_directory, 'Config/path_details_prod.ini')
config = configparser.ConfigParser()

config.read(config_file_path)


encDetails=config['Paths']['encrypted_config']
key=config['Paths']['key']
fernet = Fernet(key)
decDetails = fernet.decrypt(encDetails).decode()

database_details = {}
for line in decDetails.split("\n"):
    key, value = line.split(" = ")
    database_details[key] = value


current_directory = os.getcwd()
print(current_directory)
data_directory=os.path.join(current_directory,'Config','Data')
print(data_directory)
if not os.path.exists(data_directory):
    os.makedirs(data_directory,exist_ok=True)
    print(f'created directory {data_directory}')

puid = str(uuid.uuid4())

unique_folder=puid
new_directory=os.path.join(data_directory,unique_folder)

if not os.path.exists(new_directory):
    os.makedirs(new_directory,exist_ok=True)

download_path = new_directory

ERAPATH = os.path.abspath(new_directory)
print(ERAPATH)

db_host = database_details["Host"]
db_port = database_details["Port"]
db_name = database_details["Database"]
db_user = database_details["User"]
db_password = database_details["Password"]


threadLocal = threading.local()

db_params = {
    'host': database_details["Host"],
    'port': database_details["Port"],
    'database': database_details["Database"],
    'user': database_details["User"],
    'password': database_details["Password"]
}



def create_connection():
    try:
        connection = psycopg2.connect(**db_params)        
        return connection
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = "create_connection line : "+str(exc_tb.tb_lineno)+" " +str(e)
        raise Exception(message)
    
def insert_log(message,connection):    
    try:        
        if connection:        
            cursor = connection.cursor()   
            message = list(message)  # Convert tuple to list for manipulation
            message = [int(item) if isinstance(item, np.integer) else item for item in message]
            message = tuple(message)  # Convert list back to tuple
                     
            insert_query = sql.SQL('''
                                   INSERT INTO tbl_botlogs
                                   (uid,created_time, loglevel,siteid,accountid,inventoryid, message,process_type,userid_master,credentialmasterid) 
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s,%s);''')
            cursor.execute(insert_query, message)            
            connection.commit()
        # pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = "Exception : insert_log line :"+str(exc_tb.tb_lineno)+" " +str(e)         
        raise Exception(message)


def get_inventory_details(puid,credentialmasterid,ip_address,connection):
    if connection:
        try:
            cursor = connection.cursor()
            inp_value=1
            update_query=sql.SQL('''
            SELECT  inv.inventoryid , '' p_uid
            ,inv.codeddate,inv.team,inv.emr,inv.clinicid,inv.divisionid,inv.division,inv.dos,inv.patientid,inv.patientname,
            inv.dob,inv.healthplan,inv.visitid,inv.visittype,inv.coverbyname,inv.billingid,inv.billername,inv.datecompleted,
            inv.codingcomment,inv.billingcomment,inv.querytype,inv.claimstatus,inv.loc,inv.setup,inv.allocationdate,inv.filestatus,
            inv.createddate,inv.createdby,inv.subclientid,inv.clientid,inv.servicetype,inv.errorreason,inv.failurecount,scm.awsaccesskey,
            scm.awssecretkey,scm.awsregion,scm.awsbucketname,scm.awss3filepath,e_crd_master.clienturl,e_crd_master.userid,
            e_crd_master.password,e_crd_master.uplaod_api_url

            FROM public.tbl_stginventoryuploaddata inv
            INNER JOIN mst.tbl_subclientmaster scm ON inv.subclientid = scm.subclientmasterid
            INNER JOIN mst.tbl_emr_credentialmaster e_crd_master ON inv.subclientid = e_crd_master.subclientid
            where e_crd_master.userid in ('BOT2_Coding_Pro')
            -- And filestatus in (0,3)
			And inv.billingid in ('1129304923')
            And failurecount <=3
            Order by random() limit 1 
                                 ''')
            
#             ('''
# SELECT  inv.inventoryid , '' p_uid
# ,inv.codeddate,inv.team,inv.emr,inv.clinicid,inv.divisionid,inv.division,inv.dos,inv.patientid,inv.patientname,
# inv.dob,inv.healthplan,inv.visitid,inv.visittype,inv.coverbyname,inv.billingid,inv.billername,inv.datecompleted,
# inv.codingcomment,inv.billingcomment,inv.querytype,inv.claimstatus,inv.loc,inv.setup,inv.allocationdate,inv.filestatus,
# inv.createddate,inv.createdby,inv.subclientid,inv.clientid,inv.servicetype,inv.errorreason,inv.failurecount,scm.awsaccesskey,
# scm.awssecretkey,scm.awsregion,scm.awsbucketname,scm.awss3filepath,e_crd_master.clienturl,e_crd_master.userid,
# e_crd_master.password,e_crd_master.uplaod_api_url

# FROM public.tbl_stginventoryuploaddata inv
# INNER JOIN mst.tbl_subclientmaster scm ON inv.subclientid = scm.subclientmasterid
# INNER JOIN mst.tbl_emr_credentialmaster e_crd_master ON inv.subclientid = e_crd_master.subclientid
# where e_crd_master.userid in ('BOT2_Coding_Pro')
# And filestatus in (0,3)
# And failurecount <=3
# Order by random() limit 1 
#                                  ''')
            # ('''select * from public.fn_get_inventorycharts_concurrent_user(%s,%s,%s,%s);''')
            try:
                cursor.execute(update_query, (puid, 1, credentialmasterid, ip_address))

            except:
                try:
                    # Properly format the query with direct string formatting
                    query = f'''
                        SELECT * FROM public.fn_get_inventorycharts_concurrent_user(
                            '{puid}',
                            1,
                            {credentialmasterid},
                            '{ip_address}'
                        );
                    '''

                    # Execute the query
                    cursor.execute(query)

                except:
                    cursor.execute(f'''select * from public.fn_get_inventorycharts_concurrent_user({str(puid)},1,{str(credentialmasterid)},{str(ip_address)})''')
            rows = cursor.fetchall()           
            colnames = [desc[0] for desc in cursor.description]
            
            num_fixed_columns = 43

            fixed_columns = ["id","uid","codeddate","team","emr","clinicid","divisionid","division","DOS","PatientId","patientname",
                                                "dob","healthplan","VisitId","visittype","coverbyname","BillingId","billername","datecompleted","codingcomment","billingcomment","querytype",
                                                "claimstatus","loc","setup","allocationdate","FileStatus","createddate","createdby","SubClientId","ClientId",
                                                "servicetype","errorreason","failurecount","awsaccesskey","awssecretkey","awsregion","awsbucketname","awss3filepath","url","userid","password","uplaod_api_url"]


            remaining_columns = colnames[num_fixed_columns:]

            colnames_details = fixed_columns + remaining_columns

            inventory_details = pd.DataFrame(rows, columns=colnames_details)

            inventory_details['SiteId']=inventory_details['clinicid'].astype(str)+inventory_details['divisionid'].astype(str)             
            return inventory_details      
        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message = "get_inventory_details line :"+str(exc_tb.tb_lineno)+" " +str(e)         
            raise Exception(message)
            
    else:
        message="Connection not found "
        raise Exception( message)


def get_driver():
    driver = getattr(threadLocal, 'driver', None)
    if driver is None:
        # Set the path to ChromeDriver in the current working directory
        chrome_driver_path = os.path.join(os.getcwd(), "chromedriver.exe")
        print("chrome_driver_path",chrome_driver_path)
        options = webdriver.ChromeOptions()

        settings = {
            "recentDestinations": [{
                "id": "Save as PDF",
                "origin": "local",
                "account": "",
            }],
            "selectedDestinationId": "Save as PDF",
            "version": 2,
            "isHeaderFooterEnabled": False,
            "mediaSize": {
                "height_microns": 210000,
                "name": "ISO_A5",
                "width_microns": 148000,
                "custom_display_name": "A5"
            },
            "customMargins": {},
            "marginsType": 2,
            "scaling": 175,
            "scalingType": 3,
            "scalingTypePdf": 3,
            "isCssBackgroundEnabled": True
        }

        prefs = {
            "savefile.default_directory": ERAPATH,
            "download.default_directory": ERAPATH,
            "download.prompt_for_download": False,
            "download.show_open_folder_on_save": False,  # Prevents showing the download folder on save
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "printing.print_preview_sticky_settings.appState": json.dumps(settings),
            "download.show_confirm_dialog": False  # Prevents the confirmation dialog
        }
        
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--window-size=1440,900')
        options.add_argument("--lang=en")
        options.add_argument("--start-maximized")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--kiosk-printing')
        options.add_argument('--enable-print-browser')
        options.add_argument('--disable-popup-blocking')
        
        # options.add_argument("--headless")

        try:
            # Initialize the WebDriver with the specified ChromeDriver path
            service = Service(chrome_driver_path)  # Create a Service object
            driver = webdriver.Chrome(service=service, options=options)  # Use the Service object
            setattr(threadLocal, 'driver', driver)
        except Exception as e:
            print(f"Error initializing WebDriver: {e}")
            return None
            
    return driver


def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)


def find_match(div,driver,target_date,acctno,PatientId,inventory_data):

    ele_option1=2
    name_list1=[]
    file_name12=[]
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option1)}]/td[1]')))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()        
        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
        # insert_log(("",datetime.utcnow(),"INFO","","","",message,"1","",""),connection)  

    while ele_option1>0:
        try:
            name_element = driver.find_element(By.XPATH, f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option1)}]/td[1]')
            name = name_element.text
            xpath = f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option1)}]/td[2]'
            try:
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,xpath)))
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()        
                message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
                # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  

            rows=""
            try:
                rows = driver.find_elements(By.XPATH,xpath)

            except:
                elements = WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
                rows=elements

            ele_option_date=ele_option1
            ele_option1+=1
            for row in rows:    
                try:
                    dat_row=row.text
                except:
                    dat_row=row.text
                a_list=dat_row.split("\n")
                if target_date in dat_row:
                    time.sleep(1)
                    print(name)
                    # message=f"continue  updated for div div _  {str(div)} "
                    # insert_log(("",datetime.utcnow(),"INFO","","","",message,"1","",""),connection)  

                    name_list1.append(name)
                    # try:
                    #     for dat in range(len(a_list)):
                    #         index=dat+1
                    #         name_elem = driver.find_element(By.XPATH, f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[1]')
                    #         n_date = name_elem.text
                    #         if target_date in n_date:
                    #             try:
                    #                 # WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[5]/a'))).click()
                    #                 xpath = f'//div[@id="{str(div)}"]//tr[td[contains(text(), "{target_date}")]]//a[text()[contains(.,"View")]]'
                    #                 WebDriverWait(driver, 10).until(
                    #                     EC.element_to_be_clickable((By.XPATH, xpath))
                    #                 ).click()

                    #             except:
                    #                 try:
                    #                     WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[4]/a'))).click()
                    #                 except Exception as e:
                    #                     exc_type, exc_obj, exc_tb = sys.exc_info()        
                    #                     message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)} {str(xpath)}"
                    #                     # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  

                    #             try:
                    #                 names_=remove_punctuation(name).replace(' ','')
                    #                 file_name1 = str(f"MedicalHistory_{str(acctno)+str(names_)}.pdf")
                    #             except:
                    #                 file_name1 = str(f"MedicalHistory_{str(acctno)+str(index)}.pdf")

                    #             dubble_frame=medical_history(driver,file_name1,name,PatientId,div,acctno,inventory_data)
                    #             time.sleep(2)
                    #             if dubble_frame==0:
                    #                 file_name12.append(file_name1)
                    try:
                        for dat in range(len(a_list)):
                            index = dat + 1
                            name_elem = driver.find_element(
                                By.XPATH,
                                f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[1]'
                            )
                            n_date = name_elem.text
                            if target_date in n_date:
                                try:
                                    # Use the SAME precise base path as name_elem, just target td[3]/a or td[5]/a for View button
                                    view_xpath = (
                                        f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody'
                                        f'/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[3]/a'
                                    )
                                    WebDriverWait(driver, 10).until(
                                        EC.element_to_be_clickable((By.XPATH, view_xpath))
                                    ).click()

                                except:
                                    try:
                                        view_xpath_fallback = (
                                            f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody'
                                            f'/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[4]/a'
                                        )
                                        WebDriverWait(driver, 5).until(
                                            EC.element_to_be_clickable((By.XPATH, view_xpath_fallback))
                                        ).click()
                                    except Exception as e:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()
                                        message = (
                                            f"Extraction error for medical History name : "
                                            f"line no : {exc_tb.tb_lineno} {str(e)} {str(view_xpath)}"
                                        )

                                try:
                                    names_ = remove_punctuation(name).replace(' ', '')
                                    file_name1 = str(f"MedicalHistory_{str(acctno)+str(names_)}.pdf")
                                except:
                                    file_name1 = str(f"MedicalHistory_{str(acctno)+str(index)}.pdf")

                                dubble_frame = medical_history(driver, file_name1, name, PatientId, div, acctno, inventory_data)
                                time.sleep(2)
                                if dubble_frame == 0:
                                    file_name12.append(file_name1)                    
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()        
                        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
                        insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  
        except Exception as e:

            ele_option1+=1
            break
    return name_list1,file_name12    

def wait_for_download2(source_folder, max_retries=75, retry_interval=3):    
    retries = 0
    while retries < max_retries:
        try:
            # List files in the source folder
            files = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f))]
            
            # Filter out hidden files or directories (optional)
            files = [f for f in files if not f.startswith('.')]
            
            if not files:
                print("No files found in the source folder.")
                return None  # Or raise an exception
            # Find the latest file
            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(source_folder, f)))  
            stop_while=retries

            # Check if the latest file is being downloaded (has .crdownload extension)
            if latest_file.endswith(".crdownload"):
                print(f"download wait '{latest_file}' is still being downloaded. Retrying after {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 3
                continue
            if retries==stop_while:
                print("completed")
                break


        except FileNotFoundError:
            print("Source folder not found:", source_folder)
            return None
        except PermissionError as e:
            print("Permission denied while accessing files:", e)
            retries += 1
            if retries >= max_retries:
                print("Maximum retries exceeded. Giving up.")
                return None
            print(f"Retrying after {retry_interval} seconds...")
            time.sleep(retry_interval)
        except Exception as e:
            print("An error occurred:", e)
            return None



def wait_for_download(source_folder=ERAPATH, max_retries=75, retry_interval=3):
    """
    Wait for all files with .crdownload extension in the source folder to complete downloading.
    """
    retries = 0
    while retries < max_retries:
        try:
            files = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f))]
            files = [f for f in files if not f.startswith('.')]
            
            if not files:
                print("No files found in the source folder.")
                return None
            
            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(source_folder, f)))
            
            if latest_file.endswith(".crdownload"):
                print(f"'{latest_file}' is still being downloaded. Retrying after {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
            else:
                print("No ongoing downloads detected.")
                return latest_file

        except FileNotFoundError:
            print("Source folder not found:", source_folder)
            return None
        except PermissionError as e:
            print("Permission denied while accessing files:", e)
            retries += 1
            if retries >= max_retries:
                print("Maximum retries exceeded. Giving up.")
                return None
            time.sleep(retry_interval)
        except Exception as e:
            print("An error occurred:", e)
            return None

def rename_pdf_file(new_name,source_folder=ERAPATH, max_retries=5, retry_interval=3):
    """
    Rename the most recently modified PDF file in the source folder.
    """
    if not os.path.isdir(source_folder):
        print(f"Source folder '{source_folder}' does not exist.")
        return

    latest_file = wait_for_download(source_folder, max_retries=max_retries, retry_interval=retry_interval)
    
    if latest_file is None:
        print("No suitable file found or error occurred.")
        return

    if not latest_file.endswith(".pdf"):
        print(f"The latest file '{latest_file}' is not a PDF file. Skipping rename.")
        return

    new_file_path = os.path.join(source_folder, new_name + ".pdf")
    old_file_path = os.path.join(source_folder, latest_file)
    
    retries = 0
    while retries < max_retries:
        try:
            os.rename(old_file_path, new_file_path)
            print("File renamed successfully to:", new_file_path)
            return
        except OSError as e:
            if e.errno == errno.EACCES:  # File is locked or protected
                print("File is currently locked or protected. Retrying in {} seconds...".format(retry_interval))
                time.sleep(retry_interval)
                retries += 1
            else:
                print("An unexpected error occurred during renaming:", e)
                return
        except Exception as e:
            print("An error occurred while renaming the file:", e)
            return

    print("Maximum rename retries exceeded. File could not be renamed.")



def visit_dos_documents(driver,filename1):
    try:
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("workarea2")
        time.sleep(2)
        try:
            print_link = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.LINK_TEXT, "Print")))
            time.sleep(1)
            print_link.click()
        except:
            try:
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[4]/td'))).click()
            except:
                pass                
        download_directory=ERAPATH
        time.sleep(5) #987
        # try:
        #     dow_dir=wait_for_download(download_directory)
        #     dow_dir=wait_for_download1(download_directory)
        # except:
        #     pass
        try:
            wait_for_download2(download_directory)
            
        except:
            pass
        window_after = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_after[0])

        # window_after = driver.window_handles[-1]
        # time.sleep(1)
        # driver.switch_to.window(window_after)

        time.sleep(2)
        filename = filename1
        try:
            rename_pdf_file(filename)
        except:
            pass

        move_latest_file(filename)
        # message=f"DOS visit history pdf download completed for {filename} pdf"
        # insert_log(("",datetime.utcnow(),"Info","","","",message,"1","",""),connection) 
        window_after = driver.window_handles
        handles = driver.window_handles

        num_tabs = len(handles)

        if num_tabs==2:
            driver.switch_to.window(handles[1])
            driver.close()  
        driver.switch_to.window(window_after[0])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()        
        message=f"Extraction error for visit_dos_documents : line no : {exc_tb.tb_lineno} {str(e)}"
        print(message)

def PopUpFrame (driver):
    driver.switch_to.default_content()
    element_present0 = EC.presence_of_element_located((By.XPATH, "//iframe[@id='workarea1']"))
    WebDriverWait(driver, 20).until(element_present0)
    driver.switch_to.frame("workarea1")
    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
    WebDriverWait(driver, 10).until(element_present1)
    driver.switch_to.frame("GB_frame")

    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
    WebDriverWait(driver, 5).until(element_present1)
    driver.switch_to.frame("GB_frame")
    
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divControl"]/table/tbody/tr/td/table/tbody/tr/td/div[1]/table/tbody/tr/td'))).click()
    driver.switch_to.default_content()
    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
    WebDriverWait(driver, 50).until(element_present1)
    driver.switch_to.frame("workarea2")
    try:
        print_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Print")))
        time.sleep(1) 
        print_link.click()
    except:
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[5]/td'))).click()
        except:
            pass
    try:
        driver.switch_to.default_content()
        element_present0 = EC.presence_of_element_located((By.XPATH, "//iframe[@id='workarea1']"))
        WebDriverWait(driver, 20).until(element_present0)
        driver.switch_to.frame("workarea1")
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
        WebDriverWait(driver, 10).until(element_present1)
        driver.switch_to.frame("GB_frame")
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
        WebDriverWait(driver, 5).until(element_present1)
        driver.switch_to.frame("GB_frame")
        time.sleep(1)
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divControl"]/table/tbody/tr/td/table/tbody/tr/td/div[2]/table/tbody/tr/td'))).click()    
    except:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[3]/table/tbody/tr/td/table/tbody/tr/td/div[2]/table/tbody/tr/td'))).click()



def click_on_date(driver, filename1,PatientId,inventory_data,acctno):
    # time.sleep(2)
    try:
        window_billing = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing[0])

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
        WebDriverWait(driver, 15).until(element_present1)
        driver.switch_to.frame("workarea2")
        # pdfpic
        element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="pdfpic"]'))
        WebDriverWait(driver, 15).until(element_present34)
        driver.switch_to.frame("pdfpic")
        time.sleep(2)
        try:
            try:
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="open-button"]'))).click()
                # insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"Successfully downloaded Docs/Form file {str(filename1)} for {str(PatientId)} ","6",username,credentialmasterid),connection)
            except:
                try:
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="showText"]/center/div[2]/font/table/tbody/tr/td'))).click()
                    Rbutton = driver.find_element(By.XPATH, '//*[@id="notInclude"]') # Include Inactive Patient
                    Rbutton.click()
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="editDiv"]/div/table[2]/tbody/tr/td/div/table/tbody/tr[4]/td/div[1]/table/tbody/tr/td'))).click()
                    # insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"Successfully downloaded Docs/Form file {str(filename1)} for {str(PatientId)} ","6",username,credentialmasterid),connection)
                    # WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[2]/span/center/div[2]'))).click()
                except:
                    try:
                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="showText"]/center/div[2]/table/tbody/tr/td'))).click()
                        Rbutton = driver.find_element(By.XPATH, '//*[@id="notInclude"]') # Include Inactive Patient
                        Rbutton.click()
                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="editDiv"]/div/table[2]/tbody/tr/td/div/table/tbody/tr[4]/td/div[1]/table/tbody/tr/td'))).click()
                        # insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"Successfully downloaded Docs/Form file {str(filename1)} for {str(PatientId)} ","6",username,credentialmasterid),connection)
                    except:
                        Rbutton = driver.find_element(By.XPATH, "//*[@id='showText']//td[span[@class='buttonTitle' and text()='P']]") # Include Inactive Patient
                        Rbutton.click()
                        Pbutton = driver.find_element(By.XPATH, "//*[@id='editDiv']//td[span[@class='buttonTitle' and text()='P']]")
                        Pbutton.click()


        except:
            try:
                window_lab_date = driver.window_handles
                driver.switch_to.default_content()
                driver.switch_to.window(window_lab_date[0])

                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
                WebDriverWait(driver, 10).until(element_present1)
                driver.switch_to.frame("workarea2")
                ele_option=1
                target_area="Print"
                while ele_option>0:
                        try:
                            First_date=driver.find_element(By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr').text
                            if target_area in  First_date:
                                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr'))).click() #Docs Print
                                # insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"Successfully downloaded Docs/Form file {str(filename1)} for {str(PatientId)} ","6",username,credentialmasterid),connection)
                                break
                            ele_option+=1
                        except:
                            ele_option+=1
                            break
            except:
                pass            
    except:
        PopUpFrame(driver) 

    download_directory=ERAPATH
    time.sleep(2)

    try:
        wait_for_download2(download_directory)
    except:
        pass

    # time.sleep(1)
    window_after = driver.window_handles
    driver.switch_to.default_content()
    driver.switch_to.window(window_after[0])

    # window_after = driver.window_handles[-1]

    # driver.switch_to.window(window_after)

    # time.sleep(2) 987
    filename = filename1
    try:
        rename_pdf_file(filename)
    except:
        pass

    move_latest_file(filename)

    # message=f"DOC labs pdf download completed for {filename} pdf"
    # insert_log(("",datetime.utcnow(),"Info","",acctno,inventory_data,message,"6","",""),connection) 
    insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - DOCS/Forms/Labs : {str(filename)}","6",username,credentialmasterid),connection)

    window_after = driver.window_handles

    handles = driver.window_handles

    num_tabs = len(handles)

    if num_tabs==2:
        driver.switch_to.window(handles[1])
        driver.close()  

    driver.switch_to.window(window_after[0])

    try:
        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[9]/table/tbody/tr/td[3]/img'))).click()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()      
        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  



# def click_on_date(driver, filename1):
#     # time.sleep(2)
#     window_billing = driver.window_handles
#     driver.switch_to.default_content()
#     driver.switch_to.window(window_billing[0])

#     element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
#     WebDriverWait(driver, 15).until(element_present1)
#     driver.switch_to.frame("workarea2")
#     # pdfpic
#     element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="pdfpic"]'))
#     WebDriverWait(driver, 15).until(element_present34)
#     driver.switch_to.frame("pdfpic")
#     time.sleep(2)
#     try:
#         WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[2]/span/center/div[2]'))).click()
#     except:
#         try:
#             window_lab_date = driver.window_handles
#             driver.switch_to.default_content()
#             driver.switch_to.window(window_lab_date[0])

#             element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
#             WebDriverWait(driver, 10).until(element_present1)
#             driver.switch_to.frame("workarea2")
#             ele_option=1
#             target_area="Print"
#             while ele_option>0:
#                     try:
#                         First_date=driver.find_element(By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr').text
#                         if target_area in  First_date:
#                             time.sleep(1)
#                             WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
#                             time.sleep(5)
#                             break
#                         ele_option+=1
#                     except:
#                         ele_option+=1
#                         break
#         except:
#             pass 
        
#     download_directory=ERAPATH
#     time.sleep(2)

#     try:
#         wait_for_download2(download_directory)
#     except:
#         pass

#     # time.sleep(1)
#     window_after = driver.window_handles
#     driver.switch_to.default_content()
#     driver.switch_to.window(window_after[0])

#     # window_after = driver.window_handles[-1]

#     # driver.switch_to.window(window_after)

#     time.sleep(2)
#     filename = filename1
#     try:
#         rename_pdf_file(filename)
#     except:
#         pass

#     move_latest_file(filename)

#     # message=f"DOC labs pdf download completed for {filename} pdf"
#     # insert_log(("",datetime.utcnow(),"Info","","","",message,"1","",""),connection) 

#     window_after = driver.window_handles

#     handles = driver.window_handles

#     num_tabs = len(handles)

#     if num_tabs==2:
#         driver.switch_to.window(handles[1])
#         driver.close()  

#     driver.switch_to.window(window_after[0])

#     try:
#         WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[9]/table/tbody/tr/td[3]/img'))).click()
#     except Exception as e:
#         exc_type, exc_obj, exc_tb = sys.exc_info()      
#         message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
#         # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  


def medical_history(driver,filename1,name,PatientId,div,acctno,inventory_data):
    try:
        window_billing = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing[0])
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("workarea2")

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("GB_frame")

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("GB_frame")
        dubble_frame=0
        time.sleep(1)
        try:
            if div == 'div__Others':
                try:
                    # time.sleep(2) 3/21/2024 As per Discussion for Optimisation
                    source_folder = ERAPATH
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr'))).click()
                    time.sleep(3)
                    wait_for_download2(source_folder, max_retries=75, retry_interval=5)
                    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[8]/table/tbody/tr/td'))).click()
                    # insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"STEPS 6: Successfully downloaded Medical History file {str(name)} for {str(PatientId)} ","6",username,credentialmasterid),connection)
                except:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/form/div[3]/div[5]/table/tbody/tr/td'))).click()
                    time.sleep(2)
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/form/div[3]/div[8]/table/tbody/tr/td'))).click()
                    insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - Medical History : {str(name)}","6",username,credentialmasterid),connection)
            else:
                time.sleep(1)
                try:
                    print_buttons = WebDriverWait(driver,10).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@onclick='beginPrint();']"))) # 3/21/2024 As per Discussion for Optimisation
                    time.sleep(2)
                except:
                    print_buttons = WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.XPATH, '//div[@onclick="tmpPrint();"]')))
                    time.sleep(1)
                    WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, '//div[@onclick="closeMe();"]'))).click()

                for index, print_button in enumerate(print_buttons, start=1):
                    try:
                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//div[@onclick='beginPrint();']"))).click() #3/21/2024 As per Discussion for Optimisation
                        time.sleep(2)
                        # insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - Medical History : {str(name)}","6",username,credentialmasterid),connection)
                        break  
                    except:
                        try :
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr/td'))).click()
                            time.sleep(2)
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//div[@class="button_cyan"]'))).click()
                        except Exception as e:
                            print(f"Error clicking on Print button {index}: {e}")
        except:
            try:
                
                print_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[@onclick='beginPrint()']")))
                print_button.click() # this one is working for Suicide Risk Assessment Form Print but not renaming
                try:
                    window_after = driver.window_handles

                    handles = driver.window_handles

                    num_tabs = len(handles)

                    # print(f"Number of open tabs/windows: {num_tabs}")
                    if num_tabs==2:
                        driver.switch_to.window(handles[1])
                        driver.close()  
                    driver.switch_to.window(window_after[0])

                    window_billing = driver.window_handles
                    driver.switch_to.default_content()
                    driver.switch_to.window(window_billing[0])
                    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
                    WebDriverWait(driver, 15).until(element_present1)
                    driver.switch_to.frame("workarea2")

                    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
                    WebDriverWait(driver, 10).until(element_present1)
                    driver.switch_to.frame("GB_frame")

                    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
                    WebDriverWait(driver, 5).until(element_present1)
                    driver.switch_to.frame("GB_frame")

                    close_button = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//td[@width='50']/a[text()='Close']"))
                    )

                    close_button.click()
                    # dubble_frame=1
                except:
                    print("Window not found or close button not present.")
            except:
                try:
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr'))).click() 
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()        
                        message=f"print download problem : line no : {exc_tb.tb_lineno} {str(e)}"
                        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  
                    try:
                        window_after = driver.window_handles

                        handles = driver.window_handles

                        num_tabs = len(handles)

                        if num_tabs==2:
                            driver.switch_to.window(handles[1])
                            driver.close()  
                        driver.switch_to.window(window_after[0])

                        window_billing = driver.window_handles
                        driver.switch_to.default_content()
                        driver.switch_to.window(window_billing[0])
                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
                        WebDriverWait(driver, 15).until(element_present1)
                        driver.switch_to.frame("workarea2")

                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
                        WebDriverWait(driver, 10).until(element_present1)
                        driver.switch_to.frame("GB_frame")

                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
                        WebDriverWait(driver, 5).until(element_present1)
                        driver.switch_to.frame("GB_frame")

                        close_button = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, "//td[@width='50']/a[text()='Close']"))
                        )

                        close_button.click()
                        dubble_frame=1
                    except:
                        print("Window not found or close button not present.")
                except:
                    pass

                
           
        download_directory=ERAPATH
        time.sleep(2)
        try:
            if dubble_frame==0:
                wait_for_download2(download_directory)
        except:
            pass
        # time.sleep(2)
        
        filename = filename1
        if dubble_frame==0:
            try:
                rename_pdf_file(filename)
            except:
                pass
            move_latest_file(filename)
            insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - Medical History : {str(name)}","6",username,credentialmasterid),connection)
            # message=f"STEP 6 medical History pdf download completed for {filename} pdf"
            # insert_log(("",datetime.utcnow(),"Info","",acctno,inventory_data,message,"6","",""),connection) 


        window_after = driver.window_handles
        handles = driver.window_handles
        num_tabs = len(handles)
        if num_tabs==2:
            driver.switch_to.window(handles[1])
            driver.close()  
        driver.switch_to.window(window_after[0])


        window_billing = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing[0])
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("workarea2")
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="PatientHistory"]'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("PatientHistory")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()      
        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  
    return dubble_frame


# def medical_history(driver,filename1,name,PatientId,div,acctno,inventory_data):
#     try:
#         window_billing = driver.window_handles
#         driver.switch_to.default_content()
#         driver.switch_to.window(window_billing[0])
#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
#         WebDriverWait(driver, 20).until(element_present1)
#         driver.switch_to.frame("workarea2")

#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
#         WebDriverWait(driver, 20).until(element_present1)
#         driver.switch_to.frame("GB_frame")

#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
#         WebDriverWait(driver, 20).until(element_present1)
#         driver.switch_to.frame("GB_frame")
#         dubble_frame=0
#         time.sleep(1)
        
#         # wait = WebDriverWait(driver, 20)

#         # # Always start from default content
#         # driver.switch_to.default_content()

#         # # Switch to first iframe
#         # wait.until(EC.frame_to_be_available_and_switch_to_it(
#         #     (By.XPATH, "//iframe[@id='workarea2']")
#         # ))

#         # # Switch to second (nested) iframe
#         # wait.until(EC.frame_to_be_available_and_switch_to_it(
#         #     (By.XPATH, "//iframe[@id='PatientHistory']")
#         # ))

#         # Now locate the target element
#         # element = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/form/center/div/div[2]/table/tbody/tr[21]/td/table/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table/tbody/tr[1]/td/div/table[2]/tbody/tr[1]/td/div/table/tbody/tr[2]")))

        
#         try:
#             if div == 'div__Others':
#                 try:
#                     # time.sleep(2) 3/21/2024 As per Discussion for Optimisation
#                     source_folder = ERAPATH
#                     WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr'))).click()
#                     time.sleep(3)
#                     wait_for_download2(source_folder, max_retries=75, retry_interval=5)
#                     WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[9]/table/tbody/tr/td'))).click()
#                     # insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"STEPS 6: Successfully downloaded Medical History file {str(name)} for {str(PatientId)} ","6",username,credentialmasterid),connection)
#                 except:
#                     WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/form/div[3]/div[5]/table/tbody/tr/td'))).click()
#                     time.sleep(2)
#                     WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/form/div[3]/div[9]/table/tbody/tr/td'))).click()
#                     insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - Medical History : {str(name)}","6",username,credentialmasterid),connection)
#             else:
#                 time.sleep(1)
#                 try:
#                     print_buttons = WebDriverWait(driver,10).until(EC.presence_of_all_elements_located((By.XPATH, "//*[@id='div_button']//td[contains(., 'Print')]"))) # 3/21/2024 As per Discussion for Optimisation
#                     time.sleep(2)
#                 except:
#                     print_buttons = WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.XPATH, "//td[span[@class='buttonTitle' and text()='P']]")))
#                     time.sleep(1)
#                     WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, '//div[@onclick="closeMe();"]'))).click()

#                 for index, print_button in enumerate(print_buttons, start=1):
#                     try:
#                         WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//div[@onclick='beginPrint();']"))).click() #3/21/2024 As per Discussion for Optimisation
#                         time.sleep(2)
#                         # insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - Medical History : {str(name)}","6",username,credentialmasterid),connection)
#                         break  
#                     except:
#                         try :
#                             WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr/td'))).click()
#                             time.sleep(2)
#                             WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//div[@class="button_cyan"]'))).click()
#                         except Exception as e:
#                             print(f"Error clicking on Print button {index}: {e}")
#         except:
#             try:
                
#                 print_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[@onclick='beginPrint()']")))
#                 print_button.click() # this one is working for Suicide Risk Assessment Form Print but not renaming
#                 try:
#                     window_after = driver.window_handles

#                     handles = driver.window_handles

#                     num_tabs = len(handles)

#                     # print(f"Number of open tabs/windows: {num_tabs}")
#                     if num_tabs==2:
#                         driver.switch_to.window(handles[1])
#                         driver.close()  
#                     driver.switch_to.window(window_after[0])

#                     window_billing = driver.window_handles
#                     driver.switch_to.default_content()
#                     driver.switch_to.window(window_billing[0])
#                     element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
#                     WebDriverWait(driver, 15).until(element_present1)
#                     driver.switch_to.frame("workarea2")

#                     element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
#                     WebDriverWait(driver, 10).until(element_present1)
#                     driver.switch_to.frame("GB_frame")

#                     element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
#                     WebDriverWait(driver, 5).until(element_present1)
#                     driver.switch_to.frame("GB_frame")

#                     close_button = WebDriverWait(driver, 10).until(
#                         EC.presence_of_element_located((By.XPATH, "//td[@width='50']/a[text()='Close']"))
#                     )

#                     close_button.click()
#                     # dubble_frame=1
#                 except:
#                     print("Window not found or close button not present.")
#             except:
#                 try:
#                     try:
#                         WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr'))).click() 
#                     except Exception as e:
#                         exc_type, exc_obj, exc_tb = sys.exc_info()        
#                         message=f"print download problem : line no : {exc_tb.tb_lineno} {str(e)}"
#                         # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  
#                     try:
#                         window_after = driver.window_handles

#                         handles = driver.window_handles

#                         num_tabs = len(handles)

#                         if num_tabs==2:
#                             driver.switch_to.window(handles[1])
#                             driver.close()  
#                         driver.switch_to.window(window_after[0])

#                         window_billing = driver.window_handles
#                         driver.switch_to.default_content()
#                         driver.switch_to.window(window_billing[0])
#                         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
#                         WebDriverWait(driver, 15).until(element_present1)
#                         driver.switch_to.frame("workarea2")

#                         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_window"]/div[2]/iframe'))
#                         WebDriverWait(driver, 10).until(element_present1)
#                         driver.switch_to.frame("GB_frame")

#                         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="GB_frame"]'))
#                         WebDriverWait(driver, 5).until(element_present1)
#                         driver.switch_to.frame("GB_frame")

#                         close_button = WebDriverWait(driver, 10).until(
#                             EC.presence_of_element_located((By.XPATH, "//td[@width='50']/a[text()='Close']"))
#                         )

#                         close_button.click()
#                         dubble_frame=1
#                     except:
#                         print("Window not found or close button not present.")
#                 except:
#                     pass

                
           
#         download_directory=ERAPATH
#         time.sleep(2)
#         try:
#             if dubble_frame==0:
#                 wait_for_download2(download_directory)
#         except:
#             pass
#         # time.sleep(2)
        
#         filename = filename1
#         if dubble_frame==0:
#             try:
#                 rename_pdf_file(filename)
#             except:
#                 pass
#             move_latest_file(filename)
#             insert_log((puid,datetime.utcnow(),'INFO',"",acctno,inventory_data,f"Successfully downloaded file for - Medical History : {str(name)}","6",username,credentialmasterid),connection)
#             # message=f"STEP 6 medical History pdf download completed for {filename} pdf"
#             # insert_log(("",datetime.utcnow(),"Info","",acctno,inventory_data,message,"6","",""),connection) 


#         window_after = driver.window_handles
#         handles = driver.window_handles
#         num_tabs = len(handles)
#         if num_tabs==2:
#             driver.switch_to.window(handles[1])
#             driver.close()  
#         driver.switch_to.window(window_after[0])


#         window_billing = driver.window_handles
#         driver.switch_to.default_content()
#         driver.switch_to.window(window_billing[0])
#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
#         WebDriverWait(driver, 20).until(element_present1)
#         driver.switch_to.frame("workarea2")
#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="PatientHistory"]'))
#         WebDriverWait(driver, 20).until(element_present1)
#         driver.switch_to.frame("PatientHistory")
#     except Exception as e:
#         exc_type, exc_obj, exc_tb = sys.exc_info()      
#         message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
#         # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)  
#     return dubble_frame



def login_cms(url,username,password,driver):
    
    try:      
        driver.get(url)
        driver.maximize_window()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="id"]'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="id"]'))).clear()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="id"]'))).send_keys(str(username))
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="password"]'))).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="password"]'))).clear()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="password"]'))).send_keys(str(password))        
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="loginBtn"]/tbody/tr/td'))).click()
        time.sleep(3)        
        return driver
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info() 
        message=f"login_cms line no : {exc_tb.tb_lineno} {str(e)}"  
        raise Exception(message)


def convert_to_mm_dd_yyyy(date_string):
    date_formats = ["%Y-%m-%d","%Y-%d-%m", "%Y/%m/%d", "%d-%m-%Y","%m/%d/%Y","%m-%d-%Y",'%Y-%d-%m %H:%M:%S']
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, date_format)
            converted_date = parsed_date.strftime("%m/%d/%Y")
            return converted_date
        except ValueError:
            pass
    return date_string  

def find_row_index_waiting(driver,table_id, column_index, target_value):
    table = driver.find_element(By.XPATH, table_id)
    rows = table.find_elements(By.TAG_NAME, 'tr')    
    for index, row in enumerate(rows):
        cells = row.find_elements(By.TAG_NAME, 'td')        
        if len(cells) > column_index and target_value in str(cells[column_index].text):
            return index  

    return -1  

def find_row_index(driver,table_id, column_index, target_value):
    try:
        table = driver.find_element(By.XPATH, table_id)
        rows = table.find_elements(By.TAG_NAME, 'tr')        
        for index, row in enumerate(rows):
            cells = row.find_elements(By.TAG_NAME, 'td')            
            if len(cells) > column_index and target_value in str(cells[column_index].text) :
                return index  
        return -1
    except:
        return -1  



def find_rows_index(driver, table_xpath, column_index, target_value):
    try:
        # Build the XPath to locate all cells in the specified column containing the target_value
        cell_xpath = f'{table_xpath}/tbody/tr/td[{column_index + 1}][contains(text(), "{target_value}")]'
        
        # Wait for the cells to be present
        cells = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, cell_xpath))
        )
        
        # Collect the indices of the rows that contain these cells
        row_indices = []
        for cell in cells:
            # Find the row that contains this cell
            row = cell.find_element(By.XPATH, '..')  # '..' gets the parent <tr> element
            # Get the index of the row
            row_index = row.find_elements(By.XPATH, f'{table_xpath}/tbody/tr').index(row) + 1
            row_indices.append(row_index)
        
        return row_indices
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# def check_element_loaded(driver, element_xpath, timeout=10):
#     try:
#         # Wait for the element to be present and visible
#         element = WebDriverWait(driver, timeout).until(
#             EC.visibility_of_element_located((By.XPATH, element_xpath))
#         )
#         print(f"Element found and is visible.")
#         return True
#     except Exception as e:
#         # Element not found or not visible
#         print(f"Element not found or not visible. Error: {e}")
#         return False


def check_element_loaded(driver, element_xpath, timeout=10):
    try:
        # Wait for the element to be present in the DOM
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, element_xpath))
        )
        
        # Once the element is present, wait for it to be visible
        element = WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.XPATH, element_xpath))
        )
        
        print("Element found and is visible.")
        return True
    except Exception as e:
        print(f"Element not found or not visible. Error: {e}")
        return False



def normalize_text(text):
    return html.unescape(text.strip().lower())

def generate_billing_data(driver,row,PatientId,patientindex,connection,refresh_page):
    # time.sleep(2)
    try:
        patient_name_chart=row['patientname'] 
        patient_name_chart_1=" ".join(patient_name_chart.split(' ')[:2]) 
        patient_name_chart_ =patient_name_chart.replace(',','').replace(' ','')
        patient_DOB =row['dob'] 
        try:
            driver.switch_to.alert.accept()
            alert = Alert(driver)
            alert.accept()
        except:
            pass
        # if refresh_page==0:
        # refresh_page=1
        billing_image = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//td[@title="Billing"]//img'))
        )
        # Once the image is located, click it
        billing_image.click()
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
        WebDriverWait(driver, 30).until(element_present1)
        driver.switch_to.frame("workarea0") 
        time.sleep(1)
            #check add

#         element_xpath = '//*[@id="divOption"]/table/tbody/tr[9]/td/table/tbody/tr[1]/td/input[1]'
        
#         if check_element_loaded(driver, element_xpath):
#             # Proceed with further actions if the element is loaded
#             print("Proceeding with further actions.")
#         else:
#             # Handle the case where the element is not loaded
#             print("Handling the case where the element is not loaded.")


#         # dropdown_element = WebDriverWait(driver, 10).until( 987
#             # EC.presence_of_element_located((By.XPATH, '//*[@id="DivisionList_myDivisonList"]')) 987
#             # ) 987
                
#         # dropdown = Select(dropdown_element) 987
#         # dropdown.select_by_visible_text(row["division"])
        
#         # dropdown_options = [option.text.strip() for option in dropdown.options] 987

#         # Normalize the row["division"]
#         # normalized_row_division = normalize_text(row["division"])
        
#         # print(normalized_row_division)  # Output: rendr: adult & senior health
# # Selecting division in BILLING Tab from DB
#         # Find the exact match for the division in the dropdown options
#         # matching_option = None
#         # for option in dropdown_options:
#         #     if normalized_row_division in normalize_text(option) :
#         #         matching_option = option
#         #         break

#         # If we find a match, select it
#         # if matching_option:
#         #     print(f"Selecting division: {matching_option}")
#         #     dropdown.select_by_visible_text(matching_option)
#         # else:
#         #     print(f"No match found for division: {row['division']}")
#         #     insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error division name not found {row['division']}","1","",""),connection)

        
        
#         WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="filterDateMode1"]'))).click()
#         time.sleep(2)


#         window_billing1 = driver.window_handles
#         driver.switch_to.default_content()
#         driver.switch_to.window(window_billing1[0])    

#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
#         WebDriverWait(driver, 0).until(element_present1)
#         driver.switch_to.frame("workarea0")
#         time.sleep(1)

#         element_xpath = '//*[@id="divOption"]/table/tbody/tr[7]/td/table/tbody/tr[1]/td/input[1]'
        
#         if check_element_loaded(driver, element_xpath):
#             # Proceed with further actions if the element is loaded
#             print("Proceeding with further actions.")
#         else:
#             # Handle the case where the element is not loaded
#             print("Handling the case where the element is not loaded.")
#         time.sleep(2)
#         WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[7]/table/tbody/tr[7]/td/table/tbody/tr[1]/td/input[1]'))).click()
#         time.sleep(1)

#         window_billing1 = driver.window_handles
#         driver.switch_to.default_content()
#         driver.switch_to.window(window_billing1[0])  
#         time.sleep(1)
#         element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="frm_searchframe"]'))
#         WebDriverWait(driver, 50).until(element_present1)
#         driver.switch_to.frame("frm_searchframe")
#         WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).click()
#         dropdown_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="searchBy"]')))
#         dropdown = Select(dropdown_element)
#         dropdown.select_by_visible_text("Patient ID")        

#         if len(PatientId)>6:
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).clear()
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(PatientId))
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[4]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a'))).click()

#         else:
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).clear()
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(patient_name_chart_1))
#             WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
#             time.sleep(2)
#             WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a')))

#             data=driver.find_element(By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]').text
# # 
#             print(data.split('\n'))
#             for index_i,name_dob in enumerate(data.split('\n')):
#                 patient_only_name=driver.find_element(By.XPATH,f'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr[{str(index_i+1)}]/td[1]/a').text
#                 patient_only_name=patient_only_name.replace(',','').replace(' ','')
#                 if patient_DOB in name_dob and patient_name_chart_.lower() in patient_only_name.lower():
#                     WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr[{str(index_i+1)}]/td[1]/a'))).click()
#                     insert_log(("",datetime.utcnow(),'INFO',"",PatientId,row['id'],f"old Format Patient name found","1","",""),connection) 
#                     break

        # time.sleep(4)
        BillingID=row['BillingId'] 

        # dropdown_element = WebDriverWait(driver, 10).until(
        #     EC.presence_of_element_located((By.XPATH, '//*[@id="DivisionList_myDivisonList"]'))
        #     )
        
        # dropdown = Select(dropdown_element)

        # dropdown.select_by_visible_text("All")
        time.sleep(1)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@name='listBillingID']"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@name='listBillingID']"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@name='listBillingID']"))).clear()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@name='listBillingID']"))).clear()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@name='listBillingID']"))).send_keys(str(BillingID))  
        
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@name='listBillingID']"))).send_keys(Keys.ENTER)
    
        time.sleep(1)


        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])    
        time.sleep(2)                 
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("workarea0")
        # select 7
        # WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[7]/td/input[4]'))).click()


        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="billingFrame"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("billingFrame")

        element_xpath = '/html/body/form/div[3]/table/tbody/tr[2]/td[5]/a'

        if check_element_loaded(driver, element_xpath):
            # Proceed with further actions if the element is loaded
            print("Proceeding with further actions.")
        else:
            # Handle the case where the element is not loaded
            print("Handling the case where the element is not loaded.")

        column_index = 2 
        target_value = convert_to_mm_dd_yyyy(row["DOS"])

        table_id = '//*[@id="div2"]/table'  
        row_index = find_row_index(driver,table_id, column_index, target_value)                
        if row_index != -1:
            try:
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr[{row_index + 1}]/td[5]/a'))).click()
            except:
                insert_log(("",datetime.utcnow(),'Error',"",PatientId,row['id'],f"Patient not found in Billing tab for {str(PatientId)}","1","",""),connection)

        Claim_dict = {}
        service_line_dict = {}
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])    
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
        WebDriverWait(driver, 30).until(element_present1)
        driver.switch_to.frame("workarea1")
        time.sleep(1)
        driver,Claim_df,temp_dict=get_claim_level_data(driver,Claim_dict)
        driver,Service_df,temp_dict=get_service_line_data(driver,temp_dict,patientindex)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()                        
        insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error BILLING :  line no : {exc_tb.tb_lineno} {str(e)}","1","",""),connection)


    return driver,Claim_df,Service_df,target_value

def get_claim_level_data(driver,Claim_dict):
    time.sleep(1)
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="s1"]/tbody/tr[2]/td[1]/input')))
    except:
        pass
    Pt_name_and_Age_string = driver.find_element(By.XPATH , '//*[@id="s1"]/tbody/tr[2]/td[1]/input').get_attribute('value')
    Pt_name_and_Age_string_dob = driver.find_element(By.XPATH , '//*[@id="s1"]/tbody/tr[2]/td[2]/input').get_attribute('value')            
    Pt_name_and_Age_string2 = driver.find_element(By.XPATH , '//*[@id="divtitle"]/table/tbody/tr/td[1]/span').text
    Pt_name_and_Age_string2 = "".join(re.findall(r"\d{1,3}[A-Za-z]{1}",Pt_name_and_Age_string2))
    pt_name_age_list = [Pt_name_and_Age_string,Pt_name_and_Age_string_dob,Pt_name_and_Age_string2]
    try:                
        Claim_dict.update({"Patient's Name (L,F M)":pt_name_age_list[0].strip()})
        Claim_dict.update({"Patient's Birth Day":pt_name_age_list[1].strip()})
        Claim_dict.update({"Patient's Age":pt_name_age_list[2].strip()})
    except:
        try:
            Claim_dict.update({"Patient's Name (L,F M)":pt_name_age_list.strip()})
            Claim_dict.update({"Patient's Birth Day":pt_name_age_list.strip()})
            Claim_dict.update({"Patient's Age":pt_name_age_list.strip()})
        except:
            pass

    if driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[1]/table/tbody/tr/td[2]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"Medicare"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[1]/table/tbody/tr/td[3]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"Medicaid"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[1]/table/tbody/tr/td[4]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"Tricare"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[2]/table/tbody/tr/td[1]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"Champva"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[2]/table/tbody/tr/td[2]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"Group"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[2]/table/tbody/tr/td[3]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"FECA"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[2]/table/tbody/tr/td[4]/input').is_selected():
        Claim_dict.update({"Type Of Insurance":"Other"})
    else:
        Claim_dict.update({"Type Of Insurance":""})
    Claim_dict.update({"Insured ID Number":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[1]/td[3]/input').get_attribute("value")})
    if driver.find_element(By.XPATH,'//*[@id="span_b3Sex"]/input[1]').is_selected():
        Claim_dict.update({"Gender":"Male"})
    elif driver.find_element(By.XPATH,'//*[@id="span_b3Sex"]/input[2]').is_selected():
        Claim_dict.update({"Gender":"Female"})
    else:
        Claim_dict.update({"Gender":""})
    Claim_dict.update({"Insured's Name (L,F M)":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[2]/td[3]/input').get_attribute("value")})
    PatientAddressstreet = driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[3]/td[1]/table/tbody/tr[2]/td/input').get_attribute("value")
    Patient_State = driver.find_element(By.XPATH,'//*[@id="b5City"]').get_attribute('value')
    Patient_Address_state_city = driver.find_element(By.XPATH,'//*[@id="b5State"]').get_attribute("value")
    Patient_address_zip = driver.find_element(By.XPATH,'//*[@id="b5Zip"]').get_attribute("value")
    Patient_address_tel1 = driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[3]/td[1]/table/tbody/tr[6]/td/input[2]').get_attribute("value")
    Patient_address_tel2 = driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[3]/td[1]/table/tbody/tr[6]/td/input[3]').get_attribute("value")
    Claim_dict.update({"Patient's Address":PatientAddressstreet+" "+Patient_State+" "+Patient_Address_state_city+" "+Patient_address_zip+" "+Patient_address_tel1+" "+Patient_address_tel2})
    if driver.find_element(By.XPATH,'//*[@id="span_b6"]/input[1]').is_selected():
        Claim_dict.update({"Patient Relationship To Insured":'Self'})
    elif driver.find_element(By.XPATH,'//*[@id="span_b6"]/input[2]').is_selected():
        Claim_dict.update({"Patient Relationship To Insured":'Spouse'})
    elif driver.find_element(By.XPATH,'//*[@id="span_b6"]/input[3]').is_selected():
        Claim_dict.update({"Patient Relationship To Insured":'Child'})
    elif driver.find_element(By.XPATH,'//*[@id="span_b6"]/input[4]').is_selected():
        Claim_dict.update({"Patient Relationship To Insured":'Other'})
    else:
        Claim_dict.update({"Patient Relationship To Insured":''})
    if driver.find_element(By.XPATH,'//*[@id="span_b8a"]/input[1]').is_selected():
        Claim_dict.update({"Marital Status":"Single"})
    elif driver.find_element(By.XPATH,'//*[@id="span_b8a"]/input[2]').is_selected():
        Claim_dict.update({"Marital Status":"Married"})
    elif driver.find_element(By.XPATH,'//*[@id="span_b8a"]/input[3]').is_selected():
        Claim_dict.update({"Marital Status":"Other"})
    else:
        Claim_dict.update({"Marital Status":""})

    InsuredAddressstreet = driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[3]/td[3]/table/tbody/tr[2]/td/input').get_attribute("value")            
    Insured_State = driver.find_element(By.XPATH,'//*[@id="b7City"]').get_attribute('value')
    Insured_Address_state_city = driver.find_element(By.XPATH,'//*[@id="b7State"]').get_attribute("value")
    Insured_address_zip = driver.find_element(By.XPATH,'//*[@id="b7Zip"]').get_attribute("value")
    Insured_address_tel1 = driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[3]/td[3]/table/tbody/tr[6]/td/input[2]').get_attribute("value")
    Insured_address_tel2 = driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[3]/td[3]/table/tbody/tr[6]/td/input[3]').get_attribute("value")

    Claim_dict.update({"Insured's Address":InsuredAddressstreet+" "+Insured_State+" "+Insured_Address_state_city+" "+Insured_address_zip+" "+Insured_address_tel1+" "+Insured_address_tel2})

    Claim_dict.update({"Other Insured's Name":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[1]/table/tbody/tr[2]/td/input').get_attribute("value")})


    Claim_dict.update({"Policy or Group Number":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[1]/table/tbody/tr[4]/td/input').get_attribute("value")})

    Claim_dict.update({"Insurance Plan or Program Name":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[1]/table/tbody/tr[10]/td/input').get_attribute("value")})

    if driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[2]/table/tbody/tr[2]/td/input[1]').is_selected():
        Claim_dict.update({"Patient's Condition Related To Employment":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[2]/table/tbody/tr[2]/td/input[2]').is_selected():
        Claim_dict.update({"Patient's Condition Related To Employment":"No"})
    else:
        Claim_dict.update({"Patient's Condition Related To Employment":""})

    if driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[2]/table/tbody/tr[4]/td/input[1]').is_selected():
        Claim_dict.update({"Patient's Condition Related To Auto Accident":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[2]/table/tbody/tr[4]/td/input[2]').is_selected():
        Claim_dict.update({"Patient's Condition Related To Auto Accident":"No"})
    else:
        Claim_dict.update({"Patient's Condition Related To Auto Accident":""})

    if driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[2]/table/tbody/tr[6]/td/input[1]').is_selected():
        Claim_dict.update({"Patient's Condition Related To Other Accident":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[2]/table/tbody/tr[6]/td/input[2]').is_selected():
        Claim_dict.update({"Patient's Condition Related To Other Accident":"No"})
    else:
        Claim_dict.update({"Patient's Condition Related To Other Accident":""})

    Claim_dict.update({"Policy Group/FECA Number":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[3]/table/tbody/tr[2]/td/input').get_attribute("value")})

    Claim_dict.update({"Insured's Date of Birth":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[3]/table/tbody/tr[3]/td/input').get_attribute("value")})

    if driver.find_element(By.XPATH,'//*[@id="span_b11aSex"]/input[1]').is_selected():
        Claim_dict.update({"Insured's Gender":"Male"})
    elif driver.find_element(By.XPATH,'//*[@id="span_b11aSex"]/input[2]').is_selected():
        Claim_dict.update({"Insured's Gender":"Female"})
    else:
        Claim_dict.update({"Insured's Gender":""})

    Claim_dict.update({"Insurance Plan/Program Name":driver.find_element(By.XPATH,'//*[@id="s1"]/tbody/tr[4]/td[3]/table/tbody/tr[5]/td/input').get_attribute("value")})

    if driver.find_element(By.XPATH,'//*[@id="span_b11d"]/input[1]').is_selected():
        Claim_dict.update({"Has Another Health Benefit Plan?":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="span_b11d"]/input[2]').is_selected():
        Claim_dict.update({"Has Another Health Benefit Plan?":"No"})
    else:
        Claim_dict.update({"Has Another Health Benefit Plan?":""})

    Claim_dict.update({"Date of Patient and Insured Signature":driver.find_element(By.XPATH,'//*[@id="ssign"]/tbody/tr/td[1]/input[2]').get_attribute("value")})

    Claim_dict.update({"Date of Current Illness, Injury or Pregnancy(LMP)":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[1]/table[1]/tbody/tr[1]/td[1]/input').get_attribute("value")})
    Claim_dict.update({"Date of Current Illness, Injury or Pregnancy(LMP) (Qual)":driver.find_element(By.XPATH,'//*[@id="span_b14Qual"]/input').get_attribute("value")})

    Claim_dict.update({"Other Date":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[1]/table[1]/tbody/tr[1]/td[2]/input').get_attribute("value")})
    Claim_dict.update({"Other Date (Qual)":driver.find_element(By.XPATH,'//*[@id="span_b15Qual"]/input').get_attribute("value")})

    Claim_dict.update({"Dates Patient Unable To Work (From Date)":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[1]/td/input[1]').get_attribute("value")})
    Claim_dict.update({"Dates Patient Unable To Work (To Date)":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[1]/td/input[2]').get_attribute("value")})

    phyLastName = driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[1]/table[1]/tbody/tr[2]/td[1]/input[2]').get_attribute("value")
    phyFirstName = driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[1]/table[1]/tbody/tr[2]/td[1]/input[3]').get_attribute("value")
    phyMidName = driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[1]/table[1]/tbody/tr[2]/td[1]/input[4]').get_attribute("value")

    Claim_dict.update({"Name of Referring Physician":phyLastName+" "+phyFirstName+" "+phyMidName})

    #
    Claim_dict.update({"ID of Ref":driver.find_element(By.XPATH,'//*[@id="b17aInsuText"]/input').get_attribute("value")})

    Claim_dict.update({"NPI":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[1]/table[1]/tbody/tr[2]/td[2]/table/tbody/tr[2]/td[2]/input').get_attribute("value")})

    Claim_dict.update({"Hospitalization Dates Rel. To Current (From Date)":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[2]/td/input[1]').get_attribute("value")})
    Claim_dict.update({"Hospitalization Dates Rel. To Current (To Date)":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[2]/td/input[2]').get_attribute("value")})

    if driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[3]/td/input[1]').is_selected():
        Claim_dict.update({"Outside Lab?":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[3]/td/input[2]').is_selected():
        Claim_dict.update({"Outside Lab?":"No"})
    else:
        Claim_dict.update({"Outside Lab?":""})

    Claim_dict.update({"Outside Lab $Charges":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[1]/tbody/tr[3]/td/input[3]').get_attribute("value")})

    Claim_dict.update({"Resubmission Code (Claim Frequency Type Code)":driver.find_element(By.XPATH,'//*[@id="b22FrequencyTypeCode"]').get_attribute("value")})
    Claim_dict.update({"Resubmission Code (Original Reference Number)":driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[2]/tbody/tr/td/table/tbody/tr[3]/td/input').get_attribute("value")})
    Claim_dict.update({"Resubmission Code (Delay Reason Code)":driver.find_element(By.XPATH,'//*[@id="resubcodelist"]').get_attribute("value")})


    len_icd_code = len(driver.find_elements(By.XPATH,'//*[@id="tbody_icd"]/tr'))
    icd_code = []
    icd_description = []
    for w in range(len_icd_code):
        w= w+1
        icd_code.append(driver.find_element(By.XPATH,'//*[@id="tbody_icd"]/tr['+str(w)+']/td[2]/input').get_attribute('value'))
        icd_description.append(driver.find_element(By.XPATH,'//*[@id="tbody_icd"]/tr['+str(w)+']/td[3]/input').get_attribute('value'))

    temp_dict = {}
    idx = [w+1 for w in range(len(icd_code))]
    for w,j,k in zip(idx ,icd_code,icd_description):
        temp_dict.update({str(w):{str(j):str(k)}})


    icd_10or9 = driver.find_element(By.XPATH,'//*[@id="b21ICDCodeSystem"]').get_attribute("value")
    Claim_dict.update({str(icd_10or9)+" "+"Diagnosis or Nature of Illness Of Injury":[temp_dict]})


    preauthonumber1 = driver.find_element(By.XPATH,'//*[@id="s2"]/tbody/tr/td[2]/table[3]/tbody/tr/td/input').get_attribute("value")
    preauthonumber2 = driver.find_element(By.XPATH,'//*[@id="authorization"]').text
    Claim_dict.update({"Prior Authorization Number":preauthonumber1+preauthonumber2})

    #
    Claim_dict.update({"Tax I.D.":driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[1]/input').get_attribute("value")})

    if driver.find_element(By.XPATH,'//*[@id="span_25Type"]/input[1]').is_selected():
        Claim_dict.update({"Tax I.D. SSN Or EIN":"SSN"})
    elif driver.find_element(By.XPATH,'//*[@id="span_25Type"]/input[2]').is_selected():
        Claim_dict.update({"Tax I.D. SSN Or EIN":"EIN"})
    else:
        Claim_dict.update({"Tax I.D. SSN Or EIN":""})

    #
    Claim_dict.update({"Patient's Account No":driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[2]/input').get_attribute("value")})
    acctno= driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[2]/input').get_attribute("value")
    if driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[3]/input[1]').is_selected():
        Claim_dict.update({"Accept Assignment":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[3]/input[2]').is_selected():
        Claim_dict.update({"Accept Assignment":"No"})
    else:
        Claim_dict.update({"Accept Assignment":""})

    #
    Claim_dict.update({"Total Charge":driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[4]/input[1]').get_attribute("value")})
    Claim_dict.update({"Amount Paid Charge":driver.find_element(By.XPATH,'//*[@id="Table1"]/tbody/tr/td/table/tbody/tr/td[5]/input').get_attribute("value")})

    Claim_dict.update({"Signature of Physician or Supplier Including Degrees or Credentials (Name)":driver.find_element(By.XPATH,'//*[@id="divPhysicianSignature"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/input[1]').get_attribute("value")})
    Claim_dict.update({"Signature of Physician or Supplier Including Degrees or Credentials (Date)":driver.find_element(By.XPATH,'//*[@id="b31Date"]').get_attribute("value")})
    Claim_dict.update({"Signature of Physician or Supplier Including Degrees or Credentials (User Claim Comments)":driver.find_element(By.XPATH,'//*[@id="divPhysicianSignature"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/input[4]').get_attribute("value")})
 
    location_Names=driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/input').get_attribute("value")
    Claim_dict.update({"locationName":location_Names})

    name_ = driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/input').get_attribute("value")
    address1_ = driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[3]/td[2]/input').get_attribute("value")
    address2_ = driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[4]/td[2]/input').get_attribute("value")
    address3_ = driver.find_element(By.XPATH,'//*[@id="b32City"]').get_attribute("value")
    address4_ = driver.find_element(By.XPATH,'//*[@id="b32State"]').get_attribute("value")
    address5_ = driver.find_element(By.XPATH,'//*[@id="b32Zip"]').get_attribute("value")
    address6_ = driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[6]/td[2]/input[2]').get_attribute("value")
    address7_ = driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[6]/td[2]/input[3]').get_attribute("value")

    Claim_dict.update({"Service Facility Location Information(Name/Address/zip/phone)":name_+" "+address1_+" "+address2_+" "+address3_+" "+address4_+" "+address5_+"-"+address6_+" "+address7_})

    Claim_dict.update({"Service Facility Location Information(NPI)":driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[7]/td[2]/input[1]').get_attribute("value")})

    Claim_dict.update({"Service Facility Location Information(Other ID#)":driver.find_element(By.XPATH,'//*[@id="divFacility"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[7]/td[2]/input[2]').get_attribute("value")})

    if driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/input[1]').is_selected():
        Claim_dict.update({"Billing Provider Information and Phone Number(Group Bill)":"Yes"})
    elif driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/input[2]').is_selected():
        Claim_dict.update({"Billing Provider Information and Phone Number(Group Bill)":"No"})
    else:
        Claim_dict.update({"Billing Provider Information and Phone Number(Group Bill)":""})
    name_ = driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[3]/td[2]/input').get_attribute("value")
    address1_ = driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[4]/td[2]/input').get_attribute("value")
    address2_ = driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[5]/td[2]/input').get_attribute("value")
    address3_ = driver.find_element(By.XPATH,'//*[@id="b33City"]').get_attribute("value")
    address4_ = driver.find_element(By.XPATH,'//*[@id="b33State"]').get_attribute("value")
    address5_ = driver.find_element(By.XPATH,'//*[@id="b33Zip"]').get_attribute("value")
    address6_ = driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[7]/td[2]/input[2]').get_attribute("value")
    address7_ = driver.find_element(By.XPATH,'//*[@id="divBillingAddress"]/table[2]/tbody/tr/td/div/table/tbody/tr[2]/td/table/tbody/tr[7]/td[2]/input[3]').get_attribute("value")



    Claim_dict.update({"Billing Provider Information and Phone Number":name_+" "+address1_+" "+address2_+" "+address3_+" "+address4_+" "+address5_+"-"+address6_+" "+address7_})

    #
    Claim_dict.update({"Billing Provider Information and Phone Number(PIN#)":driver.find_element(By.XPATH,'//*[@id="b33TDtext"]/input[1]').get_attribute("value")})

    Claim_dict.update({"Billing Provider Information and Phone Number(GRP#)":driver.find_element(By.XPATH,'//*[@id="b33TDtext"]/input[2]').get_attribute("value")})

    Claim_dict.update({"Billing Provider Information and Phone Number(NPI#)":driver.find_element(By.XPATH,'//*[@id="b33TDtext"]/input[3]').get_attribute("value")})
    Claim_df = pd.DataFrame.from_dict(Claim_dict)
    
    return driver,Claim_df,temp_dict

def get_service_line_data(driver,temp_dict,i):
    # time.sleep(1)
    # try:
    #     WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="tbody_cpt"]/tr')))
    # except:
    #     pass
    len_rows = len(driver.find_elements(By.XPATH,'//*[@id="tbody_cpt"]/tr'))
    len_cols = len(driver.find_elements(By.XPATH,'//*[@id="tbody_cpt"]/tr[2]/td'))
    col_v = [w for w in range(len_cols) if not w in [0,1,5,16,20,21,22]]
    service_line_dict = {}
    for p in col_v:
        list1 =[]
        list2 = []
        for r in range(2,len_rows+1):
            try:
                values = driver.find_element(By.XPATH,'/html/body/form/div[5]/div[1]/div[3]/table[2]/tbody/tr/td/div/table/tbody/tr[1]/td/table/tbody/tr['+str(r)+']/td['+str(p)+']/input').get_attribute('value')
                list1.append(values)
                if p ==18:
                    values = driver.find_element(By.XPATH,'/html/body/form/div[5]/div[1]/div[3]/table[2]/tbody/tr/td/div/table/tbody/tr[1]/td/table/tbody/tr['+str(r)+']/td['+str(p)+']/input[2]').get_attribute('value')
                    list2.append(values)
            except:
                values = driver.find_element(By.XPATH,'/html/body/form/div[5]/div[1]/div[3]/table[2]/tbody/tr/td/div/table/tbody/tr[1]/td/table/tbody/tr['+str(r)+']/td['+str(p)+']/span/input[1]').get_attribute('value')
                list1.append(values)
                if p ==18:
                    values = driver.find_element(By.XPATH,'/html/body/form/div[5]/div[1]/div[3]/table[2]/tbody/tr/td/div/table/tbody/tr[1]/td/table/tbody/tr['+str(r)+']/td['+str(p)+']/span/input[3]').get_attribute('value')
                    list2.append(values)

        col_name = driver.find_element(By.XPATH,'//*[@id="tbody_cpt"]/tr[1]/th['+str(p)+']').text
        service_line_dict.update({col_name:list1})
        if len(list2)!=0:
            service_line_dict.update({'Rendering_NPI':list2})
        continue
    dx_code = []
    for w in service_line_dict['ICD Code']:
        Diagnosis =[]
        if ',' in w:
            sep_value = "".join(w).split(',')
            for j in sep_value:
                Diagnosis.append({j:temp_dict[j]})
            service_line_dict.update({'Diagnosis Code':Diagnosis})
        elif len(w)==1:
            Diagnosis.append({i:temp_dict[w]})
        dx_code.append(Diagnosis)
    service_line_dict.update({'Diagnosis Code':dx_code})

    
    Service_df = pd.DataFrame.from_dict(service_line_dict)

    return driver,Service_df,temp_dict

def get_otp(connection, id, ip_address):
    if connection:
        try:
            cursor = connection.cursor()
            message=f'SELECT otp FROM public.tbl_emr_credential_ip_mapping WHERE credentialmasterid = {id} and ip_address = {ip_address}'
            insert_log(("",datetime.utcnow(),"INFO","","","",message,"1","",""),connection)
            try:

            # Use parameterized queries to prevent SQL injection
                cursor.execute('SELECT otp FROM public.tbl_emr_credential_ip_mapping WHERE credentialmasterid = %s and ip_address = %s', (id,ip_address,))
            except:
                # Properly format the query with direct string formatting
                query = f'''
                    SELECT otp FROM public.tbl_emr_credential_ip_mapping WHERE credentialmasterid in ({id}) and ip_address in ('{ip_address}');'''

                # Execute the query
                cursor.execute(query)



            rows = cursor.fetchone()
            return rows[0] if rows else None
        except Exception as e:
            print(f"An error occurred: {e}")
            exc_type, exc_obj, exc_tb = sys.exc_info()        
            message=f"Extraction error for get otp : line no : {exc_tb.tb_lineno} {str(e)}"
            insert_log(("",datetime.utcnow(),"ERROR","","","",message,"1","",""),connection)

            return None


def select_site(driver,connection,username,credentialmasterid,ip_address,site_patient_data,puid):
    try:
        aws_key=login_details.loc[0,"awsaccesskey"]
        aws_sec_key=login_details.loc[0,"awssecretkey"]
        aws_bucket=login_details.loc[0,"awsbucketname"]
        aws_region=login_details.loc[0,"awsregion"]
        awsS3filepath=login_details.loc[0,"awss3filepath"]
        Client_1=site_patient_data.loc[0,"clientid"]
        Client_2=site_patient_data.loc[0,"subclientid"]
        site_selected_completed=0
        name_before_parentheses=""
        time.sleep(2)
        # WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="tableLab"]/tbody/tr[6]/td/div/table/tbody/tr/td')))
    except:
        is_otp=True
        # ///TEMP
        a_azur=site_patient_data.loc[0,"azureblobconnstring"]
        a_azur_1=site_patient_data.loc[0,"azurecontainername"]
        Client_1=site_patient_data.loc[0,"clientid"]
        Client_2=site_patient_data.loc[0,"subclientid"]
        # ///TEMP END
        while is_otp:
            if is_otp==False:
                break
            try:
                if_not_otp=0
                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="phoneCode"]'))
                WebDriverWait(driver, 20).until(element_present1)
                
                message=f"otp window found"
                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"1",username,credentialmasterid),connection)
                if_not_otp=1
                time.sleep(60)
                # ///TEMP
                try:
                    window_billing1 = driver.window_handles
                    driver.switch_to.default_content()
                    driver.switch_to.window(window_billing1[0])  
                    screenshot_bytes = driver.get_screenshot_as_png()

                    blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                    container_client = blob_service_client.get_container_client(a_azur_1) 

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    unique_id = str(uuid.uuid4())

                    file_names_2="Amol_screenshot"
                    file_names_3 = f"screenshot_{timestamp}_{unique_id}.png"

                    blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

                    content_settings = ContentSettings(content_type="image/png")                
                    container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                    insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","1",username,credentialmasterid),connection)   
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)

                # ///TEMP END
                otp_data=get_otp(connection,credentialmasterid,ip_address)
                message=f"otp data {otp_data} "

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"1",username,credentialmasterid),connection)

                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="phoneCode"]'))).clear()
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="phoneCode"]'))).send_keys(str(otp_data))
                message=f'//*[@id="phoneCode"] xpath found otp enter'

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"1",username,credentialmasterid),connection)
                try:
                    window_billing1 = driver.window_handles
                    driver.switch_to.default_content()
                    driver.switch_to.window(window_billing1[0])  
                    screenshot_bytes = driver.get_screenshot_as_png()

                    blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                    container_client = blob_service_client.get_container_client(a_azur_1) 

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    unique_id = str(uuid.uuid4())

                    file_names_2="Amol_screenshot"
                    file_names_3 = f"screenshot_opt_enter_{timestamp}_{unique_id}.png"

                    blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

                    content_settings = ContentSettings(content_type="image/png")                
                    container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                    insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","1",username,credentialmasterid),connection)   
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)

                element_t = driver.find_element(By.XPATH, '//*[@id="trustMe"]')
                if not element_t.is_selected():
                    element_t.click()
                message=f'//*[@id="trustMe"] xpath found trust click'

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"1",username,credentialmasterid),connection)
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="ctl00_mainCont_inputPhoneCodePnl"]/table/tbody/tr[4]/td/div/table/tbody/tr/td'))).click()
                time.sleep(2)
                message=f'//*[@id="ctl00_mainCont_inputPhoneCodePnl"]/table/tbody/tr[4]/td/div/table/tbody/tr/td xpath found button click'

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"1",username,credentialmasterid),connection)
                try:
                    window_billing1 = driver.window_handles
                    driver.switch_to.default_content()
                    driver.switch_to.window(window_billing1[0])  
                    screenshot_bytes = driver.get_screenshot_as_png()

                    blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                    container_client = blob_service_client.get_container_client(a_azur_1) 

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    unique_id = str(uuid.uuid4())

                    file_names_2="Amol_screenshot"
                    file_names_3 = f"screenshot_after_otp_{timestamp}_{unique_id}.png"

                    blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

                    content_settings = ContentSettings(content_type="image/png")                
                    container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                    insert_log((puid,datetime.utcnow(),'INFO',site_id,"","",f"succesfully uploaded file {str(blob_name)}","1",username,credentialmasterid),connection)   
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)




                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="tableLab"]/tbody/tr[6]/td/div/table/tbody/tr/td')))
                message=f"otp window completed"
                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"1",username,credentialmasterid),connection)

                is_otp=False
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()        
                message=f"Extraction error for select_site : line no : {exc_tb.tb_lineno} {str(e)}"
                insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection) 
                if if_not_otp==0:
                    break
          
  
    try:
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])  
        screenshot_bytes = driver.get_screenshot_as_png()

        blob_service_client = BlobServiceClient.from_connection_string(a_azur)
        container_client = blob_service_client.get_container_client(a_azur_1)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())

        file_names_2="Amol_screenshot"
        file_names_3 = f"screenshot_after_otp_{timestamp}_{unique_id}.png"

        blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

        content_settings = ContentSettings(content_type="image/png")                
        container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
        insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","1",username,credentialmasterid),connection)   
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()        
        message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
        insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)


    ele_option=3
    while ele_option>0:
        try:
            First_date=driver.find_element(By.XPATH,f'/html/body/form/div[3]/div/table[2]/tbody/tr/td/div/center/table/tbody/tr[3]/td/table/tbody/tr[2]/td/select/option[{str(ele_option)}]').text
            if  First_date =='Rendr: Alex Wu Oncology (90000010)':
                match = re.search(r'^(.*?)\s*\(\d+\)$', First_date)
                if match:
                    name_before_parentheses = match.group(1).strip()   

                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/div/table[2]/tbody/tr/td/div/center/table/tbody/tr[3]/td/table/tbody/tr[2]/td/select/option[{str(ele_option)}]'))).click()
                time.sleep(1)
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="tableLab"]/tbody/tr[6]/td/div/table/tbody/tr/td'))).click()
                site_selected_completed=1
                break
            ele_option+=1
        except:
            ele_option+=1
            break
    return driver,name_before_parentheses,site_selected_completed


def move_latest_file(file_name, source_folder=ERAPATH, destination_folder=download_path, max_retries=75, retry_interval=3):    
    retries = 0
    while retries < max_retries:
        try:
            # List files in the source folder
            files = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f))]
            
            # Filter out hidden files or directories (optional)
            files = [f for f in files if not f.startswith('.')]
            
            if not files:
                print("No files found in the source folder.")
                return None  # Or raise an exception
                
            # Find the latest file
            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(source_folder, f)))  

            # Check if the latest file is being downloaded (has .crdownload extension)
            if latest_file.endswith(".crdownload"):
                print(f"Latest file '{latest_file}' is still being downloaded. Retrying after {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
                continue
            
            # Construct source and destination paths
            source_path = os.path.join(source_folder, latest_file)
            destination_path = os.path.join(destination_folder, file_name)  

            # Move the file
            shutil.move(source_path, destination_path)
            
            # Check file integrity after moving
            if os.path.getsize(destination_path) == 0:
                print("File moved but destination file is empty. There may have been an issue during the move operation.")
                return None
                
            return destination_path  # Return the destination path for reference
            
        except FileNotFoundError:
            print("Source folder not found:", source_folder)
            return None
        except PermissionError as e:
            print("Permission denied while accessing files:", e)
            retries += 1
            if retries >= max_retries:
                print("Maximum retries exceeded. Giving up.")
                return None
            print(f"Retrying after {retry_interval} seconds...")
            time.sleep(retry_interval)
        except Exception as e:
            print("An error occurred:", e)
            return None

def vaccine_f(driver, filename1,connection,PatientId):
    try:
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])                
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("workarea2")

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="EMGuildline"]'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("EMGuildline")
        ele_option=3
        target_area="Vaccine"
        time.sleep(2)

        try:
            vacaccine_button = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.LINK_TEXT, "Vaccine")))
            vacaccine_button.click()

        except:
            try:
                while ele_option>0:
                    try:
                        First_date=driver.find_element(By.XPATH,f'//*[@id="PatientHome"]/table/tbody/tr/td[8]/table/tbody/tr[{str[ele_option]}]/td/a ').text
                        if target_area in  First_date:
                            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                            break
                        ele_option+=1
                    except:
                        ele_option+=1
                        break
            except:
                pass 

        window_after = driver.window_handles[-1]

        driver.switch_to.window(window_after)

        try:

            print_link = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.LINK_TEXT, "Print")))
            time.sleep(1)
            print_link.click()
            insert_log((puid,datetime.utcnow(),'INFO',"",PatientId,"",f"succesfully downloaded Vaccine file for {str(PatientId)}","1",username,credentialmasterid),connection)
        except:
            try:
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[9]/table/tbody/tr[4]/td'))).click()
            except:
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="TDPrint"]/a'))).click()
        download_directory=ERAPATH
        try:
            wait_for_download2(download_directory)
        except:
            pass

        # window_after = driver.window_handles[-1]
        # time.sleep(1)
        # driver.switch_to.window(window_after)

        time.sleep(2)
        filename = filename1
        try:
            rename_pdf_file(filename)
        except:
            pass

        move_latest_file(filename)
        # message=f"DOS visit history pdf download completed for {filename} pdf"
        # insert_log(("",datetime.utcnow(),"Info","","","",message,"1","",""),connection) 
        window_after = driver.window_handles
        handles = driver.window_handles

        num_tabs = len(handles)

        if num_tabs==2:
            driver.switch_to.window(handles[1])
            driver.close()  
        driver.switch_to.window(window_after[0])
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()      
        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
        insert_log(("",datetime.utcnow(),"ERROR","",filename1,"",message,"1","",""),connection)  
    

def extract_dates(text):
    date_pattern = r'\b(\d{2}/\d{2}/\d{4})\b'
    dates = re.findall(date_pattern, text)
    if not dates:
        return ()
    first_date = dates[0] if len(dates) > 0 else None
    return first_date

def clean_lab_title(word):
    if word:
        # Split by ' -' and take the first part
        return word.split(' -')[0].strip()
    return word



def merge_chemo_pdfs(download_folder, Billing, Dos):
    writer = PdfWriter()
    valid_files = 0

    Dos_clean = str(Dos).replace("/", "")
    output_filename = f"Merged_Ch_Docs_{Billing}_{Dos_clean}.pdf"

    pdf_files = [
        f for f in os.listdir(download_folder)
        if f.startswith("Ch_") and f.endswith(".pdf")
    ]

    pdf_files.sort()

    if not pdf_files:
        print("No Chemo PDFs found to merge.")
        return None

    for pdf in pdf_files:
        file_path = os.path.join(download_folder, pdf)

        if os.path.getsize(file_path) == 0:
            print(f"Skipped 0KB file: {pdf}")
            continue

        try:
            reader = PdfReader(file_path)

            if len(reader.pages) == 0:
                print(f"Skipped empty PDF (no pages): {pdf}")
                continue

            for page in reader.pages:
                writer.add_page(page)

            valid_files += 1

        except Exception as e:
            print(f"Corrupted file skipped: {pdf} | Error: {e}")
            continue

    if valid_files == 0:
        print("No valid chemo PDFs to merge. Merged file not created.")
        return None

    output_path = os.path.join(download_folder, output_filename)

    try:
        with open(output_path, "wb") as f:
            writer.write(f)

        print(f"Merged file created successfully: {output_path}")
        return output_filename

    except Exception as e:
        print("Error writing merged file:", e)
        return None


def Chemo_pdf(driver,download_path,patient_data,counter):
    try:
        time.sleep(1)
        driver.switch_to.frame("GB_frame")
        driver.switch_to.frame("GB_frame")
        element = driver.find_element(By.XPATH, "/html")
        outer_html = element.get_attribute("outerHTML")
        outer_html = outer_html.replace("\n", "").replace("\t", "")
        try:
            time.sleep(3)
            Drug=driver.find_element(By.XPATH,'//*[@id="txtDrug"]')
            DrugName= Drug.get_attribute("value")
        except:
            DrugName="Drug"
        
        try:
            print =  WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divControl"]/table/tbody/tr/td/div[2]/table/tbody/tr/td')))
            time.sleep(2)
            print.click()
            AccountNo=site_patient_data.loc[0,"BillingId"]
            Dos = site_patient_data.loc[0,"DOS"]
            dash = "_"
            DrugName = re.sub(r'\.pdf$', '', DrugName, flags=re.IGNORECASE).strip()
            DrugName = re.sub(r'[^\w.-]', '_', DrugName)
            file_names_3 = str(f"Ch_{str(AccountNo) + str(dash) + str(Dos).replace('/', '')}_{DrugName}")
            time.sleep(1)
            rename_pdf_file(file_names_3)
        except:
            print =  WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[4]/div[2]/table/tbody/tr/td/div[2]/table/tbody/tr/td')))
            print.click()
            # soup = BeautifulSoup(outer_html, 'html.parser')
            # for input_tag in soup.find_all('input'):
            #     input_type = input_tag.get('type', '')
            #     input_value = input_tag.get('value', '')

            #     if input_type == 'checkbox':
                    
            #         if input_tag.get('checked'):
            #             input_tag.replace_with('[Y]')  
            #         else:
            #             input_tag.replace_with('[N]')  
            #     elif input_type == 'radio':
                    
            #         if input_tag.get('checked'):
            #             input_tag.replace_with('(O)')  
            #         else:
            #             input_tag.replace_with('( )')  
            #     else:
            #         input_tag.replace_with(input_value)

            #     # Get the modified HTML content
            #     modified_html = str(soup)    
            #     AccountNo=site_patient_data.loc[0,"BillingId"]
            #     Dos = site_patient_data.loc[0,"DOS"]
            #     dash = "_"
            #     os.makedirs(download_path, exist_ok=True)
            #     file_names_3 = str(f"Ch_{str(AccountNo) + str(dash) + str(Dos).replace('/', '')}_{DrugName}.pdf")
            #     pdf_path = os.path.join(download_path, file_names_3)
            #     with open (pdf_path,"wb") as output_file:
            #         pisa_status = pisa.CreatePDF(modified_html, dest=output_file)
            #     if pisa_status.err == 0:
            #         print(f"PDF saved at: {pdf_path}")
            #     else:
            #         print("Error occurred while converting HTML to PDF")
                
        try:
            close =  WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divtitle"]/table/tbody/tr/td[3]/a')))
            close.click()                               
        except:
            close =  WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divtitle"]/table/tbody/tr/td[3]/a')))
            close.click()
        driver.switch_to.default_content()
        driver.switch_to.frame("workarea2")
        driver.switch_to.frame("Chemotherapy")

    except Exception as e:
        print("An error occurred during the process:", e)
    return file_names_3

def Check_visit_documents_Download(download_directory,filename1,dos_run):
    try:
        if os.path.exists(os.path.join(download_directory, filename1)):
            print(f"File Downloaded {filename1}")
        else:
            try:
                window_billing = driver.window_handles
                driver.switch_to.window(window_billing[0])
                element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
                WebDriverWait(driver, 50).until(element_present34)
                driver.switch_to.frame("workarea1")

                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="InfoFrame"]'))
                WebDriverWait(driver, 50).until(element_present1)
                driver.switch_to.frame("InfoFrame")
                try:
                    # Wait for the element to be present
                    image_element = WebDriverWait(driver, 50).until(
                        EC.presence_of_element_located((By.XPATH, f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[1]/img'))
                    )

                    # Get the src attribute of the image
                    src = image_element.get_attribute('src')

                    # Check the src attribute
                    if '/eClinic/ec/images/icon_collapse.png' in src:
                        pass
                    elif '/eClinic/ec/images/icon_expand.png' in src:
                        image_element.click()
                    else:
                        print('Error')

                except Exception as e:
                    print(f"Error: {e}")

                WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[2]/td/table/tbody/tr/td/div/table/tbody/tr[1]/td/a'))).click()
                visit_dos_documents(driver,filename1)
            
            except Exception as e:
                pass

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()

    
def ExtractAndDownload(site_id,patient_data,driver,connection,download_path,invetory_data,refid,name_before_parentheses,refresh_page,username,credentialmasterid):
    try:
        claim_level_df=pd.DataFrame()
        Service_level_df=pd.DataFrame()
        file_names_dict={}
        file_names1=[] 
        # name_before_parentheses=""
        window_after_loop = driver.window_handles
        # time.sleep(2)                 
        # driver,name_before_parentheses=select_site(patient_data,driver,site_id,connection,invetory_data)

        aws_key=login_details.loc[0,"awsaccesskey"]
        aws_sec_key=login_details.loc[0,"awssecretkey"]
        aws_bucket=login_details.loc[0,"awsbucketname"]
        aws_region=login_details.loc[0,"awsregion"]
        awsS3filepath=login_details.loc[0,"awss3filepath"]
        Client_1=patient_data.loc[0,"ClientId"]
        Client_2=patient_data.loc[0,"SubClientId"]
        for i, row in patient_data.iterrows():
            if pd.isna(row["division"]) or row["division"] == '':
                patient_data.at[i, "division"] = name_before_parentheses 

        try:
            driver.switch_to.alert.accept()
            alert = Alert(driver)
            alert.accept()
        except:
            pass

        for i,row in patient_data.iterrows():
            try:
                inv_data=row['id']  
                file_names=[]

                starting_window1 = driver.window_handles
                driver.switch_to.default_content()
                driver.switch_to.window(starting_window1[0])

                PatientId=row['PatientId']  
                message=f"STEPS 3 Billing Data Extraction Beginning  patientID is : {PatientId}"

                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)                
                starting_window=driver.current_window_handle
                # time.sleep(2)  

        
                driver,claim_level_dataframe,Service_level_dataframe,target_value= generate_billing_data(driver,row,PatientId,i,connection,refresh_page)   
                acctno=claim_level_dataframe.loc[0,"Patient's Account No"]
                refresh_page=1
                # time.sleep(2)
                ele_option=1
                target_area="Reference"  
                time.sleep(1)
                while ele_option>0:
                        try:
                            First_date=driver.find_element(By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr').text
                            if target_area in  First_date:
                                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                                break
                            ele_option+=1
                        except:
                            ele_option+=1
                            break
                
                try:
                    visit = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.LINK_TEXT, "Visit History")))
                    time.sleep(1)
                    visit.click()
                except:
                    try:
                        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="DivInfoWin"]/ul/li[2]/table/tbody/tr/td/a[3]'))).click()
                    except:
                        pass   


                time.sleep(1)
                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="InfoFrame"]'))
                WebDriverWait(driver, 50).until(element_present1)
                driver.switch_to.frame("InfoFrame")



                dropdown_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="viewbydoctor"]'))
                    )
                
                dropdown = Select(dropdown_element)
                
                dropdown_options = [option.text.strip() for option in dropdown.options]

                normalized_row_division = normalize_text(row["division"])
                
                print(normalized_row_division)  # Output: rendr: adult & senior health

                matching_option = None
                for option in dropdown_options: #987
                    if normalized_row_division in normalize_text(option) :
                        matching_option = option
                        break

                if matching_option:
                    print(f"Selecting division: {matching_option}")
                    dropdown.select_by_visible_text(matching_option)
                else:
                    print(f"No match found for division: {row['division']}")
                    insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error division name not found {row['division']}","1","",""),connection)
                    
                claim_level_dataframe.loc[0,"Patient's Account No"]
                try:
                    icd_data2123=ast.literal_eval(claim_level_dataframe.loc[0,"ICD10 Diagnosis or Nature of Illness Of Injury"])
                except:            
                    icd_data2123=ast.literal_eval(str(claim_level_dataframe.loc[0,"ICD10 Diagnosis or Nature of Illness Of Injury"]))
                target_value_icd=""
                for icd_i in icd_data2123.keys():            
                    target_value_icd=list(icd_data2123[icd_i].keys())[0]



                dos_run=1
                pdf_dos=0
                try:
                    WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.XPATH, f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b')))
                except:
                    pass

                message=f"STEPS 4 MR Files and pdf download started for patientID : {PatientId}"
                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)                
                file_names_dict1={}
                patientType='new patient'
                while dos_run>0:
                    try:
                        First_date=driver.find_element(By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b').text
                        if target_value in  First_date :
                            if  (pdf_dos!=0 and target_value_icd not in  First_date ):
                                dos_run+=1
                                continue                            

                            try:
                                First_date=extract_dates(First_date)
                                second_value=driver.find_element(By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run+1)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b').text
                                second_value=extract_dates(second_value)
                                d1 = datetime.strptime(second_value, "%m/%d/%Y")
                                d2 = datetime.strptime(First_date, "%m/%d/%Y")
                                delta = d2 - d1
                                if delta.days > 3 * 365:
                                    patientType='new patient'
                                else:
                                    patientType='established patient'                
                            except Exception as e:
                                patientType='new patient'
                            
                            WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b'))).click()
                            # time.sleep(2)
                            WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[2]/td/table/tbody/tr/td/div/table/tbody/tr[1]/td/a'))).click()
                            pdf_dos+=1
                            dash="_"
                            filename1=str(f"mr_{str(acctno)+str(dash)+str((First_date)).replace('/','')}.pdf")
                            visit_dos_documents(driver,filename1)
                            Check_visit_documents_Download(ERAPATH,filename1,dos_run)
                            file_names.append(filename1)

                            window_billing = driver.window_handles
                            driver.switch_to.window(window_billing[0])
                            element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
                            WebDriverWait(driver, 50).until(element_present34)
                            driver.switch_to.frame("workarea1")

                            element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="InfoFrame"]'))
                            WebDriverWait(driver, 50).until(element_present1)
                            driver.switch_to.frame("InfoFrame")
                            ## after complted of download
                            WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b'))).click()

                            # break
                        elif pdf_dos>0:
                            second_date=driver.find_element(By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b').text
                            if second_date:
                                second_date=extract_dates(second_date)
                                pdf_dos+=1
                                WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b'))).click()
                                # time.sleep(3)
                                WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[2]/td/table/tbody/tr/td/div/table/tbody/tr[1]/td/a'))).click()
                                dash="_"
                                filename1=str(f"mr_{str(acctno)+str(dash)+str((second_date)).replace('/','')}.pdf")
                                visit_dos_documents(driver,filename1)
                                file_names.append(filename1)

                                window_billing = driver.window_handles
                                driver.switch_to.window(window_billing[0])
                                element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
                                WebDriverWait(driver, 50).until(element_present34)
                                driver.switch_to.frame("workarea1")

                                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="InfoFrame"]'))
                                WebDriverWait(driver, 50).until(element_present1)
                                driver.switch_to.frame("InfoFrame")

                                WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/div/table/tbody/tr/td/table[{str(dos_run)}]/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b'))).click()

                                if pdf_dos>=3:
                                    break
                        dos_run+=1
                    except Exception as e:
                        dos_run+=1
                        exc_type, exc_obj, exc_tb = sys.exc_info()        
                        message=f"Extraction error for visit_dos_documents : line no : {exc_tb.tb_lineno} {str(e)}"
                        insert_log(("",datetime.utcnow(),"ERROR",site_id,"",invetory_data,message,"1",username,credentialmasterid),connection) 

                        break

                filename_dos = str(f"{str(acctno)+'_'+str(convert_to_mm_dd_yyyy(row['DOS'])).replace('/','')}")

                file_names_dict1.update({str(filename_dos):file_names})
                for key, value in file_names_dict1.items():
                    file_names_1dos=key
                    file_names1dos=value

                    # upload_file_to_blob(a_azur, a_azur_1,
                    #     file_names1dos, Client_1,Client_2,download_path,connection,file_names_1dos,inv_data)                        
    

                window_billing = driver.window_handles
                driver.switch_to.window(window_billing[0])
                element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
                WebDriverWait(driver, 50).until(element_present34)
                driver.switch_to.frame("workarea1")
                try:

                    DocFormLab = WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.LINK_TEXT, "Docs/Forms/Labs")))
                    DocFormLab.click()
                except:
                    try:
                        WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[6]/ul/li[2]/table/tbody/tr/td/a[5]'))).click()
                    except:
                        pass  
                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="InfoFrame"]'))
                WebDriverWait(driver, 50).until(element_present1)
                driver.switch_to.frame("InfoFrame")
                # try:
                #     WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.XPATH,'/html/body/form/div[4]/div[2]/table[2]/tbody/tr[2]/td/table/tbody/tr/td/div/div/table/tbody/tr[1]/td/b')))
                # except:
                #     pass 
                try:
                    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[4]/div[2]/table[3]/tbody/tr[1]/td/table/tbody/tr/td[2]/table/tbody/tr/td[3]/b'))).click()
                except:
                    try:
                        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[4]/div[2]/table[2]/tbody/tr[1]/td/table/tbody/tr/td[2]/table/tbody/tr/td[3]/text()'))).click()
                    except:
                        pass    
                time.sleep(1)    
                panel_title_elements = driver.find_elements(By.CLASS_NAME, "DocLabPanelTitle")

                panel_name_list = [element.find_element(By.TAG_NAME, "b").text for element in panel_title_elements]

                misc=1
                miss_list1=[]
                misc_not=0

                try:
                    miss_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, f'/html/body/form/div[4]/div[2]/table[2]/tbody/tr[2]/td/table/tbody/tr/td/div/div/table/tbody/tr[1]/td/b')))  
                    miss_list1.append(miss_element.text.strip())
                except:
                    try:
                        while misc>0:
                            try:
                                miss_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, f'/html/body/form/div[4]/div[2]/table[2]/tbody/tr[2]/td/table/tbody/tr/td/div/div[{str(misc)}]/table/tbody/tr[1]/td/b')))
                                miss_list1.append(miss_element.text.strip())
                                misc+=1
                                break
                            except:
                                break
                    except:
                        pass
                if len(miss_list1)!=0:

                    start_value = str(miss_list1[0])

                    start_index = panel_name_list.index(start_value) if start_value in panel_name_list else -1

                    miss_list = panel_name_list[start_index:] if start_index != -1 else []
                else:
                    miss_list=[]

                message=f"STEPS 5 DOC/LABS files and  pdf download started for patientID : {PatientId}"
                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)                
        
                for lab_title_text in panel_name_list:
                    try:
                        try:
                            lab_element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, f'/html/body/form/div[3]/table/tbody/tr/td[1]')))
                        except:
                            pass
                        try:
                            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH,'/html/body/form/div[4]/div[2]/table[3]/tbody/tr[1]/td/table/tbody/tr/td[2]/table/tbody/tr/td[3]/b')))
                        except:
                            try:
                                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH,'/html/body/form/div[4]/div[2]/table[2]/tbody/tr[1]/td/table/tbody/tr/td[2]/table/tbody/tr/td[3]/b')))
                            except:
                                pass   
                        count_lab=0
                        if lab_title_text:                             
                            NEw_variable= clean_lab_title(lab_title_text)

                            xpath_dates_primary = f"//table[contains(@onclick, '{NEw_variable}')]/tbody/tr/td[@class='DocLabTabTitle']"
                            xpath_dates_secondary = f"//table[contains(@onclick, '{NEw_variable}')]/tbody/tr/td[@class='DocLabTabTitleOther']"
                            xpath_desc_primary = f"//table[contains(@onclick, '{NEw_variable}')]/tbody/tr/td[@class='DocLabTabDesc']"
                            xpath_desc_secondary = f"//table[contains(@onclick, '{NEw_variable}')]/tbody/tr/td[@class='DocLabTabDescOther']"

                            try:
                                fobt_dates1 = driver.find_elements(By.XPATH, xpath_dates_primary)                                

                                if not fobt_dates1:
                                    fobt_dates1 = driver.find_elements(By.XPATH, xpath_dates_secondary)
                                    fobt_desc1 = driver.find_elements(By.XPATH, xpath_desc_secondary)
                            except:
                                pass
                            try:
                                fobt_desc1 = driver.find_elements(By.XPATH, xpath_desc_primary)
                                if not fobt_desc1:
                                    fobt_desc1 = driver.find_elements(By.XPATH, xpath_desc_secondary)
                            except:
                                pass
                            

                            for index_p,date_element in enumerate(fobt_dates1):
                                if date_element.text == target_value:
                                    desc_date=fobt_desc1[index_p].text.strip()
                                    window_billing = driver.window_handles
                                    date_element.click()
                                    filename1 = str(f"{str(remove_punctuation(lab_title_text)).replace(' ','')}_{str(acctno)+str(remove_punctuation(desc_date)).replace(' ','')+str(count_lab)}.pdf")

                                    try:
                                        click_on_date(driver, filename1,PatientId,invetory_data,acctno)
                                    except:
                                        message=f"Error Donloading Docs/Form/Labs pdf {filename1} for patientID : {PatientId}"
                                        insert_log((refid,datetime.utcnow(),'Error',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection) 

                                    file_names.append(filename1)

                                    window_billing1 = driver.window_handles
                                    driver.switch_to.default_content()
                                    driver.switch_to.window(window_billing1[0])                                    
                                    count_lab+=1
                                    driver.switch_to.window(window_billing[0])
                                    element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
                                    WebDriverWait(driver, 20).until(element_present34)
                                    driver.switch_to.frame("workarea1")

                                    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="InfoFrame"]'))
                                    WebDriverWait(driver, 20).until(element_present1)
                                    driver.switch_to.frame("InfoFrame")

                                    if lab_title_text in miss_list:
                                        misc_not+=1
                    except:
                        break

                # time.sleep(2)
                window_billing = driver.window_handles

                driver.switch_to.default_content()

                driver.switch_to.window(window_billing[0])
                element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea1"]'))
                WebDriverWait(driver, 20).until(element_present34)
                driver.switch_to.frame("workarea1")
                try:
                    v_area=driver.find_element(By.XPATH,f'/html/body/form/div[6]/ul/li[1]/span[1]').text
                except:
                    pass
                try:
                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[6]/ul/li[1]/span[2]/img'))).click()
                except:
                    pass
                time.sleep(1) 
                ele_option=1  
                target_area="Patient Home"
                while ele_option>0:
                        try:
                            First_date=driver.find_element(By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr').text
                            if target_area in  First_date:
                                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div/table/tbody/tr[1]/td/div[{str(ele_option)}]/table/tbody/tr'))).click()

                                break
                            ele_option+=1
                        except:
                            ele_option+=1
                            break

                filename1 = str(f"VACCINE_{str(acctno)}.pdf")
                vaccine_f(driver, filename1,connection,PatientId)
                file_names.append(filename1)
                
                window_after = driver.window_handles
                handles = driver.window_handles

                num_tabs = len(handles)

                if num_tabs==2:
                    driver.switch_to.window(handles[1])
                    driver.close()  
                driver.switch_to.window(window_after[0])

                window_billing1 = driver.window_handles
                driver.switch_to.default_content()
                driver.switch_to.window(window_billing1[0])
                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
                WebDriverWait(driver, 20).until(element_present1)
                driver.switch_to.frame("workarea2")
                window_before = driver.window_handles[0]
                
                target_area="Chemotherapy"
                try:
                    ele_option=1
                    while_start=0
                    Dos = row['DOS']

                    try :
                        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="procbarTDChemotherapy"]')))
                        First_date=driver.find_element(By.XPATH,f'//*[@id="procbarTDChemotherapy"]').text
                        if target_area in First_date:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="procbarTDChemotherapy"]'))).click()
                            
                        driver.switch_to.frame("Chemotherapy")        
                                
                        try:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="PatientDemographicsBut"]'))).click()
                        except:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/table[1]/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[1]/img'))).click()
                                
                            while_start=1
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        message=f"Extraction error for Chemotharapy tab name : line no : {exc_tb.tb_lineno} {str(e)}"
                        insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)                    

                    

                    table_id = '/html/body/form/center/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/table' #Table Column Dates
                    ChemoTable=get_table_length(table_id,driver)
                    time.sleep(0.5)
                    dates = []
                    for w in range (3,ChemoTable):                                                      
                        try:
                            date = WebDriverWait(driver,2).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/table/tbody/tr[{str(w)}]/td[1]/a'))).text
                            dates.append((w,date))
                        except:
                            continue
                                
                    closest_element = None
                    min_diff=float('inf')
                    
                    # for date_str in dates:
                    #     try:
                    #         date_converted = convert_to_mm_dd_yyyy(date_str)
                    #         date = datetime.strptime(date_converted, "%m/%d/%Y")
                    #         diff = abs((datetime.strptime(Dos, "%m/%d/%Y") - date).days)  # Calculate the difference in days

                    #         if diff < min_diff:
                    #             min_diff = diff
                    #             closest_date = date_str
                        
                    #     except ValueError as e:
                    #         print(f"The closest date to DOS is:{date_str}:{e}")              
                    
                    dos_date = datetime.strptime(Dos, "%m/%d/%Y")
                    valid_dates = []
                    for w, date_str in dates:
                        try:
                            # Convert each date from string to datetime object
                            date_converted = convert_to_mm_dd_yyyy(date_str)
                            date = datetime.strptime(date_converted, "%m/%d/%Y")

                            # Check if the date is less than or equal to DOS
                            if date <= dos_date:
                                valid_dates.append((w, date))  # Add valid date with its row index
                        except ValueError as e:
                            print(f"Error with date format: {date_str}: {e}")
                    
                    if valid_dates:
                        # Find the date with the maximum value
                        max_date_entry = max(valid_dates, key=lambda x: x[1])  # max by the date value

                        # Extract the row index (w) for the max date
                        w_max, max_date = max_date_entry

                        # Now click on the row with the greatest date that is less than or equal to DOS
                        WebDriverWait(driver, 15).until(EC.element_to_be_clickable(
                            (By.XPATH, f'/html/body/form/center/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/table/tbody/tr[{str(w_max)}]/td[1]/a')
                        )).click()
                    else:
                        print("No valid dates found less than or equal to DOS.")                    

                    # for w in range (3,ChemoTable): #Date Table
                    #     try:
                    #         closest=driver.find_element(By.XPATH,f'/html/body/form/center/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/table/tbody/tr[{str(w)}]/td[1]/a').text
                    #         if closest_date in closest:
                    #             WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/table/tbody/tr[{str(w)}]/td[1]/a'))).click()          
                    #     except:
                    #         pass
                    
                    table_id = '/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[3]/div/table' #Table Column Dates
                    ChemoDrug=get_table_length(table_id,driver)
                    # for w in range(1,ChemoDrug+1):
                #         try:
                #             exact_match = driver.find_element(By.XPATH,f'/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[1]/div/table/tbody/tr[{str(w)}]/td[5]').text
                #             time.sleep(0.8)
                #             date_format = "%m/%d/%Y"
                #             dos = datetime.strptime(Dos,date_format)
                #             match = datetime.strptime(exact_match,date_format)
                #             if dos == match:
                #                 print("Chemo drug Date Match")
                #                 row_index=f'/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[1]/div/table/tbody/tr[{str(w)}]/td[5]'
                #                 Column=get_columns_count(table_id,row_index,driver)
                #                 counter=1
                #                 for i in range (1,Column+1):
                #                     WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[3]/div/table/tbody/tr[{str(w)}]/td[{str(i)}]/a'))).click()
                #                     try:
                #                         Chemo_Download = Chemo_pdf(driver,download_path,patient_data,counter)
                #                         counter+=1
                #                         file_names.append(Chemo_Download)
                                        
                #                     except:
                #                         WebDriverWait(driver, 15).until(EC.element_to_be_clickable(('By.XPATH,//*[@id="divControl"]/table/tbody/tr/td/div[2]/table/tbody/tr/td/span'))).click()

                #         except:
                #             pass
                # except:
                #     pass
                    fle_names = []
                    for w in range(1, ChemoDrug + 1):
                        try:
                            # Get the exact date for comparison
                            exact_match = driver.find_element(By.XPATH, f'/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[1]/div/table/tbody/tr[{str(w)}]/td[5]').text
                            time.sleep(0.8)
                            
                            # Date formatting
                            date_format = "%m/%d/%Y"
                            dos = datetime.strptime(Dos, date_format)
                            match = datetime.strptime(exact_match, date_format)
                            
                            # Check for date match
                            if dos == match:
                                print("Chemo drug Date Match")
                                row_index = f'/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[1]/div/table/tbody/tr[{str(w)}]/td[5]'
                                Column = get_columns_count(table_id, row_index, driver)
                                counter = 1
                                
                                for i in range(1, Column + 1):
                                    # Check for a valid <a> tag in the cell
                                    try:
                                        cell_xpath = f'/html/body/form/center/div/table[3]/tbody/tr[2]/td/table/tbody/tr[2]/td/div/div/table[2]/tbody/tr[2]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div/div/div[3]/div/table/tbody/tr[{str(w)}]/td[{str(i)}]/a'
                                        
                                        # Check if <a> exists (indicating a valid link)
                                        cell_element = driver.find_element(By.XPATH, cell_xpath)
                                        if cell_element.is_displayed() and cell_element.is_enabled():
                                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, cell_xpath))).click()
                                            try:
                                                # Download PDF if clicked
                                                Chemo_Download = Chemo_pdf(driver, download_path, patient_data, counter)
                                                counter += 1
                                                fle_names.append(Chemo_Download)
                                                file_names.append(Chemo_Download)
                                            except:
                                                # Handle any download issues (e.g., popup handling)
                                                WebDriverWait(driver, 15).until(EC.element_to_be_clickable(('By.XPATH', '//*[@id="divControl"]/table/tbody/tr/td/div[2]/table/tbody/tr/td/span'))).click()
                                    except:
                                        # Handle case where <a> doesn't exist in the cell
                                        print(f"No link in row {w}, column {i}")
                                        continue
                                if len(fle_names) > 0:
                                    AccountNo=site_patient_data.loc[0,"BillingId"]
                                    Dos = site_patient_data.loc[0,"DOS"]
                                    merged_file = merge_chemo_pdfs(download_path, AccountNo, Dos)
                                    file_names.append(merged_file)
                                    # Optional: Delete individual PDFs after merging
                                    # for file in fle_names:
                                    #     os.remove(os.path.join(download_path, file + ".pdf"))

                                    print("Chemo PDFs merged successfully.")
                                else:
                                    print("No chemo PDFs downloaded.")

                        except Exception as e:
                            print(f"Error in row {w}: {str(e)}")
                            continue
                except:
                    pass
                
                target_area="Medical History"
                ele_option=1
                while_start=0
                driver.switch_to.default_content()
                driver.switch_to.frame("workarea2")
                time.sleep(2)
                try:
                    WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="tabbarTDMedicalHistory"]/tbody/tr')))
                    First_date=driver.find_element(By.XPATH,f'//*[@id="tabbarTDMedicalHistory"]/tbody/tr').text
                    if target_area in First_date:
                        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="tabbarTDMedicalHistory"]/tbody/tr'))).click()
                        while_start=1
                        # message=f"xpath  for medical Histry found : line no 1533 "
                        # insert_log((refid,datetime.utcnow(),"INFO",site_id,PatientId,message,"1",username,credentialmasterid),connection)  
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for medical History tab name : line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                if while_start==0:
                    while ele_option>0:
                            try:
                                First_date=driver.find_element(By.XPATH,f'/html/body/form/div[10]/table/tbody/tr/td[{str(ele_option)}]/table/tbody/tr').text
                                if target_area in  First_date:
                                    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[10]/table/tbody/tr/td[{str(ele_option)}]/table/tbody/tr'))).click()
                                    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[10]/table/tbody/tr/td[{str(ele_option)}]/table/tbody/tr'))).click()

                                    break
                                ele_option+=1
                            except Exception as e:                            
                                ele_option+=1
                                break

                element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="PatientHistory"]'))
                WebDriverWait(driver, 20).until(element_present1)
                driver.switch_to.frame("PatientHistory")
                # time.sleep(2)

 
                try:
                    # WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/div[1]/table/tbody/tr[1]/td/a[1]'))).click() 
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="a__ViewAll"]')))
                    First_date=driver.find_element(By.XPATH,f'//*[@id="a__ViewAll"]').text
                    tag_val="View All"
                    if tag_val in First_date:
                        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="a__ViewAll"]'))).click() 
                        # time.sleep(2)
                        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="a__ViewAll"]'))).click() 
                    else:
                        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="a__ViewAll"]'))).click() 

                except:
                    try:
                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="PatientHistory"]'))
                        WebDriverWait(driver, 30).until(element_present1)
                        driver.switch_to.frame("PatientHistory")
                        # time.sleep(2)
                        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="a__ViewAll"]'))).click() 
                    except Exception as e:                                                                  
                        message=f"Extraction error for medical History name : line no : {exc_tb.tb_lineno} {str(e)}"
                        # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                        pass
                        
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="ClkEdit_SummaryScreeningIntervention"]/b'))).click() 
                except:
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/div[2]/table/tbody/tr[17]/td/table/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b'))).click() 
                    except:
                        try:
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/center/div/div[2]/table/tbody/tr[21]/td/table/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr/td[2]/b'))).click() 
                        except:
                            pass
                # time.sleep(3)

                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="ClkEdit_SummaryScreeningIntervention"]')))
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for medical History name : line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                    pass
                
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="div_SummaryScreeningIntervention"]/span/input[1]'))).click()
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for medical History name : line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                    pass


                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="div_SummaryScreeningIntervention"]')))
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for medical History name : line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                    pass
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="div_SummaryScreeningIntervention"]/span/input[1]'))).click()
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for medical History name : line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                    pass
                try:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="div__Others"]/table[1]/tbody/tr/td')))
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for medical History name : line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),"ERROR",site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)  
                # time.sleep(2)
                message=f"STEPS 6 Medical History pdf download started for patientID : {PatientId}"
                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)                
        
                medical_update = 0
                target_date = target_value
                
                list_div=["div__DepressionScreening","div__SLUMSExamination","div__FallRiskChecklist","div__Smoking_Tobacco","div__BMI","div__SODH",
                "div__AlcoholScreening","div__MentalHealth","   ","div__Others"]
                names_list=[]
                try:
                    for div in list_div:
                        window_medical = driver.window_handles
                        driver.switch_to.default_content()
                        driver.switch_to.window(window_medical[0])
                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
                        WebDriverWait(driver, 20).until(element_present1)
                        driver.switch_to.frame("workarea2")
                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="PatientHistory"]'))
                        WebDriverWait(driver, 20).until(element_present1)
                        driver.switch_to.frame("PatientHistory")

                        names_l,file_name12=find_match(div,driver,target_date,acctno,PatientId,invetory_data)
                        if names_l:
                            medical_update = 1
                            file_names.extend(file_name12)
                            names_list.extend(names_l)            
                except:
                    pass
                if len(file_names)!=0:
                    try:
                        update_inventory([row["id"]],connection,"medical")
                    except:
                        try:
                            update_inventory([row["chartid"]],connection,"medical")
                        except:
                            try:
                                update_inventory([row["BillingId"]],connection,"medical")
                            except:
                                pass
                file_names1.append(file_names)
                filename = str(f"{str(acctno)+'_'+str(convert_to_mm_dd_yyyy(row['DOS'])).replace('/','')}")

                file_names_dict.update({str(filename):file_names})

                claim_level_dataframe['DOS']= row['DOS']
                claim_level_dataframe['PatientId']=row['PatientId']
                claim_level_dataframe['patientType']=patientType    
                claim_level_dataframe["facilityName"]=row["division"]
                claim_level_dataframe["facilityID"]=site_id 
                claim_level_dataframe['VisitId']=row['VisitId']
                claim_level_dataframe['id']=row['id']
                claim_level_dataframe['unique_identifier']=str(row['PatientId'])+row['DOS'].replace("/","_")
                Service_level_dataframe['unique_identifier']=str(row['PatientId'])+row['DOS'].replace("/","_")

                claim_level_df= pd.concat([claim_level_df, claim_level_dataframe], ignore_index=True)

                Service_level_df= pd.concat([Service_level_df, Service_level_dataframe], ignore_index=True)

                # try:
                window_after = driver.window_handles
                handles = driver.window_handles
                num_tabs = len(handles)
                if num_tabs==2:
                    driver.switch_to.window(handles[1])
                    driver.close()   

                driver.switch_to.window(window_after[0])

                try:
                    # Locate the second image element and click it
                    image_element2 = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/form/div[9]/table/tbody/tr/td[3]/img'))
                    )
                    image_element2.click()
                    try:
                        driver.switch_to.alert.accept()
                        alert = Alert(driver)
                        alert.accept()
                    except:
                        pass
                    print("Clicked on the second image element.")

                except Exception as e:
                    print("Second image element not found or clickable:", e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Second image element not found or clickable line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((refid,datetime.utcnow(),'ERROR',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)      
                try:
                    # Locate the first image element and click it
                    image_element1 = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/form/div[8]/table/tbody/tr/td[3]/img'))
                    )
                    image_element1.click()
                    try:
                        driver.switch_to.alert.accept()
                        alert = Alert(driver)
                        alert.accept()
                    except:
                        pass
                    print("Clicked on the first image element.")

                except Exception as e:
                    print("First image element not found or clickable:", e)

                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"First image element not found or clickable line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((refid,datetime.utcnow(),'ERROR',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)

                    # driver.refresh()
                    # alert = Alert(driver)
                    # alert.accept()
                    # try:
                    #     driver.switch_to.alert.accept()
                    # except:
                    #     pass
                    # time.sleep(3)
                    # try:
                    #     driver.switch_to.alert.accept()
                    #     alert = Alert(driver)
                    #     alert.accept()
                    # except:
                    #     pass
                # except Exception as e:
                #     exc_type, exc_obj, exc_tb = sys.exc_info()        
                #     message=f"Extraction error for patientID : {row['PatientId']} line no : {exc_tb.tb_lineno} {str(e)}"
                #     insert_log((refid,datetime.utcnow(),'ERROR',site_id,PatientId,inv_data,message,"1",username,credentialmasterid),connection)      
                #     continue                
                
                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,f"STEPS 7 Extraction and pdf download completed for patientID {PatientId}","1",username,credentialmasterid),connection)      
                
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()                        
                insert_log((refid,datetime.utcnow(),'ERROR',site_id,"",invetory_data,f"Extraction error for patientID : {row['PatientId']} line no : {exc_tb.tb_lineno} {str(e)}","1",username,credentialmasterid),connection)      
                window_after = driver.window_handles
                try:
                    log_message=str(e)
                    error_message = re.search(r'Message: (.*?):', log_message).group(1)
                    id_bill1 = inv_data
                    error_resion_code(error_message,id_bill1,connection)
                except:
                    try:
                        error_message = "error not found"
                        id_bill1 = inv_data
                        error_resion_code(error_message,id_bill1,connection)
                    except:
                        pass

                # # Switch to the new window
                driver.switch_to.window(window_after[0])
                time.sleep(2)
                remove_all_pdf_files(download_path)
                driver.refresh()
                time.sleep(2)
                try:
                    alert = Alert(driver)
                    alert.accept()
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for patientID : {row['PatientId']} line no : {exc_tb.tb_lineno} {str(e)}"
                    # insert_log((refid,datetime.utcnow(),'ERROR',site_id,"",invetory_data,message,"1",username,credentialmasterid),connection)      
                    pass
                try:
                    driver.switch_to.alert.accept()
                except Exception as e:
                    pass
                time.sleep(2)
                try:
                    driver.switch_to.alert.accept()
                    alert = Alert(driver)
                    alert.accept()
                except:
                    pass

                continue


    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info() 
        message=f"extraction line no : {exc_tb.tb_lineno} {str(e)}"    
        insert_log((refid,datetime.utcnow(),'ERROR',site_id,"",invetory_data,message,"1",username,credentialmasterid),connection)          
        # driver.quit()
        claim_level_df , Service_level_df , file_names1 ,file_names_dict= pd.DataFrame(),pd.DataFrame(),"",""
        raise Exception(message)

    return claim_level_df , Service_level_df , file_names1 ,file_names_dict,refresh_page


def get_table_length(table_id, driver):
    table = WebDriverWait(driver, 10).until( EC.presence_of_element_located((By.XPATH, table_id)))
    rows = table.find_elements(By.TAG_NAME, 'tr')
    return len(rows)

def get_columns_count(table_id,row_index,driver):
    table = WebDriverWait(driver, 10).until( EC.presence_of_element_located((By.XPATH, table_id)))
    specific_row = table.find_element(By.XPATH,row_index)
    columns = table.find_elements(By.TAG_NAME, 'td')
    return len(columns)


def upadate_password(usrid,password,connection):
    try:
        if connection:  
            cursor = connection.cursor()            
            update_query = sql.SQL('''
                            UPDATE mst.tbl_emr_credentialmaster 
                            SET password = %s
                            WHERE userid = %s;''')
            cursor.execute(update_query, (password,usrid))
            connection.commit() 
    except:
        pass

def update_inventory(id_bill,connection,type):
    try:          
        if type=='billing':
            if connection:  
                for id in id_bill.keys():                          
                    cursor = connection.cursor()            
                    update_query = sql.SQL('''
                                    UPDATE public.tbl_stginventoryuploaddata
                                    SET billingid = %s
                                    WHERE inventoryid = %s;''')
                    cursor.execute(update_query, (id_bill[id],id))
                    connection.commit()
        elif type =='medical':
            if connection:
                for id in id_bill: 
                    cursor = connection.cursor()
                    update_query = sql.SQL('''
                                    UPDATE public.tbl_stginventoryuploaddata
                                    SET ismedicalrecordprocessed = true
                                    WHERE inventoryid = %s;''')
                    cursor.execute(update_query, (id,))
                    connection.commit()

        elif type=='inprocess':
            if connection:  
                for id in id_bill:                          
                    cursor = connection.cursor()            
                    update_query = sql.SQL('''
                                    UPDATE public.tbl_stginventoryuploaddata
                                    SET filestatus = %s
                                    WHERE inventoryid IN %s;''')
                    cursor.execute(update_query,(1,tuple(id_bill)))
                    connection.commit() 
        elif type=='process_success':
            if connection:                                           
                cursor = connection.cursor()            
                update_query = sql.SQL('''
                                UPDATE public.tbl_stginventoryuploaddata
                                SET filestatus = %s,
                                document_status = %s
                                WHERE inventoryid IN %s;''')
                cursor.execute(update_query, (2, 2, tuple(id_bill.keys())))
                connection.commit()                  
        elif type=='process_failed':
            if connection:                                          
                cursor = connection.cursor()            
                update_query = sql.SQL(''' 
                            UPDATE public.tbl_stginventoryuploaddata
                            SET filestatus = %s,
                            failurecount = COALESCE(failurecount, 0) + 1
                            WHERE inventoryid IN %s;''')
                try:
                    cursor.execute(update_query,(3,tuple(list(id_bill.keys()))))
                except:
                    cursor.execute(update_query,(3,tuple(id_bill)))
                connection.commit()      

        elif type=='login':
            if isinstance(id_bill, np.integer):
                id_bill = int(id_bill)
            if connection:                                          
                cursor = connection.cursor()            
                update_query = sql.SQL(''' 
                            UPDATE mst.tbl_emr_credentialmaster
                            SET 
                            logincount = COALESCE(logincount, 0) + 1
                            WHERE id = %s;''')
                
                # Execute the query with the username as parameter
                cursor.execute(update_query, (id_bill,))
                # cursor.execute(update_query, (username,))
                # Commit the transaction to apply the changes
                connection.commit()

        elif type=='logout':
            if isinstance(id_bill, np.integer):
                id_bill = int(id_bill)
            if connection:                                          
                cursor = connection.cursor()            
                update_query = sql.SQL('''
                    UPDATE mst.tbl_emr_credentialmaster
                    SET logincount = GREATEST(COALESCE(logincount, 0) - 1, 0)
                    WHERE id = %s;
                ''')
                
                # Execute the query with the username as parameter
                cursor.execute(update_query, (id_bill,))
                # cursor.execute(update_query, (username,))
                # Commit the transaction to apply the changes
                connection.commit()
  
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = "update_inventory line :"+str(exc_tb.tb_lineno)+" " +str(e)         
        raise Exception(message)

def error_resion_code(error_message,id_bill,connection):
    cursor = connection.cursor()            
    update_query = sql.SQL('''
                UPDATE public.tbl_stginventoryuploaddata
                SET errorreason = %s
                WHERE inventoryid IN %s;''')
    try:
        if isinstance(id_bill, (list, tuple)):
            cursor.execute(update_query, (error_message, tuple(id_bill)))
        else:
            cursor.execute(update_query, (error_message, (id_bill,)))  
        connection.commit()

    except Exception as e:
        print("An error occurred:", e)




def json_converter(service_data,claim_data,refID,connection,clientid,SubClientId,invetory_data):
    try: 
        id_bill={}       
        data=[]
        for i,r in claim_data.iterrows():
            insurance_template={
                    "sequence": "",
                    "isMedicare": "",
                    "isMedicaid": "",
                    "insuranceName": "",
                    "insuranceType": "",
                    "insuranceNumber": "",
                    "insuranceClass": ""
                }
            provider_template={
                    "npi": "",
                    "sequence": "",
                    "providerName": "",
                    "providerType": "",
                    "providerNumber": "",
                    "tin":"",
                    "providerCredentials": ""
                }
            # "id":str(uuid.uuid4()),
            json_template={
                "id":r["id"],
                "refID":refID,
                "DOB": "",
                "DOS": "",
                "age": '',
                "cpt": [],
                "csn": "",
                "icd": [],
                "mdm": [{
                "mdmA": {
                    "ruleText": [],
                    "finalLabel": []
                },
                "mdmB": {
                    "ruleText": [],
                    "finalLabel": []
                },
                "mdmC": {
                    "ruleText": [],
                    "finalLabel": []
                }
                }],
                "ssn": "",
                "unit": "",
                "19Note": "",
                "DOSOrg": "",
                "enc_no": "",
                "gender": "",
                "chartId": "",
                "sladate": "",
                "clientId": str(clientid),
                "examCode": "",
                "fileName": "",
                "filePath": "",
                "modifier": "",
                "workType": "",
                "accountNo": "",
                "admitDate": "",
                "doctorID2": "",
                "insurance": [],
                "mrnNumber": "",
                "patientid": "",
                "providers": [],
                "clientName": "",
                "department": "",
                "locationID": "",
                "locationName": "",
                "facilityName": "",
                "facilityID": "",
                "physicanID": "",
                "reportText": "",
                "speciality": "E/M",
                "accessionNo": "",
                "doctorName2": "",
                "orderNumber": "",
                "patientName": "",
                "patientType": "",
                "segmentName": "",
                "stateSeenIn": "",
                "subClientId": str(SubClientId),
                "visitNumber": "",
                "customfields": [],
                "hospitalName": "",
                "lastSeenDate": "",
                "locationName": "",
                "physicanName": "",
                "dischargeDate": "",
                "insuranceName": "",
                "isWorkRelated": "",
                "procedureCode": "",
                "subClientName": "",
                "cptSegmentText": "",
                "financialclass": "",
                "icdSegmentText": "",
                "isAutoAccident": "",
                "placeOfService": "",
                "intLocationDate": "",
                "patientlastname": "",
                "visitDiagnosis1": "",
                "visitDiagnosis2": "",
                "visitDiagnosis3": "",
                "visitDiagnosis4": "",
                "criticalCareCode": [],
                "patientfirstname": "",
                "patientmiddlename": "",
                "referringDoctorID": "",
                "intLocationOverride": "",
                "referringDoctorName": "",
                "additionalRoutingKey": "",
                "downloadpostChargeID": "",
                "organizationCaseCode": "",
                "primaryInsuranceName": "",
                "procedureDescription": "",
                "tertiaryInsuranceName": "",
                "primaryInsuranceNumber": "",
                "secondaryInsuranceName": "",
                "tertiaryInsuranceNumber": "",
                "secondaryInsuranceNumber": "",
                "chargeAuthorizationNumber": "",
                "primaryInsuranceIsMedicare": "",
                "tertiaryInsuranceIsMedicare": "",
                "secondaryInsuranceIsMedicare": "",
                "intLocationDownloadRoutingKey": ""
            }

            json_template.update({'patientName':r["Patient's Name (L,F M)"],
                                'DOB':r["Patient's Birth Day"],
                                'age':r["Patient's Age"],
                                'gender':r["Gender"],
                                'accountNo':r["Patient's Account No"],
                                'billingID':r["Patient's Account No"],
                                "mrnNumber":r['PatientId'],
                                'visitNumber':r["VisitId"],
                                'DOS':r['DOS'],
                                'facilityName':r["facilityName"],
                                'locationName':r["locationName"],
                                'facilityID':r['facilityID'],
                                'patientid':r['PatientId'],
                                "patientType":r["patientType"]})
            id_bill.update({r["id"]:r["Patient's Account No"]})
            insurance_template.update({"sequence": str(1),                               
                                    "insuranceName": r['Insurance Plan/Program Name'],
                                    "insuranceType": "primary",
                                    "insuranceNumber": r['Policy Group/FECA Number'],
                                    "insuranceClass": r['Type Of Insurance']})
            if r['Type Of Insurance'].lower()=='medicaid':
                insurance_template.update({"isMedicare": str(0),
                                        "isMedicaid":str(1)})
            elif r['Type Of Insurance'].lower()=='medicare':
                insurance_template.update({"isMedicare": str(1),
                                        "isMedicaid":str(0)})
            else:
                insurance_template.update({"isMedicare": str(0),
                                        "isMedicaid":str(0)})
            provider=[]  
            providerCredentials="" 
            prov_cred=r['Name of Referring Physician'].strip().split(" ") 
            if prov_cred and prov_cred[-1] in ['MD']:
                providerCredentials=prov_cred[-1]
            provider_template={"npi": str(r['NPI']),
                                    "sequence": "1",
                                    "providerName": r['Name of Referring Physician'].strip(),
                                    "providerType": "referring phy",
                                    "providerNumber": "",
                                    "tin": str(r["Tax I.D."]),
                                    "providerCredentials": providerCredentials}
            provider.append(provider_template)
            claim_service_data=service_data[service_data['unique_identifier']==r['unique_identifier']]
            providerCredentials="" 
            prov_cred=r['Signature of Physician or Supplier Including Degrees or Credentials (Name)'].strip().split(" ") 
            if prov_cred and prov_cred[-1] in ['MD']:
                providerCredentials=prov_cred[-1]
            provider_template={"npi": str(list(claim_service_data['Rendering_NPI'])[0]),
                                    "sequence": "2",
                                    "providerName": r['Signature of Physician or Supplier Including Degrees or Credentials (Name)'].strip(),
                                    "providerType": "rendering phy",
                                    "providerNumber": "",
                                    "tin": str(r["Tax I.D."]),
                                    "providerCredentials": providerCredentials}
            provider.append(provider_template)
            providerCredentials="" 
            prov_cred=r['Billing Provider Information and Phone Number'].strip().split(" ") 
            if prov_cred and prov_cred[-1] in ['MD']:
                providerCredentials=prov_cred[-1]
            provider_template={"npi": str(r['Billing Provider Information and Phone Number(NPI#)']),
                                    "sequence": "3",
                                    "providerName": r['Billing Provider Information and Phone Number'].strip().split(" ")[0],
                                    "providerType": "billing phy",
                                    "providerNumber": "",
                                    "tin": str(r["Tax I.D."]),
                                    "providerCredentials": providerCredentials}
            provider.append(provider_template)

            json_template.update({"providers":provider})  
            icd=[]        
            try:
                icd_data=ast.literal_eval(r['ICD10 Diagnosis or Nature of Illness Of Injury'])
            except:            
                icd_data=ast.literal_eval(str(r['ICD10 Diagnosis or Nature of Illness Of Injury']))
            for i in icd_data.keys():            
                icd_template={"code": list(icd_data[i].keys())[0],
                                    "ruleID": "",
                                    "ruleId": "",
                                    "pointer": i,
                                    "ruleText": icd_data[i][list(icd_data[i].keys())[0]],
                                    "familyCode": "",
                                    "familyGroupId": "",
                                    "matchConfidence": '1',
                                    "inventoryid":r["id"]}
                icd.append(icd_template)
                
            cpt=[]
            for i,r_ in claim_service_data.iterrows():
                cpt_template={
                    "code": '',
                    "type": "",
                    "unit": "",
                    "ruleId": "",
                    "pointer": "",
                    "modality": "",
                    "modifier": "",
                    "ruleText": "",
                    "measureID": "",
                    "familyCode": "",
                    "familyGroupId": "",
                    "matchConfidence": ""
                }
                if r_['CPT or HCPCS'] in [str(em_cpt) for em_cpt in range(99202,99500)]:
                    type="E/M"        
                else:
                    type="CPT"
                modifier=''
                modifier=",".join([str(r_['Ma']),str(r_['Mb']),str(r_['Mc']),str(r_['Md'])])
                modifier=modifier.strip(',')
                cpt_template.update({"code": r_['CPT or HCPCS'],
                                    "type": type,
                                    "unit": r_['Days or Units'],
                                    "ruleId": "",
                                    "pointer": r_['ICD Code'],
                                    "modality": "",
                                    "modifier": modifier,
                                    "ruleText": "",
                                    "measureID": "",
                                    "familyCode": "",
                                    "familyGroupId": "",
                                    "matchConfidence": "1",
                                    "fromTime":r_["From time\nmm/dd/yyyy"],
                                    "toTime":r_["To time\nmm/dd/yyyy"],
                                    "POS":r_["POS"],
                                    "EMG":r_["EMG"],                                    
                                    "ma":r_["Ma"],
                                    "mb":r_["Mb"],
                                    "mc":r_["Mc"],
                                    "md":r_["Md"],                                    
                                    "charges":r_["Charges"],                                    	
                                    "epsdtFamilyPlan":r_["EPSDT Family Plan"],
                                    "idQual":r_["ID QUAL"],
                                    "renderingProvIDNPI":r_["Rendering\nProvID/NPI"],
                                    "renderingNPI":r_["Rendering_NPI"],
                                    "inventoryid":r["id"]})
                                     # "memo":r_["Memo"],
                cpt.append(cpt_template)
            json_template.update({"icd":icd,                          
                                "insurance":[insurance_template],
                                "cpt":cpt})        
            data.append(json_template)
            # upload_to_output(connection,(refID,r["id"],str(json_template),r["id"],0,"",0))            
        insert_log(("",datetime.utcnow(),'INFO',site_id,"",invetory_data,str(data),"1","",""),connection)
        update_inventory(id_bill,connection,type="billing")
        return json.dumps(data),id_bill
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message=f"Exception : json_converter line :{exc_tb.tb_lineno} {str(e)}"
        raise Exception(message)

def enable_download_headless(browser,download_dir):
    browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd':'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    browser.execute("send_command", params)

def api_call(json_string,refID,clientid,subclientid,servicetype,api_url,connection):
    try:        
        payload={"clientid": str(clientid),
                 "subclientid": str(subclientid),
                 "workflowmasterid": 2,
                 "servicetype": str(servicetype),
                 "uploadedjson": json_string,
                 "upload_referenceid":str(refID),
                 "input_type": 1}                
        # api_url = 'http://172.30.55.63/icc_api_release/api/Upload/uploadpatientfileinfo'
        # api_url = 'https://icauatapi.azure-api.net/transaction/api/Upload/uploadpatientfileinfo'
        headers = {'Content-Type': "application/json"}
        json_payload = json.dumps(payload)

        response = requests.post(api_url, data=json_payload,headers=headers)
        status_message= "Status code: "+str(response.status_code) +" Response : "+str(response.text)
        return status_message        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message=f"Exception : api_call line :{exc_tb.tb_lineno} {str(e)}"
        raise Exception(message)

def remove_all_pdf_files(download_path):
    if not os.path.isdir(download_path):
        print(f"Error: Directory '{download_path}' does not exist.")
        return

    # Find all PDF files (case-insensitive)
    pdf_pattern = os.path.join(download_path, "*.[pP][dD][fF]")
    pdf_files = glob.glob(pdf_pattern)
    
    if not pdf_files:
        print(f"No PDF files found in '{download_path}'.")
        return

    for file_path in pdf_files:
        try:
            os.remove(file_path)
            print(f"File '{file_path}' removed successfully.")
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except PermissionError:
            print(f"Permission denied: Unable to delete '{file_path}'. File may be in use or restricted.")
        except OSError as e:
            print(f"Error deleting '{file_path}': {str(e)}")    

def remove_file(download_path, file_name):
    file_path = os.path.join(download_path, file_name)

    try:
        os.remove(file_path)
        print(f"File '{file_name}' removed successfully.")
    except FileNotFoundError:
        print(f"File '{file_name}' not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")



def upload_file_to_blob(storage_connection_string, container_name, local_pdf_path, clientid,subclientid,download_path,connection,file_names_1,invetory_data):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        for i in local_pdf_path:
            try:
                split_name=i.split("_")
                blob_name = "ica2.0/"+str(clientid)+"/"+str(subclientid)+"/2/"+str(file_names_1)+"/"+str(split_name[0])+"/"+i
                # blob_name = "ica2.0/"+str(clientid)+"/"+str(subclientid)+"/2/MR/"+acct+"/"+i 
                with open(os.path.join(download_path,i), "rb") as data:
                    content_settings = ContentSettings(content_type='application/pdf')
                    container_client.upload_blob(name=blob_name, data=data,content_settings=content_settings,overwrite=True)
                    insert_log(("",datetime.utcnow(),'INFO',"","",invetory_data,f"succesfully uploaded file {str(blob_name)}","1","",""),connection)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message=f"Exception : file {i} line :{exc_tb.tb_lineno} {str(e)}"
                insert_log(("",datetime.utcnow(),'ERROR',"","",invetory_data,f"Exception : file {i} line :{exc_tb.tb_lineno} {str(e)}","1","",""),connection)   
                pass
            

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message=f"upload_file_to_blob line :{exc_tb.tb_lineno} {str(e)}"
        raise Exception(message)


def inserttbl_chart_document(s3_key,inventoryid, billingid, file_names1, clientid, subclientid,bucket_name, connection):
    try:
        if connection:
            cursor = connection.cursor()

            # First, fetch document type ID for .pdf
            cursor.execute('SELECT id FROM public.tbl_documenttypemaster WHERE name ILIKE %s LIMIT 1', ('.pdf',))
            result = cursor.fetchone()
            if not result:
                raise Exception("Document type '.pdf' not found in tbl_documenttypemaster.")
            documenttypemasterid = result[0]

            # documentfullurl = f"icodeone/{clientid}/{subclientid}/2/{inventoryid}/{file_names1}"
            isactive = True
            createddate = datetime.utcnow()
            modifieddate = datetime.utcnow()
            bloburl = f"https://{bucket_name}.s3.dualstack.us-west-1.amazonaws.com/{s3_key}"
            contenttype = 'application/pdf'
            chart = True

            insert_query = sql.SQL('''
                INSERT INTO public.tbl_chartdocumenttransaction
                (batchid, accountno, name, description, documentfullurl, documenttypemasterid, isactive,
                 createdby, createddate, modifiedby, modifieddate, bloburl, contenttype, chart)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, 1, %s, %s, %s, %s);
            ''')
            cursor.execute(insert_query, (
                inventoryid,
                billingid,
                file_names1,
                file_names1,
                s3_key,
                documenttypemasterid,
                isactive,
                createddate,
                modifieddate,
                bloburl,
                contenttype,
                chart
            ))
            connection.commit()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = f"Exception : insert_log line :{exc_tb.tb_lineno} {str(e)}"
        raise Exception(message)


def upload_file_to_s3(id_bill,awsS3filepath, aws_access_key, aws_secret_key, aws_region, bucket_name, local_pdf_path, clientid, subclientid, download_path, connection, file_names_1, invetory_data):
    try:
        billingid_value = list(id_bill.values())[0]
        boto3.set_stream_logger(name='botocore')  # Debug logging
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        print(f"Connected to S3 bucket: {bucket_name}")
        
        local_pdf_files = [f for f in os.listdir(download_path) if f.lower().endswith(".pdf")]
        
        for i in local_pdf_files:
        # for i in local_pdf_path:
            try:
                split_name = i.split("_")
                s3_key = f"{awsS3filepath}RendrCare/{subclientid}/2/{invetory_data}/{split_name[0]}/{i}"
                file_full_path = os.path.join(download_path, i)

                print(f"Uploading file: {file_full_path} -> s3://{bucket_name}/{s3_key}")

                with open(file_full_path, "rb") as data:
                    s3_client.upload_fileobj(
                        Fileobj=data,
                        Bucket=bucket_name,
                        Key=s3_key,
                        ExtraArgs={'ContentType': 'application/pdf'}
                    )

                insert_log(("",datetime.utcnow(),'INFO',"","",invetory_data,f"successfully uploaded file {str(s3_key)}","6","",""),connection) 
                inserttbl_chart_document(s3_key,invetory_data, billingid_value, i, clientid, subclientid,bucket_name, connection)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message = f"❌ Exception uploading {i}: line {exc_tb.tb_lineno}: {str(e)}"
                print(message)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = f"upload_file_to_s3 main error: line {exc_tb.tb_lineno}: {str(e)}"
        print(message)
        raise Exception(message)


def get_login_details(ip_address,connection):
    if connection:
        try:
            puid = str(uuid.uuid4())
            cursor = connection.cursor()


            # Define the query
            update_query = sql.SQL('SELECT * FROM public.fn_get_emr_credentials_concurrent_ip(1, %s);')
            
            # Execute the query
            try:
                cursor.execute(update_query, (ip_address,))
            except psycopg2.Error as e:
                print(f"Error executing query: {e}")

            # Fetch column names from the cursor description
            columns = [desc[0] for desc in cursor.description]

            # Fetch data
            rows = cursor.fetchall()
            inventory_details = pd.DataFrame(rows, columns=columns)
            
            # Add computed column
            if 'clinicid' in inventory_details.columns and 'divisionid' in inventory_details.columns:
                inventory_details['SiteId'] = inventory_details['clinicid'].astype(str) + inventory_details['divisionid'].astype(str)
            
            return inventory_details
        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message = "get_inventory_details line :"+str(exc_tb.tb_lineno)+" " +str(e)         
            raise Exception(message)
            
    else:
        message="Connection not found "
        raise Exception( message)




def update_login(connection, clientid, subclientid, credentialmasterid, uid, type, access_type, ip_address, createdby):
    try:
        if connection:
            cursor = connection.cursor()
            
            # Define the SQL query
            insert_query = sql.SQL('''
                INSERT INTO public.tbl_emr_credential_accesslog (
                    clientid,
                    subclientid,
                    credentialmasterid,
                    uid,
                    type,
                    access_type,
                    ip_address,
                    createdby,
                    createddate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            ''')

            # Get the current timestamp for createddate
            createddate = datetime.utcnow()
          # Convert numpy types to native Python types
            if isinstance(clientid, np.integer):
                clientid = int(clientid)
            if isinstance(subclientid, np.integer):
                subclientid = int(subclientid)
            if isinstance(credentialmasterid, np.integer):
                credentialmasterid = int(credentialmasterid)
            # Define the values to insert
            values = (
                clientid,
                subclientid,
                credentialmasterid,
                uid,
                type,
                access_type,
                ip_address,
                createdby,
                createddate
            )
            
            # Execute the query
            cursor.execute(insert_query, values)
            connection.commit()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = f"Exception in insert_log function at line {exc_tb.tb_lineno}: {str(e)}"
        raise Exception(message)



    
def upload_to_output(connection,data):       
    if connection:                                  
        cursor = connection.cursor()            
        insert_query = sql.SQL('''INSERT INTO public.outputmaster("refID", inventoryid, chartdata, icachartid, status, errorreason, failurecount) VALUES (%s, %s, %s, %s, %s, %s, %s);''')
        cursor.execute(insert_query, data)            
        connection.commit() 


def get_public_ip_address():
    try:
        response = requests.get('https://checkip.amazonaws.com')
        ip_address = response.text.strip()
        print(f"Your public IP address is: {ip_address}")
        #response = requests.get('https://api.ipify.org?format=json')
        #ip_address = response.json().get('ip')
        return ip_address
    except requests.RequestException as e:
        print(f"Error getting IP address: {e}")
        return None


def rename_files(download_path, file_names):
    # Remove '.pdf' extension from the file names
    file_names = [i[:-4] for i in file_names if i.endswith('.pdf')]
    
    # Lists to hold failed and unmatched names
    failed_name = []
    unmatched_name = []
    
    # List to hold the existing PDF filenames
    existing_files = []
    
    # Gather all PDF filenames in the directory
    for filename in os.listdir(download_path):
        if filename.endswith('.pdf'):
            existing_files.append(filename[:-4])  # Store names without the .pdf extension
    
    # Identify failed names (files in the directory that are not in file_names)
    for w in existing_files:
        if w not in file_names:
            failed_name.append(w)
    
    # Identify unmatched names (expected names that are not in the existing files)
    for w in file_names:
        if w not in existing_files:
            unmatched_name.append(w)

    # Rename files
    for i in range(len(failed_name)):
        if not unmatched_name:  # Check if there are unmatched names left
            print("No more unmatched names to assign.")
            break
        
        first_unmatched = unmatched_name[0]
        del unmatched_name[0]
        first_failed = failed_name[i]
        old_file_path = os.path.join(download_path, f'{first_failed}.pdf')
        new_file_path = os.path.join(download_path, f'{first_unmatched}.pdf')

        print("old_file_path:", old_file_path)
        print("new_file_path:", new_file_path)

        # Perform the renaming
        os.rename(old_file_path, new_file_path)
        print(f'Renamed: {old_file_path} to {new_file_path}')



def delete_unique_folder(folder_name: str, data_directory: str):
    folder_path = os.path.join(data_directory, folder_name)
    if os.path.isdir(folder_path):  # Check if it is a directory
        shutil.rmtree(folder_path)
        print(f'Deleted directory: {folder_path}')
    else:
        print(f'Directory does not exist: {folder_path}')


if __name__ == "__main__":  
    try: 
        connection = create_connection()
        num_drivers=1
        drivers = [get_driver() for _ in range(num_drivers)]
        index=0
        number_of_login=0
        name_before_parentheses=''
        select_site_one_time=0
        refresh_page=0
        ip = get_public_ip_address()
        print(f"Your IP address is: {ip}")
        ip_address = ip
        puid = puid

        insert_log((puid,datetime.utcnow(),'INFO',"","","",f"MAIN steps 1 Ip_address {ip_address}","1","",""),connection)   
         
        credentialmasterid= 1

        login_details=get_login_details(ip_address,connection)  

        if len(login_details)>0:
            try:
                random_row = login_details.sample(n=1).iloc[0]
                if index==0:
                    driver=drivers[index]
                    index+=1

                url = random_row['clienturl']
                username = random_row['userid']
                password = random_row['password']

                if number_of_login==0:
                    driver=login_cms(url,username,password,driver)   
                    number_of_login=1

                clientid_login = random_row['clientid']
                subclientid_login = random_row['subclientid']
                credentialmasterid = random_row['credentialmasterid']
                type_login = 1  # e.g., 1 for login
                access_type = 1  # e.g., 1 for input
                
                createdby = 1  # assuming the ID of the creator is 1

                # update_login(connection, clientid_login, subclientid_login, credentialmasterid, puid, type_login, access_type, ip_address, createdby)

                if isinstance(credentialmasterid, np.integer):
                    credentialmasterid = int(credentialmasterid)

                insert_log((puid,datetime.utcnow(),'INFO',"","","","MAIN steps 2 Database connection established","1",username,credentialmasterid),connection)

                update_inventory(credentialmasterid,connection,type="login")
                update_login(connection, clientid_login, subclientid_login, credentialmasterid, puid, type_login, access_type, ip_address, createdby)

                insert_log((puid,datetime.utcnow(),'INFO',"","","","MAIN  step 3login click completed","1",username,credentialmasterid),connection)

                # a_azur=login_details.loc[0,"azureblobconnstring"]
                # a_azur_1=login_details.loc[0,"azurecontainername"]
                Client_1=login_details.loc[0,"clientid"]
                Client_2=login_details.loc[0,"subclientid"]
                #AWS Creds
                aws_key=login_details.loc[0,"awsaccesskey"]
                aws_sec_key=login_details.loc[0,"awssecretkey"]
                aws_bucket=login_details.loc[0,"awsbucketname"]
                aws_region=login_details.loc[0,"awsregion"]
                awsS3filepath=login_details.loc[0,"awss3filepath"]

                if select_site_one_time==0:
                    # driver,name_before_parentheses,site_selected_completed=select_site(driver,connection,username,credentialmasterid,ip,login_details,puid)
                    site_selected_completed=1
                    select_site_one_time=select_site_one_time+1

                download_path=download_path
                ERAPATH = ERAPATH
                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"MAIN steps 4 path of pdf download and upload {ERAPATH}","1",username,credentialmasterid),connection)
                if  site_selected_completed==1:
                    insert_log((puid,datetime.utcnow(),'INFO',"","","","MAIN steps 5 site window selection completed","1",username,credentialmasterid),connection)
                    

                while True:  
                    if  site_selected_completed==0:
                        insert_log((puid,datetime.utcnow(),'INFO',"","","","MAIN steps 4 site window selection is not completed","1",username,credentialmasterid),connection)
                        insert_log((puid,datetime.utcnow(),'INFO',"","","","MAIN steps 5 otp window found account may be locked pls check","1",username,credentialmasterid),connection)

                        try:
                            window_billing1 = driver.window_handles
                            driver.switch_to.default_content()
                            driver.switch_to.window(window_billing1[0])  
                            screenshot_bytes = driver.get_screenshot_as_png()

                            # blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                            # container_client = blob_service_client.get_container_client(a_azur_1) 

                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            unique_id = str(uuid.uuid4())

                            file_names_2=f"{username}"
                            file_names_3 = f"screenshot_failed_site_select_{timestamp}_{unique_id}.png"

                            # blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)
                            s3_key = f"{awsS3filepath}/{Client_1}/{Client_2}/2/{file_names_2}/site_id/{file_names_3}"
                            aws_region.put_object(
                                Bucket=bucket_name,
                                Key=s3_key,
                                Body=screenshot_bytes,
                                ContentType='image/png'  # Optional, but you can set content type to png
                            )
                            
                            print(f"Screenshot uploaded to S3 at: {s3_key}")

                            insert_log((puid,datetime.utcnow(),'INFO',"","","",f"successfully uploaded file {str(blob_name)}","1",username,credentialmasterid),connection)                               
                            # content_settings = ContentSettings(content_type="image/png")                
                            # container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                            # insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","1",username,credentialmasterid),connection)   
                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()        
                            message=f"Extraction error for scrrenshot main window : line no : {exc_tb.tb_lineno} {str(e)}"
                            insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)

                        break

                    connection = create_connection()

                    inventory_details=get_inventory_details(puid,credentialmasterid,ip_address,connection)  
                    if len(inventory_details)>0 :    
                        insert_log((puid,datetime.utcnow(),'INFO',"","","","STEPS 1 Data found for processing","1",username,credentialmasterid),connection)

                        num_siteid=int(inventory_details["divisionid"].nunique())

                        update_inventory(list(inventory_details['id']),connection,type="inprocess")
                        emr_level_data=inventory_details.groupby(by=['emr'])           
                        # insert_log(("",datetime.utcnow(),'INFO',"","","",f"found total {emr_level_data.ngroups} unique emr and total {inventory_details.shape[0]} rows","1",username,credentialmasterid),connection)
                        for grp,emr_data in emr_level_data: 
                            try:

                                emr_data.reset_index(drop=True,inplace=True)

                                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"STEPS 2 Proceesing stated for {emr_data.loc[0,'PatientId']}","1",username,credentialmasterid),connection) 

                                site_level_inventory=emr_data.groupby(by=['SiteId'])  

                                for grp,site_patient_data in site_level_inventory:
                                    try:   
                                        id_bill=[]                    

                                        startnew_window = driver.window_handles
                                        site_id= site_patient_data['SiteId'].values[0]
                                        invetory_data= ' ,'.join(map(str,((site_patient_data['id'].values).tolist())))
                                        refID=puid  

                                        # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"found total {site_patient_data.shape[0]} patient for siteid {site_id}","1",username,credentialmasterid),connection)
                                        # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"processing started for siteid {site_id}","1",username,credentialmasterid),connection)                                      
                                        site_patient_data["DOS"]=site_patient_data["DOS"].astype(str)  
                                        site_patient_data['PatientId']=site_patient_data['PatientId'].astype(str)
                                        site_patient_data["DOS"]=site_patient_data["DOS"].apply(convert_to_mm_dd_yyyy)
                                        site_patient_data["DOS"]=site_patient_data['DOS'].astype(str)   

                                        site_patient_data["dob"]=site_patient_data["dob"].astype(str)  

                                        site_patient_data["dob"]=site_patient_data["dob"].apply(convert_to_mm_dd_yyyy)
                                        site_patient_data["dob"]=site_patient_data['dob'].astype(str)  


                                        patient_id_S= site_patient_data['PatientId'].values[0]

                                        site_patient_data.fillna('',inplace=True)
                                        site_patient_data.reset_index(inplace=True)                                 
                                        driver.switch_to.window(startnew_window[0])
                                        starting_window2 = driver.window_handles
                                        driver.switch_to.default_content()
                                        driver.switch_to.window(starting_window2[0])

                                        error=False      
                                        claim_level_dataframe , Service_level_dataframe , file_names,file_names_dict,refresh_page= ExtractAndDownload(site_id,site_patient_data,driver,connection,download_path,invetory_data,refID,name_before_parentheses,refresh_page,username,credentialmasterid)
                                            
                                        insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEPS 8 Extraction process completed for patient id : {patient_id_S}","1",username,credentialmasterid),connection)                         
                                        insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEPS 9 Json convertion process started for patient id : {patient_id_S}"  ,"1",username,credentialmasterid),connection)              
                                        json_string,id_bill=json_converter(Service_level_dataframe,claim_level_dataframe,refID,connection,site_patient_data.loc[0,"ClientId"],site_patient_data.loc[0,"SubClientId"],invetory_data)
                                        insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEPS 10 PDF upload process started for patient id : {patient_id_S}","1",username,credentialmasterid),connection)                                   

                                        # rename_files(download_path, file_names[0])


                                        json_file=f"Json_file{str(site_id)}_{str(int(time.time()))}_output.json"
                                        
                                        # for key, value in file_names_dict.items():
                                        #     file_names_1=key
                                        #     file_names1=value

                                            # upload_file_to_blob(site_patient_data.loc[0,"azureblobconnstring"], site_patient_data.loc[0,"azurecontainername"],
                                            #     file_names1, site_patient_data.loc[0,"ClientId"],site_patient_data.loc[0,"SubClientId"],download_path,connection,file_names_1,invetory_data)                        
                                        aws_access_key = str(site_patient_data.loc[0, "awsaccesskey"]).strip()
                                        aws_secret_key = str(site_patient_data.loc[0, "awssecretkey"]).strip()
                                        aws_region     = str(site_patient_data.loc[0, "awsregion"]).strip()
                                        bucket_name    = str(site_patient_data.loc[0, "awsbucketname"]).strip()
                                        awsS3filepath  = str(site_patient_data.loc[0, "awss3filepath"]).strip()

                                        # Debug checks
                                        print("aws_access_key:", aws_access_key, type(aws_access_key))
                                        print("aws_secret_key:", aws_secret_key, type(aws_secret_key))
                                        print("aws_region:", aws_region, type(aws_region))
                                        print("bucket_name:", bucket_name, type(bucket_name))
                                        print("awsS3filepath:", awsS3filepath, type(awsS3filepath))

                                        # Assertions to fail fast if anything goes wrong
                                        assert isinstance(aws_access_key, str)
                                        assert isinstance(aws_secret_key, str)
                                        assert isinstance(aws_region, str)
                                        assert isinstance(bucket_name, str)
                                        assert isinstance(awsS3filepath, str)

                                        for key, value in file_names_dict.items():
                                            file_names_1 = key
                                            file_names1 = value
                                            clientid = int(inventory_details['ClientId'].iloc[0])
                                            subclientid = int(inventory_details['SubClientId'].iloc[0])
                                            
                                            upload_file_to_s3(
                                                id_bill,
                                                awsS3filepath, # 'icodeone/'
                                                aws_access_key, # 'AKIAWPPO5'
                                                aws_secret_key, # '5qWs1TT'
                                                aws_region, #'us-west-1'
                                                bucket_name, #'icodeoneuat'
                                                local_pdf_path=file_names1, #['mrblling','mr','mr']         
                                                clientid=clientid, #3
                                                subclientid=subclientid, #4
                                                download_path=download_path, #C\\user\\nreahate\\Documents\\3.0\\config\\data\\Uid
                                                connection=connection, #DataBase
                                                file_names_1=file_names_1, # '112424100_09032025'         
                                                invetory_data=invetory_data #116- Inventoryid
                                            )                                  
                            
                            
                                        insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEPS 11 PDF upload process Completed for patient id : {patient_id_S}","1",username,credentialmasterid),connection)                                                                     
                                        api_Count=0
                                        # if len(json_string)>10 :
                                        #     # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"API call started for site id : {site_id}","1",username,credentialmasterid),connection)
                                        #     api_status=api_call(json_string,refID,site_patient_data.loc[0,"ClientId"],
                                        #                         site_patient_data.loc[0,"SubClientId"],site_patient_data.loc[0,"servicetype"],
                                        #                         site_patient_data.loc[0,"uplaod_api_url"],connection)
                                        #     if "Status code: 400 Response" in api_status:
                                        #         if id_bill:
                                        #             update_inventory(id_bill,connection,type="process_failed")
                                        #             api_Count=1
                                        #             try:
                                        #                 error_message="Api failed"
                                        #                 for id_1 in (site_patient_data['id'].values).tolist():
                                        #                     error_resion_code(error_message,id_1,connection)
                                        #             except:
                                        #                 pass
                                        #     try:
                                        #         insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,api_status,"1",username,credentialmasterid),connection)   
                                        #     except:
                                        #         pass
                                        #     insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEPS 12 API call process completed for patient id : {patient_id_S}","1",username,credentialmasterid),connection)

                                        # else:
                                        #     insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"No Data found for patient Id : {patient_id_S}","1",username,credentialmasterid),connection)
                                        if id_bill and api_Count==0:
                                            update_inventory(id_bill,connection,type="process_success")
                                        print("remove_file start")
                                        try:
                                            if file_names_dict:
                                                remove_all_pdf_files(download_path)
                                                # for key, value in file_names_dict.items():
                                                #     file_names1=value
                                                #     for file in file_names1:
                                                #         remove_file(download_path,file)
                                        except Exception as e:
                                            exc_type, exc_obj, exc_tb = sys.exc_info()                        
                                            insert_log((refID,datetime.utcnow(),"ERROR","","",invetory_data,"line : "+str(exc_tb.tb_lineno)+" " +str(e)  ,"1",username,credentialmasterid),connection)
                                    
                                    except Exception as e:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()                        
                                        insert_log((refID,datetime.utcnow(),"ERROR","","",invetory_data,"line : "+str(exc_tb.tb_lineno)+" " +str(e)  ,"1",username,credentialmasterid),connection)
                                    finally:
                                        failed_id=list(set(site_patient_data["id"])-set(id_bill))
                                        if len(failed_id)>0:
                                            update_inventory(failed_id,connection,type="process_failed")
                                            insert_log((puid,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"processing failed for patient id {patient_id_S}","1",username,credentialmasterid),connection)                       

                                            refresh_page=0
                                        
                                            try:
                                                window_billing1 = driver.window_handles
                                                driver.switch_to.default_content()
                                                driver.switch_to.window(window_billing1[0])  
                                                screenshot_bytes = driver.get_screenshot_as_png()

                                                # blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                                                # container_client = blob_service_client.get_container_client(a_azur_1) 

                                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                                unique_id = str(uuid.uuid4())

                                                file_names_2=f"account_failed"
                                                file_names_3 = f"screenshot_failed_{patient_id_S}_{timestamp}_{unique_id}.png"

                                                # blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)
                                                s3_key = f"{awsS3filepath}/{Client_1}/{Client_2}/2/{file_names_2}/site_id/{file_names_3}"

                                                aws_region.put_object(
                                                    Bucket=bucket_name,
                                                    Key=s3_key,
                                                    Body=screenshot_bytes,
                                                    ContentType='image/png'  # Optional, but you can set content type to png
                                                )

                                                # blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)
                                                blob_name = str(awsS3filepath)+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

                                                content_settings = ContentSettings(content_type="image/png")            
                                                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"successfully uploaded file for {patient_id_S} path {str(blob_name)}","1",username,credentialmasterid),connection)   
                                                # container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                                                # insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file for {patient_id_S} path {str(blob_name)}","1",username,credentialmasterid),connection)   
                                            except Exception as e:
                                                exc_type, exc_obj, exc_tb = sys.exc_info()        
                                                message=f"Extraction error for scrrenshot main window : line no : {exc_tb.tb_lineno} {str(e)}"
                                                insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)

                                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"STEPS 13 ALL Proceesing completed for {emr_data.loc[0,'PatientId']}","1",username,credentialmasterid),connection) 
                            except Exception as e:
                                    exc_type, exc_obj, exc_tb = sys.exc_info()                    
                                    insert_log((puid,datetime.utcnow(),"ERROR","","","","line : "+str(exc_tb.tb_lineno)+" " +str(e)  ,"1",username,credentialmasterid),connection)

                    if len(inventory_details)==0:
                        break


            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message = "line :"+str(exc_tb.tb_lineno)+" " +str(e)  
                insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",username,credentialmasterid),connection)  
            finally:
            
                try:
                    driver.switch_to.alert.accept()
                    alert = Alert(driver)
                    alert.accept()
                except:
                    pass
                try:
                    driver.switch_to.alert.accept()
                    alert = Alert(driver)
                    alert.accept()
                except:
                    pass
                type_login = 2  # e.g., 1 for login
                access_type = 1  # e.g., 1 for input
                update_login(connection, clientid_login, subclientid_login, credentialmasterid, puid, type_login, access_type, ip_address, createdby)
                update_inventory(credentialmasterid,connection,type="logout")
                delete_unique_folder(puid,data_directory)

                try:
                    logout_button = driver.find_element(By.XPATH, '//*[@id="searchBarDiv"]/table/tbody/tr/td[3]/span')
                    logout_button.click()
                    insert_log((puid,datetime.utcnow(),'INFO',"","","","steps 5 logout click completed","1",username,credentialmasterid),connection)
                except Exception as e:

                    exc_type,exc_obj,exc_tb=sys.exc_info()
                    message='log out click is not working line :'+str(exc_tb.tb_lineno)+" "+str(e)
                    insert_log((puid,datetime.utcnow(),'INFO',"","","",message,"1",username,credentialmasterid),connection)

                    pass
                try:
                    alert = Alert(driver)
                    alert.accept()
                except:
                    pass
                
                
                insert_log((puid,datetime.utcnow(),'INFO',"","","","steps 6 browser close completed","1",username,credentialmasterid),connection)

                connection.close()
                for driver in drivers:
                    driver.quit()
                    break

        else:
            insert_log((puid,datetime.utcnow(),'INFO',"","","",f"steps 2 Ip_address {ip_address} no data found","1","",""),connection)    
            insert_log((puid,datetime.utcnow(),'INFO',"","","","steps 3 browser close completed","1","",""),connection)    
            delete_unique_folder(puid,data_directory)
            connection.close()
            for driver in drivers:
                driver.quit()
                break 

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = "line :"+str(exc_tb.tb_lineno)+" " +str(e)  
        insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"1",'',""),connection)  

        