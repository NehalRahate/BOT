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
import html
from selenium.webdriver.support.ui import Select
import socket
from selenium.webdriver.chrome.service import Service

current_directory = os.getcwd()
print("current_directory",current_directory)

config_file_path = os.path.join(current_directory, 'Config/path_details_UAT30.ini')
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


download_path = config['Paths']['download_path']
ERAPATH = config['Paths']['ERAPATH']

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

print(decDetails, "\n", db_params)

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
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = "Exception : insert_log line :"+str(exc_tb.tb_lineno)+" " +str(e)
        raise Exception(message)


 
def get_inventory_details(puid,credentialmasterid,ip_address,connection):
    if connection:
        try:
            suid = str(uuid.uuid4())
            cursor = connection.cursor()
            inp_value=1
            update_query=sql.SQL('''
                               SELECT  inv.inventoryid , '' p_uid, inv.codeddate,inv.team,inv.emr,inv.clinicid,inv.divisionid,inv.division,inv.dos,inv.patientid,inv.patientname
            ,inv.dob,inv.healthplan,inv.visitid,inv.visittype,inv.coverbyname,inv.billingid,inv.billername,inv.datecompleted,inv.codingcomment,inv.billingcomment,inv.querytype
            ,inv.claimstatus,inv.loc,inv.setup,inv.allocationdate,inv.filestatus,inv.createddate,inv.createdby,inv.subclientid,inv.clientid,inv.servicetype,inv.errorreason
            ,inv.failurecount,scm.azureblobconnstring,scm.azurecontainername,e_crd_master.clienturl,e_crd_master.userid,e_crd_master.password,e_crd_master.uplaod_api_url

            FROM public.tbl_stginventoryuploaddata inv
            INNER JOIN mst.tbl_subclientmaster scm ON inv.subclientid = scm.subclientmasterid
            INNER JOIN mst.tbl_emr_credentialmaster e_crd_master ON inv.subclientid = e_crd_master.subclientid
            where inv.inventoryid in (100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121)
			and filestatus in (0)
			and e_crd_master.userid in ('BOT6_Coding','BOT1_Coding') order by random() limit 1
                                 ''')
            # ('''select * from public.fn_get_inventorycharts_concurrent_user(%s,%s,%s,%s);''')    
            try:
                cursor.execute(update_query, (suid, 4, credentialmasterid, ip_address))

            except:
                try:
                    # Properly format the query with direct string formatting
                    query = f'''
                        SELECT * FROM public.fn_get_inventorycharts_concurrent_user(
                            '{puid}',
                            4,
                            {credentialmasterid}, 
                            '{ip_address}'
                        );
                    '''

                    # Execute the query
                    cursor.execute(query)

                except:
                    cursor.execute(f'''select * from public.get_inventorycharts_concurrent_user({str(puid)},4,{str(credentialmasterid)},{str(ip_address)})''')
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            
            num_fixed_columns = 40

            fixed_columns = ["id","uid","codeddate","team","emr","clinicid","divisionid","division","DOS","PatientId","patientname",
                                                "dob","healthplan","VisitId","visittype","coverbyname","BillingId","billername","datecompleted","codingcomment","billingcomment","querytype",
                                                "claimstatus","loc","setup","allocationdate","FileStatus","createddate","createdby","SubClientId","ClientId",
                                                "servicetype","errorreason","failurecount","azureblobconnstring","azurecontainername","url","userid","password","uplaod_api_url"]

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


def find_match(div,driver,target_date,acctno):

    ele_option1=2
    name_list1=[]
    file_name12=[]
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option1)}]/td[1]')))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()        
        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
        # insert_log(("",datetime.utcnow(),"INFO","","","",message,"4","",""),connection)  

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
                # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"4","",""),connection)  

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
                    print(name)
                    # message=f"continue  updated for div div _  {str(div)} "
                    # insert_log(("",datetime.utcnow(),"INFO","","","",message,"4","",""),connection)  

                    name_list1.append(name)
                    try:
                        for dat in range(len(a_list)):
                            index=dat+1
                            name_elem = driver.find_element(By.XPATH, f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[1]')
                            n_date = name_elem.text
                            if target_date in n_date:
                                try:
                                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[5]/a'))).click()
                                except:
                                    try:
                                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="{str(div)}"]/table[2]/tbody/tr/td/div/table/tbody/tr[{str(ele_option_date)}]/td[2]/table/tbody/tr[{str(index)}]/td[4]/a'))).click()
                                    except Exception as e:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()        
                                        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)} {str(xpath)}"
                                        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"4","",""),connection)  

                                try:
                                    names_=remove_punctuation(name).replace(' ','')
                                    file_name1 = str(f"MedicalHistory_{str(acctno)+str(names_)}.pdf")
                                except:
                                    file_name1 = str(f"MedicalHistory_{str(acctno)+str(index)}.pdf")

                                dubble_frame=medical_history(driver,file_name1)

                                if dubble_frame==0:
                                    file_name12.append(file_name1)
                                time.sleep(2)
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()        
                        message=f"Extraction error for medical Histry name : line no : {exc_tb.tb_lineno} {str(e)}"
                        insert_log(("",datetime.utcnow(),"ERROR","","","",message,"4","",""),connection) 
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



def wait_for_download(download_dir, timeout=60, retry_interval=5):
    time_elapsed = 0
    p=1
    while time_elapsed < timeout:
        files = os.listdir(download_dir)
        if files:
            # Check if there are any files with .crdownload extension
            crdownload_files = [f for f in files if f.endswith('.crdownload')]
            if not crdownload_files:
                # No files with .crdownload extension found, proceed to find the .pdf file
                pdf_files = [os.path.join(download_dir, f) for f in files if f.endswith('.pdf')]
                if pdf_files:
                    return pdf_files[0]
            else:
                # Files with .crdownload extension found, wait before retrying
                print(f"Files with .crdownload extension found. Retrying after {retry_interval} seconds...")
                time.sleep(retry_interval)
                time_elapsed += retry_interval
                continue
        time.sleep(1)
        time_elapsed += 1
    
    return 1


def wait_for_download1(download_dir, timeout=75, retry_interval=3):
    time_elapsed = 0
    previous_size = None
    file_path = None
    p=1
    while time_elapsed < timeout:
        files = os.listdir(download_dir)
        if files:
            file_path = [os.path.join(download_dir, f) for f in files if f.endswith('.pdf')]
            if file_path:
                current_size = os.path.getsize(file_path[0])
                if previous_size is None:
                    previous_size = current_size
                elif current_size == previous_size:
                    return file_path[0]
                else:
                    previous_size = current_size  # Update previous size for next iteration
        time.sleep(retry_interval)
        time_elapsed += retry_interval
    
    return p

def visit_dos_documents(driver,filename1):
    
    window_billing1 = driver.window_handles
    driver.switch_to.default_content()
    driver.switch_to.window(window_billing1[0])
    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
    WebDriverWait(driver, 50).until(element_present1)
    driver.switch_to.frame("workarea2")

    try:

        print_link = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.LINK_TEXT, "Print")))
        print_link.click()
    except:
        try:
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[4]/td'))).click()
        except:
            pass                
    download_directory=ERAPATH
    time.sleep(3)
    # try:
    #     dow_dir=wait_for_download(download_directory)
    #     dow_dir=wait_for_download1(download_directory)
    # except:
    #     pass
    try:
        wait_for_download2(download_directory)
    except:
        pass

    window_after = driver.window_handles[-1]
    time.sleep(1)
    driver.switch_to.window(window_after)

    time.sleep(2)
    filename = filename1
    move_latest_file(filename)
    # message=f"DOS visit history pdf download completed for {filename} pdf"
    # insert_log(("",datetime.utcnow(),"Info","","","",message,"4","",""),connection) 
    window_after = driver.window_handles
    handles = driver.window_handles

    num_tabs = len(handles)

    if num_tabs==2:
        driver.switch_to.window(handles[1])
        driver.close()  
    driver.switch_to.window(window_after[0])



def click_on_date(driver, filename1):
    # time.sleep(2)
    window_billing = driver.window_handles
    driver.switch_to.default_content()
    driver.switch_to.window(window_billing[0])

    element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
    WebDriverWait(driver, 20).until(element_present1)
    driver.switch_to.frame("workarea2")
    # pdfpic
    element_present34 = EC.presence_of_element_located((By.XPATH, '//*[@id="pdfpic"]'))
    WebDriverWait(driver, 15).until(element_present34)
    driver.switch_to.frame("pdfpic")
    # time.sleep(3)
    try:
        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[2]/span/center/div[2]'))).click()
    except:
        try:
            window_lab_date = driver.window_handles
            driver.switch_to.default_content()
            driver.switch_to.window(window_lab_date[0])

            element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
            WebDriverWait(driver, 15).until(element_present1)
            driver.switch_to.frame("workarea2")
            ele_option=1
            target_area="Print"
            while ele_option>0:
                    try:
                        First_date=driver.find_element(By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr').text
                        if target_area in  First_date:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                            break
                        ele_option+=1
                    except:
                        ele_option+=1
                        break
        except:
            pass 
        
    download_directory=ERAPATH
    try:
        dow_dir=wait_for_download(download_directory)
        dow_dir=wait_for_download1(download_directory)
    except:
        pass
    try:
        wait_for_download2(download_directory)
    except:
        pass

    time.sleep(1)
    window_after = driver.window_handles[-1]

    driver.switch_to.window(window_after)

    time.sleep(2)
    filename = filename1
    move_latest_file(filename)

    # message=f"DOC labs pdf download completed for {filename} pdf"
    # insert_log(("",datetime.utcnow(),"Info","","","",message,"4","",""),connection) 

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
        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"4","",""),connection)  

   
def medical_history(driver,filename1):
    try:
        time.sleep(2)
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
        try:

            print_buttons = WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@onclick='beginPrint();']")))
            for index, print_button in enumerate(print_buttons, start=1):
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[@onclick='beginPrint();']"))).click()
                    break  
                except Exception as e:
                    print(f"Error clicking on Print button {index}: {e}")
        except:
            try:
                
                print_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[@onclick='beginPrint()']")))
                print_button.click()
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
                    dubble_frame=1
                except:
                    print("Window not found or close button not present.")
            except:
                try:
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="div_button"]/div[5]/table/tbody/tr'))).click()
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()        
                        message=f"print download problem : line no : {exc_tb.tb_lineno} {str(e)}"
                        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"4","",""),connection)  
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

        try:
            if dubble_frame==0:
                wait_for_download2(download_directory)
        except:
            pass
        time.sleep(2)
        
        filename = filename1
        if dubble_frame==0:
            move_latest_file(filename)
            message=f"medical History pdf download completed for {filename} pdf"
            # insert_log(("",datetime.utcnow(),"Info","","","",message,"4","",""),connection) 


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
        # insert_log(("",datetime.utcnow(),"ERROR","","","",message,"4","",""),connection)  
    return dubble_frame



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
    
def normalize_text(text):
    return html.unescape(text.strip().lower())        

def re_generate_claim(driver,row,PatientId,starting_window,connection):
    # time.sleep(1)

    
    try:    
        # Locate the second image element and click it
        image_element2 = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/form/div[7]/table/tbody/tr/td[3]/img'))
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
        print("Second image element not found or clickable ")
    
    window_after = driver.window_handles
    handles = driver.window_handles
    num_tabs = len(handles)
    
    time.sleep(1)
    driver.switch_to.window(starting_window)
    # claim_genrate=0
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
        
        
        # element_xpath = '/html/body/form/div[58]/table/tbody/tr/td/img'
        # for no_click in range(1,5):
        #     if check_element_loaded(driver, element_xpath):
        #         # Proceed with further actions if the element is loaded
        #         print("Proceeding with further actions.")
        #         WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[58]/table/tbody/tr/td/img'))).click()                                
        #         break
        #     else:
        #         # Handle the case where the element is not loaded
        #         print("Handling the case where the element is not loaded.")

        
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[58]/table/tbody/tr/td/img'))).click()                                
        starting_window2 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(starting_window2[0])

        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
        
        WebDriverWait(driver, 20).until(element_present2)
        driver.switch_to.frame("waittinglistframe")    

        # Wait for the table to be present (modify the condition as necessary)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/form/div[1]/table/tbody/tr[5]/td/table/tbody/tr[2]/td'))
        )

        # Loop through rows 1 to 6
        for i in range(1, 7):
            input_xpath = f'/html/body/form/div[1]/table/tbody/tr[5]/td/table/tbody/tr[{i}]/td/input'

            try:
                if input_xpath == '/html/body/form/div[1]/table/tbody/tr[5]/td/table/tbody/tr[5]/td/input':
                    # Check the checkbox if the text matches
                    checkbox = driver.find_element(By.XPATH, input_xpath)
                    if not checkbox.is_selected():
                        checkbox.click()
                else:
                    # Uncheck the checkbox if the text does not match
                    checkbox = driver.find_element(By.XPATH, input_xpath)
                    if checkbox.is_selected():
                        checkbox.click()
            
            except Exception as e:
                print(f"An error occurred: {e}")

        
        starting_window2 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(starting_window2[0])

        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))

        WebDriverWait(driver, 20).until(element_present2)
        driver.switch_to.frame("waittinglistframe")            
        # time.sleep(2)

        # dropdown_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="listDivision"]')))
        # dropdown = Select(dropdown_element)
        # dropdown_options = [option.text.strip() for option in dropdown.options]
        
        # normalized_row_division = normalize_text(row["division"])
        # print(normalized_row_division)
        # matching_option = None
        # for option in dropdown_options:
        #     if normalized_row_division in normalize_text(option):
        #         matching_option = option
        #         break
        # if matching_option:
        #     print(f"Selecting division: {matching_option}")
        #     dropdown.select_by_visible_text(matching_option)
        # else:
        #     print(f"No match found for division: {row['division']}")
        #     insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error division name not found {row['division']}","1","",""),connection)
    
        
        checkbox = driver.find_element(By.XPATH, '//*[@id="divOption"]/table/tbody/tr[8]/td/input')

        if not  checkbox.is_selected():
            # If it is checked, uncheck it
            checkbox.click()
            print("Checkbox was checked and has been unchecked.")
            # Perform additional click action after unchecking
            
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divOption"]/table/tbody/tr[7]/td/table/tbody/tr[1]/td/input[1]'))).click()                


        # time.sleep(2)
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])  
        # time.sleep(2)
        element_waitting1 = EC.presence_of_element_located((By.XPATH, '//*[@id="frm_searchframe"]'))
        WebDriverWait(driver, 30).until(element_waitting1)
        driver.switch_to.frame("frm_searchframe")
        
        
        dropdown_element = driver.find_element(By.XPATH, '//*[@id="searchBy"]')  # Use the correct locator
        # Create a Select object
        select = Select(dropdown_element)
        # Method 1: Select by visible text
        select.select_by_visible_text('Patient ID')
        


        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).click()


        if len(PatientId)>6:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).clear()
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(PatientId))
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a'))).click()

        else:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).clear()
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(patient_name_chart_1))
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
            time.sleep(2)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a')))

            data=driver.find_element(By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]').text
# 
            for index_i,name_dob in enumerate(data.split('\n')):
                patient_only_name=driver.find_element(By.XPATH,f'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr[{str(index_i+1)}]/td[1]/a').text
                patient_only_name=patient_only_name.replace(',','').replace(' ','')
                if patient_DOB in name_dob and patient_name_chart_.lower() in patient_only_name.lower():
                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr[{str(index_i+1)}]/td[1]/a'))).click()
                    insert_log(("",datetime.utcnow(),'INFO',"",PatientId,row['id'],f"old Format Patient name found","4","",""),connection) 
                    break


        # WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(PatientId))
        # WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
        # WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a'))).click()

        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])

        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
        WebDriverWait(driver, 20).until(element_present2)
        driver.switch_to.frame("waittinglistframe")           

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistFrame"]'))
        WebDriverWait(driver, 30).until(element_present1)
        # driver.switch_to.frame("waittinglistFrame")

        dropdown_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="listDivision"]')))
        dropdown = Select(dropdown_element)
        dropdown_options = [option.text.strip() for option in dropdown.options]
        
        normalized_row_division = normalize_text(row["division"])
        print(normalized_row_division)
        matching_option = None
        for option in dropdown_options:
            if normalized_row_division in normalize_text(option):
                matching_option = option
                break
        if matching_option:
            print(f"Selecting division: {matching_option}")
            dropdown.select_by_visible_text(matching_option)
        else:
            print(f"No match found for division: {row['division']}")
            insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error division name not found {row['division']}","1","",""),connection)

        driver.switch_to.frame("waittinglistFrame")
        column_index = 5
        target_value = convert_to_mm_dd_yyyy(row["DOS"])
        time.sleep(2)
        table_id = '//*[@id="waitinglisttable"]/tbody'
        row_index = find_row_index_waiting(driver,table_id, column_index, target_value)                

        if row_index != -1:                    
            try:
                WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[8]/a'))).click()
            except:
                WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[9]/a'))).click()
            window_billing1 = driver.window_handles
            driver.switch_to.default_content()
            driver.switch_to.window(window_billing1[0])            
            element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
            WebDriverWait(driver, 50).until(element_present1)
            driver.switch_to.frame("workarea0")
            time.sleep(3)
            
            try:
                if driver.find_element(By.XPATH,f'//*[@id="ApproveAndPassBilling"]/table/tbody/tr/td'):
                    if "Approve and Pass to Billing" in driver.find_element(By.XPATH,f'//*[@id="ApproveAndPassBilling"]/table/tbody/tr/td').text:
                        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="ApproveAndPassBilling"]/table/tbody/tr/td'))).click()
                        window_billing1 = driver.window_handles
                        driver.switch_to.default_content()
                        driver.switch_to.window(window_billing1[0])                        
                        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
                        WebDriverWait(driver, 20).until(element_present2)
                        driver.switch_to.frame("waittinglistframe")           

                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistFrame"]'))
                        WebDriverWait(driver, 30).until(element_present1)
                        driver.switch_to.frame("waittinglistFrame")
                        try:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[8]/a'))).click()
                        except:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[9]/a'))).click()
                        window_billing1 = driver.window_handles
                        driver.switch_to.default_content()
                        driver.switch_to.window(window_billing1[0])                        
                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
                        WebDriverWait(driver, 50).until(element_present1)
                        driver.switch_to.frame("workarea0")
            except:
                pass

            ele_option=2
            target_area="Generate Claim"
            while ele_option>0:
                try:
                    First_date=driver.find_element(By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr').text
                    if target_area in  First_date:
                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                        time.sleep(2)
                        break
                    ele_option+=1
                except:
                    ele_option+=1
                    break
            time.sleep(1)
            WebDriverWait(driver, 40).until(EC.alert_is_present())
            driver.switch_to.alert.accept()

            try:
                alert=WebDriverWait(driver, 40).until(EC.alert_is_present(()))
                if alert:
                    driver.switch_to.alert.dissmis()                                                                                    
            except Exception as e:                            
                    pass
                
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
                    EC.presence_of_element_located((By.XPATH, '/html/body/form/div[7]/table/tbody/tr/td[3]/img'))
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
                print("Second image element not found or clickable ")
                # exc_type, exc_obj, exc_tb = sys.exc_info()        
                # message=f"Second image element not found or clickable line no : {exc_tb.tb_lineno} {str(e)}"    

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()                        
        insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error Waitting :  line no : {exc_tb.tb_lineno} {str(e)}","4","",""),connection) 
    time.sleep(1)
    driver.switch_to.window(starting_window)
    # try:
    #     driver.refresh()
    #     driver.switch_to.alert.accept()
    #     alert = Alert(driver)
    #     alert.accept()
    # except:
    #     pass


def generate_claim_from_waiting(driver,row,PatientId,starting_window,connection,refresh_page):
    # time.sleep(1)
    claim_genrate=0
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
        
        
        # element_xpath = '/html/body/form/div[58]/table/tbody/tr/td/img'
        # for no_click in range(1,5):
        #     if check_element_loaded(driver, element_xpath):
        #         # Proceed with further actions if the element is loaded
        #         print("Proceeding with further actions.")
        #         WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[58]/table/tbody/tr/td/img'))).click()                                
        #         break
        #     else:
        #         # Handle the case where the element is not loaded
        #         print("Handling the case where the element is not loaded.")

        if refresh_page==0:
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[58]/table/tbody/tr/td/img'))).click()                                
            starting_window2 = driver.window_handles
            driver.switch_to.default_content()
            driver.switch_to.window(starting_window2[0])

            element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
            
            WebDriverWait(driver, 20).until(element_present2)
            driver.switch_to.frame("waittinglistframe")    

            # Wait for the table to be present (modify the condition as necessary)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/form/div[1]/table/tbody/tr[5]/td/table/tbody/tr[2]/td')))

            # Loop through rows 1 to 6
            for i in range(1, 7):
                input_xpath = f'/html/body/form/div[1]/table/tbody/tr[5]/td/table/tbody/tr[{i}]/td/input'

                try:
                    if input_xpath == '/html/body/form/div[1]/table/tbody/tr[5]/td/table/tbody/tr[5]/td/input':
                        # Check the checkbox if the text matches
                        checkbox = driver.find_element(By.XPATH, input_xpath)
                        if not checkbox.is_selected():
                            checkbox.click()
                    else:
                        # Uncheck the checkbox if the text does not match
                        checkbox = driver.find_element(By.XPATH, input_xpath)
                        if checkbox.is_selected():
                            checkbox.click()
                
                except Exception as e:
                    print(f"An error occurred: {e}")

        
        starting_window2 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(starting_window2[0])

        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
        
        WebDriverWait(driver, 20).until(element_present2)
        driver.switch_to.frame("waittinglistframe")            
        # time.sleep(2)
        
        # dropdown_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="listDivision"]')))
        # dropdown = Select(dropdown_element)
        # dropdown_options = [option.text.strip() for option in dropdown.options]
        
        # normalized_row_division = normalize_text(row["division"])
        # print(normalized_row_division)
        # matching_option = None
        # for option in dropdown_options:
        #     if normalized_row_division in normalize_text(option):
        #         matching_option = option
        #         break
        # if matching_option:
        #     print(f"Selecting division: {matching_option}")
        #     dropdown.select_by_visible_text(matching_option)
        # else:
        #     print(f"No match found for division: {row['division']}")
        #     insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error division name not found {row['division']}","1","",""),connection)
    
        
        checkbox = driver.find_element(By.XPATH, '//*[@id="divOption"]/table/tbody/tr[8]/td/input')

        if not  checkbox.is_selected():
            # If it is checked, uncheck it
            checkbox.click()
            print("Checkbox was checked and has been unchecked.")
            # Perform additional click action after unchecking
            
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="divOption"]/table/tbody/tr[7]/td/table/tbody/tr[1]/td/input[1]'))).click()                


        # time.sleep(2)
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])  
        # time.sleep(2)
        element_waitting1 = EC.presence_of_element_located((By.XPATH, '//*[@id="frm_searchframe"]'))
        WebDriverWait(driver, 30).until(element_waitting1)
        driver.switch_to.frame("frm_searchframe")
        
        
        dropdown_element = driver.find_element(By.XPATH, '//*[@id="searchBy"]')  # Use the correct locator
        # Create a Select object
        select = Select(dropdown_element)
        # Method 1: Select by visible text
        select.select_by_visible_text('Patient ID')
        


        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).click()


        if len(PatientId)>6:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).clear()
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(PatientId))
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a'))).click()

        else:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).clear()
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(patient_name_chart_1))
            WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
            time.sleep(2)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a')))

            data=driver.find_element(By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]').text
# 
            for index_i,name_dob in enumerate(data.split('\n')):
                patient_only_name=driver.find_element(By.XPATH,f'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr[{str(index_i+1)}]/td[1]/a').text
                patient_only_name=patient_only_name.replace(',','').replace(' ','')
                if patient_DOB in name_dob and patient_name_chart_.lower() in patient_only_name.lower():
                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr[{str(index_i+1)}]/td[1]/a'))).click()
                    insert_log(("",datetime.utcnow(),'INFO',"",PatientId,row['id'],f"old Format Patient name found","4","",""),connection) 
                    break


        # WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchStr"]'))).send_keys(str(PatientId))
        # WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="btnSearch"]/table/tbody/tr/td'))).click()
        # WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[2]/td/div/div[2]/table/tbody/tr/td[1]/a'))).click()

        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])

        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
        WebDriverWait(driver, 20).until(element_present2)
        driver.switch_to.frame("waittinglistframe")           

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistFrame"]'))
        WebDriverWait(driver, 30).until(element_present1)
        

        dropdown_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="listDivision"]')))
        dropdown = Select(dropdown_element)
        dropdown_options = [option.text.strip() for option in dropdown.options]
        
        normalized_row_division = normalize_text(row["division"])
        print(normalized_row_division)
        matching_option = None
        for option in dropdown_options:
            if normalized_row_division in normalize_text(option):
                matching_option = option
                break
        if matching_option:
            print(f"Selecting division: {matching_option}")
            dropdown.select_by_visible_text(matching_option)
        else:
            print(f"No match found for division: {row['division']}")
            insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error division name not found {row['division']}","1","",""),connection)
        
        driver.switch_to.frame("waittinglistFrame")
        
        column_index = 5
        target_value = convert_to_mm_dd_yyyy(row["DOS"])
        time.sleep(2)
        table_id = '//*[@id="waitinglisttable"]/tbody'
        row_index = find_row_index_waiting(driver,table_id, column_index, target_value)                

        if row_index != -1:                    
            try:
                WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[8]/a'))).click()
            except:
                WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[9]/a'))).click()
            window_billing1 = driver.window_handles
            driver.switch_to.default_content()
            driver.switch_to.window(window_billing1[0])            
            element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
            WebDriverWait(driver, 50).until(element_present1)
            driver.switch_to.frame("workarea0")
            time.sleep(3)
            
            try:
                if driver.find_element(By.XPATH,f'//*[@id="ApproveAndPassBilling"]/table/tbody/tr/td'):
                    if "Approve and Pass to Billing" in driver.find_element(By.XPATH,f'//*[@id="ApproveAndPassBilling"]/table/tbody/tr/td').text:
                        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="ApproveAndPassBilling"]/table/tbody/tr/td'))).click()
                        window_billing1 = driver.window_handles
                        driver.switch_to.default_content()
                        driver.switch_to.window(window_billing1[0])                        
                        element_present2 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistframe"]'))
                        WebDriverWait(driver, 20).until(element_present2)
                        driver.switch_to.frame("waittinglistframe")           

                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="waittinglistFrame"]'))
                        WebDriverWait(driver, 30).until(element_present1)
                        driver.switch_to.frame("waittinglistFrame")
                        try:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[8]/a'))).click()
                        except:
                            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[2]/table/tbody/tr[{row_index+1}]/td[9]/a'))).click()
                        window_billing1 = driver.window_handles
                        driver.switch_to.default_content()
                        driver.switch_to.window(window_billing1[0])                        
                        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
                        WebDriverWait(driver, 50).until(element_present1)
                        driver.switch_to.frame("workarea0")
            except:
                pass

            ele_option=2
            target_area="Generate Claim"
            while ele_option>0:
                try:
                    First_date=driver.find_element(By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr').text
                    if target_area in  First_date:
                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[4]/table/tbody/tr/td/div[{str(ele_option)}]/table/tbody/tr'))).click()
                        time.sleep(2)
                        break
                    ele_option+=1
                except:
                    ele_option+=1
                    break
            time.sleep(1)
            WebDriverWait(driver, 40).until(EC.alert_is_present())  
            driver.switch_to.alert.accept()
            
            claim_genrate=1

            try:
                alert=WebDriverWait(driver, 40).until(EC.alert_is_present(()))
                if alert:
                    driver.switch_to.alert.dissmis()                                                                                    
            except Exception as e:                            
                    pass
                    
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()                        
        insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error Waitting :  line no : {exc_tb.tb_lineno} {str(e)}","4","",""),connection) 
    time.sleep(1)
    driver.switch_to.window(starting_window)
    # try:
    #     driver.refresh()
    #     driver.switch_to.alert.accept()
    #     alert = Alert(driver)
    #     alert.accept()
    # except:
    #     pass

    return driver,claim_genrate


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


def generate_billing_data(driver,row,PatientId,patientindex,connection):
    time.sleep(2)
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
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[66]/table/tbody/tr/td/img'))).click()
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
        WebDriverWait(driver, 20).until(element_present1)
        driver.switch_to.frame("workarea0") 
        time.sleep(1)
            #check add
        start_time = time.time()

        element_xpath = '//*[@id="divOption"]/table/tbody/tr[9]/td/table/tbody/tr[1]/td/input[1]'
        
        if check_element_loaded(driver, element_xpath):
            # Proceed with further actions if the element is loaded
            print("Proceeding with further actions.")
        else:
            # Handle the case where the element is not loaded
            print("Handling the case where the element is not loaded.")


        dropdown_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="DivisionList_myDivisonList"]'))
            )
        
        dropdown = Select(dropdown_element)

        dropdown.select_by_visible_text("All")
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[7]/td/input[2]'))).click()


        checkbox = driver.find_element(By.XPATH, '//*[@id="divOption"]/table/tbody/tr[16]/td/input')

        if checkbox.is_selected():
            # If it is checked, uncheck it
            checkbox.click()
            print("Checkbox was checked and has been unchecked.")
            # Perform additional click action after unchecking

        else:
            # If it is unchecked, check it
            checkbox.click()
            print("Checkbox was unchecked and has been checked.")
            # Uncheck it again
            checkbox.click()
            print("Checkbox has been unchecked again.")

        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[17]/td/input[1]'))).click()
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[17]/td/input[2]'))).click()
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[9]/td/table/tbody/tr[3]/td/input'))).click()
        ## select 1
        # WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[7]/td/input[3]'))).click()
        ## select 7
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[7]/table/tbody/tr[7]/td/input[4]'))).click()


        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])    
        time.sleep(1)                 
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea0"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("workarea0")
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
            
        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Time taken to complete the process: {time_taken:.2f} seconds")

        # column_index = 2 
        start_time = time.time()
        target_value = convert_to_mm_dd_yyyy(row["DOS"])

        # table_id = '//*[@id="div2"]/table'  
        # row_index = find_row_index(driver,table_id, column_index, target_value)  

        # if row_index != -1:                    
        #     WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr[{row_index + 1}]/td[5]/a'))).click()

        table_xpath = '//*[@id="div2"]/table'
        column_index = 4
        # target_name = patient_name_chart
        target_name='DONG, XUE CHUN'

        row_indices = find_rows_index(driver, table_xpath, column_index, target_name)
        
        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Time taken to complete the process: {time_taken:.2f} seconds")

        if len(row_indices)>=1:
            for row_indices_ in row_indices:
                First_date=driver.find_element(By.XPATH,f'/html/body/form/div[3]/table[1]/tbody/tr[{row_indices_}]/td[3]').text
                First_name=driver.find_element(By.XPATH,f'/html/body/form/div[3]/table/tbody/tr[{row_indices_}]/td[5]/a').text

                if First_date in  target_value and First_name in target_name :
                    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/table/tbody/tr[{row_indices_}]/td[5]/a'))).click()
                    break
        
        target_name='LI, JINHANG'

        row_indices = find_rows_index(driver, table_xpath, column_index, target_name)

        
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
        insert_log(("",datetime.utcnow(),'ERROR',"",PatientId,row['id'],f"Extraction error BILLING :  line no : {exc_tb.tb_lineno} {str(e)}","4","",""),connection)


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
    time.sleep(1)
    try:
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="tbody_cpt"]/tr')))
    except:
        pass
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
            # Use parameterized queries to prevent SQL injection
            cursor.execute('SELECT otp FROM public.tbl_emr_credential_ip_mapping WHERE credentialmasterid = %s and ip_address = %s', (id,ip_address,))
            rows = cursor.fetchone()
            return rows[0] if rows else None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None


def select_site(driver,connection,username,credentialmasterid,ip_address,site_patient_data,puid):
    try:
        site_selected_completed=0
        name_before_parentheses=""
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="tableLab"]/tbody/tr[6]/td/div/table/tbody/tr/td')))
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
                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"4",username,credentialmasterid),connection)
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
                    insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","4",username,credentialmasterid),connection)   
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)

                # ///TEMP END
                otp_data=get_otp(connection,credentialmasterid,ip_address)
                message=f"otp data {otp_data} "

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"4",username,credentialmasterid),connection)

                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="phoneCode"]'))).clear()
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="phoneCode"]'))).send_keys(str(otp_data))
                message=f'//*[@id="phoneCode"] xpath found otp enter'

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"4",username,credentialmasterid),connection)
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
                    insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","4",username,credentialmasterid),connection)   
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)

                element_t = driver.find_element(By.XPATH, '//*[@id="trustMe"]')
                if not element_t.is_selected():
                    element_t.click()
                message=f'//*[@id="trustMe"] xpath found trust click'

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"4",username,credentialmasterid),connection)
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="ctl00_mainCont_inputPhoneCodePnl"]/table/tbody/tr[4]/td/div/table/tbody/tr/td'))).click()
                time.sleep(2)
                message=f'//*[@id="ctl00_mainCont_inputPhoneCodePnl"]/table/tbody/tr[4]/td/div/table/tbody/tr/td xpath found button click'

                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"4",username,credentialmasterid),connection)
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
                    insert_log((puid,datetime.utcnow(),'INFO',site_id,"","",f"succesfully uploaded file {str(blob_name)}","4",username,credentialmasterid),connection)   
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)


                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH,f'//*[@id="tableLab"]/tbody/tr[6]/td/div/table/tbody/tr/td')))
                message=f"otp window completed"
                insert_log((puid,datetime.utcnow(),"INFO","","","",message,"4",username,credentialmasterid),connection)

                is_otp=False
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()        
                message=f"Extraction error for select_site : line no : {exc_tb.tb_lineno} {str(e)}"
                insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection) 
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
        insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","4",username,credentialmasterid),connection)   
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()        
        message=f"Extraction error for scrrenshot win : line no : {exc_tb.tb_lineno} {str(e)}"
        insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)


    ele_option=1
    while ele_option>0:
        try:
            First_date=driver.find_element(By.XPATH,f'/html/body/form/div[3]/div/table[2]/tbody/tr/td/div/center/table/tbody/tr[3]/td/table/tbody/tr[2]/td/select/option[{str(ele_option)}]').text
            if  First_date:
                match = re.search(r'^(.*?)\s*\(\d+\)$', First_date)
                if match:
                    name_before_parentheses = match.group(1).strip()   

                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'/html/body/form/div[3]/div/table[2]/tbody/tr/td/div/center/table/tbody/tr[3]/td/table/tbody/tr[2]/td/select/option[{str(ele_option)}]'))).click()
                # time.sleep(1)
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

def vaccine_f(driver, filename1,connection):
    try:
        window_billing1 = driver.window_handles
        driver.switch_to.default_content()
        driver.switch_to.window(window_billing1[0])                
        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="workarea2"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("workarea2")

        element_present1 = EC.presence_of_element_located((By.XPATH, '//*[@id="EMGuildline"]'))
        WebDriverWait(driver, 50).until(element_present1)
        driver.switch_to.frame("EMGuildline")
        ele_option=3
        target_area="Vaccine"
        try: 
            if target_area in driver.find_element(By.XPATH,f'//*[@id="PatientHome"]/table/tbody/tr/td[8]/table/tbody/tr[5]/td/a').text:
                print(driver.find_element(By.XPATH,f'//*[@id="PatientHome"]/table/tbody/tr/td[8]/table/tbody/tr[5]/td/a').text)
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,f'//*[@id="PatientHome"]/table/tbody/tr/td[8]/table/tbody/tr[5]/td/a'))).click()

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
        time.sleep(1)
        driver.switch_to.window(window_after)

        try:

            print_link = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.LINK_TEXT, "Print")))
            print_link.click()
        except:
            try:
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'/html/body/form/div[5]/table/tbody/tr[4]/td'))).click()
            except:
                pass                
        download_directory=ERAPATH
        try:
            wait_for_download2(download_directory)
        except:
            pass

        window_after = driver.window_handles[-1]
        time.sleep(1)
        driver.switch_to.window(window_after)

        time.sleep(2)
        filename = filename1
        move_latest_file(filename)
        # message=f"DOS visit history pdf download completed for {filename} pdf"
        # insert_log(("",datetime.utcnow(),"Info","","","",message,"4","",""),connection) 
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
        insert_log(("",datetime.utcnow(),"ERROR","",filename1,"",message,"4","",""),connection)  
    

def extract_dates(text):
    date_pattern = r'\b(\d{2}/\d{2}/\d{4})\b'
    dates = re.findall(date_pattern, text)
    if not dates:
        return ()
    first_date = dates[0] if len(dates) > 0 else None
    return first_date

 
    
def ExtractAndDownload(site_id,patient_data,driver,connection,download_path,invetory_data,refid,name_before_parentheses,refresh_page,username,credentialmasterid):
    try:
        claim_level_df=pd.DataFrame()
        Service_level_df=pd.DataFrame()
        file_names_dict={}
        file_names1=[] 
        claim_genrate=0
        # name_before_parentheses=""
        window_after_loop = driver.window_handles
        # time.sleep(2)                 
        # driver,name_before_parentheses=select_site(patient_data,driver,site_id,connection,invetory_data)

        a_azur=patient_data.loc[0,"azureblobconnstring"]
        a_azur_1=patient_data.loc[0,"azurecontainername"]
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
                message=f"STEP 3 Claim genration started for patientID : {PatientId}"
                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"4",username,credentialmasterid),connection)                
                starting_window=driver.current_window_handle
                driver.switch_to.window(startnew_window[0])

                # time.sleep(1)  
                driver,claim_genrate= generate_claim_from_waiting(driver,row,PatientId,starting_window,connection,refresh_page)                
                refresh_page=1
                
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
                        EC.presence_of_element_located((By.XPATH, '/html/body/form/div[7]/table/tbody/tr/td[3]/img'))
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
                    print("Second image element not found or clickable ")
                    # exc_type, exc_obj, exc_tb = sys.exc_info()        
                    # message=f"Second image element not found or clickable line no : {exc_tb.tb_lineno} {str(e)}"
                try:
                    re_generate_claim(driver,row,PatientId,starting_window,connection)
                except:
                    pass    
                
                message=f"STEP 4 generate_claim completed for patientID : {PatientId}"

                insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,message,"4",username,credentialmasterid),connection)                

                try:
                    window_after = driver.window_handles
                    handles = driver.window_handles
                    num_tabs = len(handles)
                    if num_tabs==2:
                        driver.switch_to.window(handles[1])
                        driver.close()   

                    driver.switch_to.window(window_after[0])
                    # driver.refresh()

                    # try:
                    #     alert = Alert(driver)
                    #     alert.accept()
                    #     driver.switch_to.alert.accept()
                    # except:
                    #     pass
                    # # time.sleep(3)
                    # try:
                    #     driver.switch_to.alert.accept()
                    #     alert = Alert(driver)
                    #     alert.accept()
                    # except:
                    #     pass
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()        
                    message=f"Extraction error for patientID : {row['PatientId']} line no : {exc_tb.tb_lineno} {str(e)}"
                    insert_log((refid,datetime.utcnow(),'ERROR',site_id,PatientId,inv_data,message,"4",username,credentialmasterid),connection)      
                    continue                
                # insert_log((refid,datetime.utcnow(),'INFO',site_id,PatientId,inv_data,f"Extraction completed for patientID {PatientId}","4",username,credentialmasterid),connection)      
                
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()                        
                insert_log((refid,datetime.utcnow(),'ERROR',site_id,"",invetory_data,f"Extraction error for patientID : {row['PatientId']} line no : {exc_tb.tb_lineno} {str(e)}","4",username,credentialmasterid),connection)      
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
                # time.sleep(2)
                
                # driver.refresh()
                # time.sleep(2)
                # try:
                #     alert = Alert(driver)
                #     alert.accept()
                # except Exception as e:
                #     exc_type, exc_obj, exc_tb = sys.exc_info()        
                #     message=f"Extraction error for patientID : {row['PatientId']} line no : {exc_tb.tb_lineno} {str(e)}"
                #     # insert_log((refid,datetime.utcnow(),'ERROR',site_id,"",invetory_data,message,"4",username,credentialmasterid),connection)      
                #     pass
                # try:
                #     driver.switch_to.alert.accept()
                # except Exception as e:
                #     pass
                # # time.sleep(2)
                # try:
                #     driver.switch_to.alert.accept()
                #     alert = Alert(driver)
                #     alert.accept()
                # except:
                #     pass

                continue


    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info() 
        message=f"extraction line no : {exc_tb.tb_lineno} {str(e)}"    
        insert_log((refid,datetime.utcnow(),'ERROR',site_id,"",invetory_data,message,"4",username,credentialmasterid),connection)          
        # driver.quit()
        raise Exception(message)

    return claim_genrate,refresh_page

def upadate_password(usrid,password,connection):
    try:
        if connection:  
            cursor = connection.cursor()            
            update_query = sql.SQL('''
                            UPDATE icamu.emr_credentialmaster
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
                                    SET claimstatus = %s
                                    WHERE inventoryid IN %s;''')
                    cursor.execute(update_query,(1,tuple(id_bill)))
                    connection.commit() 
        elif type=='process_success':
            if connection:                                           
                cursor = connection.cursor()            
                update_query = sql.SQL('''
                                UPDATE public.tbl_stginventoryuploaddata
                                SET claimstatus = %s
                                WHERE inventoryid IN %s;''')
                try:
                    cursor.execute(update_query,(2,tuple(list(id_bill.keys()))))
                except:
                    cursor.execute(update_query,(2,tuple(id_bill)))
                connection.commit()
        elif type=='process_failed':    
            if connection:                                          
                cursor = connection.cursor()            
                update_query = sql.SQL(''' 
                            UPDATE public.tbl_stginventoryuploaddata
                            SET claimstatus = %s,
                            claimsfailurecount = COALESCE(claimsfailurecount, 0) + 1
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
        insert_log(("",datetime.utcnow(),'INFO',site_id,"",invetory_data,str(data),"4","",""),connection)
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
                    insert_log(("",datetime.utcnow(),'INFO',"","",invetory_data,f"succesfully uploaded file {str(blob_name)}","4","",""),connection)   
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message=f"Exception : file {i} line :{exc_tb.tb_lineno} {str(e)}"
                insert_log(("",datetime.utcnow(),'ERROR',"","",invetory_data,f"Exception : file {i} line :{exc_tb.tb_lineno} {str(e)}","4","",""),connection)   
                pass
            

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message=f"upload_file_to_blob line :{exc_tb.tb_lineno} {str(e)}"
        raise Exception(message)

    
def upload_to_output(connection,data):       
    if connection:                                  
        cursor = connection.cursor()            
        insert_query = sql.SQL('''INSERT INTO public.outputmaster("refID", inventoryid, chartdata, icachartid, status, errorreason, failurecount) VALUES (%s, %s, %s, %s, %s, %s, %s);''')
        cursor.execute(insert_query, data)            
        connection.commit() 



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

def get_login_details(ip_address,connection):
    if connection:
        try:
            puid = str(uuid.uuid4())
            cursor = connection.cursor()


            # Define the query
            update_query = sql.SQL('SELECT * FROM public.fn_get_emr_credentials_concurrent_ip(4,%s);')
            
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
            print(inventory_details)
            
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
        puid = str(uuid.uuid4())
        insert_log(('',datetime.utcnow(),'INFO',"","","",f"step 1 Ip_address {ip_address}","4","",""),connection)

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
                access_type = 4  # e.g., 1 for input
                
                createdby = 1  # assuming the ID of the creator is 1

                if isinstance(credentialmasterid, np.integer):
                    credentialmasterid = int(credentialmasterid)
                    
                insert_log((puid,datetime.utcnow(),'INFO',"","","","step 2 Database connection established","4",username,credentialmasterid),connection)

                update_inventory(credentialmasterid,connection,type="login")
                update_login(connection, clientid_login, subclientid_login, credentialmasterid, puid, type_login, access_type, ip_address, createdby)

                insert_log((puid,datetime.utcnow(),'INFO',"","","","step 3 login click completed","4",username,credentialmasterid),connection)

                a_azur=login_details.loc[0,"azureblobconnstring"]
                a_azur_1=login_details.loc[0,"azurecontainername"]
                Client_1=login_details.loc[0,"clientid"]
                Client_2=login_details.loc[0,"subclientid"]


                if select_site_one_time==0:
                    driver,name_before_parentheses,site_selected_completed=select_site(driver,connection,username,credentialmasterid,ip,login_details,puid)
                    select_site_one_time=select_site_one_time+1
                    
                download_path=download_path
                ERAPATH = ERAPATH
                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"step 4 path of pdf download and upload {ERAPATH}","4",username,credentialmasterid),connection)
                if  site_selected_completed==1:
                    insert_log((puid,datetime.utcnow(),'INFO',"","","","step 5 site window selection completed","4",username,credentialmasterid),connection)
                    

                while True:  
                    if  site_selected_completed==0:
                        insert_log((puid,datetime.utcnow(),'INFO',"","","","site window selection is not completed","4",username,credentialmasterid),connection)
                        insert_log((puid,datetime.utcnow(),'INFO',"","","","otp window found account may be locked pls check","4",username,credentialmasterid),connection)

                        try:
                            window_billing1 = driver.window_handles
                            driver.switch_to.default_content()
                            driver.switch_to.window(window_billing1[0])  
                            screenshot_bytes = driver.get_screenshot_as_png()

                            blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                            container_client = blob_service_client.get_container_client(a_azur_1) 

                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            unique_id = str(uuid.uuid4())

                            file_names_2=f"{username}"
                            file_names_3 = f"screenshot_failed_site_select_{timestamp}_{unique_id}.png"

                            blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

                            content_settings = ContentSettings(content_type="image/png")                
                            container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                            insert_log((puid,datetime.utcnow(),'INFO',"","","",f"succesfully uploaded file {str(blob_name)}","4",username,credentialmasterid),connection)   
                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()        
                            message=f"Extraction error for scrrenshot main window : line no : {exc_tb.tb_lineno} {str(e)}"
                            insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)

                        break


                    connection = create_connection()
        
                    inventory_details=get_inventory_details(puid,credentialmasterid,ip_address,connection) 
                    if len(inventory_details)>0:    
                        insert_log((puid,datetime.utcnow(),'INFO',"","","","STEP 1 Data found for processing","4",username,credentialmasterid),connection)

                        
                        num_siteid=int(inventory_details["divisionid"].nunique())

                        update_inventory(list(inventory_details['id']),connection,type="inprocess")
                        emr_level_data=inventory_details.groupby(by=['emr'])           
                        # insert_log(("",datetime.utcnow(),'INFO',"","","",f"found total {emr_level_data.ngroups} unique emr and total {inventory_details.shape[0]} rows","4",username,credentialmasterid),connection)
                        for grp,emr_data in emr_level_data: 
                            try:            

                                emr_data.reset_index(drop=True,inplace=True)
                                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"STEP 2 Proceesing stated for {emr_data.loc[0,'PatientId']}","4",username,credentialmasterid),connection) 

                                site_level_inventory=emr_data.groupby(by=['SiteId'])  

                                for grp,site_patient_data in site_level_inventory:
                                    try:   
                                        id_bill=[]                    

                                        startnew_window = driver.window_handles
                                        site_id= site_patient_data['SiteId'].values[0]
                                        invetory_data= ' ,'.join(map(str,((site_patient_data['id'].values).tolist())))
                                        refID=str(uuid.uuid1())  

                                        # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"found total {site_patient_data.shape[0]} patient for siteid {site_id}","4",username,credentialmasterid),connection)
                                        site_patient_data["DOS"]=site_patient_data["DOS"].astype(str)  
                                        site_patient_data['PatientId']=site_patient_data['PatientId'].astype(str)
                                        site_patient_data["DOS"]=site_patient_data["DOS"].apply(convert_to_mm_dd_yyyy)
                                        site_patient_data["DOS"]=site_patient_data['DOS'].astype(str)   
                                        patient_id_S= site_patient_data['PatientId'].values[0]

                                        site_patient_data["dob"]=site_patient_data["dob"].astype(str)  

                                        site_patient_data["dob"]=site_patient_data["dob"].apply(convert_to_mm_dd_yyyy)
                                        site_patient_data["dob"]=site_patient_data['dob'].astype(str)  

                                        site_patient_data.fillna('',inplace=True)
                                        site_patient_data.reset_index(inplace=True)                                 
                                        driver.switch_to.window(startnew_window[0])
                                        starting_window2 = driver.window_handles
                                        driver.switch_to.default_content()
                                        driver.switch_to.window(starting_window2[0])
                                        error=False                                  


                                        claim_genrate,refresh_page = ExtractAndDownload(site_id,site_patient_data,driver,connection,download_path,invetory_data,refID,name_before_parentheses,refresh_page,username,credentialmasterid)
                                        # if claim_genrate ==1:
                                        #     insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"claim genration completed for patient id : {patient_id_S}","4",username,credentialmasterid),connection)                         
                                        # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"Json convertion process started for site id : {site_id}"  ,"4",username,credentialmasterid),connection)              
                                        # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"PDF upload process started for site id : {site_id}","4",username,credentialmasterid),connection)                                   

                                        
                                        # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"PDF upload process Completed for site id : {site_id}","4",username,credentialmasterid),connection)                                                                     
                                        api_Count=0
                                        # if len(json_string)>10 :
                                        #     # insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"API call started for site id : {site_id}","4",username,credentialmasterid),connection)
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
                                        #         insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,api_status,"4",username,credentialmasterid),connection)   
                                        #     except:
                                        #         pass
                                        #     insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"API call process completed for site id : {site_id}","4",username,credentialmasterid),connection)

                                        # else:
                                        #     insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"No Data found for SiteId : {site_id}","4",username,credentialmasterid),connection)
                                        if api_Count==0 and claim_genrate==1:
                                            insert_log((refID,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEP 5 claim genration completed successfully for patient id : {patient_id_S}","4",username,credentialmasterid),connection)                         

                                            ID_new=list(set(site_patient_data["id"])-set(id_bill))                        

                                            update_inventory(ID_new,connection,type="process_success")
                                            
                                        # else:
                                        #     failed_id=list(set(site_patient_data["id"])-set(id_bill))                        
                                        #     if len(failed_id)>0:
                                        #         update_inventory(failed_id,connection,type="process_failed")
                                        # print("remove_file start")
                                        # try:
                                        #     if file_names_dict:
                                        #         for key, value in file_names_dict.items():
                                        #             file_names1=value
                                        #             for file in file_names1:
                                        #                 remove_file(download_path,file)
                                        # except Exception as e:
                                        #     exc_type, exc_obj, exc_tb = sys.exc_info()                        
                                        #     insert_log((refID,datetime.utcnow(),"ERROR","","",invetory_data,"line : "+str(exc_tb.tb_lineno)+" " +str(e)  ,"4",username,credentialmasterid),connection)
                                        # time.sleep(2)
                                    
                                    except Exception as e:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()                        
                                        insert_log((refID,datetime.utcnow(),"ERROR","","",invetory_data,"line : "+str(exc_tb.tb_lineno)+" " +str(e)  ,"4",username,credentialmasterid),connection)
                                    finally:
                                        failed_id=list(set(site_patient_data["id"])-set(id_bill))                        
                                        if claim_genrate==0:
                                            update_inventory(failed_id,connection,type="process_failed")
                                            refresh_page=0
                                            insert_log((puid,datetime.utcnow(),'INFO',site_id,"",invetory_data,f"STEP 5 processing failed for patient id {patient_id_S}","4",username,credentialmasterid),connection)                       

                                            try:
                                                window_billing1 = driver.window_handles
                                                driver.switch_to.default_content()
                                                driver.switch_to.window(window_billing1[0])  
                                                screenshot_bytes = driver.get_screenshot_as_png()

                                                blob_service_client = BlobServiceClient.from_connection_string(a_azur)
                                                container_client = blob_service_client.get_container_client(a_azur_1) 

                                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                                unique_id = str(uuid.uuid4())

                                                file_names_2=f"account_failed"
                                                file_names_3 = f"screenshot_failed_{patient_id_S}_{timestamp}_{unique_id}.png"

                                                blob_name = "ica2.0/"+str(Client_1)+"/"+str(Client_2)+"/2/"+str(file_names_2)+"/"+"site_id"+"/"+str(file_names_3)

                                                content_settings = ContentSettings(content_type="image/png")                
                                                container_client.upload_blob(name=blob_name, data=screenshot_bytes,content_settings=content_settings,overwrite=True) 
                                                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"STEP 5 succesfully uploaded file for {patient_id_S} path {str(blob_name)}","4",username,credentialmasterid),connection)   
                                            except Exception as e:
                                                exc_type, exc_obj, exc_tb = sys.exc_info()        
                                                message=f"Extraction error for scrrenshot main window : line no : {exc_tb.tb_lineno} {str(e)}"
                                                insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)

                                            
                                            window_after = driver.window_handles
                                            handles = driver.window_handles
                                            num_tabs = len(handles)
                                            if num_tabs==2:
                                                driver.switch_to.window(handles[1])
                                                driver.close()   

                                            driver.switch_to.window(window_after[0])
                                            driver.refresh()

                                            try:
                                                alert = Alert(driver)
                                                alert.accept()
                                                driver.switch_to.alert.accept()
                                            except:
                                                pass
                                            # time.sleep(3)
                                            try:
                                                driver.switch_to.alert.accept()
                                                alert = Alert(driver)
                                                alert.accept()
                                            except:
                                                pass

                                insert_log((puid,datetime.utcnow(),'INFO',"","","",f"STEP 6  ALL Proceesing completed for {emr_data.loc[0,'PatientId']}","4",username,credentialmasterid),connection) 
                            except Exception as e:
                                    exc_type, exc_obj, exc_tb = sys.exc_info()                    
                                    insert_log((puid,datetime.utcnow(),"ERROR","","","","line : "+str(exc_tb.tb_lineno)+" " +str(e)  ,"4",username,credentialmasterid),connection)


                    if len(inventory_details)==0:
                        break


            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message = "line :"+str(exc_tb.tb_lineno)+" " +str(e)  
                insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",username,credentialmasterid),connection)  
            finally:
            
                try:
                    # time.sleep(2)
                    driver.switch_to.alert.accept()
                    alert = Alert(driver)
                    alert.accept()
                except:
                    pass
                try:
                    # time.sleep(1)
                    driver.switch_to.alert.accept()
                    alert = Alert(driver)
                    alert.accept()
                except:
                    pass
                type_login = 2  # e.g., 1 for login
                access_type = 4 # e.g., 1 for input
                update_login(connection, clientid_login, subclientid_login, credentialmasterid, puid, type_login, access_type, ip_address, createdby)
                update_inventory(credentialmasterid,connection,type="logout")
                try:
                    logout_button = driver.find_element(By.XPATH, '//*[@id="searchBarDiv"]/table/tbody/tr/td[3]/span')
                    logout_button.click()
                    insert_log((puid,datetime.utcnow(),'INFO',"","","","steps 5 logout click completed","4",username,credentialmasterid),connection)
                except Exception as e:

                    exc_type,exc_obj,exc_tb=sys.exc_info()
                    message='log out click is not working line :'+str(exc_tb.tb_lineno)+" "+str(e)
                    insert_log((puid,datetime.utcnow(),'INFO',"","","",message,"4",username,credentialmasterid),connection)


                try:
                    alert = Alert(driver)
                    # Accept the alert (to simulate clicking "Leave")
                    alert.accept()
                except:
                    pass
                insert_log((puid,datetime.utcnow(),'INFO',"","","","steps 6 browser close completed","4",username,credentialmasterid),connection)

                connection.close() 
                for driver in drivers:
                    driver.quit()
                    break
        


        else:
            insert_log((puid,datetime.utcnow(),'INFO',"","","",f"step 2 Ip_address {ip_address} no data found","4","",""),connection)    
            insert_log((puid,datetime.utcnow(),'INFO',"","","","step 3 browser close completed","4","",""),connection)    
            connection.close()
            for driver in drivers:
                driver.quit()
                break 
            
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message = "line :"+str(exc_tb.tb_lineno)+" " +str(e)  
        insert_log((puid,datetime.utcnow(),"ERROR","","","",message,"4",'',""),connection)