import json 
import os 
import sys
import ast

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
        # insert_log(("",datetime.utcnow(),'INFO',site_id,"",invetory_data,str(data),"1","",""),connection)
        # update_inventory(id_bill,connection,type="billing")
        return json.dumps(data),id_bill
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        message=f"Exception : json_converter line :{exc_tb.tb_lineno} {str(e)}"
        raise Exception(message)

# Service_level_dataframe = 
# claim_level_dataframe = 
# refID =
# connection = 
# ClientId = 
# SubclientId = 
# invetory_data = 



# json_string,id_bill=json_converter(Service_level_dataframe,claim_level_dataframe,refID,connection,site_patient_data.loc[0,"ClientId"],site_patient_data.loc[0,"SubClientId"],invetory_data)








# conda create -n myenv python=3.10

# import ssl
# import socket

# hostname = 'your.blob.core.windows.net'
# context = ssl.create_default_context()

# # with socket.create_connection((hostname, 443)) as sock:
# #     with context.wrap_socket(sock, server_hostname=hostname) as ssock:
# #         print(ssock.version())


# import os
# import sys
# from datetime import datetime
# import certifi
# from azure.storage.blob import BlobServiceClient, ContentSettings
# from azure.core.pipeline.transport import RequestsTransport




# def upload_file_to_blob(storage_connection_string, container_name, local_pdf_path, clientid, subclientid, download_path, file_names_1, invetory_data):
#     try:
#         # Create a transport with certifi's CA bundle to avoid SSL issues
#         transport = RequestsTransport(certificate_path=certifi.where())
        
#         # Initialize BlobServiceClient with custom transport
#         blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string, transport=transport)
#         container_client = blob_service_client.get_container_client(container_name)

#         for i in local_pdf_path:
#             try:
#                 split_name = i.split("_")
#                 blob_name = f"ica2.0/{clientid}/{subclientid}/2/{file_names_1}/{split_name[0]}/{i}"
                
#                 file_path = os.path.join(download_path, i)
#                 with open(file_path, "rb") as data:
#                     file_path = os.path.join(download_path, i)  # i is filename

#                     content_settings = ContentSettings(content_type='application/pdf')
#                     container_client.upload_blob(name=blob_name, data=data, content_settings=content_settings, overwrite=True)

#             except Exception as e:
#                 exc_type, exc_obj, exc_tb = sys.exc_info()
#                 message = f"Exception: file {i} line: {exc_tb.tb_lineno} {str(e)}"

#                 # Optionally, you can raise here or continue
#                 pass

#     except Exception as e:
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         message = f"upload_file_to_blob line: {exc_tb.tb_lineno} {str(e)}"
#         raise Exception(message)



# azureblobconn='DefaultEndpointsProtocol=https;AccountName=ica2storage;AccountKey=FXoy7HSWurqC6AwV6tCIriYBMwfSXHjiNIkKV5e/Ctmc6XdDwSRwjAorv0WT8e5qkWpC092bVtMB+ASt5i238A==;EndpointSuffix=core.windows.net'
# conatiner='ica-uat'
# file_names1= ['mr_1124730231_09202025.pdf']
# clientid=3
# subclientid=3
# download_path = r'C:\Users\nrahate\Documents\KT_Code_Rendr_care\Input Bot\Config\Data\b75df7f8-43bb-49ff-b4c5-bf56e245ee8a'
# file_names_1 = '1124730231_09202025'
# invetory_data = '72293'


# upload_file_to_blob(azureblobconn, conatiner,
#     file_names1, clientid,subclientid,download_path,file_names_1,invetory_data)  







































