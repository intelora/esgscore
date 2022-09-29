import time
import requests
import json
from requests.structures import CaseInsensitiveDict

csvfilename = "finalscore.csv"
serverurl = "https://adb-6734083905565598.18.azuredatabricks.net"

def step1():
    url = serverurl + "/api/2.1/jobs/run-now"
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer dapi1c03de216441655be7b44c248738a09d"
    headers["Content-Type"] = "application/json"

    data = """
    {
        "job_id": 168756754981601,
        "notebook_params": {
        "myinput": "apollo type; apollo pharma",
        "startdate": "2022-05-04",
        "enddate": "2022-05-05"
    }
    }

    """

    resp = requests.post(url, headers=headers, data=data)
    runid=resp.json()
    runid1 = runid["run_id"]
    # print(resp.text)
    # print( run_id)
    print(f"Run_id1:{runid1}")
    return runid1


def step2(runid):
    url = serverurl +  f"/api/2.1/jobs/runs/get?run_id={runid}"
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer dapi1c03de216441655be7b44c248738a09d"
    resp = requests.get(url, headers=headers)
    runid=resp.json()["tasks"][0]["run_id"]
    # print(resp.text)
    print(f"Run_id2:{runid}")
    return runid


def step3(runid):
    url = serverurl + f"/api/2.1/jobs/runs/get-output?run_id={runid}"
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer dapi1c03de216441655be7b44c248738a09d"
    resp = requests.get(url, headers=headers)
    esgscore=resp.json()
    # print(resp.text)
    # if esgscore["notebook_output"]=="notebook_outbook":
    #     # runid3 = esgscore["notebook_outbook"]
    #     print(f"ESGSCORE:{notebook_outbook}")
    # elif esgscore["error"]=="error":
    #     print(f"error:{error}")
    # else:
    #     print("not_output")    
    return esgscore 


def writedataintocsv(esgscoredata, csvfilename, inputCompanynames):
    #print("data====", esgscoredata)
    esgscoredata = esgscoredata.replace("None", "\"None\"")
    jsondata = json.loads(esgscoredata.replace("'", "\""))
    inputCompanynamelist = inputCompanynames.split(";")

    allkeys  = list(jsondata["E"].keys())
    f = open(csvfilename, "w")
    print("writing data into file.")
    f.write("company_name, E, S, G")
    for singleKey in allkeys:
        singleKeylabel = singleKey
        if(singleKey!="industry_tone"):
            singleKeylabel = singleKey.replace("_tone", "")
            inputCompanynamelist.remove(singleKeylabel)

        filedata = ("\n"+singleKeylabel+","+str(jsondata["E"][singleKey])+", "+str(jsondata["S"][singleKey])+", "+str(jsondata["G"][singleKey]))
        f.write(filedata)

    if(len(inputCompanynamelist)>0):
        for remainCompanyname in inputCompanynamelist:
            print("\n=========================\nData Not Avilable, so System not able to calculate ESG for \""+remainCompanyname+"\" ")
            f.write("\n"+remainCompanyname+",DataNotAvilable,DataNotAvilable,DataNotAvilable" )

    f.close()


#new id = 148303,  149594, with 3 comp 150979, non wala id 
rundiFromserver = step1()
taskidFromserver = step2(rundiFromserver)
print(taskidFromserver)
esgscoredata =  step3(taskidFromserver)
print("raw response from server", esgscoredata)
while(esgscoredata["metadata"]["state"]["life_cycle_state"]== "PENDING" or esgscoredata["metadata"]["state"]["life_cycle_state"]=="RUNNING"):
    esgscoredata = step3(taskidFromserver)
    print("Task is "+esgscoredata["metadata"]["state"]["life_cycle_state"]+" at server so system will try again after 3 min")
    time.sleep(180)

inputCompanyname = esgscoredata["metadata"]["overriding_parameters"]["notebook_params"]["myinput"]
print("request input company names = ", inputCompanyname)

print("\n\ngot ESG score response from server")
print("\nconfirmed server response", esgscoredata)
writedataintocsv(esgscoredata["notebook_output"]["result"], csvfilename, inputCompanyname)

