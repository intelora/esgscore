import time
import requests
import json
from requests.structures import CaseInsensitiveDict

csvfilename = "finalscore.csv"

def step1():
    url = "https://adb-6734083905565598.18.azuredatabricks.net/api/2.1/jobs/run-now"

    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer dapi1c03de216441655be7b44c248738a09d"
    headers["Content-Type"] = "application/json"

    data = """
    {
        "job_id": 168756754981601,
        "notebook_params": {
        "myinput": "reliance;apple;ibm",
        "startdate": "2022-05-02",
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
    url = f"https://adb-6734083905565598.18.azuredatabricks.net/api/2.1/jobs/runs/get?run_id={runid}"
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer dapi1c03de216441655be7b44c248738a09d"
    resp = requests.get(url, headers=headers)
    runid=resp.json()["tasks"][0]["run_id"]
    # print(resp.text)
    print(f"Run_id2:{runid}")
    return runid


def step3(runid):
    url = f"https://adb-6734083905565598.18.azuredatabricks.net/api/2.1/jobs/runs/get-output?run_id={runid}"
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


def writedataintocsv(esgscoredata, csvfilename):
    jsondata = json.loads(esgscoredata.replace("'", "\""))
    allkeys  = list(jsondata["E"].keys())
    f = open(csvfilename, "w")
    f.write("company_name, E, S, G")
    for singleKey in allkeys:
        singleKeylabel = singleKey
        if(singleKey!="industry_tone"):
            singleKeylabel = singleKey.replace("_tone", "")
        filedata = ("\n"+singleKeylabel+","+str(jsondata["E"][singleKey])+", "+str(jsondata["S"][singleKey])+", "+str(jsondata["G"][singleKey]))
        f.write(filedata)
    f.close()




#new id = 148303, then 149594, then 150979
#rundiFromserver = step1()
taskidFromserver = 150979 #step2(rundiFromserver)
print(taskidFromserver)
esgscoredata =  step3(taskidFromserver)
print(esgscoredata)
print(">>>>>>>>>>>>>")
while(esgscoredata["metadata"]["state"]["life_cycle_state"]== "PENDING" or esgscoredata["metadata"]["state"]["life_cycle_state"]=="RUNNING"):
    esgscoredata = step3(taskidFromserver)
    print("trying again after 3 min")
    time.sleep(180)
print(esgscoredata["notebook_output"])
writedataintocsv(esgscoredata["notebook_output"]["result"], csvfilename)

