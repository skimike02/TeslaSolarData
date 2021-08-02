import requests
import json
import datetime
import pytz
from pytz import timezone


def GetProducts(access_token):
    url="https://owner-api.teslamotors.com/api/1/products"
    headers = {"Authorization": "Bearer "+access_token}
    payload={}
    r = requests.get(url, data=json.dumps(payload), headers=headers)
    if r.status_code!=200:
        a="Error: http response "+str(r.status_code)+" "+r.reason
        return a
    else:
        a=json.loads(r.content.decode('utf-8'))
        response=a['response']
        return response
    
def GetBatteries(access_token):
    battery_ids=[]
    products=GetProducts(access_token)
    for product in products:
         if 'energy_site_id' in product:
             battery_ids.append(product['id'])
    return battery_ids
    
def GetSolarHistory(access_token,site_id,date):
    url=f"https://owner-api.teslamotors.com/api/1/energy_sites/{site_id}/calendar_history"
    headers = {"Authorization": "Bearer "+access_token}
    payload={}
    kind='power'
    period='day'
    loc_dt=timezone('US/Pacific').localize(date-datetime.timedelta(minutes=1)+datetime.timedelta(days=1))
    end_date=loc_dt.astimezone(pytz.utc).isoformat()[:-6]
    #end_date = date.replace(hour=7, minute=59, second=59)+datetime.timedelta(days=1)
    #end_date = end_date.isoformat()
    params={'kind':kind,'period': period}
    params['end_date']=end_date+'Z'
    r = requests.get(url, data=json.dumps(payload), headers=headers, params=params)
    if r.status_code!=200:
        a="Error: http response "+str(r.status_code)+" "+r.reason
    else:
        a=json.loads(r.content.decode('utf-8'))
    return a['response']

def GetLiveStatus(access_token,site_id):
    url=f"https://owner-api.teslamotors.com/api/1/energy_sites/{site_id}/live_status"
    headers = {"Authorization": "Bearer "+access_token}
    payload=[]
    r = requests.get(url, data=json.dumps(payload), headers=headers)
    if r.status_code!=200:
        a="Error: http response "+str(r.status_code)+" "+r.reason
    else:
        a=json.loads(r.content.decode('utf-8'))
    return a['response']

def GetStatus(access_token,battery_id):
    url=f"https://owner-api.teslamotors.com/api/1/powerwalls/{battery_id}"
    headers = {"Authorization": "Bearer "+access_token}
    payload=[]
    r = requests.get(url, data=json.dumps(payload), headers=headers)
    if r.status_code!=200:
        a="Error: http response "+str(r.status_code)+" "+r.reason
    else:
        a=json.loads(r.content.decode('utf-8'))
    return a['response']

#for battery_id in GetBatteries(access_token):
#    print (GetStatus(access_token,battery_id))
    
def SetStatus(access_token,site_id,mode):
    url=f"https://owner-api.teslamotors.com/api/1/energy_sites/{site_id}/operation"
    UA = "Mozilla/5.0 (Linux; Android 10; Pixel 3 Build/QQ2A.200305.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/85.0.4183.81 Mobile Safari/537.36"
    X_TESLA_USER_AGENT = "TeslaApp/3.10.9-433/adff2e065/android/10"
    headers = {
        "User-Agent": UA,
        "x-tesla-user-agent": X_TESLA_USER_AGENT,
        "X-Requested-With": "com.teslamotors.tesla",
        "Authorization": "Bearer "+access_token}
    payload={"default_real_mode": mode}
    r = requests.post(url, json=payload, headers=headers)
    return r