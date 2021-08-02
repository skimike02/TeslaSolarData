import os
import base64
import hashlib
import requests
import time
from lxml import html
from urllib.parse import parse_qs


CLIENT_ID = "81527cff06843c8634fdc09e8ac0abefb46ac849f38fe1e431c2ef2106796384"
UA = "Mozilla/5.0 (Linux; Android 10; Pixel 3 Build/QQ2A.200305.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/85.0.4183.81 Mobile Safari/537.36"
X_TESLA_USER_AGENT = "TeslaApp/3.10.9-433/adff2e065/android/10"
authorize_url="https://auth.tesla.com/oauth2/v3/authorize"
token_url="https://auth.tesla.com/oauth2/v3/token"

def handshake():
    verifier_bytes = os.urandom(86)
    challenge = base64.urlsafe_b64encode(verifier_bytes).rstrip(b'=')
    challenge_bytes = hashlib.sha256(challenge).digest()
    challenge_sum = base64.urlsafe_b64encode(challenge_bytes).rstrip(b'=')
    state=base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode("utf-8")
    return challenge, challenge_sum, state

def authenticate(email,pw):
    challenge, challenge_sum, state=handshake()
    
    MAX_ATTEMPTS = 10

    headers = {
        "User-Agent": UA,
        "x-tesla-user-agent": X_TESLA_USER_AGENT,
        "X-Requested-With": "com.teslamotors.tesla",
    }
    
    headers = {
    "x-tesla-user-agent": X_TESLA_USER_AGENT,
    "X-Requested-With": "com.teslamotors.tesla",
    }

    params={
        'client_id':'ownerapi',
        'code_challenge':challenge_sum,
        'code_challenge_method':'S256',
        'redirect_uri':'https://auth.tesla.com/void/callback',
        'response_type':'code',
        'scope':'openid email offline_access',
        'state':state}
    
    #Establish the session
    s=requests.Session()
    for i in range(MAX_ATTEMPTS):
        r=s.get(authorize_url,headers=headers,params=params,timeout=10)
        if r.ok and "<title>" in r.text:
            print(f"GET authorization form successful after {i + 1} attempt(s).")
            if "Enter the characters in the picture" in r.text:
                captcha=True
                captcha = s.get('https://auth.tesla.com/captcha', headers=headers)
                file = open("captcha.svg", "wb")
                file.write(captcha.content)
                file.close()
                print(f"captcha.svg saved to {os.path.abspath(os.getcwd())}")
                print("please provide the captcha text:")
                captcha_text=input()
            else:
                print("skipping captcha")
            break
        time.sleep(3)
    else:
        raise ValueError(f"GET authorization form unsuccessful after {MAX_ATTEMPTS} attempts.")
                
    data = {
        "identity": email,
        "credential": pw
    }
    
    if captcha:
        data["captcha"]=captcha_text
    
    tree=html.fromstring(r.content)
    for i in tree.xpath('//input'):
        if i.name not in data.keys() and i.type=='hidden':
            data[i.name]=i.value
            
    #Send email and pw
    for i in range(MAX_ATTEMPTS):
        r=s.post(authorize_url, headers=headers, data=data, params=params, allow_redirects=False)
        if r.ok and (r.status_code == 302 or "<title>" in r.text):
            print(f"POST authorization form successful after {i + 1} attempt(s).")
            break

        time.sleep(3)
    else:
        raise ValueError(f"POST authorization form unsuccessful after {MAX_ATTEMPTS} attempts.")
        
    #determine if MFA needed
    if r.status_code == 200 and "/mfa/verify" in r.text :
        mfa = True 
    else:
        mfa = False
    
    if mfa==True:
        mfa_uri='https://auth.tesla.com/oauth2/v3/authorize/mfa/factors'
        transaction_ID=data['transaction_id']
        r=s.get(mfa_uri, headers=headers, params={'transaction_id':transaction_ID})
        
        #Select MFA device if multiple exist:
        if len(r.json()['data'])>1:   
            print("Multiple MFA devices found. Please select which MFA device you will use.")
            n=1
            for i in r.json()['data']:
                print(n,"-",i['name'])
                n=n+1
            device=int(input())-1
        else:
            device=0
        factor_id=r.json()['data'][device]['id']
        
        #Send passcode
        for i in range(3):
            print('Please enter your passcode:')
            passcode=input()
            r = s.post("https://auth.tesla.com/oauth2/v3/authorize/mfa/verify",
                          headers=headers,
                          json={"transaction_id": transaction_ID, "factor_id": factor_id, "passcode": passcode})
            if r.json()['data']['approved'] and r.json()['data']['valid'] :
                print('passcode approved')
                break
        else:
            raise ValueError("Incorrect passcode entered")
        
        #Get code
        for attempt in range(MAX_ATTEMPTS):
            r = s.post(authorize_url, headers=headers, params=params, data=data, allow_redirects=False)
            if r.headers.get("location"):
                print(f"Got location in {attempt + 1} attempt(s).")
                break
            time.sleep(3)
        else:
            raise ValueError(f"Didn't get location in {MAX_ATTEMPTS} attempts.")
        
    else:
        print('No MFA detected')
    code = parse_qs(r.headers["location"])["https://auth.tesla.com/void/callback?code"]
    #Get access token
    payload = {
    "grant_type": "authorization_code",
    "client_id": "ownerapi",
    "code_verifier": challenge.decode("utf-8"),
    "code": code,
    "redirect_uri": "https://auth.tesla.com/void/callback",
        }
    
    r = s.post(token_url, headers=headers, json=payload)
    access_token = r.json()["access_token"]
    refresh_token=r.json()["refresh_token"]
            
    #Provide access token to get long-lived token
    headers["authorization"] = "bearer " + access_token
    payload = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "client_id": CLIENT_ID,
    }
    r = s.post("https://owner-api.teslamotors.com/oauth/token", headers=headers, json=payload)
    resp=r.json()
    resp['refresh_token']=refresh_token #refresh token needs to be from auth.tesla.com. owner-api refresh token is not used.
    return resp

def refresh(refresh_token):
    headers = {
    
        "x-tesla-user-agent": X_TESLA_USER_AGENT,
        "X-Requested-With": "com.teslamotors.tesla"}
    
    payload = {
    "grant_type": "refresh_token",
    "client_id": "ownerapi",
    "refresh_token": refresh_token,
    "scope": 'openid email offline_access'
        }
    r = requests.post(token_url, headers=headers, json=payload)
    access_token = r.json()["access_token"]
    refresh_token=r.json()["refresh_token"]
            
    #Provide access token to get long-lived token
    headers["authorization"] = "bearer " + access_token
    payload = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "client_id": CLIENT_ID,
    }
    r = requests.post("https://owner-api.teslamotors.com/oauth/token", headers=headers, json=payload)
    resp=r.json()
    resp['refresh_token']=refresh_token
    return resp    
