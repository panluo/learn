import json, requests
#get local ip
ip_path = 'http://httpbin.org/ip'

#this is a free api, get your own key if you wish to use it extensively
country_path = 'http://api.ipinfodb.com/v3/ip-country/?key=5d3d0cdbc95df34b9db4a7b4fb754e738bce4ac914ca8909ace8d3ece39cee3b&ip%s'

def public_ip():
    req = requests.get(ip_path)

    if req.status_code == 200:
        text = json.loads(req.text)
        return text['origin']

def country_by_ip(ip):

    req = requests.get(country_path % ip)

    if req.status_code == 200:
        split = req.text.split(';')
        print split
        return split[-1]
if __name__ == "__main__":
    ip = public_ip()
    print("IP : %s, Country : %s" %("36.48.26.188",country_by_ip(ip)))
