import requests
from requests.models import StreamConsumedError
from requests.exceptions import Timeout

teleURL = 'http://192.168.4.120:1444/api/transformer/sendNotificationToTelegramGroup'
messages = "Test"
pload = {'message':messages}
r = requests.post(teleURL, data = pload, timeout = 5, verify = False)