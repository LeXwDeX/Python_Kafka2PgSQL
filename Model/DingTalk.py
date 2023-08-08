import json
import logging
import requests
import time
import hmac
import hashlib
import base64
import urllib.parse


class DingTalkNotifier:
    def __init__(self, config):
        self.base_url = config['url']
        self.access_token = config['access_token']
        self.secret = config['token']
        self.headers = {'Content-Type': 'application/json'}
    
    def _generate_url(self):
        """生成带有timestamp和sign的URL"""
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = f'{timestamp}\n{self.secret}'
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return f'{self.base_url}?access_token={self.access_token}&timestamp={timestamp}&sign={sign}'
    
    def send_message(self, message):
        """发送文本消息"""
        url = self._generate_url()
        payload = {
                "msgtype": "text",
                "text": {
                        "content": message
                }
        }
        try:
            response = requests.post(url, headers=self.headers, data=json.dumps(payload))
            if response.status_code != 200:
                raise Exception(f"Request to DingTalk API failed with status {response.status_code}: {response.text}")
            return response.json()
        except Exception as e:
            logging.error(f"钉钉机器人发送失败： {e}")
            print(f"钉钉机器人发送失败： {e}")
