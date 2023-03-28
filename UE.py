#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import websocket
import json

future_ws = f'wss://fstream.binance.com/ws/!ticker@arr'

def on_message(ws,message):
    json_msg = json.loads(message)
    print(json_msg)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print('connection end')

ws = websocket.WebSocketApp(future_ws,
    on_message=on_message,
    on_close=on_close,
    on_error=on_error)
ws.run_forever()

