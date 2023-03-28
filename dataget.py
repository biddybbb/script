#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import aiohttp
import time
import asyncio
import zlib
import json
import zlib
import hashlib
import hmac
import base64
import traceback
import random, csv, sys
import logging, logging.handlers

def empty_call(msg):
    pass

URL = 'wss://fstream.binance.com/ws/'

def inflate(data):
    '''
        解压缩数据
    '''
    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

class BinanceCoinSwapWs:

    def __init__(self, params, is_print=0):
        self.params = params
        self.exchange = ''
        self.coin = ''
        self.symbol = 'cfxusdt'
        self.data = dict()
        self.data['trade'] = []
        self.data['force'] = []
        self.data['position'] = {"longPos":0, "shortPos":0, "longAvg":0, "shortAvg":0}
        self.callback = {
            "onMarket":self.save_market,
            "onPosition":empty_call,
            "onAccount":empty_call,
            "onEquity":empty_call,
            "onOrder":empty_call,
            "onTicker":empty_call,
            }
        self.depth_update = []
        self.need_flash = 1
        self.updata_u = None
        self.last_update_id = None
        self.depth = dict()
        self.depth['bids'] = dict()
        self.depth['asks'] = dict()
        self.bbo = dict()
        self.bbo['bp'] = None
        self.bbo['ap'] = None
        self.bbo['bv'] = None
        self.bbo['av'] = None
        self.is_print = is_print
        self.proxy = None
        if 'win' in sys.platform:
            self.proxy = "http://127.0.0.1:7890"
        self.logger = self.get_logger()
    
    def get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        # log to txt
        formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
        handler = logging.handlers.RotatingFileHandler(f"log.log",maxBytes=1024*1024,backupCount=10)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def save_market(self, msg):
        #date = time.strftime('%Y-%m-%d',time.localtime())
        date='2023-02-17'
        if msg:
            if len(msg['data']) > 1:
                with open(f'./history/{self.symbol}_{date}.csv',
                            'a',
                            newline='',
                            encoding='utf-8') as f:
                    writer = csv.writer(f, delimiter=',')
                    writer.writerow(msg['data'])
                if self.is_print:print(f'写入行情 {self.symbol}')

    async def get_sign(self):
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['X-MBX-APIKEY'] = self.params['access_key']
        params = {
            'timestamp':int(time.time())*1000,
            'recvWindow':5000,
        }
        query_string = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
        signature = hmac.new(self.params['secret_key'].encode(), msg=query_string.encode(), digestmod=hashlib.sha256).hexdigest()
        params['signature']=signature
        url = 'https://dapi.binance.com/dapi/v1/listenKey'
        session = aiohttp.ClientSession()
        response = await session.post(
            url, 
            params=params,
            headers=headers, 
            timeout=5, 
            proxy=self.proxy
            )
        login_str = await response.text()
        await session.close()
        return eval(login_str)['listenKey']

    async def get_depth_flash(self):
        headers = {}
        headers['Content-Type'] = 'application/json'
        url = f'https://fapi.binance.com/fapi/v1/depth?symbol={self.symbol}&limit=1000'
        session = aiohttp.ClientSession()
        response = await session.get(
            url, 
            headers=headers, 
            timeout=5, 
            proxy=self.proxy
            )
        depth_flash = await response.text()
        await session.close()
        return eval(depth_flash)

    def _update_depth(self, msg):
        msg = eval(msg)
        self.depth_update.append(msg)
        if self.need_flash == 0: # 可以更新深度
            for i in self.depth_update[:]:
                if i['u'] > self.last_update_id: # 后续更新本地副本
                    # 开始更新深度
                    for j in i['b']:
                        price = float(j[0])
                        amount = float(j[1])
                        if amount > 0:
                            self.depth['bids'][price] = amount 
                        else:
                            if price in self.depth['bids']:del(self.depth['bids'][price])
                    for j in i['a']:
                        price = float(j[0])
                        amount = float(j[1])
                        if amount > 0:
                            self.depth['asks'][price] = amount 
                        else:
                            if price in self.depth['asks']:del(self.depth['asks'][price])
                    self.last_update_id = i['u']
                    # print(f'更新深度{self.last_update_id}')
                    self.depth_update.remove(i)
            self.data['depth'] = []
            buyP = list(self.depth['bids'].keys())
            buyP.sort(reverse=True) # 从大到小
            sellP = list(self.depth['asks'].keys())
            sellP.sort(reverse=False) # 从小到大
            level = 100
            for i in range(level):self.data['depth'].append(float(buyP[i]))
            for i in range(level):self.data['depth'].append(float(self.depth['bids'][buyP[i]]))
            for i in range(level):self.data['depth'].append(float(sellP[i]))
            for i in range(level):self.data['depth'].append(float(self.depth['asks'][sellP[i]]))


    def _update_ticker(self, msg):
        msg = eval(msg)
        bp = float(msg['b'])
        ap = float(msg['a'])
        mp = (bp + ap)/2
        self.bbo['bp'] = bp
        self.bbo['ap'] = ap
        self.bbo['bv'] = float(msg['B'])
        self.bbo['av'] = float(msg['A'])
        self.callback['onTicker']({"exchange":'binancecoinswap','mp':mp,'bp':bp,'ap':ap})

    def _update_depth20(self, msg):
        msg = eval(msg)
        self.data['depth'] = []
        level = 20
        for i in range(level):self.data['depth'].append(float(msg['b'][i][0]))
        for i in range(level):self.data['depth'].append(float(msg['b'][i][1]))
        for i in range(level):self.data['depth'].append(float(msg['a'][i][0]))
        for i in range(level):self.data['depth'].append(float(msg['a'][i][1]))
        mp = self.data['depth'][0]/2 + self.data['depth'][40]/2
        self.callback['onTicker']({"exchange":'binancecoinswap','mp':mp})
    
    def _update_trade(self, msg):
        msg = json.loads(msg)
        self.data['trade'].append([
            'sell' if msg['m'] else 'buy',
            float(msg['q']),
            float(msg['p']),
            ])

    def _update_force(self, msg):
        msg = json.loads(msg)
        self.data['force'].append([
            'buy' if msg['o']['S'].lower() == 'buy' else 'sell',
            float(msg['o']['q']),
            float(msg['o']['p']),
            ])

    def _update_account(self, msg):
        msg = eval(msg)
        for i in msg['a']['B']:
            if i['a'].lower() == self.coin.lower():
                self.data['equity'] = float(i['wb'])
        self.callback['onEquity'](self.data['equity'])
    
    def _update_order(self, msg):
        msg = json.loads(msg)
        i = msg['o']
        if i['s'] == self.params['symbol']:
            if i['X'] == 'NEW':  # 新增订单
                pass
                # self.callback['onOrder']({"newOrder":newOrder})
            if i['X'] == 'FILLED':  # 删除订单
                self.callback['onOrder']({"deleteOrder":i['i']})
            if i['X'] == 'CANCELED':  # 删除订单
                self.callback['onOrder']({"deleteOrder":i['i']})

    def _update_position(self, msg):
        long_pos, short_pos = 0, 0
        long_avg, short_avg = 0, 0 
        msg = eval(msg)
        for i in msg['a']['P']:
            if i['s'] == self.symbol:
                if i['ps'] == 'LONG':
                    long_pos += abs(float(i['pa']))
                    long_avg = abs(float(i['ep']))
                if i['ps'] == 'SHORT':
                    short_pos += abs(float(i['pa']))
                    short_avg = abs(float(i['ep']))
        self.data['position'] = {"longPos":long_pos, "shortPos":short_pos, "longAvg":long_avg, "shortAvg":short_avg}
        self.callback['onPosition'](self.data['position'])

    def _get_data(self):
        market_data = list()
        # 添加时间信息
        nowTime = time.time()
        market_data.append(nowTime)
        # 添加深度信息
        if 'depth' not in self.data:return {'exchange': 'binance_usdt_swap','data':[]}
        for i in self.data['depth']:
            market_data.append(i)
        # 添加成交信息
        buy_num=0
        buy_v=0
        buy_q=0
        sell_num=0
        sell_v=0
        sell_q=0
        max_trade=0
        min_trade=0
        for i in self.data['trade']:
            if i[0]=='sell':
                sell_num+=1
                sell_v+=i[1]*i[2]
                sell_q+=i[1]
            if i[0]=='buy':
                buy_num+=1
                buy_v+=i[1]*i[2]
                buy_q+=i[1]
            ###
            if max_trade == 0:
                max_trade = i[2]
            else:
                max_trade = max(max_trade, i[2])
            if min_trade == 0:
                min_trade = i[2]
            else:
                min_trade = min(min_trade, i[2])
        for i in [buy_num,sell_num,buy_q,sell_q,buy_v,sell_v,max_trade,min_trade,self.bbo['bp'],self.bbo['ap'],self.bbo['bv'],self.bbo['av']]:
            market_data.append(i)
        self.data['trade'] = []
        # 添加强平信息
        # buy_num=0
        # buy_v=0
        # buy_q=0
        # sell_num=0
        # sell_v=0
        # sell_q=0
        # for i in self.data['force']:
        #     if i[0]=='sell':
        #         sell_num+=1
        #         sell_v+=i[1]*i[2]
        #         sell_q+=i[1]
        #     if i[0]=='buy':
        #         buy_num+=1
        #         buy_v+=i[1]*i[2]
        #         buy_q+=i[1]
        # for i in [buy_num,sell_num,buy_q,sell_q,buy_v,sell_v]:
        #     market_data.append(i)
        # self.data['force'] = []
        return {'exchange': 'binance_usdt_swap','data':market_data}

    async def go(self):
        interval = 0.01
        if self.is_print:print(f'Ws循环器启动 interval {interval}')
        ### onTrade
        while 1:
            try:
                # 更新市场信息
                market_data = self._get_data()
                self.callback['onMarket'](market_data)
            except:
                traceback.print_exc()
            await asyncio.sleep(interval)

    async def run(self, is_auth=0):
        session = aiohttp.ClientSession()
        while True:
            try:
                # 尝试连接
                print(f'binance_usdt_swap 尝试连接ws')
                # 登陆
                if is_auth:
                    listenKey = await self.get_sign()
                else:
                    listenKey = 'qqlh'
                async with session.ws_connect(
                        URL+listenKey,
                        proxy=self.proxy,
                        timeout=3,
                        receive_timeout=3,
                        ) as _ws:
                    print(f'binance_usdt_swap ws连接成功')
                    self.logger.info(f'binance_usdt_swap ws连接成功')
                    # 订阅
                    symbol = self.symbol.lower()
                    channels=[
                        f"{symbol}@depth@100ms",
                        f"{symbol}@bookTicker",
                        f"{symbol}@aggTrade",
                        f"{symbol}@trade",
                        f"{symbol}@forceOrder",
                        ]
                    sub_str = json.dumps({"method": "SUBSCRIBE", "params": channels, "id":random.randint(1,1000)})
                    await _ws.send_str(sub_str)
                    self.need_flash = 1
                    while True:
                        # 接受消息
                        try:
                            msg = await _ws.receive()
                        except:
                            print(f'binance_usdt_swap ws长时间没有收到消息 准备重连...')
                            self.logger.error(f'binance_usdt_swap ws长时间没有收到消息 准备重连...')
                            break
                        msg = msg.data
                        # print(msg)
                        # 处理消息
                        if 'depthUpdate' in msg:self._update_depth(msg)
                        # if 'depthUpdate' in msg:self._update_depth20(msg)
                        elif 'aggTrade' in msg:self._update_trade(msg)
                        elif 'bookTicker' in msg:self._update_ticker(msg)
                        elif 'forceOrder' in msg:self._update_force(msg)
                        elif 'ACCOUNT_UPDATE' in msg:self._update_position(msg)
                        elif 'ACCOUNT_UPDATE' in msg:self._update_account(msg)
                        elif 'ORDER_TRADE_UPDATE' in msg:self._update_order(msg)
                        elif 'ping' in msg:await _ws.send_str('pong')
                        elif 'listenKeyExpired' in msg:raise Exception('key过期重连')
                        if self.need_flash:
                            depth_flash = await self.get_depth_flash()
                            self.last_update_id = depth_flash['lastUpdateId']
                            # 检查已有更新中是否包含
                            self.depth['bids'] = dict()
                            self.depth['asks'] = dict()
                            for i in depth_flash['bids']:self.depth['bids'][float(i[0])] = float(i[1])
                            for i in depth_flash['asks']:self.depth['asks'][float(i[0])] = float(i[1])
                            self.need_flash = 0
            except:
                _ws = None
                traceback.print_exc()
                print(f'binance_usdt_swap ws连接失败 开始重连...')
                self.logger.error(f'binance_usdt_swap ws连接失败 开始重连...')
                # await asyncio.sleep(1)

if __name__ == "__main__":
    ws = BinanceCoinSwapWs('config.toml', is_print=1)

    loop = asyncio.get_event_loop()

    tasks = [
        asyncio.ensure_future(ws.run(is_auth=0)),
        asyncio.ensure_future(ws.go()),
    ]
    loop.run_until_complete(asyncio.wait(tasks))


# In[ ]:




