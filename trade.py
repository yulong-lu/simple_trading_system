from datetime import datetime, timedelta
import pandas as pd
import time
from smtplib import SMTP
from sqlalchemy.sql.functions import now


#===得到下次执行的时间
def next_run_time(time_interval, ahead_time=1):
    if time_interval.endswith('m'):
        now_time = datetime.now()
        time_interval = int(time_interval.strip('m'))

        target_min = (int(now_time.minute / time_interval) + 1) * time_interval
        if target_min < 60:
            target_time = now.time.replace(minute=target_min, second=0, microsecond=0)
        else:
            if now.time.hour == 23:
                target_time = now.time.replace(hour=0, minute=0, second=0, microsecond=0)
                target_time += timedelta(days=1)
            else:
                target_time = now.time.replace(hour=now.time.hour + 1, minute=0, second=0, microsecond=0)

        # sleep直到目标时间点再运行
        if (target_time - datetime.now()).seconds > ahead_time+1:
            print("距离目标时间不足",ahead_time,"秒，下个周期再运行")
            target_time += timedelta(minutes=time_interval)
            print("下次运行时间：",target_time)
            return target_time
    else:
        exit('time_interval doesn\'t end with m')

#===获取okx交易所的K线数据
def get_okx_candle_data(exchange, symbol, time_interval):
    content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, since=0)
    df = pd.DataFrame(content, dtype=float)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
    df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
    df = df[['candle_begin_time_GMT8', 'open', 'high',' low',' close',' volume']]
    return df

#===信号产生函数
def signal_moving_average(df, para=[5, 60]):
    """
    简单的移动平均线策略：短期均线上穿长期均线，买入；短期均线下穿长期均线，卖出
    :param df: 数据框
    :param para: [ma_short, ma_long]
    """
    ma_short = para[0]
    ma_long = para[1]

    # 计算均线
    df['ma_short'] = df['close'].rolling(ma_short, min_periods=1).mean()
    df['ma_long'] = df['close'].rolling(ma_long, min_periods=1).mean()
    # 找出交易信号
    condition1 = df['ma_short'] > df['ma_long']
    condition2 = df['ma_short'].shift(1) <= df['ma_long'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = 1
    #找出卖出信号
    condition1 = df['ma_short'] < df['ma_long']
    condition2 = df['ma_short'].shift(1) >= df['ma_long'].shift(1)
    df.loc[condition1 & condition2, 'signal'] = 0

    df.drop(['ma_short', 'ma_long'], axis=1, inplace=True)

    # 由signal计算出实际的每天持有仓位
    # signal信号出现的位置，我们要在这些位置买入股票，但应在本栏未来。
    df ['pos']=df ['signal']. shift()
    df ['pos']. fillna (method='ffill', inplace=True)
    # 填充刚开始的空白数据，我们默认最初的仓位是空仓。
    df ['pos']. fillna (value=0,inplace=True)

    return df


#===下单函数
def place_order(exchange, order_type, buy_or_sell, symbol, price, amount):
    """
    下单
    :param exchange: 交易所
    :param order_type: limit，market
    :param buy_or_sell: buy，sell
    :param symbol: 交易对
    :param price: 当market订单的时候，price无效
    :param amount: 数量
    """
    order_info = None  # 给 order_info 设置一个默认值
    for i in range(5):
        try:
            # 限价单
            if order_type == 'limit':
                if buy_or_sell == 'buy':
                    order_info = exchange.create_limit_buy_order(symbol, amount, price)  # 买单
                elif buy_or_sell == 'sell':

                    order_info = exchange.create_limit_sell_order(symbol, amount, price)  # 卖单

            # 市价单
            elif order_type == 'market':
                if buy_or_sell == 'buy':
                    order_info = exchange.create_market_buy_order(symbol=symbol, amount=amount)  # 买单
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_market_sell_order(symbol=symbol, amount=amount)  # 卖单
            else:
                pass

            print('下单成功: ', order_type, buy_or_sell, symbol, price, amount)
            print('下单信息: ', order_info, '\n')
            return order_info

        except Exception as e:
            print('下单失败,1s后重新尝试: ', e)
            time.sleep(1)
    print('下单失败次数过多，程序终止')
    return order_info


