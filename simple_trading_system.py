from datetime import datetime, timedelta
import pandas as pd
from time import sleep
import ccxt
from trade import next_run_time, get_okx_candle_data, signal_moving_average, place_order

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
"""
自动交易主要流程

# 通过while循环，不断的循环
# 每次循环中需要做的操作有
1. 获取账户信息
2. 获取实时数据
3. 根据最新数据进行策略判断
4. 根据判断结果下单、卖出或者、继续持有、或者进行其他操作
5. 休息
"""



time_interval = '5m'  # 时间间隔设置，这里设置5min
exchange = ccxt.okx()  # 创建交易所，此处为okx交易所
exchange.apiKey = 'b0195d81-5bf2-498e-9060-ebd21b19adb8'#此处填写你在okx申请的apiKey，这里所填写的是样本
exchange.secret = '3B8795EACD6541ABF9F8E48ADE5731CC'

symbol = 'ETH/BTC'  # 交易对
base_coin = symbol.split('/')[1]
trade_coin = symbol.split('/')[0]

para = [20, 200]  # 策略参数

# ------主程序开始------
while True:
    # ===获取账户中的资产

    balance = exchange.fetch_balance()['total']
    base_coin_amount = float(balance[base_coin])
    trade_coin_amount = float(balance[trade_coin])
    # ====sleep直到指定的时间
    run_time = next_run_time(time_interval)
    sleep(max(0, (run_time - datetime.now()).seconds))
    while True:  # 在指定时间前持续执行
        if datetime.now() < run_time:
            continue
        else:
            break

    # ===获取数据
    while True:
        df = get_okx_candle_data(exchange, symbol, time_interval)
        # 判断是否有包含当前分钟的数据
        _temp = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(minutes=int(time_interval)))]
        if _temp.empty:
            print('获取数据不包含当前分钟的数据，重新获取')
            continue
        else:
            break

    # ===产生交易信号
    df = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]
    df = signal_moving_average(df, para=para)

    signal = df.iloc[-1]['signal']
    print('\n交易信号', signal)

    #===卖出品种
    if trade_coin_amount > 0 and signal == 0:
        print('\n卖出')
    # 委托卖出
    price = exchange.fetch_ticker(symbol)['bid']  # 获取买一价格
    place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price)

    #===买入品种
    if trade_coin_amount == 0 and signal == 1:
        print('已满仓')
        # 获取买入价格
        price = exchange.fetch_ticker(symbol)['ask']  # 获取最新卖价
        # 计算买入数量
        buy_amount = base_coin_amount / price
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount)


    print('本次运行完毕')
    # ===休息
    sleep(6 * 1)



