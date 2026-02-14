import logging
from datetime import date, timedelta
import backtrader_next as bt
from bn_quik import QuikStore

# Настройка логирования
logging.basicConfig(
    #level=logging.DEBUG,  # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    level=logging.INFO,  # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщений
    datefmt='%Y-%m-%d %H:%M:%S'  # Формат времени
)


class SmaCross(bt.Strategy):
    """
    - Отображает статус подключения
    - Покупает при пересечении MA1 снизу вверх MA2, продает при пересечении MA1 сверху вниз MA2
    - Продает при пересечении MA1 сверху вниз MA2, покупает при пересечении MA1 снизу вверх MA2
    """
    params = (  # Параметры торговой системы
        ('name', None),  # Название торговой системы
        ('MA1', 5),
        ('MA2', 10),
    )

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные
        self.logger = logging.getLogger('SmaCross')
        self.Order = None
        self.ma1 = bt.nind.SMA(self.data.close, period=self.p.MA1)
        self.ma2 = bt.nind.SMA(self.data.close, period=self.p.MA2)


    def next(self):
        """Приход нового бара тикера"""
        if not self.isLive:
            return
        if self.Order:  # Если есть открытый ордер, ждем его исполнения
            return

        if self.crossover(self.ma1, self.ma2):
            pos = self.getposition()
            self.logger.info(f'Buy signal: {self.data._name} Cur pos size: {pos.size} ')
            if pos:
                self.logger.info(f'Closing position before buy: {pos.size} ')
                self.close(size=pos.size)
            self.logger.info(f'Placing buy order for size 1 ')
            self.Order = self.buy(size=1)
        elif self.crossover(self.ma2, self.ma1):
            pos = self.getposition()
            self.logger.info(f'Sell signal: {self.data._name} Cur pos size: {pos.size} ')
            if pos:
                self.logger.info(f'Closing position before sell: {pos.size} ')
                self.close(size=pos.size)
            self.logger.info(f'Placing sell order for size 1 ')
            self.Order = self.sell(size=1)


    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статсуса приходящих баров"""
        data_status = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        self.logger.info(f'Notify Data: {data._name} - Status: {data_status}')  # Статус приходит для каждого тикера отдельно
        self.isLive = data_status == 'LIVE'  # В Live режим переходим после перехода первого тикера

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:  # Order is submitted/accepted
            return  # Do nothing until the order is completed
        elif order.status in [order.Canceled]:  # Canceled, Margin, Rejected
            print('Order was Canceled', self.data.datetime.datetime(0))
        elif order.status in [order.Margin]:  # Canceled, Margin, Rejected
            print('Order was Margin ', self.data.datetime.datetime(0))
        elif order.status in [order.Rejected]:  # Canceled, Margin, Rejected
            print('Order was Rejected', self.data.datetime.datetime(0))
        self.Order = None  # Reset order

    def crossover(self, ma1, ma2):
        try:
            return ma1[-1] <= ma2[-1] and ma1[0] > ma2[0]
        except IndexError:
            return False

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    try:
        # symbol = 'TQBR.SBER'  # Тикер
        symbol = 'QJSIM.SBER' # Тикер для симуляции Quik Demo
        # symbol = 'SPBFUT.SiZ5'  # Для фьючерсов: Si (RTS) - Z (December) - 5 (2025)

        #store = QuikStore(trade_account_id='NL0011100043')
        store = QuikStore(trade_account_id='NL0011100043', limit_kind=-1) # for Demo=> limit_kind=-1 
        # store = QuikStore(trade_account_id='SPBFUT0004u', quicknotify=True) # Срочный рынок
        cerebro = bt.Cerebro(quicknotify=True)

        data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, live_bars=True)
        cerebro.adddata(data)

        broker = store.getbroker()
        cerebro.setbroker(broker)

        cerebro.addstrategy(SmaCross)
        cerebro.run()

    except KeyboardInterrupt:
        print("\nПрервано пользователем (Ctrl+C)")
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
