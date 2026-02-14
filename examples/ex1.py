import logging
import backtrader_next as bt
from bn_quik import QuikStore

# Настройка логирования
logging.basicConfig(
    # level=logging.DEBUG,  # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    level=logging.INFO,  # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщений
    datefmt='%Y-%m-%d %H:%M:%S'  # Формат времени
)


class StratPrintBars(bt.Strategy):
    """
    - Отображает статус подключения
    - При приходе нового бара отображает его цены/объем
    - Отображает статус перехода к новым барам
    """
    params = (  # Параметры торговой системы
        ('wait_all_datas', False),  # Ждать прихода баров всех тикеров перед обработкой
        ('symbols', None),  # Список торгуемых тикеров. По умолчанию торгуем все тикеры
    )

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные
        self.logger = logging.getLogger('StratPrintBars')


    def next(self):
        """Приход нового бара тикера"""
        if self.p.wait_all_datas:  # Ждать бары всех тикеров
            lastdatetimes = [data.datetime.datetime(0) for data in self.datas]  # Дата и время последнего бара каждого тикера
            if lastdatetimes.count(lastdatetimes[0]) != len(lastdatetimes):  # Если дата и время последних баров не идентичны
                return  # то еще не пришли все новые бары.
            print("================== NEXT for all datas =================")

        for data in self.datas:  # Пробегаемся по всем запрошенным тикерам
            if not self.p.symbols or data._name in self.p.symbols:  # Если торгуем все тикеры или данный тикер
                self.logger.info(f'{data._name} - {bt.TimeFrame.Names[data.p.timeframe]} {data.p.compression} - Open={data.open[0]:.2f}, High={data.high[0]:.2f}, Low={data.low[0]:.2f}, Close={data.close[0]:.2f}, Volume={data.volume[0]:.0f} [{data.datetime.datetime(0)}]')

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статсуса приходящих баров"""
        data_status = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        self.logger.info(f'Notify Data: {data._name} - Status: {data_status}')  # Статус приходит для каждого тикера отдельно
        self.isLive = data_status == 'LIVE'  # В Live режим переходим после перехода первого тикера


if __name__ == '__main__':
    try:
        # symbol = 'TQBR.SBER'  # Тикер
        symbol = 'QJSIM.SBER' # Тикер для симуляции для Demo QUIK
        # symbol = 'SPBFUT.SiZ5'  # Для фьючерсов: Si (RTS) - Z (December) - 5 (2025)

        store = QuikStore(trade_account_id='NL0011XXXXXX', limit_kind=-1) #  limit_kind=-1 - только для Demo Quik
        # store = QuikStore(trade_account_id='NL0011XXXXXX') #  для реального счета 
        # store = QuikStore(trade_account_id='SPBFUT0XXXXX') # Срочный рынок

        cerebro = bt.Cerebro(quicknotify=True)

        # Исторические данные и новые бары будут загружены автоматически при установке live_bars=True
        data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, live_bars=True)

        cerebro.adddata(data)

        broker = store.getbroker()
        cerebro.setbroker(broker)

        cerebro.addstrategy(StratPrintBars)
        cerebro.run()

    except KeyboardInterrupt:
        print("\nПрервано пользователем (Ctrl+C)")
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
