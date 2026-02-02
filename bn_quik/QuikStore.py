import logging
import collections
import os
import sys
import threading
import asyncio
import atexit
from typing import List
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from pytz import timezone
from dateutil import parser
from decimal import ROUND_DOWN, Decimal


from backtrader_next.metabase import MetaParams
from backtrader_next.utils.py3 import with_metaclass
from backtrader_next import TimeFrame

from quik_python import Quik, Candle, CandleInterval
from quik_python.data_structures import ParamTable, ParamNames, SecurityInfo, TradeAccounts, FuturesClientHolding, Trade
from quik_python.data_structures.depo_limit_ex import DepoLimitEx
from quik_python.data_structures.futures_limits import FuturesLimitType, FuturesLimits
from quik_python.data_structures.money_limit_ex import MoneyLimitEx
from quik_python.data_structures.portfolio_info_ex import PortfolioInfoEx
from quik_python.data_structures.stop_order import StopOrder
from quik_python.data_structures.order import Order
from quik_python.data_structures.transaction_reply import TransactionReply

class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._instance = None
        cls._loop = None
        cls._loop_thread = None

    def __call__(cls, *args, **kwargs):
        # Создаём единственный экземпляр
        if cls._instance is None:
            cls._instance = super(MetaSingleton, cls).__call__(*args, **kwargs)

            # Создаём event loop в отдельном потоке, если его нет
            if cls._loop is None:
                try:
                    # Проверяем, есть ли уже активный loop (например, FastAPI)
                    running_loop = asyncio.get_running_loop()
                    cls._loop = running_loop
                except RuntimeError:
                    # Нет активного цикла — создаём свой в отдельном потоке
                    cls._loop = asyncio.new_event_loop()
                    cls._loop_thread = threading.Thread(
                        target=cls._run_loop_forever,
                        args=(cls._loop,),
                        daemon=True  # Daemon поток завершится автоматически
                    )
                    cls._loop_thread.start()
                    # Wait a bit for loop to start
                    import time
                    time.sleep(0.05)
        return cls._instance

    @staticmethod
    def _run_loop_forever(loop):
        """Run event loop forever in a separate thread."""
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def run_sync(cls, coro):
        """Безопасно запускает корутину в любом контексте."""
        try:
            # Пытаемся получить текущий running loop
            loop = asyncio.get_running_loop()
            # Мы уже в асинхронном контексте → используем run_coroutine_threadsafe
            if loop == cls._loop:
                # We're in the same loop thread, can't use run_until_complete
                future = asyncio.ensure_future(coro, loop=loop)
                # This shouldn't happen in normal flow
                raise RuntimeError("Cannot run_sync from within the async loop thread")
            else:
                # Different loop - use threadsafe
                future = asyncio.run_coroutine_threadsafe(coro, cls._loop)
                return future.result()
        except RuntimeError:
            # Нет активного event loop в текущем потоке → используем наш loop с threadsafe
            if cls._loop is None or not cls._loop.is_running():
                raise RuntimeError("Event loop is not running")

            future = asyncio.run_coroutine_threadsafe(coro, cls._loop)
            return future.result()

    def run_async(cls, coro):
        """Запускает корутину без ожидания результата."""
        if cls._loop is None or not cls._loop.is_running():
            raise RuntimeError("Event loop is not running")

        # Schedule task in the running loop
        future = asyncio.run_coroutine_threadsafe(coro, cls._loop)
        return future


class Account:
    '''Account class to hold trade account information'''
    def __init__(self, trade_account_id: str, client_code: str, firm_id: str, class_codes: List[str]):
        self.client_code: str = client_code
        self.order_client_code: str = client_code
        self.firm_id: str = firm_id
        self.class_codes: List[str] = class_codes
        self.trade_account_id: str = trade_account_id
        self.futures: bool = False
        self.is_ucp: bool = False

    def to_dict(self):
        return {
            'trade_account_id': self.trade_account_id,
            'client_code': self.client_code,
            'firm_id': self.firm_id,
            'class_codes': self.class_codes,
            'futures': self.futures,
            'is_ucp': self.is_ucp,
        }

    @classmethod
    def from_dict(cls, odict):
        acc = cls(
            trade_account_id=odict.get('trade_account_id'),
            client_code=odict.get('client_code'),
            firm_id=odict.get('firm_id'),
            class_codes=odict.get('class_codes', []),
        )
        acc.futures = odict.get('futures', False)
        acc.is_ucp = odict.get('is_ucp', False)
        return acc
        

class QuikStore(with_metaclass(MetaSingleton, object)):
    '''
    Quik Store Class
    ===========================================
    Store class to handle data and broker for Quik via quik_python
    '''

    logger = logging.getLogger('QuikStore')

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will autoregister

    params = (
        ('host', '127.0.0.1'),
        ('port', 34130),
        ('lots', True),  # Входящий остаток в лотах (задается брокером)
        ('limit_kind', 1),  # Основной режим торгов T1
        #('limit_kind', -1),  # Quik Demo
        ('currency', 'SUR'),  # Валюта
        ('futures_firm_id', "SPBFUT"),  # Идентификатор фирмы для фьючерсов
        ('edp', False),  # Единая денежная позиция
        ('slippage_steps', 10),  # Кол-во шагов цены для проскальзывания
        ('data_dir', 'DataQuik'),  # Каталог для хранения данных

        ('client_code_for_orders', None),  # Номер торгового терминала. У брокера Финам требуется для совершения торговых операций
        ('trade_account_id', None),
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        # Import here to avoid circular imports
        from .QuikData import QuikData
        
        super(QuikStore, self).__init__()
        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start
        self.qdata = {}
        self.qdata_last = defaultdict(QuikData)
        self.notifs = collections.deque()  # store notifications for cerebro
        self.tz_msk = timezone('Europe/Moscow')  # ВременнАя зона МСК

        app_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        # Проверяем, является ли data_dir относительным или абсолютным путем
        if os.path.isabs(self.p.data_dir):
            # Абсолютный путь - проверяем его существование
            self.data_path = os.path.join(self.p.data_dir, '')
            if not os.path.exists(self.data_path):
                raise RuntimeError(f"Data directory does not exist: {self.data_path}")
        else:
            # Относительный путь - создаем его относительно приложения
            self.data_path = os.path.join(app_dir, self.p.data_dir, '')
            os.makedirs(self.data_path, exist_ok=True)

        self.new_bars = []  # Новые бары по всем подпискам на тикеры из QUIK

        self._qapi = Quik(port=self.p.port, host=self.p.host )

        self._cash = 0.0
        self._value = 0.0
        self._stop_event = asyncio.Event()  # Событие для остановки async loop
        self._qapi_task = None  # Задача для async работы
        self._started = False  # Флаг запуска store
        self._accounts:list[Account] = []
        self._ticker_info = {}
        self._sec_code2name = {}
        self._ticker_stepprice = {}  # Кэш шагов цены по тикерам

        # Thread synchronization locks для безопасности многопоточного доступа
        # Threading locks используются для данных с гибридным доступом
        # (sync методы из main thread + async методы из event loop thread).
        # Это безопасно, т.к. event loop работает в отдельном потоке (см. MetaSingleton).
        
        self._lock_qdata = threading.RLock()        # Гибридный: Async(_on_new_candle/_register_data/_unregister_data) + может быть sync доступ
        self._lock_notifs = threading.Lock()        # Гибридный: Sync(get_notifications/put_notification) + может быть async
        self._lock_accounts = threading.Lock()      # Только Async: get_accounts (но может расшириться)
        self.lock_store_data = threading.RLock()   # Гибридный: Используется в QuikBroker для критических операций с данными
        
        # Примечание: threading.Lock безопасен в данной архитектуре благодаря отдельному потоку для event loop.

        # Регистрируем автоматическую очистку при завершении программы
        atexit.register(self._cleanup_on_exit)

    def _cleanup_on_exit(self):
        """Автоматическая очистка при завершении программы"""
        try:
            if self._started:
                self.logger.info("QuikStore: автоматическое завершение при выходе из программы")
                self.stop()
        except Exception:
            pass  # Игнорируем ошибки при завершении
        finally:
            # Принудительное завершение для гарантии
            import sys
            import os
            os._exit(0)  # Принудительно завершаем процесс

    def __del__(self):
        """Деструктор для гарантированной очистки"""
        try:
            if self._started:
                self.stop()
        except Exception:
            pass  # Игнорируем ошибки в деструкторе

    @property
    def quik_api(self) -> Quik:
        return self._qapi

    def put_notification(self, msg, *args, **kwargs):
        with self._lock_notifs:
            self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        with self._lock_notifs:
            self.notifs.append(None)  # put a mark / threads could still append
            return [x for x in iter(self.notifs.popleft, None)]

    def start(self, data=None, broker=None):
        """Запуск QuikStore и async задач"""
        if self._started:
            self.logger.info("QuikStore already started")
            return

        if data is not None and data not in self.datas:
            self.datas.append(data)

        if broker is not None:
            self.broker = broker

        self._stop_event.clear()  # Сбрасываем флаг остановки
        self.logger.info("Starting QuikStore...")
        self._qapi_task = self.__class__.run_sync(self._start_async())
        self._started = True
        self.logger.info("QuikStore initialized and ready")

    def stop(self):
        """Останавливает асинхронную работу"""
        if not self._started:
            return

        self.quik_api.candles.remove_new_candle_handler(self._on_new_candle)
        self.quik_api.events.remove_on_connected(self._on_connected)
        self.quik_api.events.remove_on_disconnected(self._on_disconnected)
        self.quik_api.events.remove_on_trans_reply(self._on_transaction_reply)
        self.quik_api.events.remove_on_trans_reply(self._on_trade)
        self.quik_api.events.remove_on_order(self._on_order)
        self.quik_api.events.remove_on_stop_order(self._on_stop_order)

        self.logger.info("Stopping QUIK async loop...")
        self._stop_event.set()  # Устанавливаем флаг остановки

        # Закрываем соединение с QUIK
        try:
            self.quik_api.disconnect()
        except Exception as e:
            self.logger.error("Error closing connection to QUIK: %s", e)

        # Останавливаем event loop
        if self.__class__._loop and self.__class__._loop.is_running():
            self.__class__._loop.call_soon_threadsafe(self.__class__._loop.stop)

        # Ждем завершения потока
        if self.__class__._loop_thread and self.__class__._loop_thread.is_alive():
            self.__class__._loop_thread.join(timeout=2.0)

        self._started = False
        self.logger.info("QUIK async loop stopped")


    async def _start_async(self):
        """Start the asynchronous event loop for the provider."""
        try:
            await self.quik_api.initialize()
            self.logger.info("QUIK initialized successfully")

            if not self.quik_api.is_service_alive():
                raise RuntimeError("Не удалось подключиться к QUIK")

            status = await self.quik_api.service.is_connected()
            if not status:
                raise RuntimeError("Quik не подключен к торгам")

            self.logger.info("QUIK connected successfully")

            self.quik_api.candles.add_new_candle_handler(self._on_new_candle)
            self.quik_api.events.add_on_connected(self._on_connected)
            self.quik_api.events.add_on_disconnected(self._on_disconnected)
            self.quik_api.events.add_on_trans_reply(self._on_transaction_reply)
            self.quik_api.events.add_on_trade(self._on_trade)
            self.quik_api.events.add_on_order(self._on_order)
            self.quik_api.events.add_on_stop_order(self._on_stop_order)

        except Exception as e:
            self.logger.error("Ошибка подключения: %s", e)
            raise RuntimeError(f"Ошибка подключения: {e}") from e
        finally:
            self.logger.info("QUIK _start_async ended")

    def _on_new_candle(self, candle: Candle):
        """Callback for new candle events"""
        logging.debug("New candle received: %s.%s Interval: %s Time: %s", candle.class_code, candle.sec_code, candle.interval, candle.datetime.to_datetime())
        data_id = self._get_data_id(candle.class_code, candle.sec_code, candle.interval)
        with self._lock_qdata:
            qdata = self.qdata.get(data_id, None)
        if qdata is not None:
            qdata._on_new_candle(candle)

    def _on_connected(self, reply):
        self.logger.info(reply)

    def _on_disconnected(self, reply):
        self.logger.info(reply)

    async def _on_transaction_reply(self, reply: TransactionReply):
        # self.logger.debug(reply)
        if self.broker:
            await self.broker.on_trans_reply(reply)

    async def _on_order(self, order : Order):
        # self.logger.info(order)
        if self.broker:
            await self.broker.on_order(order)

    async def _on_stop_order(self, stop_order:StopOrder):
        # self.logger.info(stop_order)
        if self.broker:
            await self.broker.on_stop_order(stop_order)
            pass

    async def _on_trade(self, trade: Trade):
        # self.logger.debug(trade)
        if self.broker:
            await self.broker.on_trade(trade)

    def _get_data_by_id(self, data_id: str):
        """Получение данных по тикеру и временному интервалу"""
        with self._lock_qdata:
            return self.qdata.get(data_id, None)

    def _register_data(self, data):
        """Регистрация данных по тикеру и временному интервалу"""
        data_id = data.data_id
        with self._lock_qdata:
            if data_id not in self.qdata:
                self.qdata[data_id] = data
                v = self.qdata_last.get(data_id, None)
                if (v is not None and data.candle_interval < v.candle_interval) or v is None:
                    self.qdata_last[(data.class_code, data.sec_code)] = data
            else:
                raise ValueError(f"Data for {data_id} already registered") 

    def _unregister_data(self, data):
        """Убрать регистрацию данных по тикеру и временному интервалу"""
        data_id = data.data_id
        with self._lock_qdata:
            if data_id in self.qdata:
                del self.qdata[data_id]


    def _subscribe_to_candles(self, class_code: str, sec_code: str, interval: CandleInterval)-> bool:
        """Подписка на новые бары по тикеру и временному интервалу"""
        self.logger.debug("Subscribing to candles for %s.%s at interval %s", class_code, sec_code, interval)
        try:
            self.__class__.run_sync(self.quik_api.candles.subscribe(class_code, sec_code, interval))
            return True
        except Exception as e:
            self.logger.error("Error subscribing to candles: %s", e)
            return False

    def _unsubscribe_from_candles(self, class_code: str, sec_code: str, interval: CandleInterval)-> bool:
        """Отписка от новых баров по тикеру и временному интервалу"""
        self.logger.debug("Unsubscribing from candles for %s.%s at interval %s", class_code, sec_code, interval)
        try:
            self.__class__.run_sync(self.quik_api.candles.unsubscribe(class_code, sec_code, interval))
            return True
        except Exception as e:
            self.logger.error("Error unsubscribing from candles: %s", e)
            return False

    def _is_subscribed_to_candles(self, class_code: str, sec_code: str, interval: CandleInterval)-> bool:
        self.logger.debug("Checking subscription to candles for %s.%s at interval %s", class_code, sec_code, interval)
        try:
            return self.__class__.run_sync(self.quik_api.candles.is_subscribed(class_code, sec_code, interval))
        except Exception as e:
            self.logger.error("Error checking subscription to candles: %s", e)
            return False


    def _get_last_candles(self, class_code: str, sec_code: str, interval: CandleInterval, count:int = 1000) -> pd.DataFrame | None:
        """Получение последних баров по тикеру и временному интервалу"""
        self.logger.debug("Getting last %s candles for %s.%s at interval %s", count, class_code, sec_code, interval)
        try:
            candles = self.__class__.run_sync(self.quik_api.candles.get_last_candles(class_code, sec_code, interval, count))
            df = pd.DataFrame({
                # 'datetime': pd.to_datetime([c.datetime.to_datetime() for c in candles]),
                'datetime': [c.datetime.to_datetime() for c in candles],
                'open': [c.open for c in candles],
                'high': [c.high for c in candles],
                'low': [c.low for c in candles],
                'close': [c.close for c in candles],
                'volume': [c.volume for c in candles]
            })
            df.set_index('datetime', inplace=True)
            return df
        except Exception as e:
            self.logger.error("Error getting last candles: %s", e)
            return None

    async def _get_trade_accounts(self) -> list[TradeAccounts]:
        """Получение информации о торговых счетах из QUIK"""
        self.logger.debug("Getting trade accounts")
        try:
            return await self.quik_api.clazz.get_trade_accounts()
        except Exception as e:
            self.logger.error("Error getting trade accounts: %s", e)
            return []

    async def get_ticker_info(self, class_code: str, sec_code: str) -> SecurityInfo | None:
        """Получение информации о тикере из QUIK"""
        key = (class_code, sec_code)
        self.logger.debug("Getting ticker info for %s.%s", class_code, sec_code)
        val = self._ticker_info.get(key, None)
        if val is not None:
            return val
        else:
            try:
                val = await self.quik_api.clazz.get_security_info(class_code, sec_code)
                if val is not None:
                    self._ticker_info[key] = val
                return val
            except Exception as e:
                self.logger.error("Error getting ticker info: %s", e)
                self._ticker_info[key] = None
                return None

    def get_ticker_info_sync(self, class_code: str, sec_code: str) -> dict:
        """Синхронное получение информации о тикере из QUIK"""
        info = QuikStore.run_sync(self.get_ticker_info(class_code, sec_code))
        return info.to_dict() if info else {}
    
    async def get_portfolio_info_ex(self, firm_id: str, client_code: str) -> PortfolioInfoEx | None:
        self.logger.debug("Getting extended portfolio info for %s.%s", firm_id, client_code)
        try:
            return await self.quik_api.trading.get_portfolio_info_ex(firm_id, client_code)
        except Exception as e:
            self.logger.error("Error getting extended portfolio info: %s", e)
            return None

    async def _get_ticker_info_ex(self, class_code: str, sec_code: str, param_name:ParamNames) -> ParamTable | None:
        """Получение расширенной информации о тикере из QUIK"""
        self.logger.debug("Getting extended ticker info for %s.%s %s", class_code, sec_code, param_name)
        try:
            return await self.quik_api.trading.get_param_ex(class_code, sec_code, param_name)
        except Exception as e:
            self.logger.error("Error getting extended ticker info: %s", e)
            return None

    async def get_all_depo_limits(self) -> List[DepoLimitEx]:
        """Получение всех лимитов по бумагам из QUIK"""
        try:
            return await self.quik_api.trading.get_depo_limits()
        except Exception as e:
            self.logger.error("Error getting all depo limits: %s", e)
            return []

    async def get_money_limits(self) -> List[MoneyLimitEx]:
        """Получение всех лимитов по деньгам из QUIK"""
        try:
            return await self.quik_api.trading.get_money_limits()
        except Exception as e:
            self.logger.error("Error getting all money limits: %s", e)
            return []

    async def get_futures_limit(self, firm_id: str, acc_id: str, curr_code: str = "") -> FuturesLimits | None:
        """Получение всех лимитов по фьючерсам из QUIK"""
        try:
            return await self.quik_api.trading.get_futures_limit(firm_id, acc_id, FuturesLimitType.MONEY, curr_code)
        except Exception as e:
            self.logger.error("Error getting futures limits: %s", e)
            return None

    async def get_orders(self) -> list[Order]:
        """Получение всех заявок из QUIK"""
        try:
            return await self.quik_api.orders.get_orders()
        except Exception as e:
            self.logger.error("Error getting orders: %s", e)
            return []

    async def get_stop_orders(self) -> list[StopOrder]:
        """Получение всех стоп-заявок из QUIK"""
        try:
            return await self.quik_api.stop_orders.get_stop_orders()
        except Exception as e:
            self.logger.error("Error getting stop orders: %s", e)
            return []

    async def get_order_by_number(self, order_num:int) -> Order | None:
        """Получение информации о заявке по её номеру"""
        try:
            return await self.quik_api.orders.get_order_by_number(order_num)
        except Exception as e:
            self.logger.error("Error getting order by number: %s", e)
            return None

    async def get_futures_client_holdings(self) -> list[FuturesClientHolding]:
        """Получение всех позиций по фьючерсам из QUIK"""
        try:
            return await self.quik_api.trading.get_futures_client_holdings()
        except Exception as e:
            self.logger.error("Error getting futures client holdings: %s", e)
            return []

    def _get_quik_datetime_now(self) -> datetime | None:
        """Получение текущей даты и времени из QUIK"""
        try:
            dt = self.__class__.run_sync(self.quik_api.service.get_info_param("TRADEDATE"))
            tm = self.__class__.run_sync(self.quik_api.service.get_info_param("SERVERTIME"))
            return self._parse_datetime_auto(f'{dt} {tm}')
        except Exception:
            return datetime.now(self.tz_msk).replace(tzinfo=None)  # То время МСК получаем из локального времени

    def _parse_datetime_auto(self, datetime_str: str) -> datetime:
        """Парсит строку даты и времени в объект datetime с учётом МСК"""
        try:
            v = parser.parse(datetime_str, dayfirst=("." in datetime_str))
            if v.tzinfo is None:
                v = self.tz_msk.localize(v)
            else:
                v = v.astimezone(self.tz_msk)
            return v.replace(tzinfo=None)
        except ValueError:
            return datetime.now(self.tz_msk).replace(tzinfo=None)  # То время МСК получаем из локального времени


    async def parse_ticker_name(self, name:str) -> tuple[str, str] | None:
        parts = name.split('.')
        if len(parts) >= 2:
            return (parts[0], '.'.join(parts[1:]))
        else:
            sec_code = name
            data = self._sec_code2name.get(sec_code)
            if data:
                return data
            else:
                class_codes:List[str] = await self.quik_api.clazz.get_classes_list()
                class_code:str = await self.quik_api.clazz.get_security_class(','.join(class_codes), sec_code)
                if class_code:
                    self._sec_code2name[sec_code] = (class_code, sec_code)
                return class_code, sec_code

    def get_ticker_name(self, class_code: str, sec_code: str) -> str:
        return f'{class_code}.{sec_code}'



    def _bt_timeframe_2_quik(self, timeframe:TimeFrame, compression:int=1) -> CandleInterval:
        """
        Перевод интервала из BackTrader в QUIK

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал QUIK
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            if compression == 1:
                return CandleInterval.M1  # 1 минута
            elif compression == 2:
                return CandleInterval.M2  # 2 минуты
            elif compression == 3:
                return CandleInterval.M3  # 3 минуты
            elif compression == 4:
                return CandleInterval.M4  # 4 минуты
            elif compression == 5:
                return CandleInterval.M5  # 5 минут
            elif compression == 6:
                return CandleInterval.M6  # 6 минут
            elif compression == 10:
                return CandleInterval.M10  # 10 минут
            elif compression == 15:
                return CandleInterval.M15  # 15 минут
            elif compression == 20:
                return CandleInterval.M20  # 20 минут
            elif compression == 30:
                return CandleInterval.M30  # 30 минут
            elif compression == 60:
                return CandleInterval.H1  # 1 час
            elif compression == 120:
                return CandleInterval.H2  # 2 часа
            elif compression == 240:
                return CandleInterval.H4  # 4 часа
            else:
                raise NotImplementedError(f"Unsupported compression: {compression}")
        elif timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return CandleInterval.D1  # В минутах
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return CandleInterval.W1  # В минутах
        elif timeframe == TimeFrame.Months:  # Месячный временной интервал
            return CandleInterval.MN  # В минутах
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем


    def _bt_timeframe_to_str(self, timeframe:TimeFrame, compression:int=1) -> str:
        """
        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал для имени файла истории и расписания
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            if compression < 60 and compression in [1,2,3,4,5,6,10,15,20,30]:
                return f'M{compression}'
            elif compression == 60:
                return 'H1'
            elif compression == 120:
                return 'H2'
            elif compression == 240:
                return 'H4'
            else:
                raise NotImplementedError
        elif timeframe == TimeFrame.Days:
            return 'D1'
        elif timeframe == TimeFrame.Weeks:
            return 'W1'
        elif timeframe == TimeFrame.Months:
            return 'MN1'
        raise NotImplementedError

    def _get_data_id(self, class_code: str, sec_code: str, interval: CandleInterval) -> str:
        """
        Получение уникального идентификатора данных по тикеру и временному интервалу
        """
        return f'{class_code}.{sec_code}.{interval.name}'

    async def send_transaction(self, transaction) -> int:
        """Отправка транзакции в QUIK"""
        try:
            trans_id = await self.quik_api.trading.send_transaction(transaction)
            return trans_id
        except Exception as e:
            self.logger.error('Error sending transaction: %s', e)
            return -1

    async def get_accounts(self) -> list[Account]:
        """Получение списка счетов"""
        with self._lock_accounts:
            if not self._accounts:
                money_limits:MoneyLimitEx = await self.get_money_limits()
                if len(money_limits) == 0:
                    self.logger.error('get_money_limts: Ошибка нет лимитов по деньгам')
                    return self._accounts

                try:
                    lst:list[TradeAccounts] = await self._get_trade_accounts()
                    for acc in lst:
                        client_code = next((money_limit.client_code for money_limit in money_limits if money_limit.firm_id == acc.firm_id), None)
                        class_codes = acc.get_class_codes_list()
                        acc_info = Account(trade_account_id=acc.trd_acc_id, client_code=client_code,
                            firm_id=acc.firm_id, class_codes=class_codes
                            )
                        acc_info.futures = ('SPBFUT' in class_codes) or (acc.firm_id == self.p.futures_firm_id)
                        self._accounts.append(acc_info)
                except Exception as e:
                    self.logger.error('get_accounts: Ошибка получения списка счетов: %s', e)
            return self._accounts

    async def get_last_price(self, class_code: str, sec_code: str) -> float | None:
        with self._lock_qdata:
            data = self.qdata_last.get((class_code, sec_code), None)
        if data and len(data):
            return data.close[0]
        param_ex = await self._get_ticker_info_ex(class_code, sec_code, ParamNames.LAST)
        if param_ex:
            return param_ex.get_numeric_value()
        return None

    async def _get_step_price(self, class_code: str, sec_code: str) -> float | None:
        key = (class_code, sec_code)
        step_price = None
        if key in self._ticker_stepprice:
            step_price, ts = self._ticker_stepprice[key]
            if ts + timedelta(hours=1) < datetime.now(self.tz_msk):  # Если устарело значение шага цены
                step_price = None
        if step_price is None:
            param_ex = await self._get_ticker_info_ex(class_code, sec_code, ParamNames.STEPPRICE)
            if param_ex:
                step_price = param_ex.get_numeric_value()
                self._ticker_stepprice[key] = (step_price, datetime.now(self.tz_msk))
        return step_price

    async def quik_price_to_SUR(self, class_code: str, sec_code: str, quik_price: float) -> float:
        """
        Перевод цены QUIK в цену в рублях за штуку
        """
        si = await self.get_ticker_info(class_code, sec_code)
        if not si:
            return quik_price  # то цена не изменяется
        if class_code in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            return quik_price / 100 * si.face_value  # проценты номинала облигации
        elif class_code == 'SPBFUT':  # Фьючерсы
            step_price = await self._get_step_price(class_code, sec_code)
            if step_price is not None:
                lot_size = si.lot_size
                if step_price and lot_size > 1:
                    min_price_step = si.min_price_step
                    lot_price = quik_price // min_price_step * step_price
                    return lot_price / lot_size  # Цена за штуку
        return quik_price

    async def price_to_valid_price(self, class_code: str, sec_code: str, price: float) -> Decimal:
        """Перевод цены в рублях за штуку в корректную цену QUIK"""
        si = await self.get_ticker_info(class_code, sec_code)
        if not si:
            return Decimal(str(price))
        # Возращаем цену, которую примет QUIK в заявке
        order_price = Decimal(str(price))
        min_price_step = Decimal(str(si.min_price_step))
        order_price = (order_price // min_price_step) * min_price_step
        return order_price.quantize(Decimal(f'1e-{si.scale}'), rounding=ROUND_DOWN)

    async def _price_to_quik_price(self, class_code: str, sec_code: str, price: float) -> Decimal:
        """Перевод цены в рублях за штуку в цену QUIK"""
        si = await self.get_ticker_info(class_code, sec_code)
        if not si:
            return Decimal(str(price))
        min_price_step = si.min_price_step
        order_price = price
        if class_code in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            order_price = price * 100 / si.face_value  # проценты номинала облигации
        elif class_code == 'SPBFUT':  # Для рынка фьючерсов
            lot_size = si.lot_size
            step_price = await self._get_step_price(class_code, sec_code)
            if step_price and lot_size > 1:
                lot_price = price * lot_size
                order_price = lot_price / step_price * min_price_step
        # Возращаем цену, которую примет QUIK в заявке
        order_price = Decimal(str(order_price))
        min_price_step = Decimal(str(min_price_step))
        order_price = (order_price // min_price_step) * min_price_step
        return order_price.quantize(Decimal(f'1e-{si.scale}'), rounding=ROUND_DOWN)

