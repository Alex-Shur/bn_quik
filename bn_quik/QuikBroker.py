import time
import threading
import asyncio
from decimal import Decimal
import logging  # Будем вести лог
from collections import defaultdict, OrderedDict, deque  # Словари и очередь
from datetime import datetime, date

from backtrader_next import BrokerBase, Order, BuyOrder, SellOrder
from backtrader_next.position import Position
from backtrader_next.utils.py3 import with_metaclass
from quik_python.data_structures import Transaction, Trade, TransactionReply
from quik_python.data_structures.money_limit_ex import MoneyLimitEx
from quik_python.data_structures.order import OrderTradeFlags
from quik_python.data_structures.portfolio_info_ex import PortfolioInfoEx
from quik_python.data_structures.stop_order import StopOrder
from quik_python.data_structures.transaction_types import TransactionAction, TransactionOperation, TransactionType

from .QuikStore import QuikStore, Account


class MetaQuikBroker(BrokerBase.__class__):
    # noinspection PyMethodParameters
    def __init__(cls, name, bases, dct):
        super(MetaQuikBroker, cls).__init__(name, bases, dct)
        QuikStore.BrokerCls = cls


# noinspection PyProtectedMember,PyArgumentList
class QuikBroker(with_metaclass(MetaQuikBroker, BrokerBase)):

    params = (
        ('slippage_steps', 10),  # Кол-во шагов цены для проскальзывания
        ('client_code_for_orders', None),  # Номер торгового терминала. У брокера Финам требуется для совершения торговых операций
        ('trade_account_id', None),
    )

    account:Account = None

    def __init__(self, **kwargs):
        super(QuikBroker, self).__init__()
        self.logger = logging.getLogger('QuikBroker')
        self.store = QuikStore(**kwargs)
        self.notifs = deque()  # Очередь уведомлений брокера о заявках
        self.cash = 0.0  # текущие свободные средства
        self.value = 0.0  # текущая стоимость всех позиций + текущие свободне средства
        self.trade_nums = {}  # Список номеров сделок по тикеру для фильтрации дублей сделок
        self.orders = OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = defaultdict(deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)
        self._positions = defaultdict(Position)  ##!!

        # Thread synchronization locks для безопасности многопоточного доступа
        # Threading locks используются для данных с гибридным доступом
        # (sync методы из main thread + async методы из event loop thread).
        # Это безопасно, т.к. event loop работает в отдельном потоке (см. QuikStore.MetaSingleton).
        
        self._lock_orders = threading.RLock()    # Гибридный: Sync(buy/sell/cancel) + Async(place_order/_on_trans_reply/_on_trade/cancel_order)
        self._lock_trades = None                 # Asyncio Lock: Только Async(_on_trade). Инициализируется в start()
        self._lock_notifs = threading.Lock()     # Гибридный: Sync(get_notification/next) + Async(_on_trans_reply/_on_trade)
        self._lock_cash = threading.Lock()       # Гибридный: Sync(getcash/getvalue) + Async(_getcash/_getvalue/_on_trade)
        self._lock_positions = threading.RLock() # Гибридный: Sync(getposition) + Async(_getvalue/_get_all_active_positions)
        
        # Примечание: _lock_trades использует asyncio.Lock для оптимальной производительности в async коде.
        # Остальные блокировки используют threading.Lock т.к. доступ к данным происходит из разных потоков.

    async def __get_account(self):
        if not self.account:
            acc_list = await self.store.get_accounts()
            self.account = next((a for a in acc_list if a.trade_account_id == self.p.trade_account_id), None)
            # Если для заявок брокер устанавливает отдельный код клиента, то задаем его в параметре client_code_for_orders
            # В остальных случаях получаем код клиента из заявки (счета). Для фьючерсов кода клиента нет
            if self.account:
                self.account.order_client_code = self.p.client_code_for_orders if self.p.client_code_for_orders else self.account.client_code
                self.account.is_ucp = await self.store._is_ucp_client(self.account.firm_id, self.account.client_code)
                self.logger.info(f"Account {self.account.trade_account_id} is UCP: {self.account.is_ucp}")
        return self.account

    async def __start_a(self):
        # Инициализируем asyncio.Lock для _lock_trades в контексте event loop
        if self._lock_trades is None:
            self._lock_trades = asyncio.Lock()
        
        self.store.broker = self
        self.account = await self.__get_account()
        if not self.account:
            raise ValueError(f'QuikBroker: Не найден счет с trade_account_id={self.p.trade_account_id}')
        await self._get_all_active_positions()
        self.cash = await self._getcash()
        self.value = await self._getvalue(None)

    def start(self):
        super(QuikBroker, self).start()
        self.store.__class__.run_sync(self.__start_a())

    def stop(self):
        super(QuikBroker, self).stop()
        self.store.broker = None
        self.store.BrokerCls = None

    def getcash(self):
        """Свободные средства по счету"""
        if not self.store.BrokerCls:
            return 0
        with self._lock_cash:
            return self.cash

    def getvalue(self, datas=None):
        """
        Стоимость всех позиций, позиции/позиций, по счету + свободные средства
        если datas==None
        """
        if not self.store.BrokerCls:  # Если брокера нет в хранилище
            return 0
        if datas is None:
            with self._lock_cash:
                return self.value + self.cash
        else:
            return self.store.__class__.run_sync(self._getvalue(datas))

    def getposition(self, data):
        """Позиция по тикеру
        """
        with self._lock_positions:
            return self._positions[data.p.dataname]


    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.store.__class__.run_sync(self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, **kwargs))
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на покупку на биржу
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.store.__class__.run_sync(self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, **kwargs))
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке новой заявки на продажу на биржу
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.store.__class__.run_sync(self.cancel_order(order))

    def get_notification(self):
        with self._lock_notifs:
            if not self.notifs:
                return None
            return self.notifs.popleft()

    def next(self):
        with self._lock_notifs:
            self.notifs.append(None)

    async def create_order(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера."""
        if is_buy:
            order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype,
                            valid=valid, oco=oco, parent=parent, transmit=transmit)
        else:
            order = SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype,
                            valid=valid, oco=oco, parent=parent, transmit=transmit)
        order.addcomminfo(self.getcommissioninfo(data))
        order.addinfo(**kwargs)
        class_code = data.class_code
        sec_code = data.sec_code

        if order.exectype in (Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical):
            # Эти типы заявок не реализованы
            self.logger.warning('Постановка заявки %s по тикеру %s.%s отклонена. Работа с заявками %s не реализована', order.ref, class_code, sec_code, order.exectype)
            order.reject(self)
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order

        order.addinfo(account=self.account)  # Передаем в заявку счет
        si = await self.store._get_ticker_info(class_code, sec_code)
        if not si:
            self.logger.error('create_order: Постановка заявки %s по тикеру %s.%s отменена. Тикер не найден', order.ref, class_code, sec_code)
            order.reject(self)
            return order

        order.addinfo(min_price_step=float(si.min_price_step))
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок

        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                self.logger.error('create_order: Постановка заявки %s по тикеру %s.%s отменена. Родительская заявка не найдена', order.ref, class_code, sec_code)
                order.reject(self)
                return order
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)

        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return await self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return await self.place_order(order.parent)  # Отправляем родительскую заявку на биржу

        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим


    # TODO add support FILL_OR_KILL - exec_type
    async def place_order(self, order: Order):
        """Отправка заявки (транзакции) на биржу"""
        class_code = order.data.class_code
        sec_code = order.data.sec_code
        # Размер позиции в лотах.
        if order.data.derivative:
            quantity = abs(order.size)  # Для деривативов размер позиции уже в штуках
            order.size = await self.store._lots_to_size(class_code, sec_code, order.size)
        else:
            quantity = abs(await self.store._size_to_lots(class_code, sec_code, order.size))

        trans_id = int(time.time() * 1000) % 100000000  # time in milliseconds
        order.info["trans_id"] = trans_id

        transaction = Transaction()
        transaction.TRANS_ID = trans_id
        transaction.CLIENT_CODE = order.info['account'].order_client_code
        transaction.ACCOUNT = order.info['account'].trade_account_id
        transaction.CLASSCODE = class_code
        transaction.SECCODE = sec_code
        transaction.OPERATION = TransactionOperation.B if order.isbuy() else TransactionOperation.S
        transaction.QUANTITY = int(quantity)  # Кол-во в лотах
        transaction.ACTION = TransactionAction.NEW_ORDER if order.exectype in (Order.Market, Order.Limit) else TransactionAction.NEW_STOP_ORDER

        min_price_step = order.info['min_price_step']  # Получаем из заявки минимальный шаг цены
        # TODO FIXME next based on my Robot code #alex
        slippage = min_price_step * self.store.p.slippage_steps  # Размер проскальзывания в деньгах для выставления рыночной цены фьючерсов

        # Рыночная заявка
        if order.exectype == Order.Market:
            transaction.TYPE = TransactionType.M
            if order.data.derivative:  # Для деривативов
                last_price = float(await self.store._get_last_price(class_code, sec_code))
                # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
                last_price = last_price + slippage if order.isbuy() else last_price - slippage
                market_price = await self.store._price_to_valid_price(class_code, sec_code, last_price)
            else:
                market_price = Decimal(0)  # Цена рыночной заявки должна быть нулевой
            transaction.PRICE = market_price  # Рыночную цену QUIK ставим в заявку

        # Лимитная заявка
        elif order.exectype == Order.Limit:
            transaction.TYPE = TransactionType.L  # Лимитная заявка
            limit_price = await self.store._price_to_valid_price(class_code, sec_code, order.price)
            transaction.PRICE = limit_price  # Лимитную цену QUIK Ставим в заявку

        # Стоп заявка
        elif order.exectype == Order.Stop:
            stop_price = await self.store._price_to_valid_price(class_code, sec_code, order.price)
            transaction.STOPPRICE = stop_price  # Стоп цену QUIK ставим в заявкуСтавим в заявку
            if order.data.derivative:  # Для деривативов
                market_price = stop_price + slippage if order.isbuy() else stop_price - slippage
                market_price = await self.store._price_to_valid_price(class_code, sec_code, market_price)  # Из документации QUIK: При покупке/продаже фьючерсов по рынку нужно ставить цену хуже последней сделки
            else:  # Для остальных рынков
                market_price = Decimal(0)  # Цена рыночной заявки должна быть нулевой
            transaction.PRICE = market_price  # Рыночную цену QUIK ставим в заявку

        # Стоп-лимитная заявка
        elif order.exectype == Order.StopLimit:
            stop_price = await self.store._price_to_valid_price(class_code, sec_code, order.price)
            transaction.STOPPRICE = stop_price  # Стоп цену QUIK ставим в заявку
            limit_price = await self.store._price_to_valid_price(class_code, sec_code, order.pricelimit)
            transaction.PRICE = limit_price  # Лимитную цену QUIK Ставим в заявку

        # Для стоп заявок
        if order.exectype in (Order.Stop, Order.StopLimit):
            expiry_date = 'GTC'  # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            if order.valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiry_date = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(order.valid, date):  # Если заявка поставлена до даты
                expiry_date = order.valid.strftime('%Y%m%d')  # то будем держать ее до указанной даты
            transaction.EXPIRY_DATE = expiry_date  # Срок действия стоп заявки

        order.addinfo(op='new')
        trans_id = await self.store._send_transaction(transaction)
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        if trans_id < 0:  # Если возникла ошибка при постановке заявки на уровне QUIK
            self.logger.error('place_order: Ошибка отправки заявки в QUIK %s.%s  %s', class_code, sec_code, transaction.error_message)  # то заявка не отправляется на биржу, выводим сообщение об ошибке
            order.addinfo(op='error')
            order.reject(self)

        with self._lock_orders:
            self.orders[trans_id] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order


    async def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():
            return None

        trans_id = order.info["trans_id"]
        with self._lock_orders:
            if trans_id not in self.orders:
                return None

        order_num = order.info['order_num']
        is_stop = order.exectype in [Order.Stop, Order.StopLimit]
        is_stop_order = is_stop and await self.store._get_order_by_number(order_num) is None

        transaction = Transaction()
        transaction.TRANS_ID = trans_id
        transaction.CLASSCODE = order.data.class_code
        transaction.SECCODE = order.data.sec_code

        if is_stop_order:  # Для стоп заявки
            transaction.ACTION = TransactionAction.KILL_STOP_ORDER
            transaction.STOP_ORDER_KEY = str(order_num)
        else:  # Для лимитной заявки
            transaction.ACTION = TransactionAction.KILL_ORDER
            transaction.ORDER_KEY = str(order_num)
        order.addinfo(op='cancel')
        await self.store._send_transaction(transaction)
        return order

    async def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        for order_ref, oco_ref in self.ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                await self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = self.ocos[order.ref]  # то получаем номер транзакции связанной заявки
            await self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    await self.place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    await self.cancel_order(child)  # Отменяем дочернюю заявку


    async def _on_trans_reply(self, data: TransactionReply):
        """Обработчик события ответа на транзакцию пользователя"""
        self.logger.debug('on_trans_reply: data=%s', str(data))
        qk_trans_reply = data
        order_num = qk_trans_reply.order_num
        trans_id = qk_trans_reply.trans_id
        with self._lock_orders:
            if trans_id not in self.orders:  # Пришла заявка не из автоторговли
                self.logger.debug('on_trans_reply: Заявка с номером %s. Номер транзакции %s. Заявка была выставлена не из торговой системы. Выход', order_num, trans_id)
                return
            order: Order = self.orders[trans_id]
        order.addinfo(order_num=order_num)
        self.logger.debug('on_trans_reply: Заявка %s с номером %s. Номер транзакции %s.', order.ref, order_num, trans_id)
        # result_msg = qk_trans_reply.result_msg.lower()
        status = qk_trans_reply.status  # Статус транзакции
        if status == 3 or status == 15:
            order_op = order.info["op"]
            if order_op == 'new':
                self.logger.debug('on_trans_reply: Заявка %s переведена в статус принята на бирже (Order.Accepted)', order.ref)
                order.accept(self)  # Заявка принята на бирже (Order.Accepted)
                order.addinfo(op='done')
            elif order_op == 'cancel':
                self.logger.debug('on_trans_reply: Заявка %s переведена в статус отменена (Order.Canceled)', order.ref)
                with self.store._lock_store_data:
                    order.cancel()  # Отменяем существующую заявку (Order.Canceled)
                order.addinfo(op='done')
        elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):  # Транзакция не выполнена (ошибка заявки):
            # - Не найдена заявка для удаления
            # - Вы не можете снять данную заявку
            # - Превышен лимит отправки транзакций для данного логина
            if status in (4, 5):
                self.logger.debug('on_trans_reply: Заявка %s. Ошибка. Выход', order.ref)
                self.logger.error('on_trans_reply: Заявка %s. Ошибка. Выход', order.ref)
                order.addinfo(op='error')
                return  # то заявку не отменяем, выходим, дальше не продолжаем
            try:
                self.logger.debug('on_trans_reply: Заявка %s переведена в статус отклонена (Order.Rejected)', order.ref)
                with self.store._lock_store_data:
                    order.reject(self)  # Отклоняем заявку (Order.Rejected)
            except (KeyError, IndexError) as e:
                self.logger.error('on_trans_reply: Exception for change order.status: %s', e)
                order.status = Order.Rejected  # все равно ставим статус заявки Order.Rejected
            order.addinfo(op='rejected')
        elif status == 6:  # Транзакция не прошла проверку лимитов сервера QUIK
            try:
                self.logger.debug('on_trans_reply: Заявка %s переведена в статус не прошла проверку лимитов (Order.Margin)', order.ref)
                with self.store._lock_store_data:
                    order.margin()  # Для заявки не хватает средств (Order.Margin)
            except (KeyError, IndexError) as e:
                self.logger.error('on_trans_reply: Exception for change order.status: %s', e)
                order.status = Order.Margin  # все равно ставим статус заявки Order.Margin
            order.addinfo(op='margin')
        with self._lock_notifs:
            self.notifs.append(order.clone())  # Уведомляем брокера о заявке
        if order.status != Order.Accepted:  # Если новая заявка не зарегистрирована
            self.logger.debug('on_trans_reply: Заявка %s. Проверка связанных и родительских/дочерних заявок', order.ref)
            await self.oco_pc_check(order)  # то проверяем связанные и родительскую/дочерние заявки (Canceled, Rejected, Margin)
        self.logger.debug('on_trans_reply: Заявка %s. Выход', order.ref)


    async def _on_trade(self, data: Trade):
        """Обработчик события получения новой / изменения существующей сделки.
        Выполняется до события изменения существующей заявки. Нужен для определения цены исполнения заявок.
        """
        self.logger.debug('on_trade: data=%s\n---------------------------------', str(data))  # Для отладки
        qk_trade = data
        trade_num = qk_trade.trade_num  # Номер сделки (дублируется 3 раза)
        order_num = qk_trade.order_num  # Номер заявки на бирже
        trans_id = qk_trade.trans_id
        with self._lock_orders:
            if trans_id not in self.orders:
                self.logger.debug('on_trade: Заявка с номером %s. Номер транзакции %s. Заявка была выставлена не из торговой системы. Выход', order_num, trans_id)
                return
            order: Order = self.orders[trans_id]
        order.addinfo(order_num=order_num)  # Сохраняем номер заявки на бирже (может быть переход от стоп заявки к лимитной с изменением номера на бирже)
        self.logger.debug('on_trade: Заявка %s с номером %s. Номер транзакции %s. Номер сделки %s order=%s', order.ref, order_num, trans_id, trade_num, order)
        class_code = qk_trade.class_code  # Код режима торгов
        sec_code = qk_trade.sec_code  # Код тикера

        dataname = self.store._get_ticker_name(class_code, sec_code)

        # Защита от дублей сделок (критичная секция - check-then-act)
        # Используем asyncio.Lock для оптимальной работы в async контексте
        async with self._lock_trades:
            if dataname not in self.trade_nums.keys():  # Если это первая сделка по тикеру
                self.trade_nums[dataname] = []  # то ставим пустой список сделок
            elif trade_num in self.trade_nums[dataname]:  # Если номер сделки есть в списке (фильтр для дублей)
                self.logger.debug('on_trade: Заявка %s. Номер сделки %s есть в списке сделок (дубль). Выход', order.ref, trade_num)
                return
            self.trade_nums[dataname].append(trade_num)  # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)

        size = qk_trade.qty  # Абсолютное кол-во
        if self.store.p.lots:  # Если входящий остаток в лотах
            size = await self.store._lots_to_size(class_code, sec_code, size)  # то переводим кол-во из лотов в штуки
        if qk_trade.flags & OrderTradeFlags.IS_SELL.value:
            size *= -1  # Продажа - кол-во ставим отрицательным
        price = float(qk_trade.price) # Цена сделки
        self.logger.debug('on_trade: Заявка %s. size=%s, price=%s', order.ref, size, price)
        try:
            with self.store._lock_store_data:
                dt = order.data.datetime[0]  # Дата и время исполнения заявки. Последняя известная
            self.logger.debug('on_trade: Заявка %s. Дата/время исполнения заявки по бару %s', order.ref, dt)
        except (KeyError, IndexError) as e:  # При ошибке
            self.logger.error('on_trade: Exception for get datetime of order: %s', e)
            dt = datetime.now(self.store.tz_msk)
            self.logger.debug('on_trade: Заявка %s. Дата/время исполнения заявки по текущему %s', order.ref, dt)

        position = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = position.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
        with self._lock_orders:
            order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if order.executed.remsize:  # Если заявка исполнена частично (осталось что-то к исполнению)
            self.logger.debug('on_trade: Заявка %s исполнилась частично. Остаток к исполнения %s', order.ref, order.executed.remsize)
            if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                self.logger.debug('on_trade: Заявка %s переведена в статус частично исполнена (Order.Partial)', order.ref)
                order.partial()  # Переводим заявку в статус Order.Partial
                with self._lock_notifs:
                    self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если заявка исполнена полностью (ничего нет к исполнению)
            self.logger.debug('on_trade: Заявка %s переведена в статус полностью исполнена (Order.Completed)', order.ref)
            order.completed()  # Переводим заявку в статус Order.Completed
            with self._lock_notifs:
                self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self.logger.debug('on_trade: Заявка %s. Проверка связанных и родительских/дочерних заявок', order.ref)
            await self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
        self.logger.debug('on_trade: Заявка %s. Выход', order.ref)
        with self._lock_cash:
            self.cash = await self._getcash()
            self.value = await self._getvalue(None)


    async def _on_order(self, order : Order):
        self.logger.debug('on_order trans_id=%s order_num=%s  ext_status=%s  flags=%s  state=%s', order.trans_id, order.order_num, order.ext_order_status, order.flags, order.state)

    async def _on_stop_order(self, stop_order:StopOrder):
        self.logger.debug('on_stop_order trans_id=%s order_num=%s  linked_order=%s  flags=%s  state=%s', stop_order.trans_id, stop_order.order_num, stop_order.linked_order, stop_order.flags, stop_order.state)


    def submit(self, order):
        """Отправка заявки на биржу (требуется BrokerBase)"""
        return self.store.__class__.run_sync(self.place_order(order))


    def add_order_history(self, orders, notify=True):
        """Добавление истории заявок (требуется BrokerBase)"""
        # TODO: Реализовать добавление истории заявок
        pass


    def set_fund_history(self, fund_history):
        """Установка истории средств (требуется BrokerBase)"""
        # TODO: Реализовать установку истории средств
        pass


    async def _getvalue(self, datas=None):
        """
        если datas is None:
            Equity = Стоимость всех позиций по счету + наличные деньги на счете
        иначе:
            Стоимость всех позиций по тикерам в datas
        """
        value = 0.0
        with self._lock_positions:
            for ticker_name, position in self._positions.items():
                if datas and not any(data.p.dataname == ticker_name for data in datas):
                    continue
                class_code, sec_code = await self.store._parse_ticker_name(ticker_name)
                last_price =  await self.store._get_last_price(class_code, sec_code)
                if last_price:
                    last_price = await self.store._quik_price_to_SUR(class_code, sec_code, last_price)
                    value += position.size * last_price
        return value

    async def _get_all_active_positions(self):  ##!! CHECK ME and UseME
        positions = defaultdict(Position)
        acc = self.account
        if not acc:
            raise ValueError('QuikBroker: Не задан account для получения позиций')
        if acc.futures:
            futures_holdings = await self.store._get_futures_client_holdings()
            for fut in futures_holdings:
                if fut.total_net != 0:
                    self.logger.debug("Futures Position: %s TotalNet: %s AvrPosnPrice: %s", fut.sec_code, fut.total_net, fut.avr_pos_nprice)
                    class_code = "SPBFUT"
                    sec_code = fut.sec_code
                    size = int(fut.total_net)
                    if self.store.p.lots:
                        size = await self.store._lots_to_size(class_code, sec_code, size)
                    dataname = self.store._get_ticker_name(class_code, sec_code)
                    price = await self.store._quik_price_to_SUR(class_code, sec_code, fut.avr_pos_nprice)
                    positions[dataname] = Position(size, price)
        else:
            depo_limits = await self.store._get_all_depo_limits()
            account_depo_limits = [limit for limit in depo_limits
                                    if limit.client_code == acc.client_code and
                                    limit.firm_id == acc.firm_id and
                                    limit.limit_kind.value == self.store.p.limit_kind and
                                    limit.current_bal != 0]
            for limit in account_depo_limits:
                class_code, sec_code = await self.store._parse_ticker_name(limit.sec_code)
                size = int(limit.current_bal)
                if self.store.p.lots:  # Если входящий остаток в лотах
                    size = await self.store._lots_to_size(class_code, sec_code, size)
                # Переводим средневзвешенную цену приобретения позиции (входа) в цену в рублях за штуку
                price = await self.store._quik_price_to_SUR(class_code, sec_code, float(limit.wa_position_price))
                dataname = self.store._get_ticker_name(class_code, sec_code)
                positions[dataname] = Position(size, price)
        with self._lock_positions:
            self._positions = positions

    async def _getcash(self):
        """Получение текущего баланса по всем счетам"""
        self.logger.debug('call _getcash()')
        money_limits:MoneyLimitEx = await self.store._get_money_limits()
        if len(money_limits) == 0:
            self.logger.error("_getcash: Ошибка получения баланса - нет лимитов по деньгам")
            return 0.0
        cash = 0.0
        acc = self.account
        if not acc:
            raise ValueError('QuikBroker: Не задан account для получения позиций')
        if acc.futures:
            if self.store.p.edp:
                portf:PortfolioInfoEx = await self.store._get_portfolio_info_ex(acc.firm_id, acc.client_code)
                if portf:
                    cash += portf.all_assets
                else:
                    self.logger.error('_getcash: QUIK не вернул информацию по счету с firm_id=%s, client_code=%s. Проверьте правильность значений', acc.firm_id, acc.client_code)
            else:
                # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
                fut_limits = await self.store._get_futures_limit(acc.firm_id, acc.trade_account_id, self.store.p.currency)
                if fut_limits:
                    cash += fut_limits.cbp_limit + fut_limits.var_margin + fut_limits.accruedint
                else:
                    self.logger.error('_getcash: QUIK не вернул фьючерсные лимиты с firm_id=%s, trade_account_id=%s. Проверьте правильность значений', acc.firm_id, acc.trade_account_id)
        else:
            balance = None
            for limit in money_limits:
                if (limit.client_code == acc.client_code
                    and limit.firm_id == acc.firm_id
                    and limit.limit_kind.value == self.store.p.limit_kind
                    and limit.curr_code == self.store.p.currency
                    ):
                    balance = limit.current_bal
                    break
            if balance is None:
                self.logger.error('_getcash: QUIK не вернул денежный лимит для client_code=%s, firm_id=%s. Проверьте правильность значений', acc.client_code, acc.firm_id)
            else:
                cash += balance
        return cash

