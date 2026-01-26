import logging
from datetime import datetime, timedelta, time
from time import sleep
import os.path
import csv
import queue
import pandas as pd

from backtrader_next.feed import AbstractDataBase
from backtrader_next.utils.py3 import with_metaclass
from backtrader_next import TimeFrame, date2num
from quik_python.data_structures.candle import CandleInterval
from quik_python.data_structures.param_names import ParamNames

from .QuikStore import QuikStore



class MetaQuikData(AbstractDataBase.__class__):
    # noinspection PyMethodParameters
    def __init__(cls, name, bases, dct):
        super(MetaQuikData, cls).__init__(name, bases, dct)
        QuikStore.DataCls = cls


class QuikData(with_metaclass(MetaQuikData, AbstractDataBase)):
    """Данные QUIK"""
    params = (
        ('drop_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('live_bars', False),  # False - только история, True - история и новые бары
        ('count', 2000),
    )
    delimiter = ';'
    dt_format = '%Y-%m-%d %H:%M:%S'
    sleep_time_sec = 0.01  # Время ожидания в секундах (10 мс), если не пришел новый бар. Для снижения нагрузки/энергопотребления процессора
    delta = 3  # Корректировка в секундах при проверке времени окончания бара
    candle_interval: CandleInterval = None

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def __init__(self, **kwargs):
        self.store = QuikStore(**kwargs)  # Хранилище QUIK
        self.class_code, self.sec_code = QuikStore.run_sync(self.store.parse_ticker_name(self.p.dataname))
        # tf = self.store._bt_timeframe_to_str(self.p.timeframe, self.p.compression)
        self.candle_interval = self.store._bt_timeframe_2_quik(self.p.timeframe, self.p.compression)
        self._data_id = self.store._get_data_id(self.class_code, self.sec_code, self.candle_interval)
        self.logger = logging.getLogger(f'QuikData.{self._data_id}')
        self.derivative = False # Для деривативов не используем конвертацию цен и кол-ва
        self.file_name = f'{self.store.data_path}{self._data_id}.csv'  # Полное имя файла истории
        self.hist_candles = pd.DataFrame()
        self.hist_candles_pos = 0  # Позиция текущего бара в исторических данных
        self._new_bars = queue.Queue()
        self.dt_last_open = datetime.min  # Дата и время открытия последнего полученного бара
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения баров. False = История, True = Новые бары
        self.info = {}

    @property
    def data_id(self):
        return self._data_id

    def setenvironment(self, env):
        """Добавление хранилища QUIK в cerebro"""
        super(QuikData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища QUIK в cerebro

    def start(self):
        super(QuikData, self).start()

        self.derivative = self.class_code == self.store.p.futures_firm_id
        if self.p.live_bars:  # Если получаем историю и новые бары
            if not self.store._is_subscribed_to_candles(self.class_code, self.sec_code, self.candle_interval):
                self.store._subscribe_to_candles(self.class_code, self.sec_code, self.candle_interval)  # Подписываемся на новые бары
        self.info = self.store.get_ticker_info_sync(self.class_code, self.sec_code)

        cur_datetime = self.store._get_quik_datetime_now()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        df_file = self._load_bars_from_file()  # Получаем бары из файла
        last_dt = df_file.index[-1] if not df_file.empty else None
        count = self.p.count
        if last_dt is not None:
            delta_count = int(((cur_datetime - last_dt).total_seconds() / 60 // self.candle_interval.value) + 10)
            count = max(count, delta_count)

        df_candles = self._load_bars_from_history(count)  # Получаем бары из истории
        if not df_candles.empty and last_dt:
            df_candles = df_candles[df_candles.index >= last_dt]
            # Корректируем df_file, чтобы не было дубликата бара с last_dt
            if last_dt is not None and not df_candles.empty and df_candles.index[0] == last_dt:
                df_file = df_file.iloc[:-1]
            else:
                # Если last_dt не найден в df_candles, то очищаем df_file
                df_file = pd.DataFrame()

        df_list = [df for df in [df_file, df_candles] if not df.empty]
        candles = pd.concat(df_list).sort_index().drop_duplicates() if df_list else pd.DataFrame()
        if not candles.empty:
            # Фильтруем по времени
            mask = candles.index <= cur_datetime
            if self.p.fromdate:
                mask &= candles.index >= self.p.fromdate
            if self.p.todate:
                mask &= candles.index <= self.p.todate
            candles = candles[mask]

            if self.p.drop_price_doji:
                # Удаляем дожи 4-х цен
                mask_doji = candles['high'] == candles['low']
                doji_count = mask_doji.sum()
                if doji_count > 0:
                    self.logger.debug('Удаление дожи 4-х цен: %d бар(ов)', doji_count)
                    candles = candles[~mask_doji]
            # фильтруем по времени окончания бара
            candles["dtclose"] = [self.__get_bar_close_date_time(idx) for idx in candles.index]
            dt_quik_now = self.store._get_quik_datetime_now() +timedelta(seconds=self.delta)
            mask_close = candles["dtclose"] <= dt_quik_now
            candles = candles[mask_close]
            candles.drop(columns=["dtclose"], inplace=True)

        self.dt_last_open = candles.index[-1] if not candles.empty else datetime.min
        if not candles.empty:
            self.logger.debug('Загружено бар из файла и истории: %d с %s по %s', len(candles), candles.index[0], candles.index[-1])  

        if not candles.empty:
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических бар
        self.hist_candles = candles
        self._save_bars_to_file(candles)  # Сохраняем полученные бары в файл истории
        if self.p.live_bars:  # Если получаем историю и новые бары
            self.store._register_data(self)


    def stop(self):
        super(QuikData, self).stop()
        if self.p.live_bars:  # Если была подписка/расписание
            if self.store._is_subscribed_to_candles(self.class_code, self.sec_code, self.candle_interval):
                self.logger.debug('Отмена подписки %s', self.data_id)
                self.store._unsubscribe_from_candles(self.class_code, self.sec_code, self.candle_interval)
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых бар

        # Отменяем регистрацию данных
        self.store._unregister_data(self)
        self.store.DataCls = None  # Удаляем класс данных в хранилище
        # Останавливаем store если нет других активных подписок
        if len(self.store.datas) == 0:
            self.store.stop()

    def load(self):
        with self.store._lock_store_data:
            return super().load()

    def _load(self):
        """Загрузка бара из истории или нового бара"""
        if self.hist_candles_pos < len(self.hist_candles):
            row = self.hist_candles.iloc[self.hist_candles_pos]
            bar = row.to_dict()
            bar['datetime'] = row.name  # Добавляем индекс (datetime) в словарь
            self.hist_candles_pos += 1
        elif not self.p.live_bars:
            self.put_notification(self.DISCONNECTED)
            self.logger.debug('Бары из файла/истории отправлены в ТС. Новые бары получать не нужно. Выход')
            return False
        else:
            if self._new_bars.empty():  # Если новый бар еще не появился
                if self.live_mode:
                    sleep(self.sleep_time_sec)  # Ждем для снижения нагрузки/энергопотребления процессора
                return None  # то нового бара нет, будем заходить еще

            self.last_bar_received = self._new_bars.qsize() == 1
            bar = self._new_bars.get()
            if self.last_bar_received:  # Получаем последний возможный бар
                self.logger.debug('Получение последнего возможного на данный момент бара')

            if not self._is_bar_valid(bar):
                return None  # то пропускаем бар, будем заходить еще

            self._save_bar_to_file(bar)  # Сохраняем бар в конец файла
            if self.last_bar_received and not self.live_mode:
                self.put_notification(self.LIVE)
                self.live_mode = True
            elif self.live_mode and not self.last_bar_received:
                self.put_notification(self.DELAYED)
                self.live_mode = False

        self.lines.datetime[0] = date2num(bar['datetime'])  # Переводим в формат хранения даты/времени в BackTrader
        self.lines.open[0] = bar['open']
        self.lines.high[0] = bar['high']
        self.lines.low[0] = bar['low']
        self.lines.close[0] = bar['close']
        self.lines.volume[0] = int(bar['volume'])
        self.lines.openinterest[0] = 0
        return True

    def _on_new_candle(self, candle):
        """Обработка нового бара из хранилища QUIK"""
        bar = {
            'datetime': candle.datetime.to_datetime(),
            'open': candle.open,
            'high': candle.high,
            'low': candle.low,
            'close': candle.close,
            'volume': candle.volume,
        }
        self._new_bars.put(bar)


    def _load_bars_from_file(self) -> pd.DataFrame:
        """Получение бар из файла"""
        if not os.path.isfile(self.file_name):
            return pd.DataFrame()
        self.logger.debug('Получение бар из файла %s', self.file_name)
        df = pd.read_csv(self.file_name, delimiter=self.delimiter, parse_dates=["datetime"], index_col=0)
        df.index.name = 'datetime'
        if len(df) > 0:
            self.logger.debug('Получено бар из файла: %d с %s по %s', len(df), df.index[0], df.index[-1])
        else:
            self.logger.debug('Из файла новых бар не получено')
        return df

    def _save_bars_to_file(self, df:pd.DataFrame) -> None:
        """Сохранение бар в файл"""
        self.logger.debug('Сохранение бар в файл %s', self.file_name)
        df.to_csv(self.file_name, sep=self.delimiter, date_format=self.dt_format)

    def _load_bars_from_history(self, count:int=1000) -> pd.DataFrame:
        """Получение бар из истории"""
        self.logger.debug('Получение всех бар из истории')
        df_candles = self.store._get_last_candles(self.class_code, self.sec_code, self.candle_interval, count)
        if df_candles is None:
            self.logger.debug('Ошибка загрузки из истории новых бар не получено')
            raise Exception('Ошибка загрузки из истории новых бар не получено')
        return df_candles

    def _save_bar_to_file(self, bar) -> None:
        """Сохранение бара в конец файла"""
        if not os.path.isfile(self.file_name):
            self.logger.warning('Файл %s не найден и будет создан', self.file_name)
            with open(self.file_name, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter=self.delimiter)
                writer.writerow(bar.keys())
        with open(self.file_name, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=self.delimiter)
            csv_row = bar.copy()
            csv_row['datetime'] = csv_row['datetime'].strftime(self.dt_format)
            writer.writerow(csv_row.values())


    def _is_bar_valid(self, bar) -> bool:
        """Проверка бара на соответствие условиям выборки"""
        dt_open = bar['datetime']  # Дата и время открытия бара МСК
        if dt_open <= self.dt_last_open:
            # Если пришел бар из прошлого (дата открытия меньше последней даты открытия)
            self.logger.debug(f'Дата/время открытия бара {dt_open} <= последней даты/времени открытия {self.dt_last_open}')
            return False
        if (self.p.fromdate and dt_open < self.p.fromdate) or (self.p.todate and dt_open > self.p.todate):
            self.logger.debug('Дата/время открытия бара %s за границами диапазона %s - %s', dt_open, self.p.fromdate, self.p.todate)
            self.dt_last_open = dt_open
            return False
        if self.p.drop_price_doji and bar['high'] == bar['low']:
            self.logger.debug('Бар %s - дожи 4-х цен', dt_open)
            self.dt_last_open = dt_open
            return False

        dt_close = self.__get_bar_close_date_time(dt_open)  # Дата и время закрытия бара
        dt_market_now = self.store._get_quik_datetime_now()  # Datetime из QUIK
        dt_market_now_corrected = dt_market_now + timedelta(seconds=self.delta)
        # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
        if dt_close > dt_market_now_corrected:
            self.logger.debug(f'Datetime {dt_close} закрытия бара на {dt_open} еще не наступило. Текущее время {dt_market_now}')
            return False

        self.dt_last_open = dt_open
        return True


    def __get_bar_close_date_time(self, dt_open):
        """Дата и время закрытия бара"""
        if self.p.timeframe == TimeFrame.Days:
            return dt_open + timedelta(days=1)
        elif self.p.timeframe == TimeFrame.Weeks:
            return dt_open + timedelta(weeks=1)
        elif self.p.timeframe == TimeFrame.Months:
            month = dt_open.month + 1
            year = dt_open.year
            if month > 12:
                month = 1
                year += 1
            return datetime(year, month, 1)
        elif self.p.timeframe == TimeFrame.Years:
            return dt_open.replace(year=dt_open.year + 1)
        elif self.p.timeframe == TimeFrame.Minutes:
            return dt_open + timedelta(minutes=self.p.compression)
        elif self.p.timeframe == TimeFrame.Seconds:
            return dt_open + timedelta(seconds=self.p.compression)
        raise NotImplementedError

