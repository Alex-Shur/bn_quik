<div align="center">

# bn_quik  

[![PyPi Release](https://img.shields.io/pypi/v/bn_quik?color=32a852&label=PyPi)](https://pypi.org/project/bn_quik/)
[![Total downloads](https://img.shields.io/pepy/dt/bn_quik?label=%E2%88%91&color=skyblue)](https://pypistats.org/packages/bn_quik)
[![Made with Python](https://img.shields.io/badge/Python-3.11+-c7a002?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/github/license/Alex-Shur/bn_quik?color=9c2400)](https://github.com/Alex-Shur/bn_quik/blob/master/LICENSE)
</div>

Интеграция [Backtrader-Next](https://github.com/smalinin/backtrader_next) с торговым терминалом QUIK для реальной торговли


Установка
================
Внимание для работы **bn_quik** необходимо в торговом терминале QUIK установить и запустить Lua коннектор.
```
pip install bn_quik
```

Параметры объектов
================

#### QuikStore
|Параметр| Значение по умолчанию | Обязательный |  |
|---|---|---|---|
|trade_account_id|  | Да | Торговый счет
|client_code_for_orders| | Да(для Finam) | Номер торгового терминала. У брокера Финам требуется для совершения торговых операций|
|host| "127.0.0.1"|  | Host с Quik Lua коннектором|
|port| 34130 |  | |
|lots| True | | # Входящий остаток в лотах (задается брокером)
|limit_kind| 1 | | Основной режим торгов T1 (Для Demo Quik -1)
|currency| "SUR" | |  Валюта |
|futures_firm_id| "SPBFUT"| | Идентификатор фирмы для фьючерсов|
|edp| False| | Единая денежная позиция
|slippage_steps| 10 |  | Кол-во шагов цены для проскальзывания, для рыночных ордеров
|data_dir| "DataQuik" | |  Каталог для хранения данных, свечные данные тикеров и состояние объекта Broker с ордерами
----

#### QuikData
|Параметр| Значение по умолчанию | Обязательный |  |
|---|---|---|---|
|drop_price_doji| True | |  False - не пропускать дожи 4-х цен, True - пропускать
|live_bars| False | | False - только история, True - история и новые бары
|count| 2000 | | Количество запрашиваемых исторических баров по умолчанию

Как получить Demo доступ к Quik
================
Демо доступ можно получить на сервере [ARQA](https://arqatech.com/ru/support/demo/)

Примеры использования коннектора 
================
Все примеры находятся в папке **examples**

Использование Lua коннектора
================
Cкопировать содержимое папки **lua** c [GIT репозитария](https://github.com/Alex-Shur/bn_quik) 
в отдельную папку, которая будет доступна приложению QUIK.

> ***ВНИМАНИЕ** Для корректной работы с получением свечных данных используйте обновлённые Lua скрипты из [QUIK Python](https://github.com/Alex-Shur/quik_python) 
или [bn_quik](https://github.com/Alex-Shur/bn_quik)
Данные Lua скрипты будут также корректно работать и с QUIKSharp клиентами.*

В терминале QUIK, через диалоговое окно работы со скриптами Lua, запустить "QuikSharp.lua" из скопированной ранее папки. [Подробнее о Lua скриптах](lua/USAGE.RU.md).

В случае возникновения проблем с работоспособностью демонстрационных приложений убедитесь что:
1. Терминал QUIK загружен и подключен к сереверу.
2. Скрипт QuikSharp.lua запущен и не выдает никаких ошибок в соответствующем диалоговом окне.
3. Никакие сторонние программы не используют для своих нужд порты 34130 и 34131. 
    Данные порты используются по умолчанию для связи библиотеки с терминалом.
4. Проверьте настройки что соединения не блокируются в Windows Firewall. 
