<div align="center">

# bn_quik  

[![PyPi Release](https://img.shields.io/pypi/v/bn_quik?color=32a852&label=PyPi)](https://pypi.org/project/bn_quik/)
[![Total downloads](https://img.shields.io/pepy/dt/bn_quik?label=%E2%88%91&color=skyblue)](https://pypistats.org/packages/bn_quik)
[![Made with Python](https://img.shields.io/badge/Python-3.11+-c7a002?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/github/license/Alex-Shur/bn_quik?color=9c2400)](https://github.com/Alex-Shur/bn_quik/blob/master/LICENSE)
</div>

Live trading intergartion of backtrader-next with QUIK trade terminal

Установка
================
```
pip install bn_quik
```

Использование
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
