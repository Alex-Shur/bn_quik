## Основные команды

### Установка и настройка

#### Способ 1: Установка UV через встроенный скрипт
```bash
# На Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# На Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Способ 2: Установка UV через pip
```bash
pip install uv
```

#### Инициализация проекта
```bash
uv sync
```

### Запуск примеров
ex1.py  - печатает приходящие бары
```bash
uv run ex1.py
```

ex2.py - стратегия пересечения 2х SMA, торгует акцию Сбербанк, подключение Demo Quik
```bash
uv run ex2.py
```

ex2_futures.py  - стратегия пересечения 2х SMA, торгует фьючерс на серебро SVH6, подключение Demo Quik
```bash
uv run ex1.py
```

