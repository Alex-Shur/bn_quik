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
```bash
uv run ex1.py
```

