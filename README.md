# Emoji Creation Bot

`Emoji Creation Bot` — Telegram-бот для создания собственных анимированных `custom emoji` паков на основе готовых шаблонов. Пользователь выбирает стиль пака, вводит текст или ник, при необходимости загружает SVG-логотип, смотрит превью и получает готовый набор эмодзи для Telegram.

## Что умеет бот

- Создаёт анимированные emoji pack'и из шаблонных TGS-стикеров.
- Подставляет пользовательский текст, ник и `username` в шаблоны.
- Поддерживает несколько типов паков, включая `passport`.
- Показывает превью перед созданием набора.
- Принимает SVG-логотипы и встраивает их в эмодзи.
- Позволяет выбирать шрифт для новых эмодзи.
- Создаёт наборы напрямую через Telegram Bot API.

## Как это работает

Бот загружает шаблоны из заранее заданных sticker set'ов, модифицирует их под данные пользователя, генерирует превью и затем создаёт новый `custom emoji` набор через методы Telegram API `uploadStickerFile`, `createNewStickerSet` и `addStickerToSet`.

Для хранения пользователей, операций и созданных паков используется локальный файл `db.json`. FSM-состояния работают в памяти через `MemoryStorage`.

## Стек

- Python 3.9+
- aiogram 3
- Pillow
- fonttools
- svgelements
- rlottie-python
- imageio-ffmpeg
- python-dotenv

## Переменные окружения

Создайте файл `.env` в корне проекта.

Обязательная переменная:

```env
BOT_TOKEN=your_telegram_bot_token
```

Опциональные переменные:

```env
SUPPORT_USERNAME=your_support_username
CHANNEL_USERNAME=your_channel_username
CHANNEL_URL=https://t.me/your_channel_username
PRIVACY_URL=https://example.com/privacy
TERMS_URL=https://example.com/terms
```

## Установка

```bash
python -m venv .venv
```

### Windows

```bash
.venv\Scripts\activate
pip install -r requirements.txt
```

### Linux / macOS

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

```bash
python bot.py
```

Если `BOT_TOKEN` не задан, приложение завершится с ошибкой.

## Структура проекта

- `bot.py` — основная логика бота, FSM, интерфейс, работа с Telegram API и создание паков.
- `sticker_utils.py` — обработка TGS/SVG, генерация текста, превью и подготовка анимированных эмодзи.
- `requirements.txt` — зависимости проекта.
- `db.json` — локальная JSON-база пользователей и операций.

## Особенности

- Для `passport`-режима используются отдельные шаги ввода имени, `username` и SVG-логотипа.
- SVG проходит валидацию перед использованием.
- Для ускорения работы применяются кэши шаблонов, файлов и результатов кастомизации.
- Интерфейс бота использует premium emoji в сообщениях и кнопках.

## Замечания

- Бот зависит от существующих шаблонных sticker set'ов, заданных в коде.
- Для корректного рендера превью должен быть установлен `rlottie-python`.
- Для работы с SVG должен быть установлен `svgelements`.
