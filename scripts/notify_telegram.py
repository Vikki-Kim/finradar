"""
scripts/notify_telegram.py
Отправляет уведомления в Telegram при изменении ставок.
Запускается из GitHub Actions после parse_rates.py
"""
import json
import os
import httpx

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

def send(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("Telegram не настроен — пропускаем уведомление")
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        print("Telegram уведомление отправлено")
    except Exception as e:
        print(f"Ошибка Telegram: {e}")

def main():
    try:
        with open("data/rates.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("rates.json не найден")
        return

    changes = data.get("changes", [])
    stats = data.get("stats", {})
    updated = data.get("updated_at_display", "")

    if not changes and stats.get("changes_count", 0) == 0:
        print("Изменений нет — уведомление не нужно")
        return

    # Формируем сообщение
    msg = f"📡 <b>ФинРадар — Изменение ставок</b>\n"
    msg += f"<i>{updated}</i>\n\n"

    if changes:
        msg += "🔔 <b>Изменились ставки:</b>\n"
        for item in changes[:10]:
            arrow = "↑" if item.get("changed", 0) > 0 else "↓"
            diff = abs(item.get("changed", 0))
            msg += f"{arrow} <b>{item['bank']}</b> — {item['product']}\n"
            msg += f"   Новая ставка: <b>{item['rate']}%</b> ({arrow}{diff:.2f} п.п.)\n"

    # Добавляем топ вкладов
    deposits = data.get("deposits", [])
    if deposits:
        best = deposits[0]
        msg += f"\n💰 <b>Лучший вклад:</b> {best['bank']} — {best['rate']}%\n"
        if best.get("term"):
            msg += f"   {best['product']}, {best['term']}\n"

    # КС ЦБ
    kr = data.get("key_rate", {})
    if kr.get("rate"):
        msg += f"\n🏛 КС ЦБ РФ: <b>{kr['rate']}%</b>\n"

    msg += f"\n🔗 finradar.ru"

    send(msg)
    print(f"Уведомление отправлено: {len(changes)} изменений")

if __name__ == "__main__":
    main()
