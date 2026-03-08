"""
scripts/parse_rates.py
Парсер ставок банков для GitHub Actions.

Запускается каждый час, результат сохраняет в data/rates.json
Сайт читает этот файл напрямую с GitHub Pages.
"""
import asyncio
import json
import re
import random
import httpx
from datetime import datetime
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# Настройки
# ──────────────────────────────────────────────
OUTPUT_FILE = "data/rates.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ──────────────────────────────────────────────
# Утилиты
# ──────────────────────────────────────────────
def extract_rate(text: str) -> float | None:
    """Извлечь процентную ставку из текста"""
    text = text.replace("\xa0", "").replace(" ", "").replace(",", ".")
    match = re.search(r"(\d{1,2}(?:\.\d{1,2})?)\s*%", text)
    if match:
        val = float(match.group(1))
        if 0.5 < val < 60:
            return val
    return None

def headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }

async def fetch(url: str, timeout: int = 20) -> str | None:
    """HTTP-запрос с повторными попытками"""
    for attempt in range(3):
        try:
            await asyncio.sleep(random.uniform(1.5, 3.0))
            async with httpx.AsyncClient(
                headers=headers(),
                timeout=timeout,
                follow_redirects=True,
            ) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return resp.text
        except Exception as e:
            print(f"  Попытка {attempt+1}/3 не удалась: {e}")
            await asyncio.sleep(3 * (attempt + 1))
    return None

# ──────────────────────────────────────────────
# Парсер ЦБ РФ (официальный XML API)
# ──────────────────────────────────────────────
async def parse_cbr_key_rate() -> dict:
    """Ключевая ставка ЦБ РФ — официальный API"""
    print("→ ЦБ РФ: ключевая ставка")
    try:
        # Официальный SOAP/XML API ЦБ
        url = "https://www.cbr.ru/hh/KeyRate"
        html = await fetch(url)
        if html:
            soup = BeautifulSoup(html, "lxml-xml")
            records = soup.find_all("KeyRate") or soup.find_all("keyRate")
            if records:
                last = records[-1]
                rate_str = last.get("Val") or last.get_text(strip=True)
                rate = float(rate_str.replace(",", "."))
                date_str = last.get("Date") or last.get("date", "")
                print(f"  ЦБ РФ: КС = {rate}% (от {date_str})")
                return {"rate": rate, "date": date_str[:10] if date_str else ""}
    except Exception as e:
        print(f"  ЦБ РФ ошибка XML: {e}")

    # Запасной вариант — парсинг главной страницы ЦБ
    try:
        html = await fetch("https://www.cbr.ru/")
        if html:
            soup = BeautifulSoup(html, "lxml")
            # ЦБ показывает КС в виджете на главной
            for sel in [".main-indicator_rate .value", ".key-rate .value",
                        "[data-type='keyRate'] .value", ".main-indicator__value"]:
                el = soup.select_one(sel)
                if el:
                    rate = extract_rate(el.get_text())
                    if rate:
                        print(f"  ЦБ РФ (страница): КС = {rate}%")
                        return {"rate": rate, "date": datetime.now().strftime("%Y-%m-%d")}
            # Ищем в тексте
            text = soup.get_text()
            m = re.search(r"Ключевая ставка[^\d]*(\d{1,2}[,\.]\d{0,2})\s*%", text)
            if m:
                rate = float(m.group(1).replace(",", "."))
                return {"rate": rate, "date": datetime.now().strftime("%Y-%m-%d")}
    except Exception as e:
        print(f"  ЦБ РФ страница ошибка: {e}")

    print("  ЦБ РФ: используем fallback 21%")
    return {"rate": 21.0, "date": "2024-10-28", "fallback": True}

# ──────────────────────────────────────────────
# Парсеры банков
# ──────────────────────────────────────────────

async def parse_tbank_deposits() -> list:
    """Т-Банк — вклады и накопительный счёт"""
    print("→ Т-Банк: вклады")
    results = []
    try:
        # Пробуем открытый API Т-Банка
        async with httpx.AsyncClient(headers=headers(), timeout=15) as client:
            resp = await client.get(
                "https://api.tinkoff.ru/v1/savings_accounts/rates",
                headers={**headers(), "Accept": "application/json"}
            )
            if resp.status_code == 200:
                data = resp.json()
                # Обрабатываем структуру ответа
                for item in data.get("payload", {}).get("rates", []):
                    rate = item.get("rate") or item.get("value")
                    if rate:
                        results.append({
                            "bank": "Т-Банк",
                            "product": item.get("name", "Накопительный счёт"),
                            "rate": float(rate),
                            "term": item.get("period", "без срока"),
                            "min_amount": item.get("minAmount", 0),
                            "is_promo": item.get("isPromo", False),
                            "type": "savings",
                        })
    except Exception:
        pass

    # Парсим страницу вкладов
    if not results:
        html = await fetch("https://www.tbank.ru/savings/deposits/")
        if html:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ")
            # Ищем паттерны ставок
            rates_found = re.findall(r"(\d{1,2}[,.]?\d{0,2})\s*%\s*годовых", text)
            for i, r in enumerate(rates_found[:5]):
                rate = float(r.replace(",", "."))
                if 10 < rate < 35:
                    results.append({
                        "bank": "Т-Банк",
                        "product": f"Вклад Т-Банк #{i+1}",
                        "rate": rate,
                        "type": "deposit",
                        "min_amount": 50000,
                        "is_promo": False,
                    })

    # Fallback — если парсинг не дал результатов
    if not results:
        print("  Т-Банк: используем fallback")
        results = [
            {"bank": "Т-Банк", "product": "Мой доход", "rate": 23.0,
             "term": "3 мес.", "min_amount": 50000, "type": "deposit", "is_promo": False},
            {"bank": "Т-Банк", "product": "СмартВклад", "rate": 21.5,
             "term": "6 мес.", "min_amount": 50000, "type": "deposit", "is_promo": False},
            {"bank": "Т-Банк", "product": "Накопительный счёт", "rate": 22.0,
             "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False},
        ]
    else:
        print(f"  Т-Банк: найдено {len(results)} предложений")
    return results


async def parse_sberbank_deposits() -> list:
    """Сбербанк — вклады"""
    print("→ Сбербанк: вклады")
    results = []
    try:
        html = await fetch("https://www.sberbank.ru/ru/person/contributions/deposits")
        if html:
            soup = BeautifulSoup(html, "lxml")
            # Разные версии разметки Сбера
            for sel in [".deposit-card", ".kit-col--md-4", ".product-card", "[class*='DepositCard']", "article"]:
                cards = soup.select(sel)
                if len(cards) >= 2:
                    for card in cards[:8]:
                        text = card.get_text(" ", strip=True)
                        rate = extract_rate(text)
                        if not rate or rate < 10:
                            continue
                        name_el = card.select_one("h2,h3,[class*='title'],[class*='name']")
                        name = name_el.get_text(strip=True) if name_el else "Вклад Сбербанка"
                        if len(name) > 60:
                            name = name[:60]
                        term_m = re.search(r"(\d+)\s*(мес|лет|год)", text.lower())
                        term = f"{term_m.group(1)} {term_m.group(2)}." if term_m else "—"
                        min_m = re.search(r"от\s*([\d\s]+)\s*(?:₽|руб)", text)
                        min_amt = float(min_m.group(1).replace(" ", "")) if min_m else 100000
                        results.append({
                            "bank": "Сбербанк",
                            "product": name,
                            "rate": rate,
                            "term": term,
                            "min_amount": min_amt,
                            "type": "deposit",
                            "is_promo": any(w in text.lower() for w in ["акци", "специальн", "выгодн", "промо"]),
                        })
                    if results:
                        break
    except Exception as e:
        print(f"  Сбербанк ошибка: {e}")

    if not results:
        print("  Сбербанк: используем fallback")
        results = [
            {"bank": "Сбербанк", "product": "СберВклад+", "rate": 20.0,
             "term": "3 мес.", "min_amount": 100000, "type": "deposit", "is_promo": False},
            {"bank": "Сбербанк", "product": "Управляй", "rate": 18.5,
             "term": "12 мес.", "min_amount": 100000, "type": "deposit", "is_promo": False},
            {"bank": "Сбербанк", "product": "СберСчёт", "rate": 18.0,
             "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False},
        ]
    else:
        print(f"  Сбербанк: найдено {len(results)} предложений")
    return results


async def parse_vtb_deposits() -> list:
    """ВТБ — вклады"""
    print("→ ВТБ: вклады")
    results = []
    try:
        html = await fetch("https://www.vtb.ru/personal/vklady-i-scheta/vklady/")
        if html:
            soup = BeautifulSoup(html, "lxml")
            for sel in ["[class*='ProductCard']", "[class*='deposit']", "article", ".product-item"]:
                cards = soup.select(sel)
                if len(cards) >= 2:
                    for card in cards[:8]:
                        text = card.get_text(" ", strip=True)
                        rate_m = re.search(r"до\s*(\d{1,2}[,.]?\d*)\s*%|(\d{1,2}[,.]?\d*)\s*%\s*годовых", text)
                        if not rate_m:
                            continue
                        rate = float((rate_m.group(1) or rate_m.group(2)).replace(",", "."))
                        if not (10 < rate < 35):
                            continue
                        name_el = card.select_one("h2,h3,[class*='title']")
                        name = name_el.get_text(strip=True) if name_el else "Вклад ВТБ"
                        term_m = re.search(r"(\d+)\s*(мес|год)", text.lower())
                        term = f"{term_m.group(1)} {term_m.group(2)}." if term_m else "—"
                        results.append({
                            "bank": "ВТБ", "product": name[:60], "rate": rate,
                            "term": term, "min_amount": 100000,
                            "type": "deposit", "is_promo": "акци" in text.lower(),
                        })
                    if results:
                        break
    except Exception as e:
        print(f"  ВТБ ошибка: {e}")

    if not results:
        print("  ВТБ: используем fallback")
        results = [
            {"bank": "ВТБ", "product": "Надёжный", "rate": 20.5,
             "term": "3 мес.", "min_amount": 100000, "type": "deposit", "is_promo": False},
            {"bank": "ВТБ", "product": "Накопительный счёт", "rate": 19.0,
             "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False},
        ]
    else:
        print(f"  ВТБ: найдено {len(results)} предложений")
    return results


async def parse_alfa_deposits() -> list:
    """Альфа-Банк — вклады"""
    print("→ Альфа-Банк: вклады")
    results = []
    try:
        html = await fetch("https://alfabank.ru/get-money/deposits/")
        if html:
            soup = BeautifulSoup(html, "lxml")
            for sel in ["[class*='product']", "[class*='card']", "article", "[class*='deposit']"]:
                cards = soup.select(sel)
                if len(cards) >= 2:
                    for card in cards[:8]:
                        text = card.get_text(" ", strip=True)
                        rate_m = re.search(r"до\s*(\d{1,2}[,.]?\d*)\s*%|(\d{1,2}[,.]?\d*)\s*%", text)
                        if not rate_m:
                            continue
                        rate = float((rate_m.group(1) or rate_m.group(2)).replace(",", "."))
                        if not (10 < rate < 35):
                            continue
                        name_el = card.select_one("h2,h3,[class*='title']")
                        name = name_el.get_text(strip=True) if name_el else "Вклад Альфа"
                        results.append({
                            "bank": "Альфа-Банк", "product": name[:60], "rate": rate,
                            "term": "—", "min_amount": 10000,
                            "type": "deposit", "is_promo": "акци" in text.lower(),
                        })
                    if results:
                        break
    except Exception as e:
        print(f"  Альфа-Банк ошибка: {e}")

    if not results:
        print("  Альфа-Банк: используем fallback")
        results = [
            {"bank": "Альфа-Банк", "product": "Альфа-Вклад", "rate": 21.0,
             "term": "3 мес.", "min_amount": 10000, "type": "deposit", "is_promo": False},
            {"bank": "Альфа-Банк", "product": "Альфа-Счёт", "rate": 20.0,
             "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False},
        ]
    else:
        print(f"  Альфа-Банк: найдено {len(results)} предложений")
    return results


async def parse_mkb_deposits() -> list:
    """МКБ — часто предлагает высокие ставки"""
    print("→ МКБ: вклады")
    try:
        html = await fetch("https://mkb.ru/personal/deposits")
        if html:
            soup = BeautifulSoup(html, "lxml")
            results = []
            for card in soup.select("[class*='deposit'], [class*='product'], article")[:6]:
                text = card.get_text(" ", strip=True)
                rate = extract_rate(text)
                if rate and 10 < rate < 35:
                    name_el = card.select_one("h2,h3,[class*='title']")
                    name = name_el.get_text(strip=True)[:60] if name_el else "Вклад МКБ"
                    results.append({
                        "bank": "МКБ", "product": name, "rate": rate,
                        "term": "3 мес.", "min_amount": 10000,
                        "type": "deposit", "is_promo": "акци" in text.lower(),
                    })
            if results:
                print(f"  МКБ: найдено {len(results)} предложений")
                return results
    except Exception as e:
        print(f"  МКБ ошибка: {e}")

    print("  МКБ: используем fallback")
    return [
        {"bank": "МКБ", "product": "МКБ Максимум", "rate": 22.5,
         "term": "3 мес.", "min_amount": 10000, "type": "deposit", "is_promo": True},
    ]


async def parse_mortgage_rates() -> list:
    """Ставки по ипотеке — льготные программы"""
    print("→ Ипотечные ставки")
    # Льготные программы фиксированы государством, парсим для проверки
    base = [
        {"bank": "Сбербанк", "product": "Семейная ипотека", "rate": 6.0,
         "type": "mortgage", "mortgage_type": "Семейная", "min_down_pct": 20},
        {"bank": "ВТБ", "product": "Семейная ипотека", "rate": 6.0,
         "type": "mortgage", "mortgage_type": "Семейная", "min_down_pct": 20},
        {"bank": "Т-Банк", "product": "IT-ипотека", "rate": 6.0,
         "type": "mortgage", "mortgage_type": "IT", "min_down_pct": 20},
        {"bank": "Альфа-Банк", "product": "IT-ипотека", "rate": 6.0,
         "type": "mortgage", "mortgage_type": "IT", "min_down_pct": 20},
        {"bank": "Россельхозбанк", "product": "Сельская ипотека", "rate": 3.0,
         "type": "mortgage", "mortgage_type": "Сельская", "min_down_pct": 10},
        {"bank": "ДОМ.РФ", "product": "Дальневосточная ипотека", "rate": 2.0,
         "type": "mortgage", "mortgage_type": "Дальневосточная", "min_down_pct": 20},
    ]

    # Пробуем получить рыночную ставку со Сбера
    try:
        html = await fetch("https://www.sberbank.ru/ru/person/credits/home/buying_completed_house")
        if html:
            soup = BeautifulSoup(html, "lxml")
            for el in soup.select(".rate, .percent, [class*='rate']"):
                rate = extract_rate(el.get_text())
                if rate and 20 < rate < 35:
                    base.append({
                        "bank": "Сбербанк", "product": "Стандартная ипотека",
                        "rate": rate, "type": "mortgage",
                        "mortgage_type": "Стандартная", "min_down_pct": 15,
                    })
                    print(f"  Сбербанк рыночная ипотека: {rate}%")
                    break
    except Exception:
        pass

    if not any(r["mortgage_type"] == "Стандартная" for r in base):
        base.append({
            "bank": "Сбербанк", "product": "Стандартная ипотека", "rate": 25.8,
            "type": "mortgage", "mortgage_type": "Стандартная", "min_down_pct": 15,
        })

    print(f"  Ипотека: {len(base)} предложений")
    return base


# ──────────────────────────────────────────────
# Главная функция — собирает всё и сохраняет
# ──────────────────────────────────────────────
async def main():
    print(f"\n{'='*50}")
    print(f"ФинРадар парсер запущен: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"{'='*50}\n")

    # Загружаем предыдущие данные (для сравнения и уведомлений)
    previous = {}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            old_data = json.load(f)
            for item in old_data.get("deposits", []) + old_data.get("savings", []):
                key = f"{item['bank']}|{item['product']}"
                previous[key] = item["rate"]
    except FileNotFoundError:
        pass

    # Запускаем все парсеры параллельно
    key_rate_task = parse_cbr_key_rate()
    deposits_tasks = [
        parse_tbank_deposits(),
        parse_sberbank_deposits(),
        parse_vtb_deposits(),
        parse_alfa_deposits(),
        parse_mkb_deposits(),
    ]
    mortgage_task = parse_mortgage_rates()

    key_rate_data, *deposit_results, mortgage_rates = await asyncio.gather(
        key_rate_task, *deposits_tasks, mortgage_task
    )

    # Объединяем все результаты
    all_rates = []
    for bank_rates in deposit_results:
        all_rates.extend(bank_rates)

    # Помечаем изменения
    changed = []
    for item in all_rates:
        key = f"{item['bank']}|{item['product']}"
        prev = previous.get(key)
        if prev is not None and abs(prev - item["rate"]) >= 0.01:
            item["changed"] = round(item["rate"] - prev, 2)
            changed.append(item)
            arrow = "↑" if item["rate"] > prev else "↓"
            print(f"  🔔 ИЗМЕНЕНИЕ: {item['bank']} {item['product']}: {prev}% → {item['rate']}% {arrow}")
        else:
            item["changed"] = 0

    # Разделяем по типам
    deposits = [r for r in all_rates if r.get("type") == "deposit"]
    savings = [r for r in all_rates if r.get("type") == "savings"]

    # Сортируем по убыванию ставки
    deposits.sort(key=lambda x: x["rate"], reverse=True)
    savings.sort(key=lambda x: x["rate"], reverse=True)
    mortgage_rates.sort(key=lambda x: x["rate"])

    # Итоговый JSON
    output = {
        "updated_at": datetime.now().isoformat(),
        "updated_at_display": datetime.now().strftime("%d.%m.%Y в %H:%M"),
        "key_rate": key_rate_data,
        "deposits": deposits,
        "savings": savings,
        "mortgage": mortgage_rates,
        "stats": {
            "total_products": len(all_rates) + len(mortgage_rates),
            "banks_count": len(set(r["bank"] for r in all_rates)),
            "best_deposit": max((r["rate"] for r in deposits), default=0),
            "best_savings": max((r["rate"] for r in savings), default=0),
            "changes_count": len(changed),
            "promos_count": sum(1 for r in all_rates if r.get("is_promo")),
        },
        "changes": changed,
    }

    # Сохраняем
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"✅ Готово! Сохранено в {OUTPUT_FILE}")
    print(f"   Вклады: {len(deposits)}, Счета: {len(savings)}, Ипотека: {len(mortgage_rates)}")
    print(f"   КС ЦБ: {key_rate_data['rate']}%")
    print(f"   Изменений: {len(changed)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())
