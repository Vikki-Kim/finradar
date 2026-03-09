"""
scripts/parse_rates.py
Парсер ставок банков — через внутренний API banki.ru
Banki.ru сам собирает ставки со всех банков ежедневно.
GitHub-серверы его не блокируют т.к. это публичный сайт.
"""
import asyncio
import json
import re
import httpx
from datetime import datetime

OUTPUT_FILE = "data/rates.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://www.banki.ru/",
    "Origin": "https://www.banki.ru",
}

async def get_key_rate() -> dict:
    print("→ ЦБ РФ: ключевая ставка")
    soap = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <KeyRate xmlns="http://web.cbr.ru/">
      <fromDate>2024-01-01</fromDate>
      <ToDate>2030-01-01</ToDate>
    </KeyRate>
  </soap:Body>
</soap:Envelope>"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx",
                content=soap,
                headers={"Content-Type": "text/xml; charset=utf-8",
                         "SOAPAction": "http://web.cbr.ru/KeyRate"},
            )
        rates = re.findall(r"<Val>([\d,\.]+)</Val>", resp.text)
        dates = re.findall(r"<DT>([\d\-T:]+)</DT>", resp.text)
        if rates:
            rate = float(rates[-1].replace(",", "."))
            date = dates[-1][:10] if dates else ""
            print(f"  КС ЦБ = {rate}% (от {date})")
            return {"rate": rate, "date": date}
    except Exception as e:
        print(f"  ЦБ SOAP ошибка: {e}")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://www.cbr-xml-daily.ru/keyrate.json")
        if resp.status_code == 200:
            data = resp.json()
            rate = data.get("KeyRate") or data.get("keyRate")
            if rate:
                print(f"  КС ЦБ (запасной) = {rate}%")
                return {"rate": float(rate), "date": datetime.now().strftime("%Y-%m-%d")}
    except Exception as e:
        print(f"  Запасной КС ошибка: {e}")

    print("  КС: используем fallback 21%")
    return {"rate": 21.0, "date": "2024-10-28", "fallback": True}


async def get_deposits() -> list:
    print("→ Banki.ru: вклады")
    results = []
    try:
        url = "https://www.banki.ru/investment/deposits/api/deposits/"
        params = {"page": 1, "page_size": 20, "currency": "RUR",
                  "order_field": "rate", "order": "desc", "type": "term"}
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            resp = await client.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("data", data.get("results", data.get("items", [])))
            for item in items[:15]:
                rate = item.get("rate") or item.get("max_rate") or item.get("interest_rate")
                if not rate:
                    continue
                bank = (item.get("bank") or {})
                bank_name = bank.get("name") if isinstance(bank, dict) else item.get("bank_name", "Банк")
                results.append({
                    "bank": bank_name,
                    "product": item.get("name", "Вклад"),
                    "rate": float(rate),
                    "term": item.get("term_label") or item.get("period") or "—",
                    "min_amount": float(item.get("min_amount") or 0),
                    "type": "deposit",
                    "is_promo": bool(item.get("is_promo") or item.get("special")),
                    "changed": 0,
                })
            if results:
                print(f"  Вклады (API): {len(results)} предложений")
                return results
    except Exception as e:
        print(f"  Banki.ru вклады ошибка: {e}")

    print("  Вклады: используем fallback")
    return [
        {"bank": "Т-Банк", "product": "Мой доход", "rate": 23.0, "term": "3 мес.", "min_amount": 50000, "type": "deposit", "is_promo": False, "changed": 0},
        {"bank": "МКБ", "product": "МКБ Максимум", "rate": 22.5, "term": "3 мес.", "min_amount": 10000, "type": "deposit", "is_promo": True, "changed": 0},
        {"bank": "Альфа-Банк", "product": "Альфа-Вклад", "rate": 21.0, "term": "3 мес.", "min_amount": 10000, "type": "deposit", "is_promo": False, "changed": 0},
        {"bank": "ВТБ", "product": "Надёжный", "rate": 20.5, "term": "3 мес.", "min_amount": 100000, "type": "deposit", "is_promo": False, "changed": 0},
        {"bank": "Сбербанк", "product": "СберВклад+", "rate": 20.0, "term": "3 мес.", "min_amount": 100000, "type": "deposit", "is_promo": False, "changed": 0},
    ]


async def get_savings() -> list:
    print("→ Banki.ru: накопительные счета")
    results = []
    try:
        url = "https://www.banki.ru/investment/deposits/api/deposits/"
        params = {"page": 1, "page_size": 10, "currency": "RUR",
                  "order_field": "rate", "order": "desc", "type": "saving"}
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            resp = await client.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()
            for item in (data.get("data") or data.get("results") or [])[:8]:
                rate = item.get("rate") or item.get("max_rate")
                if not rate:
                    continue
                bank = item.get("bank") or {}
                results.append({
                    "bank": bank.get("name") if isinstance(bank, dict) else item.get("bank_name", "Банк"),
                    "product": item.get("name", "Накопительный счёт"),
                    "rate": float(rate),
                    "term": "без срока",
                    "min_amount": float(item.get("min_amount") or 0),
                    "type": "savings",
                    "is_promo": False,
                    "changed": 0,
                })
            if results:
                print(f"  Накопительные: {len(results)} предложений")
                return results
    except Exception as e:
        print(f"  Накопительные ошибка: {e}")

    print("  Накопительные: используем fallback")
    return [
        {"bank": "Т-Банк", "product": "Накопительный счёт", "rate": 22.0, "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False, "changed": 0},
        {"bank": "Альфа-Банк", "product": "Альфа-Счёт", "rate": 20.0, "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False, "changed": 0},
        {"bank": "ВТБ", "product": "Накопительный ВТБ", "rate": 19.0, "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False, "changed": 0},
        {"bank": "Сбербанк", "product": "СберСчёт", "rate": 18.0, "term": "без срока", "min_amount": 0, "type": "savings", "is_promo": False, "changed": 0},
    ]


async def get_mortgage() -> list:
    print("→ Banki.ru: ипотека")
    results = []
    try:
        url = "https://www.banki.ru/products/hypothec/api/hypothec/"
        params = {"page": 1, "page_size": 15, "currency": "RUR", "order_field": "rate", "order": "asc"}
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            resp = await client.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()
            for item in (data.get("data") or data.get("results") or [])[:12]:
                rate = item.get("rate") or item.get("min_rate")
                if not rate:
                    continue
                bank = item.get("bank") or {}
                name = item.get("name", "Ипотека")
                name_l = name.lower()
                mtype = ("Семейная" if "семейн" in name_l else
                         "IT" if "it" in name_l else
                         "Дальневосточная" if "дальнев" in name_l else
                         "Сельская" if "сельск" in name_l else
                         "Льготная" if "льготн" in name_l else "Стандартная")
                results.append({
                    "bank": bank.get("name") if isinstance(bank, dict) else item.get("bank_name", "Банк"),
                    "product": name,
                    "rate": float(rate),
                    "type": "mortgage",
                    "mortgage_type": mtype,
                    "min_down_pct": float(item.get("first_payment") or 20),
                    "changed": 0,
                })
            if results:
                print(f"  Ипотека: {len(results)} предложений")
                return results
    except Exception as e:
        print(f"  Ипотека ошибка: {e}")

    print("  Ипотека: используем fallback")
    return [
        {"bank": "ДОМ.РФ", "product": "Дальневосточная ипотека", "rate": 2.0, "type": "mortgage", "mortgage_type": "Дальневосточная", "min_down_pct": 20, "changed": 0},
        {"bank": "Россельхозбанк", "product": "Сельская ипотека", "rate": 3.0, "type": "mortgage", "mortgage_type": "Сельская", "min_down_pct": 10, "changed": 0},
        {"bank": "Сбербанк", "product": "Семейная ипотека", "rate": 6.0, "type": "mortgage", "mortgage_type": "Семейная", "min_down_pct": 20, "changed": 0},
        {"bank": "ВТБ", "product": "IT-ипотека", "rate": 6.0, "type": "mortgage", "mortgage_type": "IT", "min_down_pct": 20, "changed": 0},
        {"bank": "Сбербанк", "product": "Стандартная ипотека", "rate": 25.8, "type": "mortgage", "mortgage_type": "Стандартная", "min_down_pct": 15, "changed": 0},
    ]


async def main():
    print(f"\n{'='*50}")
    print(f"ФинРадар запущен: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"{'='*50}\n")

    previous = {}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            old = json.load(f)
            for item in old.get("deposits", []) + old.get("savings", []):
                previous[f"{item['bank']}|{item['product']}"] = item["rate"]
    except FileNotFoundError:
        pass

    key_rate, deposits, savings, mortgage = await asyncio.gather(
        get_key_rate(), get_deposits(), get_savings(), get_mortgage()
    )

    changed = []
    for item in deposits + savings:
        key = f"{item['bank']}|{item['product']}"
        prev = previous.get(key)
        if prev and abs(prev - item["rate"]) >= 0.01:
            item["changed"] = round(item["rate"] - prev, 2)
            changed.append(item)
            arrow = "↑" if item["rate"] > prev else "↓"
            print(f"  🔔 {item['bank']} {item['product']}: {prev}% → {item['rate']}% {arrow}")

    deposits.sort(key=lambda x: x["rate"], reverse=True)
    savings.sort(key=lambda x: x["rate"], reverse=True)
    mortgage.sort(key=lambda x: x["rate"])

    output = {
        "updated_at": datetime.now().isoformat(),
        "updated_at_display": datetime.now().strftime("%d.%m.%Y в %H:%M"),
        "key_rate": key_rate,
        "deposits": deposits,
        "savings": savings,
        "mortgage": mortgage,
        "stats": {
            "total_products": len(deposits) + len(savings) + len(mortgage),
            "banks_count": len(set(r["bank"] for r in deposits + savings)),
            "best_deposit": max((r["rate"] for r in deposits), default=0),
            "best_savings": max((r["rate"] for r in savings), default=0),
            "changes_count": len(changed),
            "promos_count": sum(1 for r in deposits if r.get("is_promo")),
        },
        "changes": changed,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"✅ Сохранено в {OUTPUT_FILE}")
    print(f"   Вклады: {len(deposits)}, Счета: {len(savings)}, Ипотека: {len(mortgage)}")
    print(f"   КС ЦБ: {key_rate['rate']}%")
    print(f"   Изменений: {len(changed)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())
