import asyncio
from core_logic import collect_raw_data, generate_ai_summary

async def test():
    raw = await collect_raw_data()
    print("Collectors:", {k: len(v) for k, v in raw.items() if isinstance(v, list)})
    ai = await generate_ai_summary()
    print("AI trends:", len(ai.get('trends', [])))
    for t in ai['trends']:
        products = t.get('hul_products', [])
        p_names = [p.get('product_name') or p.get('name') or 'Unknown' for p in products]
        print(f"  [{t.get('label')}] {t.get('trend_name')} | products: {p_names}")

asyncio.run(test())
