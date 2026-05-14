"""
SQL-backed product matcher.
Loads HUL products from Supabase once, caches in memory, scores by keyword overlap.
Zero Gemini calls — pure Python keyword matching.
"""
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_cache: Optional[list] = None


def _load_products() -> list:
    global _cache
    if _cache is not None:
        return _cache
    try:
        import db
        client = db.get_client()
        if not client:
            logger.warning("No Supabase client; product matching disabled.")
            _cache = []
            return _cache
        resp = client.table("hul_products").select(
            "brand, product_name, segment, concern, key_ingredients, keywords"
        ).execute()
        _cache = resp.data or []
        logger.info(f"Product matcher loaded {len(_cache)} HUL products from DB")
    except Exception as e:
        logger.error(f"Failed to load products for matching: {e}")
        _cache = []
    return _cache


_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "are", "was", "from", "has",
    "its", "can", "not", "but", "all", "new", "into", "our", "your", "more",
    "skin", "hair",  # too generic — kept in product tokens, stripped from query
    "users", "seek", "like", "also", "which", "when", "how", "make", "being",
    "helps", "help", "use", "used", "best", "good", "great", "india", "indian",
    "solutions", "issues", "common", "practical", "inspired", "tutorials", "bold",
    "seeking", "seeking", "they", "their", "them", "have", "been", "will", "one",
}

# Fix 10: short beauty ingredient codes that must not be filtered by length
_KNOWN_SHORT_TOKENS = {"aha", "bha", "pha", "spf", "uva", "uvb", "uv", "q10", "ce"}


def _tokens(text: str, strip_stopwords: bool = False) -> set[str]:
    if not text:
        return set()
    t = text.lower()
    parts = re.split(r'[\s,/|&;+\-_()]+', t)
    # Fix 10: keep tokens ≥ 3 chars OR known short beauty codes (AHA, BHA, SPF, UV…)
    toks = {p.strip() for p in parts if len(p.strip()) > 2 or p.strip() in _KNOWN_SHORT_TOKENS}
    if strip_stopwords:
        toks -= _STOPWORDS
    return toks


def _product_tokens(p: dict) -> tuple[set, set, set]:
    """Returns (concern_tokens, ingredient_tokens, all_tokens)."""
    concern_toks = _tokens(p.get('concern') or '')
    ingr_toks = _tokens(p.get('key_ingredients') or '')

    all_toks = set()
    all_toks.update(concern_toks)
    all_toks.update(ingr_toks)
    all_toks.update(_tokens(p.get('segment') or ''))
    all_toks.update(_tokens(p.get('product_name') or ''))
    for kw in (p.get('keywords') or []):
        all_toks.update(_tokens(kw or ''))

    return concern_toks, ingr_toks, all_toks


# Category → expected segment keywords for segment-aware boosting
_SEGMENT_SIGNALS = {
    "skincare":  {"skincare", "skin", "face", "moisturizer", "serum", "spf", "sunscreen", "acne", "brightening"},
    "makeup":    {"makeup", "lip", "kajal", "foundation", "blush", "mascara", "primer", "nail", "kohl", "eye"},
    "haircare":  {"hair", "shampoo", "conditioner", "scalp", "dandruff", "keratin", "oil", "serum"},
}

def _segment_bonus(p: dict, category_hint: str) -> int:
    """Return +2 if product segment matches expected category, -1 if it clearly doesn't."""
    if not category_hint:
        return 0
    cat = category_hint.lower()
    seg = (p.get("segment") or "").lower()
    prod_name = (p.get("product_name") or "").lower()

    # Determine expected segment from category hint
    expected = None
    for key in _SEGMENT_SIGNALS:
        if key in cat:
            expected = key
            break

    if not expected:
        return 0

    # Check product segment
    if expected in seg:
        return 2
    # Cross-segment penalty: makeup trend → don't recommend hair products
    if expected == "makeup" and "hair" in seg:
        return -2
    if expected == "skincare" and "hair" in seg and "face" not in prod_name:
        return -1
    if expected == "haircare" and ("makeup" in seg or "lip" in prod_name or "foundation" in prod_name):
        return -2
    return 0


def match_products(trend_text: str, limit: int = 3, category_hint: str = "") -> list[dict]:
    """
    Return up to `limit` HUL products most relevant to trend_text.
    category_hint ("Skincare", "Haircare", "Makeup") boosts segment-matched products.
    Each result: {brand, product_name, match_reason}
    """
    products = _load_products()
    if not products:
        return []

    query_toks = _tokens(trend_text, strip_stopwords=True)
    if not query_toks:
        return []

    scored = []
    seen_names: set = set()

    for p in products:
        concern_toks, ingr_toks, all_toks = _product_tokens(p)

        overlap = query_toks & all_toks
        if not overlap:
            continue

        score = len(overlap)
        if query_toks & concern_toks:
            score += 3
        if query_toks & ingr_toks:
            score += 2
        score += _segment_bonus(p, category_hint)

        if score <= 0:
            continue

        # Deduplicate by normalised product name prefix (catches size variants)
        name = (p.get('product_name') or '').replace('||', ' ').strip()
        name_key = name[:35].lower()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        scored.append({
            'score': score,
            'brand': p.get('brand', ''),
            'product_name': name,
            'match_reason': ', '.join(sorted(overlap)[:4]),
        })

    scored.sort(key=lambda x: -x['score'])
    return [
        {'brand': r['brand'], 'product_name': r['product_name'], 'match_reason': r['match_reason']}
        for r in scored[:limit]
    ]


def invalidate_cache():
    """Force reload on next call (use after re-importing products)."""
    global _cache
    _cache = None
