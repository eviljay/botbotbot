from typing import Any, Dict, List


def find_keyword_items(node: Any) -> List[Dict[str, Any]]:
    """
    Рекурсивно шукає перший список dict'ів, у яких є ключ 'keyword'.
    Це той самий код, що спрацював у debug-скрипті.
    """
    if isinstance(node, list):
        if node and all(isinstance(x, dict) and "keyword" in x for x in node):
            return node

        for x in node:
            found = find_keyword_items(x)
            if found:
                return found

    elif isinstance(node, dict):
        for v in node.values():
            found = find_keyword_items(v)
            if found:
                return found

    return []


def filter_keywords(
    items: List[Dict[str, Any]],
    min_search_volume: int = 10,
) -> List[Dict[str, Any]]:
    """
    Фільтр для keyword-ів:
    - search_volume >= min_search_volume
    - можна легко додати ще фільтри (competition, brand/non-brand і т.д.)
    """
    filtered: List[Dict[str, Any]] = []

    for item in items:
        sv = item.get("search_volume") or 0
        if sv < min_search_volume:
            continue
        filtered.append(item)

    return filtered
