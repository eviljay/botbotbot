PRICE_TO_CREDITS = {
    49: 50,
    99: 120,
    199: 260,
}

def calc_credits_from_amount(amount: float) -> int:
    return PRICE_TO_CREDITS.get(int(amount), 0)