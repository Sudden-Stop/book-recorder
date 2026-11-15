from . import cross, kalshi, polymarket

PARSERS = {"polymarket": polymarket, "kalshi": kalshi, "cross": cross}


def tables_of(venue: str) -> tuple[str, ...]:
    return PARSERS[venue].TABLES
