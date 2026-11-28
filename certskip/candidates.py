"""Bounded workload-derived candidate expansion; every result still needs proof."""
from .predicates import Interval, Region


def _hull(a, b):
    if tuple(n for n, _ in a.terms) != tuple(n for n, _ in b.terms):
        return None
    terms = []
    for (name, x), (_, y) in zip(a.terms, b.terms):
        if x == y:
            terms.append((name, x))
            continue
        values = [v for v in (x.lower, x.upper, y.lower, y.upper) if v is not None]
        if any(type(v) is not int for v in values):
            return None
        lower = None if x.lower is None or y.lower is None else min(x.lower, y.lower)
        upper = None if x.upper is None or y.upper is None else max(x.upper, y.upper)
        if lower is None and upper is None:
            # Dropping this dimension creates a valid broader conjunction.
            continue
        lc = any(i.lower == lower and i.lower_closed for i in (x, y))
        uc = any(i.upper == upper and i.upper_closed for i in (x, y))
        terms.append((name, Interval(lower, upper, lc, uc)))
    return Region(tuple(terms))


def generate_candidates(workload, limit=1000, strategy="queries"):
    if strategy not in ("queries", "merged"):
        raise ValueError("Candidate strategy must be queries or merged")
    observed = workload.candidates(limit)
    if strategy == "queries" or len(observed) < 2:
        return observed
    # Reserve half the cap for hulls; retain frequent exact queries first.
    # At most 4*limit pairs are considered, avoiding quadratic search.
    seeds = observed[:max(2, (limit + 1) // 2)]
    result = list(seeds)
    seen = set(result)
    attempts = 0
    for offset in range(1, len(seeds)):
        for i in range(len(seeds) - offset):
            if len(result) >= limit or attempts >= 4 * limit:
                break
            attempts += 1
            hull = _hull(seeds[i], seeds[i + offset])
            if hull is not None and hull not in seen:
                result.append(hull)
                seen.add(hull)
        if len(result) >= limit or attempts >= 4 * limit:
            break
    for query in observed:
        if len(result) >= limit:
            break
        if query not in seen:
            result.append(query)
            seen.add(query)
    return tuple(result)
