"""Review tagger, version tagger-v1 (docs/api/review.md).

Deterministic keyword rules, no LLM, so the Review & Risk agent's evidence
is reproducible. A review can get several complaint tags; it is "positive"
only when no complaint matches.

Complaint tags: cleanliness, cancellation, missing_amenity, noise,
host_unresponsive, price_mismatch. Other tag: positive.
"""
import re

TAGGER_VERSION = "tagger-v1"

RULES = {
    "cleanliness": [r"\bdirty\b", r"\bdust", r"\bfilthy\b", r"\bunclean\b", r"\bnot clean\b",
                    r"\bstain", r"\bmold", r"\bsmell", r"cleanliness was (?:disappointing|poor|bad)"],
    "cancellation": [r"\bcancel"],
    "missing_amenity": [r"\bmissing\b", r"\bnot working\b", r"\bbroken\b", r"\bdid ?n[o']t have\b",
                        r"\bwas(?:n't| not) available\b", r"\bno (?:wifi|kitchen|parking|pool|washer)\b"],
    "noise": [r"\bnois", r"\bloud\b"],
    "host_unresponsive": [r"\bunresponsive\b", r"\bno response\b", r"\bnever (?:replied|responded)\b",
                          r"\bdid ?n[o']t (?:reply|respond)\b", r"\bslow to (?:reply|respond)\b"],
    "price_mismatch": [r"price mismatch", r"\b(?:unexpected|hidden|extra) (?:fee|charge)", r"\bovercharg"],
}
COMPLAINTS = list(RULES)


def tag(text):
    """Sorted list of tags for one review text."""
    t = text.lower()
    tags = [name for name, patterns in RULES.items() if any(re.search(p, t) for p in patterns)]
    return sorted(tags) or ["positive"]
