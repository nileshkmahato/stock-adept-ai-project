import re

def clean_number(text):
    if not text:
        return None
    text = text.replace("₹", "").replace(",", "").replace("−", "-").strip()
    try:
        return float(text)
    except ValueError:
        return text

def parse_change(text):
    if not text:
        return None, None

    text = text.replace("−", "-")
    abs_m = re.search(r"[-+]?\d+(\.\d+)?", text)
    pct_m = re.search(r"\(([-+]?\d+(\.\d+)?)%\)", text)

    return (
        float(abs_m.group()) if abs_m else None,
        float(pct_m.group(1)) if pct_m else None,
    )
