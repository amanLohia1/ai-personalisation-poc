def normalize_country_string(value):
    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    mapping = {
        "CA": "Canada",
        "Canada": "Canada",
        "GB": "United Kingdom",
        "United Kingdom": "United Kingdom",
        "IN": "India",
        "India": "India",
        "US": "United States",
        "United States": "United States",
    }

    return mapping.get(value, value)
