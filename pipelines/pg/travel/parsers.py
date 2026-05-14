from utils import get


def _lower(value):
    return value.lower() if isinstance(value, str) else value


def parse_inline(ad: dict, row: dict):
    return {
        "id": f"{row['id']}-{ad.get('rank')}",
        "user_id": row["related_user_id"],
        "rank": ad.get("rank"),
        "site": ad.get("site"),
        "headline": ad.get("headline"),
        "product_type": ad.get("productType"),
        "description": ad.get("description"),
        "booking_button_text": ad.get("bookingButtonText"),
        "cpc_price": get(ad, "cpcEstimate", "price"),
        "cpc_currency": get(ad, "cpcEstimate", "currency"),
    }


def inline_ad(row, origin_date, destination_date, origin, destination, cabin_class, travelers):
    for ad in get(row, "data", "kwargs", "inlineItems", default=[]):
        yield {
            **parse_inline(ad, row),
            "cabin_class": cabin_class,
            "travelers": travelers,
            "start_date": origin_date,
            "end_date": destination_date,
            "origin": str(origin),
            "destination": str(destination),
            "os": get(row, "data", "kwargs", "os"),
            "country": get(row, "data", "kwargs", "country"),
            "created_at": row["created_at"],
            "version": 2.0,
        }


def car_ads(row: dict):
    params = get(row, "data", "kwargs", "params")

    origin_date = get(params, "pickUpDate")
    destination_date = get(params, "dropOffDate")
    origin = get(params, "pickUpLocation", "locationQuery")
    destination = get(params, "dropOffLocation", "locationQuery")

    return inline_ad(row, origin_date, destination_date, origin, destination, None, None)


def flight_ads(row: dict):
    params = get(row, "data", "kwargs", "params")

    leg = get(params, "legs", 0)
    origin_date = get(leg, "date")
    origin = get(leg, "originAirport")
    destination = get(leg, "destinationAirport")
    cabin_class = get(params, "cabin")
    passengers = len(get(params, "passengers", default=[1]))

    return inline_ad(row, origin_date, origin_date, origin, destination, cabin_class, passengers)


def hotel_ads(row: dict):
    params = get(row, "data", "kwargs", "params")

    check_in_date = get(params, "checkinDate")
    check_out_date = get(params, "checkoutDate")
    origin = get(params, "cityId")
    adults = get(params, "adults")

    return inline_ad(row, check_in_date, check_out_date, origin, origin, None, adults)


def ad_request_stats(row: dict):
    name = row["name"]

    if name == "ad_fetch":
        data = row["data"]
        country = get(data, "countryCode")
        os = get(data, "OS")
        device_type = get(data, "deviceType")
        source = get(data, "source")
        vertical = row.get("vertical")
        items = get(data, "response", "inlineItems", default=[])
    else:
        kwargs = get(row, "data", "kwargs")
        country = get(kwargs, "country")
        os = get(kwargs, "os")
        device_type = get(kwargs, "deviceType")
        source = get(kwargs, "source")
        vertical = name.rsplit(".", 1)[-1] if name.startswith("InlineAdsViewSet.") else row.get("vertical")
        items = get(kwargs, "inlineItems", default=[])

    return {
        "id": row["id"],
        "user_id": row["related_user_id"],
        "name": name,
        "vertical": _lower(vertical),
        "country": _lower(country),
        "os": _lower(os),
        "device_type": _lower(device_type),
        "source": _lower(source),
        "ad_count": len(items),
        "request_time": row["created_at"],
    }


def legacy_inline_ad(row: dict):
    os = get(row, "data", "OS")
    country = get(row, "data", "countryCode")

    params = get(row, "data", "params")
    leg = get(params, "legs", 0)

    origin_date = get(leg, "date")
    destination_date = get(leg, "date")
    origin = get(leg, "originAirport")
    destination = get(leg, "destinationAirport")

    origin_date = get(params, "checkinDate", default=origin_date)
    destination_date = get(params, "checkoutDate", default=destination_date)
    origin = get(params, "cityId", default=origin)
    destination = get(params, "cityId", default=destination)

    origin_date = get(params, "pickUpDate", default=origin_date)
    destination_date = get(params, "dropOffDate", default=destination_date)
    origin = get(params, "pickUpLocation", "locationQuery", default=origin)
    destination = get(params, "dropOffLocation", "locationQuery", default=destination)

    occupants = get(params, "adults")
    passengers = get(params, "passengers", default=[1])
    travelers = occupants if occupants else len(passengers)

    cabin_class = get(params, "cabinClass")

    for ad in get(row, "data", "response", "inlineItems", default=[]):
        yield {
            **parse_inline(ad, row),
            "os": os,
            "country": country,
            "start_date": origin_date,
            "end_date": destination_date,
            "origin": origin,
            "destination": destination,
            "created_at": row["created_at"],
            "version": 1.0,
            "cabin_class": cabin_class,
            "travelers": travelers,
        }
