from utils import get


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
        "cpc": ad.get("cpcEstimate"),
    }


def inline_ad(row, origin_date, destination_date, origin, destination):
    for ad in get(row, "data", "kwargs", "inlineItems", default=[]):
        yield {
            **parse_inline(ad, row),
            "start_date": origin_date,
            "end_date": destination_date,
            "origin": origin,
            "destination": destination,
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

    return inline_ad(row, origin_date, destination_date, origin, destination)


def flight_ads(row: dict):
    params = get(row, "data", "kwargs", "params")

    leg = get(params, "legs", 0)
    origin_date = get(leg, "date")
    origin = get(leg, "originAirport")
    destination = get(leg, "destinationAirport")

    return inline_ad(row, origin_date, origin_date, origin, destination)


def hotel_ads(row: dict):
    params = get(row, "data", "kwargs", "params")

    check_in_date = get(params, "checkinDate")
    check_out_date = get(params, "checkoutDate")
    origin = get(params, "cityId")

    return inline_ad(row, check_in_date, check_out_date, origin, origin)


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
        }
