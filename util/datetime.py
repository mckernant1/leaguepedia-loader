from datetime import datetime
from decimal import Decimal


def transform_datetime_utc(date_time) -> Decimal:
    try:
        return Decimal(str(datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').timestamp() * 1000))
    except (TypeError, ValueError):
        return Decimal(-1)
