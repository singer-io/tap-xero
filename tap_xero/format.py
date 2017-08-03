from singer.utils import strftime
import datetime


def _format_path(path, obj, fn):
    key, *rest = path
    if not rest:
        if key in obj:
            obj[key] = fn(obj[key])
    elif key == "*":
        for sub_obj in obj:
            _format_path(rest, sub_obj, fn)
    else:
        _format_path(rest, obj[key], fn)


def format_datetimes(dt_fields, obj):
    for path in dt_fields:
        if not isinstance(path, list):
            path = [path]
        _format_path(path, obj, strftime)
    return obj


def _format_date(date):
    _date = date.date() if isinstance(date, datetime.datetime) else date
    return _date.isoformat()


def format_dates(date_fields, obj):
    for path in date_fields:
        if not isinstance(path, list):
            path = [path]
        _format_path(path, obj, _format_date)
    return obj
