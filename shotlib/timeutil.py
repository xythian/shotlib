#
# Time related utility functions
#
import pytz
import calendar
from datetime import datetime, timedelta

def utcnow():
    return datetime.utcnow().replace(tzinfo=pytz.utc)

now = utcnow

pacific = pytz.timezone('US/Pacific')

def utcfromtimestamp(ts):
    return datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)

def utcfromtuple(tpl):
    return datetime.utcfromtimestamp(calendar.timegm(tpl)).replace(tzinfo=pytz.utc)

def pst(t=None):
    if t is None:
        t = now()
    return pacific.normalize(t.astimezone(pacific))    

def relative_time_func(thresholds):
    def relative_time(when, format, now=None):
        if now is None:
            now = utcnow()
        if when > now:
            return format(when)
        delta = now - when
        for threshold, unit, name in thresholds:
            if delta < threshold:
                value = (delta.days * 86400 + delta.seconds) // unit
                return "%d %s%s ago" % (value, name, ('', 's')[value != 1])
        return format(when)
    return relative_time

relative_time = relative_time_func(((timedelta(hours=1), 60, "minute"),
                                    (timedelta(days=1), 3600, "hour")))
