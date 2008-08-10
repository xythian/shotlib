from datetime import datetime, timedelta
import pytz

# TODO: fix exif timezone?

def parse_exif_datetime(dt, tzinfo=pytz.timezone('US/Pacific')):
    dt = str(dt)
    # 2005:07:10 18:07:37
    # 0123456789012345678
    #           1       
    if len(dt) == 19:
        year, month, day = int(dt[:4]), int(dt[5:7]), int(dt[8:10])
        hour, minute, second = int(dt[11:13]), int(dt[14:16]), int(dt[17:19])
        return datetime(year, month, day, hour, minute, second, tzinfo=tzinfo)
    return None

def parse_iso8601(dt='', d='', t=''):
   if not dt and not d and not t:
      return None
   if dt:  
       try:
           idx = dt.index('T')
           d = dt[:idx]
           t = dt[idx+1:]
       except ValueError:
           d = dt
           t = ''   
   try:
      year, month, day = int(d[:4]), int(d[4:6]), int(d[6:])
   except ValueError:
      return None
   hour, minute, second = 0,0,0
   tzsign = '-'
   tzhour, tzminute = 0, 0
   tzname = 'UTC'
   if t:
      if len(t) == 11:
         try:
            hour, minute, second = int(t[:2]), int(t[2:4]), int(t[4:6])
            tzhour, tzminute = int(t[7:9]), int(t[9:])
            tzsign = t[6]
            tzname = t[6:]
         except ValueError:
            pass
      elif len(t) == 9:
         try:
            hour, minute, second = int(t[:2]), int(t[2:4]), 0
            tzhour, tzminute = int(t[6:8]), int(t[8:])            
            tzsign = t[4]
            tzname = t[4:]
         except ValueError:
            pass
   tzoffset = int(tzsign + str((tzhour * 60) + tzminute))
   tz = pytz.FixedOffset(tzoffset)
   return datetime(year, month, day, hour, minute, second, tzinfo=tz)
