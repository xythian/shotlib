#
# Convenience functions for dealing with urllib
#
import urllib2, urllib
import gzip, zlib
from cStringIO import StringIO

class NoRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_302(self, req, fp, code, msg, headers):
        infourl = urllib2.addinfourl(fp, headers, req.get_full_url())
        infourl.status = code
        infourl.code = code
        return infourl
    http_error_300 = http_error_302
    http_error_301 = http_error_302
    http_error_303 = http_error_302
    http_error_307 = http_error_302

class HeadRequest(urllib2.Request):
    def get_method(self):
        return "HEAD"

_opener = urllib2.build_opener(NoRedirectHandler())

def fetch(url_or_request=None, **kwargs):
    global _opener
    try:
        return _opener.open(url_or_request, **kwargs)
    except urllib2.HTTPError, ex:
        return ex

def maybe_uncompress(response):
    data = response.read()
    if response.headers.get('content-encoding', '') == 'gzip':
        try:
            data = gzip.GzipFile(fileobj=StringIO(data)).read()
        except Exception, e:
            pass
    elif response.headers.get('content-encoding', '') == 'deflate':
        try:
            data = zlib.decompress(data, -zlib.MAX_WBITS)
        except Exception, e:
            pass
    return data

def user_agent_fetch_url(url, requestType=urllib2.Request):
    global _opener
    headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0',
               'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Language' : 'en-us,en;q=0.5',
               'Accept-Encoding' : 'gzip, deflate',
               'Accept-Charset' : 'ISO-8859-1,utf-8;q=0.7,*;q=0.7'}
    req = requestType(url=url, headers=headers)
    try:
        response = _opener.open(req, timeout=10)
        return (response.code, response)
    except urllib2.HTTPError, ex:
        return (ex.getcode(), ex)
    except urllib2.URLError, ex:
        return (504, ex)
    
