#
# Convenience functions for dealing with urllib
#
import urllib2, urllib

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

_opener = urllib2.build_opener(NoRedirectHandler())

def fetch(url_or_request=None):
    global _opener
    try:
        return _opener.open(url_or_request)
    except urllib2.HTTPError, ex:
        return ex

