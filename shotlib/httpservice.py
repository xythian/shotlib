from shotlib.service import Service
from shotlib import wsgiserver
from optparse import OptionParser, OptionGroup
from urlparse import urlparse
import shotweb

class ShutdownException(Exception):
    pass

class HttpService(Service):
    external_uri = None
    server = None
    default_host = "0.0.0.0"
    default_port = 8080
    def option_parser(self):
        parser = super(HttpService, self).option_parser()
        group = OptionGroup(parser, "HTTP Options", "HTTP Server Options")
        group.add_option("--host", action="store", default=self.default_host,
                         dest="host")
        group.add_option("-p", "--port", action="store", default=self.default_port, type="int")
        group.add_option("--external", action="store", default=self.external_uri, dest="external_uri")
        parser.add_option_group(group)
        return parser

    def create_application(self):
        pass

    def shutdown(self):
        if self.server:
            self.server.interrupt = ShutdownException()
        else:
            self.quit = True

    def signal_term(self, signum, frame):
        self.shutdown()

    def signal_int(self, signum, frame):
        self.shutdown()

    @property
    def server_name(self):
        if self.external_uri:            
            scheme, netloc, path, params, query, fragment = urlparse(self.external_uri)
            if netloc.find(':') > 0:
                host, port = netloc.split(':')
                return host
            else:
                return netloc
        else:
            return None
    
    def run(self):
        app = self.create_application()
        
        if self.external_uri:
            app = shotweb.proxy_root_middleware(self.external_uri)(app)
        server = wsgiserver.CherryPyWSGIServer((self.options.host,
                                                self.options.port),
                                               app,
                                               server_name=self.server_name)
        if self.quit:
            return

        self.server = server
        try:
            server.start()
        except KeyboardInterrupt:
            pass
        except SystemExit:
            pass
        except ShutdownException:
            pass
        
        server.stop()
