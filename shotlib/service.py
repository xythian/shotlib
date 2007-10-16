#
# Standardize the command line UI for starting/stopping all shotlib service
# daemons.
#

from shotlib.daemonize import daemonize
from shotlib.log import SpreadLogHandler
from optparse import OptionParser, OptionGroup
import time
import errno
import signal
import logging
import os
import sys

LOG = logging.getLogger('Service')

class Service(object):
    def __init__(self, name='service'):
        self.name = name
        try:
            self.hostname = open('/etc/hostname').readline().strip()
        except:
            self.hostname = 'unknown'
        self.quit = False
        
    def option_parser(self):
        parser = OptionParser()
        group = OptionGroup(parser, 'Service Options',
                            'Standard service options')
                            
        group.add_option("-n", "--name",
                          action="store", default=self.name, dest="name",
                          help="Sets the service name")
        group.add_option("-d", "--daemon",
                          action="store_true", default=False, dest="daemon",
                          help="Run as a daemon (default: false)")
        group.add_option("-l", "--log",
                          action="store", default="-", dest="log",
                          help="Specify logging destination (default: stdout = '-')")
        group.add_option("--level",
                          action="store", default="DEBUG", dest="level",
                          help="Set the logging level (DEBUG, INFO, WARN, ERROR) default: DEBUG")
        parser.add_option_group(group)
        group = OptionGroup(parser, "Daemon options",
                            "Options applicable only to daemon mode")
        group.add_option('-a', '--action',
                         action='store', default='start', dest='action',
                         help='Specifies daemon action (start/stop)')
        group.add_option('--kill', action='store_true', default=False,
                         dest='kill', help='For --action stop, indicates a willingness to use SIGKILL to stop a stubborn process that does not respond to SIGTERM')
                         
        group.add_option("--pidfile",
                          action="store", default=self.name + '.pid', dest="pidfile",
                          help="Sets the pid file")
        group.add_option("-o", "--out",
                          action="store", default=self.name + ".output.log", dest="out",
                          help="Where to redirect stdout and stderr.")
        group.add_option("--home",
                          action="store", default=".", dest="home",
                          help="Set service home directory")
        parser.add_option_group(group)

        return parser

    def configure_logging(self, options):
        formatter = logging.Formatter(('[%s:%s:' % (self.name, self.hostname)) + '%(process)d:%(name)s:%(module)s] %(asctime)s %(levelname)s %(message)s')
        consolefmt = logging.Formatter('%(module)s %(message)s')
        if options.log == '-':
            if not options.daemon:
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(consolefmt)
            else:
                console = logging.FileHandler(options.out)
                console.setFormatter(formatter)
        else:
            console = logging.FileHandler(options.log)
            console.setFormatter(formatter)
        l = logging.getLogger()
        l.addHandler(console)
        if options.daemon:
            sl = SpreadLogHandler()
            sl.setFormatter(formatter)
            l.addHandler(sl)
        l.setLevel(getattr(logging, self.options.level))

    def handle_options(self, options, parser):
        self.options = options
        self.name = options.name
        self.configure_logging(options)

    def handle_args(self, args, parser):
        pass

    def daemonize(self, options):
        daemonize(stdout=options.out,
                  stderr=options.out,
                  newhome=options.home,
                  pidfile=options.pidfile,
                  closeall=False)


    def signal_term(self, signum, frame):
        self.quit = True

    def signal_int(self, signum, frame):
        self.quit = True

    def signal_usr1(self, signum, frame):
        pass

    def signal_usr2(self, signum, frame):
        pass

    def signal_hup(self, signum, frame):
        pass

    def install_signal_handlers(self):
        def handle_sig(name, func):
            def handle(signum, frame):
                LOG.info('Received SIG%s', name)
                func(signum, frame)
            return handle
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, handle_sig('HUP', self.signal_hup))
        signal.signal(signal.SIGUSR1, handle_sig('USR1', self.signal_usr1))
        signal.signal(signal.SIGUSR2, handle_sig('USR2', self.signal_usr2))
        signal.signal(signal.SIGTERM, handle_sig('TERM', self.signal_term))
        signal.signal(signal.SIGPWR, handle_sig('PWR', self.signal_term))        
        signal.signal(signal.SIGINT, handle_sig('INT', self.signal_int))

    def get_pid(self, options, parser):
        pidfile = os.path.join(options.home, options.pidfile)
        if not os.path.exists(pidfile):
            parser.error('pidfile %s missing' % options.pidfile)
        try:
            pid = int(open(pidfile).readline().strip())
        except:
            parser.error('Unable to read pidfile')
        return pid

    def service_stop(self, options, parser):
        pid = self.get_pid(options, parser)
        try:
            os.kill(pid, signal.SIGTERM)
        except os.error, v:
            if v.errno == errno.EPERM:
                parser.error('Unable to kill %d, operation not permitted' % pid)
            elif v.errno == errno.ESRCH:
                parser.error('Unable to kill %d, no such process' % pid)
            else:
                parser.error('Unable to kill %d, %s', (pid, str(v)))
        # delivered the message, wait a bit seeing if it was received
        for waiting in xrange(10):
            time.sleep(.5)
            try:
                os.kill(pid, 0)
            except os.error, v:                
                if v.errno == errno.EPERM:
                    parser.error('Unable to kill %d, operation not permitted (after we already sent sigterm??' % pid)
                elif v.errno == errno.ESRCH:
                    # victory!                
                    return
        if not options.kill:
            print 'Stubborn process is not dying.  Retry stop with --kill to SIGKILL it'
            return
        try:
            os.kill(pid, signal.SIGKILL)
        except os.error, v:
            if v.errno == errno.EPERM:
                parser.error('Unable to kill %d, operation not permitted' % pid)
            elif v.errno == errno.ESRCH:
                parser.error('Unable to kill %d, no such process' % pid)
            else:
                parser.error('Unable to kill %d, %s', (pid, str(v)))

    def log(self, msg, *args):
        LOG.info(msg, *args)
    
    def main(self):
        parser = self.option_parser()
        (options, args) = parser.parse_args()
        self.handle_options(options, parser)
        self.handle_args(args, parser)        
        if options.daemon or options.action != 'start':
            if options.action == 'start':
                self.daemonize(options)
            elif options.action == 'stop':
                self.service_stop(options, parser)
                return
            else:
                parser.error('Unknown action: %s' % options.action)
                
        self.install_signal_handlers()
        LOG.info('Starting %s (pid: %d)', self.name, os.getpid())
        try:
            self.run()
            LOG.info('Service %s exiting normally', self.name)            
        except:
            LOG.error('Service exiting with error', exc_info=True)
        try:
            os.remove(os.path.join(self.options.home, self.options.pidfile))
        except:
            pass

    def run(self):
        while not self.quit:
            LOG.info('Nothing to do, sleeping for 20 seconds...')
            time.sleep(20.)
        LOG.info('Going home..')


if __name__ == '__main__':
    service = Service()
    service.main()
