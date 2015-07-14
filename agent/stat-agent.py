#!//usr/bin/env python

import socket
import os
import sys
import argparse
import signal
import logging
import logging.handlers
import fileinput
import pymongo
# from bson.objectid import ObjectId

redis_exists = False
try:
    import redis
    redis_exists = True
except ImportError:
    pass

using_daemon = False
try:
    from daemon import runner
    using_daemon = True
except ImportError:
    pass



class StatRec(object):

    def __init__(self, line = None):
        self.time_point = 0.0
        self.cli_addr = ''
        self.srv_addr = ''
        self.orig_size = 0
        self.comp_size = 0
        self.__defined = line and self.parse(line)


    def __str__(self):
        return '%s\t%s->%s\t%s\t%s' % (self.time_point, self.srv_addr, self.cli_addr, self.orig_size, self.comp_size)


    def parse(self, line):
        l = line.split()
        try:
            self.time_point = self.__str2num(l[0])
            self.cli_addr =   l[1]
            self.srv_addr =   l[2]
            self.orig_size =  self.__str2num(l[3])
            self.comp_size =  self.__str2num(l[4])
        except (IndexError, ValueError):
            return False
        return True


    def __str2num(self, val):
        try:
            return int(val)
        except ValueError:
            return int(float(val))


    def valid(self):
        return self.__defined



class StatSocket(object):

    socket_name = '/tmp/statistic_adapter.ipc'
    buf_size = 4096


    def __init__(self, logger):
        self.log = logger
        try:
            os.unlink(StatSocket.socket_name)
        except OSError:
                pass


    def __del__(self):
        self.close()


    def __num(self, val):
        try:
            return int(val)
        except ValueError:
            try:
                return int(float(val))
            except ValueError:
                return val


    def open(self):
        try:
            self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self.server_socket.bind(StatSocket.socket_name)
            os.chmod(StatSocket.socket_name, 0777) # S_IROTH + S_IWOTH + S_IRGRP + S_IWGRP)
        except IOError as e:
            self.log.critical('Error open/bind socket: %s' % e)
            return False
        except OSError as e:
            self.log.critical('Error change access mode to Unix Socket: %s' % e)
        return True


    def close(self):
        self.server_socket.close()
        try:
            os.unlink(StatSocket.socket_name)
        except OSError:
            pass


    def read(self, numeric = False):
        try:
            packet = self.server_socket.recvfrom(StatSocket.buf_size)[0]
        except IOError as e:
            self.log.error('Error reading from socket socket: %s' % e)
            return None
        if(numeric):
            return self.__num(packet)
        else:
            return packet



class CentralDB(object):

    complex_index_name = 'http_compress'
    id_index_name = 'unique_session_id_1'

    def __init__(self, logger, host = None, port = None, login = None, passwd = None):
        self.log = logger
        self.db = None
        self.client = None
        self.host = host or 'qamongodb1.privatecom.com'
        self.port = port or 27017
        self.login = login or 'mapi'
        self.passwd = passwd or 'zxcASD'


    def connect(self):
        if not self.db or not self.db.connection or not self.db.connection.alive():
            try:
                self.client = pymongo.MongoClient(self.host, self.port)
                self.db = self.client.mig_test_mapi
                if not self.db.authenticate(self.login, self.passwd):
                    self.log.critical('Failed to authenticate to MongoDB')
                    return False
                if not CentralDB.complex_index_name in self.db.sessions.index_information().keys():
                    self.log.info('Creating MongoDB index %s' % CentralDB.complex_index_name)
                    self.db.sessions.create_index([('server_ip', pymongo.ASCENDING),
                                                   ('Framed-IP-Address', pymongo.ASCENDING),
                                                   ('started', pymongo.DESCENDING),
                                                   ('closed', pymongo.ASCENDING)],
                                                  name = CentralDB.complex_index_name)
            except pymongo.errors.ClientFailure as e:
                self.log.critical('Problem with connection to MongoDB: %s' % e)
                return False
        return True


    def disconnect(self):
        if self.client:
            self.client.close()


    def update_session(self, stat):
        # {'closed': {'$exists': False}},
        # {'closed': '0'},
        # {'closed': 'None'},
        # docs = self.db.sessions.find(
        #     {'server_ip': stat.srv_addr,
        #      'client_ip': stat.cli_addr,
        #      'started': {'$lte': stat.time_point},
        #      '$or': [{'closed': 0},
        #              {'closed': {'$gte': stat.time_point}}]}).sort('started', pymongo.DESCENDING).limit(1)
        #     res = self.db.sessions.update(
        #         {'unique_session_id': doc['unique_session_id']},
        #         {'$inc': {'compress.before_size': stat.orig_size},
        #          '$inc': {'compress.after_size': stat.comp_size}})
        # since ver 3.x
        # doc = self.db.sessions.find_one_and_update(
        #     {'server_ip': stat.srv_addr,
        #      'client_ip': stat.cli_addr,
        #      'started': {'$lte': stat.time_point},
        #      '$or': [{'closed': 0},
        #              {'closed': {'$gte': stat.time_point}}]},
        #      {'$inc': {'compress.before_size': stat.orig_size},
        #       '$inc': {'compress.after_size': stat.comp_size}},
        #      sort=[('started', pymongo.DESCENDING)])

        doc = self.db.sessions.find_one(
            {'server_ip': stat.srv_addr,
             'Framed-IP-Address': stat.cli_addr,
             'started': {'$lte': stat.time_point},
             '$or': [{'closed': 0},
                     {'closed': {'$gte': stat.time_point}}]},
            sort = [('started', pymongo.DESCENDING)])

        if not doc:
            self.log.error('Session not found in MongoDB (%s: srv:%s, cli:%s)' %
                          (stat.time_point, stat.srv_addr, stat.cli_addr))
            return False
        else:
            self.log.debug('Session found in MongoDB: %s' % doc['unique_session_id'])

        try:
            res = self.db.sessions.update(
                {'unique_session_id': doc['unique_session_id']},
                {'$inc': {'HTTP_compress.before_size': stat.orig_size,
                          'HTTP_compress.after_size' : stat.comp_size}})
        except pymongo.errors.OperationFailure as e:
            self.log.error('Fail to update document: %s' % e)
            return False
        if not res:
            self.log.error('Session %s update returns empty result' % doc['unique_session_id'])
        else:
            self.log.debug('Session %s updated' % doc['unique_session_id'])

        return True;



class Storage(object):

    fail_postfix = 'failed'
    unhandles_poetfix = 'unhandled'

    def __init__(self, storage_name, logger):
        self.log = logger
        self.name = storage_name
        self.log.info('Processing storage file "%s"' % self.name)
        # self.storage = None
        # self.failed = None


    def failed(self, line):
        with open('%s.%s' % (self.name, Storage.fail_postfix), "a") as fd:
                fd.write(line)


    def process(self):
        self.db = CentralDB(logger = self.log)
        if not self.db.connect():
            return False
        for stat in fileinput.input(self.name):
            rec = StatRec(stat)
            if not rec.valid():
                self.log.error('Error parsing data, skipping line: "%s"' % stat)
                continue
            if not self.db.update_session(rec):
                self.failed(stat)
        return True



class Cmd(object):

  start = 'start'
  stop = 'stop'
  restart = 'restart'
  status = 'status'



class StatServer(object):

  module_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
  log_dir = '/tmp'
  pid_dir = '/var/run'
  log2tty = True
  logtag = 'compress_agent'
  description = 'Daemon updates MongoDb with HTTP compression statistic'


  def __init__(self, cmd = Cmd.start):
      self.busrstq = []
      if using_daemon:
          self.stdin_path = '/dev/null'
          self.stdout_path = os.path.join(StatServer.log_dir, '%s.stdout' % StatServer.module_name)
          self.stderr_path = os.path.join(StatServer.log_dir, '%s.stderr' % StatServer.module_name)
          self.pidfile_path = os.path.join(StatServer.pid_dir, '%s.pid' % StatServer.module_name)
          self.pidfile_timeout = 5
          self.log_init()
          #if cmd and cmd not in [Cmd.start, Cmd.restart]:
          #return


  def __del__(self):
    self.log.info('Stopped')


  def log_init(self):
    self.log = logging.getLogger(StatServer.logtag)
    self.log.setLevel(logging.INFO)
    # str_format = '%(name)s: %(message)s'
    # logging.Formatter(str_format)
    # metalog workaround here
    formatter = logging.Formatter('%(asctime)s %(name)s: %(levelname)s %(message)s', '%b %e %H:%M:%S')
    slh = logging.handlers.SysLogHandler(address = '/dev/log') # facility = SysLogHandler.LOG_DAEMON
    slh.setFormatter(formatter)
    slh.setLevel(logging.INFO)
    self.log.addHandler(slh)
    self.log_handlers = [slh.socket.fileno(),]

    # Debug purpose only (log to stdout)
    if StatServer.log2tty == True:
      hterminal = logging.StreamHandler(sys.stdout)
      hterminal.setLevel(logging.INFO)
      # date_format = '%m/%d/%Y %H:%M:%S'
      str_format = '%(asctime)s (%(levelname)s) %(name)s: %(message)s'
      formatter = logging.Formatter(str_format) #, date_format)
      hterminal.setFormatter(formatter)
      self.log.addHandler(hterminal)
      self.log_handlers = self.log_handlers + [hterminal,]


  def run(self):
    self.log.info('Starting')
    while(True):
        try:
            try:
                if not self.sock is None:
                    self.log.warning('Reniting')
            except (AttributeError, NameError):
                self.log.info('Initing')
            self.sock = StatSocket(logger = self.log)
            self.sock.open()
            self.log.info('Ready and waiting for data')
            try:
                while(True):
                    stat_burst = self.sock.read()
                    self.log.info('Got data file: "%s"' % stat_burst)
                    ###
                    for name, processed in self.busrstq[:]:
                        i = self.busrstq.index((name, processed))
                        if not processed:
                            storage = Storage(name, self.log)
                            if not storage.process():
                                continue
                        try:
                            os.remove(name)
                            del self.busrstq[i]
                        except OSError as e:
                            self.log.error('Fail to remove storage file %s: %s' % (name, e))
                            if not processed:
                                self.busrstq[i] = (name, True)
                    ###
                    storage = Storage(stat_burst, self.log)
                    if storage.process():
                        try:
                            os.remove(stat_burst)
                        except OSError as e:
                            self.log.error('Fail to remove storage file %s: %s' % (stat_burst, e))
                            self.busrstq.append((stat_burst, True))
                    else:
                        self.busrstq.append((stat_burst, False))

            except (KeyboardInterrupt, SystemExit):
                raise KeyboardInterrupt

        except (KeyboardInterrupt, SystemExit):
            self.log.info('Exiting...')
            return


  def exit(self, signum = None, frame = None):
    try:
      if self.got_sigterm is True:
        # Avoide of repeatedly sent SIGTERM
        return
    except (AttributeError, NameError):
      pass
    self.got_sigterm = True
    if self.sock: self.sock.close()
    if self.db: self.db.disconnect()
    self.log.info('Canceled')
    sys.exit(0)




def main():
  if not using_daemon:
    s = StatServer()
    s.run()
    return

  argparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                      description=StatServer.description)
  choices = [Cmd.start, Cmd.stop]
  argparser.add_argument('command', choices = choices)
  args = argparser.parse_args()

  StatServer.log2tty = False
  updater = StatServer(args.command)
  sys.argv[1] = args.command

  signal.signal(signal.SIGTERM, updater.exit)
  # signal.signal(signal.SIGHUP, updater.reload)
  try:
    daemon_runner = runner.DaemonRunner(updater)
    # daemon_runner.daemon_context.files_preserve = updater.log_handlers + [updater.sock.fileno(),]
    daemon_runner.daemon_context.working_directory = os.getcwd()
    daemon_runner.do_action()
  except runner.DaemonRunnerStopFailureError as e:
    updater.log.error('Stop %s daemon error: %s' % (os.path.basename(sys.argv[0]), e))




######

if __name__ == '__main__':
    main()
