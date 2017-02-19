#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# blockstatd.py
# The graphite-blockstat daemon

#   graphite-blockstat -- Send Linux blockstat metrics to Graphite
#   Copyright (c) 2017 Bodhi Digital LLC., Isabell Cowan
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import os
import os.path
import time
import datetime
import socket
from signal import signal, SIGINT
from time import sleep
from getopt import gnu_getopt, GetoptError
from typing import List, Dict, Union, Tuple
from abc import abstractmethod
from enum import Enum, IntEnum
from re import sub as re_sub

__title__ = "blockstatd"
__description__ = "The graphite-blockstat daemon"
__project__ = "graphite-blockstat"
__project_description__ = "Send Linux blockstat metrics to Graphite"
__author__ = "Isabell Cowan"
__copyright__ = "Copyright 2017, Bodhi Digital LLC., Isabell Cowan"
__credits__ = ["Isabell Cowan <isabellcowan@gmail.com>"]
__license__ = "GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>."
__version__ = "0.2.2b"
__maintainer__ = "Isabell Cowan <isabellcowan@gmail.com>"
__email__ = "isabellcowan@gmail.com"
__status__ = "Development"


class LogLevel(IntEnum):
    """
    The log level.
    """

    error = 0
    warn = 1
    info = 2
    debug = 3


loglevel = LogLevel.warn

def eprint(*args, **kwargs):
    """
    Print to stderr.
    """

    kwargs['file'] = sys.stderr

    print(*args, **kwargs)

def error(*args, **kwargs):
    global loglevel

    if loglevel >= LogLevel.error:
        eprint("E:", *args, **kwargs)

def warn(*args, **kwargs):
    global loglevel

    if loglevel >= LogLevel.warn:
        eprint("W:", *args, **kwargs)

def info(*args, **kwargs):
    global loglevel

    if loglevel >= LogLevel.info:
        eprint("I:", *args, **kwargs)

def debug(*args, **kwargs):
    global loglevel

    if loglevel >= LogLevel.debug:
        eprint("D:", *args, **kwargs)


class StatType(IntEnum):
    """
    Type of statistic (read/write).
    """

    read_io         = 0
    read_merges     = 1
    read_sectors    = 2
    read_ticks      = 3
    write_io        = 4
    write_merges    = 5
    write_sectors   = 6
    write_ticks     = 7
    in_flight       = 8
    io_ticks        = 9
    time_in_queue   = 10

class ASendBuffer:
    """
    Abstract class for send buffer.
    """

    _messages = None  # type: str

    def __init__(self):
        self.clear()

    def put(self, message: str):
        if self._messages:
            self._messages += "\n"

        self._messages += message

    def get_messages(self) -> str:
        return self._messages

    def clear(self):
        self._messages = ""

    @abstractmethod
    def flush(self):
#       Implementers must run self.clear() if successful
        pass

class StdoutBuffer(ASendBuffer):
    """
    Standard buffer (passthrough).
    """

    def flush(self):
        print(self._messages)
        self.clear()

class GraphiteBuffer(ASendBuffer):
    """
    Graphite buffer.
    """

    _server_and_port = None  # type: Tuple[str, int]

    def __init__(self, server: str, port: int):
        ASendBuffer.__init__(self)

        self._server_and_port = (server, port)

    def flush(self):
        graphite_server = socket.socket()

        debug("Connecting to graphite server.")

        for attempt in range(0, 3):
            try:
                graphite_server.connect(self._server_and_port)
            except Exception as e:
                warn("Failed to connect to the graphite server: {0}".format(e))
                continue

            try:
                graphite_server.sendall((self.get_messages() + "\n").encode("UTF-8"))
                self.clear()
                break
            except Exception as e:
                graphite_server.close()
                warn("Failed to send to the graphite server: {0}".format(e))
                continue

        debug("Closing connection to graphite server.")

        graphite_server.close()

class IOutput:
    """
    Interface for metric output.
    """

    @abstractmethod
    def send(self, send_buffer: ASendBuffer, stat_type: StatType, block: str, stat: int):
        pass

class HumanOutput(IOutput):
    """
    Human readable formatter for metrics.
    """

    def send(self, send_buffer: ASendBuffer, the_time: int, stat_type: StatType, block: str, stat: int):
        pretty_time = datetime.datetime.fromtimestamp(the_time)\
                                       .strftime('%Y-%m-%d %H:%M:%S')
        send_buffer.put("{0} {2}[{1}]: {3}".format(pretty_time, stat_type.name, block, str(stat)))

class GraphiteOutput(IOutput):
    """
    Output formated for graphite.
    """

    def send(self, send_buffer: ASendBuffer, the_time: int, stat_type: StatType, block: str, stat: int):
        short_hostname = socket.gethostname().split('.', 1)[0]

        send_buffer.put("blockstat.{4}.{2}.{1} {3} {0}".format(
                the_time, stat_type.name, block, stat, short_hostname))

class OutForm(Enum):
    """
    Output format and target.
    """

    graphite = "graphite"
    human = "human"

class BlockStat:
    """
    Collect blockstats from /sys.
    """

    _block = None  # type: str
    _stats = None  # type: Dict[StatType, int]
    _time = None  # type: int

    def __init__(self, block: str):
        self._block = block
        self._stats = dict()
        self._time = 0.0

    def collect(self):
        blockstat_str = None  # type: str
        statfile = "/sys/block/{0}/stat".format(self._block)

        self._time = int(time.time())

        try:
            with open(statfile, "rt") as blockstat:
                blockstat_str = blockstat.read()
                blockstat_str = blockstat_str.lstrip().rstrip()
                blockstat_str = re_sub("\s+", " ", blockstat_str)
        except IOError as e:
            _stats = dict()
            self._time = 0.0
            warn(e)
            return

        blockstat_map = blockstat_str.split(" ")

        for stat_type in StatType:
            try:
                self._stats[stat_type] = int(blockstat_map[stat_type.value])
            except ValueError as e:
                error("Got garbage from {0}".format(statfile))
                raise e

    def sendto(self, output: IOutput, send_buffer: ASendBuffer):
        for stat_type, stat in self._stats.items():
            output.send(send_buffer, self._time, stat_type, self._block, stat)

class MainClass:
    """
    Main program logic. Defines main().

    .. note:: Static only class.
    """

    _blocks = set()  # type: Set[str]
    _interval = None  # type: int
    _pidfile = None  # type: Union[None, str]
    _daemonize = None  # type: bool
    _outform = None  # type: OutForm
    _server = None  # type: Union[None, str]
    _server_port = None  # type: int

    _shortOptions = "i:p:o:s:DavqhV"  # type: str
    _longOptions = [  # type: List[str]
        "interval=",
        "pidfile=",
        "output=",
        "server=",
        "daemonize",
        "all",
        "verbose",
        "quiet",
        "help",
        "version"
    ]

    @staticmethod
    def main(argv: List[str], envp: Dict[str, str]) -> int:
        """
        Main function.

        :param argv: Command line arguments.
        :param envp: Environment variables.
        :return: Exit status.
        """

        MainClass._parse_opts(argv)
        MainClass._parse_env(envp)
        MainClass._set_default_opts()

        MainClass._print_copy_notice()
        eprint()

        debug("Loglevel: {0}".format(loglevel))
        debug("Using interval: {0}".format(MainClass._interval))
        debug("Using block devices: {0}".format(", ".join(MainClass._blocks)))

        if MainClass._daemonize:
            debug("Will daemonize.")
        else:
            debug("Will not deamonize.")

        if MainClass._pidfile is not None:
            debug("Using PID file: {0}".format(MainClass._pidfile))
        else:
            debug("Not using a PID file.")

        debug("Using output format: {0}".format(MainClass._outform.value))

        if MainClass._outform == OutForm.human:
            output = HumanOutput()
            send_buffer = StdoutBuffer()
        else:
            debug("Using server: {0}:{1}".format(MainClass._server, MainClass._server_port))
            output = GraphiteOutput()
            send_buffer = GraphiteBuffer(MainClass._server, MainClass._server_port)

        blockstats = []

        for blockdev in MainClass._blocks:
            blockstats.append(BlockStat(blockdev))

        def graceful_stop(signum, frame):
            if MainClass._pidfile is not None:
                os.remove(MainClass._pidfile)

            if signum == SIGINT:
                info("Received SIGINT, stopping gracefully.")

            exit(0)

        if not MainClass._daemonize or not os.fork():
            signal(SIGINT, graceful_stop)

            if MainClass._pidfile is not None:
                if os.path.exists(MainClass._pidfile):
                    error("PID file already exists.")
                    exit(1)
                elif not os.path.isdir(os.path.dirname(MainClass._pidfile)):
                    error("PID file dirname does not exist or is not a directory.")
                    exit(1)

                try:
                    with open(MainClass._pidfile, "wt") as pidfile:
                        pidfile.write(str(os.getpid()) + "\n")
                except IOError as e:
                    error("Failed to write PID file: {0}".format(e))
                    exit(1)

            MainClass._do_metrics(output, send_buffer, blockstats)

        return 0

    @staticmethod
    def _do_metrics(output: IOutput, send_buffer: ASendBuffer, blockstats: List[BlockStat]):
        """
        Run collect and sendto at the appropriate interval.
        """

        while True:
            for blockstat in blockstats:
                blockstat.collect()

            for blockstat in blockstats:
                blockstat.sendto(output, send_buffer)

            send_buffer.flush()

            sleep(MainClass._interval)

    @staticmethod
    def _parse_opts(argv: List[str]):
        """
        Parse command line.

        :param argv: Command line arguments.
        """

        global loglevel

        opts = None  # type: List[Tuple[str, str]]
        args = None  # type: List[str]

        try:
            opts, args = gnu_getopt(
                argv[1:],
                MainClass._shortOptions,
                MainClass._longOptions)
        except GetoptError as e:
            error("Error parsing command line: {0}".format(e.msg))
            exit(2)

        if args:
            for a in args:
                if not a or a == "." or a == ".." or "/" in a:
                    error("Illegal command line argument `{0}'.  ".format(a) +
                          "Block device names cannot be empty, "
                          "`.', `..', or contain a `/'.")
                    exit(2)

                MainClass._blocks.add(a)

        for o, a in opts:
            if o in ("-i", "--interval"):
                try:
                    MainClass._interval = int(a)
                except ValueError:
                    error("Illegal interval, must be integer (seconds).")
                    exit(2)
            elif o in ("-p", "--pidfile"):
                if a[:1] != "/":
                    error("PID file paths must be absolute.")
                    exit(2)

                MainClass._pidfile = a
            elif o in ("-o", "--output"):
                if a in [outform.value for outform in OutForm]:
                    MainClass._outform = OutForm(a)
                else:
                    error("Unrecognized output form, see help for details.")
                    exit(2)
            elif o in ("-s", "--server"):
                server_and_port = a.rsplit(":", 1)
                if 1 < len(server_and_port):
                    try:
                        MainClass._server_port = int(server_and_port[1])
                    except ValueError:
                        error("Illegal port number, must be digits only.")
                        exit(2)

                MainClass._server = server_and_port[0]
            elif o in ("-D", "--daemonize"):
                MainClass._daemonize = True
            elif o in ("-a", "--all"):
                for block in os.listdir("/sys/block/"):
                    MainClass._blocks.add(block)
            elif o in ("-v", "--verbose"):
                if LogLevel.debug > loglevel:
                    loglevel += 1
            elif o in ("-q", "--quiet"):
                if LogLevel.error < loglevel:
                    loglevel -= 1
            elif o in ("-h", "--help"):
                MainClass._print_help()
                exit(0)
            elif o in ("-V", "--version"):
                MainClass._print_version()
                MainClass._print_copy_notice()
                exit(0)
            else:
                assert False, "Unhandled option."

    @staticmethod
    def _parse_env(envp: Dict[str, str]):
        """
        Parse the environment and set options.

        :param envp: The environment.
        """

        pass

    @staticmethod
    def _set_default_opts():
        """
        Set the default options.
        """

        if MainClass._interval is None:
            MainClass._interval = 60

        if MainClass._daemonize is None:
            MainClass._daemonize = False

        if MainClass._server_port is None:
            MainClass._server_port = 2003

        if MainClass._outform is None:
            if MainClass._server is None:
                MainClass._outform = OutForm.human
            else:
                MainClass._outform = OutForm.graphite
        elif MainClass._outform == OutForm.graphite:
            if MainClass._server is None:
                error("With the graphite output format, a server must be specified.  "
                      "See --help for more information.")
                exit(2)

        if not MainClass._blocks:
            error("Atleast one block device must be specified.")
            exit(2)

    @staticmethod
    def _get_version_string() -> str:
        """
        Return the version string.
        """

        return "{0} ({1}) {2}".format(__title__, __project__, __version__)

    @staticmethod
    def _print_version():
        """
        Print version information.
        """

        print(MainClass._get_version_string())

    @staticmethod
    def _print_copy_notice():
        """
        Print copyright notice.
        """

        eprint(MainClass._get_version_string() + "\n"
               "{0}\n".format(__copyright__) +
               "License: {0}\n".format(__license__) +
               "This program comes with ABSOLUTELY NO WARRANTY.\n"
               "This is free software, and you are welcome to redistribute it under certain conditions.")

    @staticmethod
    def _print_help():
        """
        Print Usage.
        """

        print("Usage: {0} [OPTIONS] <blockdev [...]>\n".format(__file__) +
              "  Collect metrics on <blockdev>.  This argument may be specified more than once.\n"
              "\n"
              "  -i --interval <inter>:         Use <inter> as the collection interval.\n"
              "  -p --pidfile <pidf>:           Use <pidf> as the PID file, it's path must be absolute.\n"
              "                                 It will be removed before exiting if the process recives SIGINT.\n"
              "  -o --output <outform>:         Use the output format <outform>, values are `human' and `graphite'.\n"
              "  -s --server <server[:<port>]>: Use the graphite server <server> (and port <port>) to connect.\n"
              "  -D --daemonize:                Fork to the background.\n"
              "  -a --all:                      Use all block devices found in `/sys/block'.\n"
              "  -v --verbose:                  Be more verbose.\n"
              "  -q --quiet:                    Be less verbose.\n"
              "  -h --help:                     Show this.\n"
              "  -V --version:                  Show version.")

def __init__():
    """
    Setup and execute MainClass.main().
    Similar to C-runtime _start.
    """

    exit_status = MainClass.main(sys.argv, os.environ)

    exit(exit_status)

if __name__ == '__main__':
    __init__()

# vim: set ts=4 sw=4 et syn=python: