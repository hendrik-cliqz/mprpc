# cython: profile=False
# -*- coding: utf-8 -*-

import gevent.socket
import logging
import msgpack

from constants import MSGPACKRPC_REQUEST, MSGPACKRPC_RESPONSE, SOCKET_RECV_SIZE
from exceptions import MethodNotFoundError, RPCProtocolError


cdef class RPCServer:
    """RPC server.

    This class is assumed to be used with gevent StreamServer.

    :param str pack_encoding: (optional) Character encoding used to pack data
        using Messagepack.
    :param str unpack_encoding: (optional) Character encoding used to unpack
        data using Messagepack.

    Usage:
        >>> from gevent.server import StreamServer
        >>> import mprpc
        >>>
        >>> class SumServer(mprpc.RPCServer):
        ...     def sum(self, x, y):
        ...         return x + y
        ...
        >>>
        >>> server = StreamServer(('127.0.0.1', 6000), SumServer())
        >>> server.serve_forever()
    """

    def __init__(self, *args, **kwargs):
        self.pack_encoding = kwargs.pop('pack_encoding', 'utf-8')
        self.unpack_encoding = kwargs.pop('unpack_encoding', 'utf-8')
        self.use_bin_type = kwargs.pop('use_bin_type', False)
        self._tcp_no_delay = kwargs.pop('tcp_no_delay', False)
        self._methods = {}

        if args and isinstance(args[0], gevent.socket.socket):
            self.__call__(args[0], None)

    def __call__(self, sock, _):
        cdef bytes data
        cdef object req
        cdef tuple args
        cdef int msg_id
        cdef bint begin_new_message

        if self._tcp_no_delay:
            sock.setsockopt(gevent.socket.IPPROTO_TCP, gevent.socket.TCP_NODELAY, 1)

        unpacker = msgpack.Unpacker(encoding=self.unpack_encoding,
                                          use_list=False)
        packer = msgpack.Packer(encoding=self.pack_encoding, use_bin_type=self.use_bin_type)

        while True:
            data = sock.recv(SOCKET_RECV_SIZE)
            if not data:
                break

            unpacker.feed(data)
            try:
                req = unpacker.next()
            except StopIteration:
                continue

            if type(req) != tuple:
                msg = (MSGPACKRPC_RESPONSE, -1, "Invalid protocol", None)
                sock.sendall(packer.pack(msg))
                logging.debug('Protocol error, received unexpected data: {}'.format(data))
                continue

            (msg_id, method, args) = self._parse_request(req)

            try:
                ret = method(*args)

            except Exception, e:
                logging.debug('Protocol error, received unexpected data: {}'.format(data))
                msg = (MSGPACKRPC_RESPONSE, msg_id, str(e), None)
                sock.sendall(packer.pack(msg))

            else:
                msg = (MSGPACKRPC_RESPONSE, msg_id, None, ret)
                sock.sendall(packer.pack(msg))

    cdef tuple _parse_request(self, tuple req):
        if (len(req) != 4 or req[0] != MSGPACKRPC_REQUEST):
            raise RPCProtocolError('Invalid protocol')

        cdef tuple args
        cdef int msg_id

        (_, msg_id, method_name, args) = req

        method = self._methods.get(method_name, None)

        if method is None:
            if method_name.startswith('_'):
                raise MethodNotFoundError('Method not found: %s', method_name)

            if not hasattr(self, method_name):
                raise MethodNotFoundError('Method not found: %s', method_name)

            method = getattr(self, method_name)
            if not hasattr(method, '__call__'):
                raise MethodNotFoundError('Method is not callable: %s', method_name)

            self._methods[method_name] = method

        return (msg_id, method, args)
