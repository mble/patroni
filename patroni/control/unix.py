"""Bounded one-request Unix transport."""
import logging
import os
import socket
import stat
import struct

from threading import BoundedSemaphore, Event, RLock, Thread
from typing import Callable, cast, Dict, Optional

from .protocol import ErrorCode, ProtocolError, read_frame, Response, write_frame

logger = logging.getLogger(__name__)

DEFAULT_BACKLOG = 32
DEFAULT_MAX_WORKERS = 16
DEFAULT_SOCKET_MODE = 0o600
DEFAULT_TIMEOUT = 5.0
ACCEPT_TIMEOUT = 0.2
STALE_CONNECT_TIMEOUT = 0.1
UNSAFE_WORLD_BITS = stat.S_IWOTH


def peer_check(expected_uid: Optional[int] = None,
               expected_gid: Optional[int] = None) -> Callable[[socket.socket], None]:
    """Build a Linux peer-credential verifier."""
    uid = os.geteuid() if expected_uid is None else expected_uid
    gid = os.getegid() if expected_gid is None else expected_gid

    def check(stream: socket.socket) -> None:
        if not hasattr(socket, 'SO_PEERCRED'):
            raise ProtocolError(ErrorCode.FORBIDDEN, 'peer credentials are unavailable')
        size = struct.calcsize('3i')
        raw = stream.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, size)
        _, peer_uid, peer_gid = struct.unpack('3i', raw)
        if peer_uid != uid or peer_gid != gid:
            raise ProtocolError(ErrorCode.FORBIDDEN, 'peer credentials do not match')

    return check


class UnixServer:
    """Serve one bounded request per local connection."""

    def __init__(self, path: str, handler: Callable[[object], Response],
                 verify_peer: Optional[Callable[[socket.socket], None]] = None,
                 timeout: float = DEFAULT_TIMEOUT, max_workers: int = DEFAULT_MAX_WORKERS,
                 socket_mode: int = DEFAULT_SOCKET_MODE) -> None:
        if timeout <= 0 or max_workers < 1:
            raise ValueError('invalid Unix server limits')
        if socket_mode not in (0o600, 0o660):
            raise ValueError('invalid control socket mode')
        _path(path, socket_mode)

        self._path = path
        self._handler = handler
        self._verify_peer = verify_peer or peer_check()
        self._timeout = timeout
        self._max_workers = max_workers
        self._socket_mode = socket_mode
        self._listener: Optional[socket.socket] = None
        self._inode: Optional[int] = None
        self._closed = Event()
        self._thread: Optional[Thread] = None
        self._workers = BoundedSemaphore(max_workers)
        self._connections: Dict[int, socket.socket] = {}
        self._lock = RLock()

    def start(self) -> None:
        if self._thread is not None:
            return
        _prepare(self._path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(self._path)
            os.chmod(self._path, self._socket_mode)
            self._inode = os.lstat(self._path).st_ino
            listener.listen(DEFAULT_BACKLOG)
            listener.settimeout(ACCEPT_TIMEOUT)
        except Exception:
            listener.close()
            raise
        self._listener = listener
        thread = Thread(target=self._serve, name='agent-unix')
        thread.daemon = True
        self._thread = thread
        thread.start()

    def close(self) -> None:
        self._closed.set()
        listener = self._listener
        if listener is not None:
            listener.close()
        with self._lock:
            connections = tuple(self._connections.values())
        for stream in connections:
            stream.close()
        thread = self._thread
        if thread is not None:
            thread.join(1)
        _unlink_owned(self._path, self._inode)

    def _serve(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._closed.is_set():
            try:
                stream, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._closed.is_set():
                    logger.exception('Control socket accept failed')
                return
            if not self._workers.acquire(False):
                stream.close()
                continue
            worker = Thread(target=self._handle, args=(stream,), name='agent-unix-worker')
            worker.daemon = True
            worker.start()

    def _handle(self, stream: socket.socket) -> None:
        identity = id(stream)
        with self._lock:
            self._connections[identity] = stream
        try:
            stream.settimeout(self._timeout)
            self._verify_peer(stream)
            request = read_frame(stream)
            response = self._handler(request)
            if not isinstance(cast(object, response), Response):
                raise ProtocolError(ErrorCode.INTERNAL, 'handler response is invalid')
            write_frame(stream, response)
        except ProtocolError as exc:
            try:
                write_frame(stream, Response('', exc.code, None))
            except Exception:
                pass
        except Exception:
            logger.exception('Control socket request failed')
            try:
                write_frame(stream, Response('', ErrorCode.INTERNAL, None))
            except Exception:
                pass
        finally:
            with self._lock:
                self._connections.pop(identity, None)
            stream.close()
            self._workers.release()


def _path(path: str, socket_mode: int) -> None:
    if not os.path.isabs(path) or os.path.normpath(path) != path:
        raise ValueError('control socket path must be canonical and absolute')
    parent = os.path.dirname(path)
    parent_stat = os.stat(parent, follow_symlinks=False)
    if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.geteuid():
        raise ValueError('control socket parent is unsafe')
    if parent_stat.st_mode & UNSAFE_WORLD_BITS:
        raise ValueError('control socket parent is world-writable')
    if parent_stat.st_mode & stat.S_IWGRP and socket_mode != 0o660:
        raise ValueError('control socket parent is group-writable')
    if socket_mode == 0o660 and parent_stat.st_gid != os.getegid():
        raise ValueError('control socket parent group does not match')


def _prepare(path: str) -> None:
    try:
        path_stat = os.lstat(path)
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(path_stat.st_mode) or path_stat.st_uid != os.geteuid():
        raise ValueError('control socket path is unsafe')

    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(STALE_CONNECT_TIMEOUT)
    try:
        probe.connect(path)
    except (ConnectionRefusedError, FileNotFoundError):
        os.unlink(path)
        return
    finally:
        probe.close()
    raise ValueError('control socket is already active')


def _unlink_owned(path: str, inode: Optional[int]) -> None:
    if inode is None:
        return
    try:
        path_stat = os.lstat(path)
        if stat.S_ISSOCK(path_stat.st_mode) and path_stat.st_uid == os.geteuid() and path_stat.st_ino == inode:
            os.unlink(path)
    except FileNotFoundError:
        pass
