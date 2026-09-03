import datetime
import unittest

from uuid import uuid4

from patroni.control import BootstrapState, CheckpointMode, CloneMode, CommandKind, \
    DesiredRole, DivergencePolicy, LifecycleCommand, ReloadMode, StopMode
from patroni.control.protocol import Capability, ErrorCode, HEADER, Hello, MAGIC, MAX_FRAME_BYTES, \
    Operation, pack, PROTOCOL_MAJOR, PROTOCOL_MINOR, ProtocolError, read_frame, Request, unpack


class FragmentedStream:

    def __init__(self, payload):
        self.payload = payload

    def recv(self, size):
        chunk = self.payload[:1]
        self.payload = self.payload[1:]
        return chunk


def command():
    return LifecycleCommand(
        str(uuid4()), CommandKind.STOP, DesiredRole.UNCHANGED, 10,
        StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None, BootstrapState.IDLE,
        None, None,
    )


class TestControlProtocol(unittest.TestCase):

    def test_nested_domain_round_trip(self) -> None:
        request = Request(str(uuid4()), Operation.SUBMIT, str(uuid4()), str(uuid4()), 1, command())

        self.assertEqual(request, unpack(pack(request)))

    def test_string_enum_type_round_trip(self) -> None:
        request = Request(str(uuid4()), Operation.HELLO, str(uuid4()), '', 1, None)

        decoded = unpack(pack(request))

        self.assertIsInstance(decoded.operation, Operation)

    def test_hello_capabilities_round_trip(self) -> None:
        hello = Hello(str(uuid4()), PROTOCOL_MAJOR, PROTOCOL_MINOR, tuple(Capability), None, 0)

        self.assertEqual(hello, unpack(pack(hello)))

    def test_datetime_round_trip(self) -> None:
        value = datetime.datetime(2026, 9, 2, 12, 0, tzinfo=datetime.timezone.utc)

        self.assertEqual(value, unpack(pack(value)))

    def test_fragmented_frame_is_read_exactly(self) -> None:
        request = Request(str(uuid4()), Operation.HELLO, str(uuid4()), '', 1, None)

        self.assertEqual(request, read_frame(FragmentedStream(pack(request))))

    def test_partial_frame_is_rejected(self) -> None:
        request = Request(str(uuid4()), Operation.HELLO, str(uuid4()), '', 1, None)

        for frame in (pack(request)[:4], pack(request)[:-1]):
            with self.subTest(size=len(frame)), self.assertRaises(ProtocolError):
                read_frame(FragmentedStream(frame))

    def test_oversized_frame_is_rejected_before_body(self) -> None:
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR, MAX_FRAME_BYTES + 1)

        with self.assertRaises(ProtocolError) as raised:
            unpack(frame)

        self.assertEqual(ErrorCode.BAD_REQUEST, raised.exception.code)

    def test_unknown_record_is_rejected(self) -> None:
        payload = b'{"$type":"Unknown","fields":[]}'
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR, len(payload)) + payload

        with self.assertRaises(ProtocolError):
            unpack(frame)

    def test_unknown_enum_is_rejected(self) -> None:
        payload = b'{"$enum":"Operation","value":"unknown"}'
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR, len(payload)) + payload

        with self.assertRaises(ProtocolError):
            unpack(frame)

    def test_invalid_utf8_is_rejected(self) -> None:
        payload = b'\xff'
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR, len(payload)) + payload

        with self.assertRaises(ProtocolError):
            unpack(frame)

    def test_unknown_major_version_is_rejected(self) -> None:
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR + 1, PROTOCOL_MINOR, 0)

        with self.assertRaises(ProtocolError) as raised:
            unpack(frame)

        self.assertEqual(ErrorCode.VERSION, raised.exception.code)

    def test_older_minor_version_is_rejected(self) -> None:
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR - 1, 0)

        with self.assertRaises(ProtocolError) as raised:
            unpack(frame)

        self.assertEqual(ErrorCode.VERSION, raised.exception.code)

    def test_duplicate_json_key_is_rejected(self) -> None:
        payload = b'{"key":1,"key":2}'
        frame = HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR, len(payload)) + payload

        with self.assertRaises(ProtocolError):
            unpack(frame)

    def test_nonfinite_number_is_rejected(self) -> None:
        with self.assertRaises(ProtocolError):
            pack(float('nan'))
