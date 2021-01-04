
import typing
import logging
import asyncio
import serial
import time
import binascii
import threading
import pyuavcan.transport
from pyuavcan.transport import Timestamp
from pyuavcan.transport.can.media import Media, FilterConfiguration, Envelope
from pyuavcan.transport.can.media import DataFrame
from pyuavcan.transport.can._frame import FrameFormat



_logger = logging.getLogger(__name__)

CLI_END_OF_LINE = b'\r\n'
CLI_END_OF_TEXT = b'\x03'
ACK = b'\r'
NACK = b'\a'
DEFAULT_BITRATE = 1000000
ACK_TIMEOUT = 0.5


class SLCANMedia(Media):
    """
    Media interface for SLCAN-compatible CAN bus adapters, with extension to support CLI commands.

    Some info on SLCAN can be found here:
        - Linux tree: drivers/net/can/slcan.c (http://lxr.free-electrons.com/source/drivers/net/can/slcan.c)
        - https://files.zubax.com/docs/Generic_SLCAN_API.pdf
        - http://www.can232.com/docs/canusb_manual.pdf
        - http://www.fischl.de/usbtin/

    The CLI extension allows to execute arbitrary CLI commands on the adapter. The commands differ from regular SLCAN
    exchange in the following ways:
        - CLI commands are echoed back.
        - Every output line of a CLI command, including echo, is terminated with CR LF (\r\n).
        - After the last line follows the ASCII End Of Text character (ETX, ^C, ASCII code 0x03) on a separate
          line (terminated with CR LF).
        - CLI commands must not begin with whitespace characters.
    Example:
        Input command "stat\r\n" may produce the following output lines:
        - Echo: "stat\r\n"
        - Data: "First line\r\n", "Second line\r\n", ...
        - End Of Text marker: "\x03\r\n"
    Refer to https://kb.zubax.com for more info.
    """

    def __init__(self, iface_name: str, mtu: int, baud_rate: int, loop: typing.Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._mtu = mtu
        self._iface_name = iface_name
        self._baud_rate = baud_rate
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        self._conn = serial.Serial(self._iface_name, self._baud_rate, timeout=0.1)
        self._closed = False
        self._loopback_enabled = False
        self._maybe_thread: typing.Optional[threading.Thread] = None

        _init_adapter(self._conn)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def interface_name(self) -> str:
        return self._iface_name

    @property
    def mtu(self) -> int:
        return self._mtu

    @property
    def number_of_acceptance_filters(self) -> int:
        """ 512 (Set arbitrarily) """
        return 512

    def start(self, handler: Media.ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        print("starting")
        if self._maybe_thread is None:
            self._maybe_thread = threading.Thread(
                target=self._thread_function, name=str(self), args=(handler,), daemon=True
            )
            self._maybe_thread.start()
            if no_automatic_retransmission:
                _logger.info("%s non-automatic retransmission is not supported", self)
        else:
            raise RuntimeError("The RX frame handler is already set up")

    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        # TODO: support acceptance filters
        self._config_filters = set(configuration)

    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        num_sent = 0
        for frame in frames:
            if self._closed:
                raise pyuavcan.transport.ResourceClosedError(repr(self))
            try:
                await asyncio.wait(
                    [self._loop.run_in_executor(None, self._conn.write, self._compile_native_frame(frame.frame))],
                    timeout=monotonic_deadline - self._loop.time(),
                    loop=self._loop
                )
            except asyncio.TimeoutError:
                break
            else:
                num_sent += 1
        return num_sent

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _stop_adapter(self._conn)
            self._conn.close()

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        return ["example-device0", "example-device1"]

    def _compile_native_frame(self, frame: DataFrame) -> bytes:
        line = '%s%d%s\r' % (('T%08X' if frame.format == FrameFormat.EXTENDED else 't%03X') % frame.identifier,
                             frame.dlc,
                             binascii.b2a_hex(frame.data).decode('ascii'))
        return line.encode('ascii')

    def _thread_function(self, handler: Media.ReceivedFramesHandler) -> None:
        def handler_wrapper(frs: typing.Sequence[typing.Tuple[Timestamp, Envelope]]) -> None:
            try:
                if not self._closed:  # Don't call after closure to prevent race conditions and use-after-close.
                    handler(frs)
            except Exception as exc:
                _logger.exception("%s: Unhandled exception in the receive handler: %s; lost frames: %s", self, exc, frs)

        print("in thread function")
        data = bytes()
        while not self._closed:
            try:
                new_data = self._conn.read_until(b'\n', 1024)
                ts_mono_ns = time.monotonic_ns()
                ts_real_ns = time.time_ns()
                data += new_data

                frames: typing.List[typing.Tuple[Timestamp, Envelope]] = []
                slcan_lines = data.split(ACK)
                slcan_lines, data = slcan_lines[:-1], slcan_lines[-1]

                # only process slcan lines (ignore cli lines)
                frames = self._process_many_slcan_lines(slcan_lines, ts_mono=ts_mono_ns, ts_real=ts_real_ns)

                if len(frames) > 0:
                    self._loop.call_soon_threadsafe(handler_wrapper, frames)

            except Exception as ex:  # pragma: no cover
                if self._conn.closed:
                    self._closed = True
                _logger.exception("%s thread failure: %s", self, ex)
                time.sleep(1)  # Is this an adequate failure management strategy?

        self._closed = True
        _logger.debug("%s thread is about to exit", self)

    def _process_slcan_line(self, line, local_ts_mono, local_ts_real) -> typing.Optional[typing.Tuple[Timestamp, Envelope]]:
        line = line.strip().strip(NACK).strip(CLI_END_OF_TEXT)
        line_len = len(line)

        if line_len < 1:
            return None

        # Checking the header, ignore all irrelevant lines
        if line[0] == b'T'[0]:
            id_len = 8
        elif line[0] == b't'[0]:
            id_len = 3
        else:
            return None

        # Parsing ID and DLC
        packet_id = int(line[1:1 + id_len], 16)
        packet_dlc = int(line[1 + id_len:2 + id_len], 16)
        packet_len = DataFrame.convert_dlc_to_length(packet_dlc)

        # Parsing the payload, detecting timestamp
        # <type> <id> <dlc> <data>         [timestamp]
        # 1      3|8  1     packet_len * 2 [4]
        with_timestamp = line_len > (2 + id_len + packet_len * 2)

        packet_data = binascii.a2b_hex(line[2 + id_len:2 + id_len + packet_len * 2])

        # TODO: support packet timestamp
        # Handling the timestamp, if present
        ts_mono = local_ts_mono
        ts_real = local_ts_real

        timestamp = Timestamp(system_ns=ts_real, monotonic_ns=ts_mono)
        frame_format = FrameFormat.EXTENDED if id_len == 8 else FrameFormat.BASE

        frame = DataFrame(frame_format, packet_id, bytearray(packet_data))
        envelope = Envelope(frame, False)
        return timestamp, envelope

    def _process_many_slcan_lines(self, lines, ts_mono, ts_real) -> typing.Iterable[typing.Tuple[Timestamp, Envelope]]:
        processed_lines = []
        for slc in lines:
            # noinspection PyBroadException
            try:
                data = self._process_slcan_line(slc, local_ts_mono=ts_mono, local_ts_real=ts_real)
                if data is not None:
                    processed_lines.append(data)
            except Exception:
                _logger.error('Could not process SLCAN line %r', slc, exc_info=True)
        return processed_lines


def _stop_adapter(conn):
    conn.write(b'C\r' * 10)
    conn.flush()


def _init_adapter(conn, bitrate=None):
    def wait_for_ack():
        _logger.info('Init: Waiting for ACK...')
        conn.timeout = ACK_TIMEOUT
        while True:
            b = conn.read(1)
            if not b:
                raise RuntimeError('SLCAN ACK timeout')
            if b == NACK:
                raise RuntimeError('SLCAN NACK in response')
            if b == ACK:
                break
            _logger.info('Init: Ignoring byte %r while waiting for ACK', b)

    def send_command(cmd):
        _logger.info('Init: Sending command %r', cmd)
        conn.write(cmd + b'\r')

    speed_code = {
        1000000: 8,
        800000: 7,
        500000: 6,
        250000: 5,
        125000: 4,
        100000: 3,
        50000: 2,
        20000: 1,
        10000: 0
    }[bitrate if bitrate is not None else DEFAULT_BITRATE]

    num_retries = 3
    while True:
        try:
            # Sending an empty command in order to reset the adapter's command parser, then discarding all output
            send_command(b'')
            try:
                wait_for_ack()
            except RuntimeError:
                pass
            time.sleep(0.1)
            conn.flushInput()

            # Making sure the channel is closed - some adapters may refuse to re-open if the channel is already open
            send_command(b'C')
            try:
                wait_for_ack()
            except RuntimeError:
                pass

            # Setting speed code
            send_command(('S%d' % speed_code).encode())
            conn.flush()
            wait_for_ack()

            # Opening the channel
            send_command(b'O')
            conn.flush()
            wait_for_ack()

            # Clearing error flags
            send_command(b'F')
            conn.flush()
            try:
                wait_for_ack()
            except RuntimeError as ex:
                _logger.warning('Init: Could not clear error flags (command not supported by the CAN adapter?): %s', ex)
        except Exception as ex:
            if num_retries > 0:
                _logger.error('Could not init SLCAN adapter, will retry; error was: %s', ex, exc_info=True)
            else:
                raise ex
            num_retries -= 1
        else:
            break

    # Discarding all input again
    time.sleep(0.1)
    conn.flushInput()