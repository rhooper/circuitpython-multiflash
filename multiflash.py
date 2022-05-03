# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
#
# SPDX-License-Identifier: MIT
#
import json
import logging
import os
import re
import shutil
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import NamedTuple, Optional
from threading import Thread

import psutil
import serial
import serial.tools.list_ports

logging.basicConfig(format="%(asctime)s %(threadName)s %(levelname)s %(message)s", level=logging.DEBUG)

DESIRED_CPY_VERSION = "7.2.5"
SOURCE_CONTENT = Path(__file__).parent / "content"
EMPTY_FS_FILES = {
    ".fseventsd",
    ".fseventsd/no_log",
    ".metadata_never_index",
    ".Trashes",
    "code.py",
    "lib",
    "boot_out.txt",
}
EMPTY_FS_IGNORE = {"fseventsd/no_log"}

COPY_IGNORE = [".DS_Store", "__pycache__", "*.pyc", "._*", ".*"]

seen_devices = dict()
in_progress = set()
most_recent_devices = []
done_devices = set()

DONE_SCRIPT = """
    from adafruit_circuitplayground import cp
    from rainbowio import colorwheel
    cp.pixels.brightness = 0.0625
    cp.pixels[:5] = [0xbb22bb for n in range(5)]
    cp.pixels[5:] = [colorwheel(n * 25) for n in range(5)]
"""


class DeviceInfo(NamedTuple):
    serial_no: str
    tty_device: Optional[str]
    mount_point: str
    last_seen_at: datetime

    def __ne__(self, other):
        return not (self.serial_no, self.tty_device, self.mount_point) == (
            other.serial_no,
            other.tty_device,
            other.mount_point,
        )

    def __repr__(self):
        return f"DeviceInfo({self.serial_no} {self.tty_device}, {self.mount_point})"


def find_devices():
    usb_data = json.loads(
        subprocess.run(
            "/usr/sbin/system_profiler -json SPUSBDataType SPStorageDataType".split(), capture_output=True
        ).stdout
    ).get("SPUSBDataType", [])

    def recurse(data):
        if type(data) is list:
            for dev in data:
                yield from recurse(dev)
            return
        if "_items" in data:
            yield from recurse(data["_items"])
        yield {k: v for k, v in data.items() if k != "_items"}

    yield from recurse(usb_data)


def find_mount_point(item: dict) -> Optional[DeviceInfo]:
    mount_point = None
    try:
        media = item.get("Media")
        if not media:
            return

        volumes = media[0].get("volumes", [])
        if volumes and "mount_point" in volumes[0]:
            mount_point = media[0]["volumes"][0]["mount_point"]

        if not mount_point:
            parts = psutil.disk_partitions()
            for part in parts:
                device_path = "/dev/" + media[0]["bsd_name"]
                if part.device == device_path:
                    mount_point = part.mountpoint
                    break

        if not mount_point:
            logging.debug("No disk device found for %s", item)
            return

        serial_no = item["serial_num"]
        return DeviceInfo(serial_no=serial_no, tty_device=None, mount_point=mount_point, last_seen_at=datetime.now())
    except (TypeError, KeyError, UnboundLocalError) as exc:
        logging.exception("Error handling discovered device", exc_info=exc)


def find_serial_port(usb_device: Optional[DeviceInfo]) -> Optional[DeviceInfo]:
    if not usb_device:
        return None

    for serial_port in serial.tools.list_ports.comports():
        if not serial_port.serial_number:
            continue

        if serial_port.serial_number != usb_device.serial_no:
            continue

        tty_path = serial_port.device.replace("cu", "tty")
        with_tty = DeviceInfo(
            tty_device=tty_path,
            serial_no=usb_device.serial_no,
            mount_point=usb_device.mount_point,
            last_seen_at=usb_device.last_seen_at,
        )
        return with_tty


def discover_devices(once=False, specific_serial_no=None, fetch=True):
    while True:
        if fetch:
            most_recent_devices.clear()
            most_recent_devices.extend(find_devices())
        for item in most_recent_devices:
            if item.get("vendor_id") != "0x239a":
                continue
            device = find_serial_port(find_mount_point(item))
            if not device:
                continue
            if specific_serial_no and device.serial_no == specific_serial_no:
                yield device
                continue
            if (
                device.serial_no in seen_devices
                and seen_devices[device.serial_no] == device
                and (datetime.now() - device.last_seen_at) <= timedelta(seconds=120)
            ):
                continue

            seen_devices[device.serial_no] = device

            if device.serial_no in done_devices and device.serial_no not in str(most_recent_devices):
                run_script(device, script=DONE_SCRIPT, description="rerun done script")

            if device and device.serial_no not in in_progress and device not in done_devices:
                yield device

        time.sleep(0.25)
        if once:
            return


def bootloader_flash(device: DeviceInfo):
    # Install CircuitPython
    logging.info("Installing cp_cpb.uf2 to %s (%s)", device.mount_point, device.serial_no)
    shutil.copy("cp_cpb.uf2", Path(device.mount_point) / "firmware.uf2")
    wait_for_device(device)
    logging.info("Done bootloader for %s", device.mount_point)


def wait_for_device(device, timeout=60, require_mount=False):
    started = datetime.now()
    logging.info("Waiting for %s", device.serial_no)
    orig_device = device
    while True:
        if datetime.now() - started > timedelta(seconds=timeout):
            raise TimeoutError("Device not seen after %d", timeout)
        devices = list(discover_devices(once=True, specific_serial_no=orig_device.serial_no))
        ok = False
        device = None
        if len(devices):
            device = devices[0]
            ok = True
            if require_mount and not device.mount_point:
                ok = False
            if not device.tty_device:
                ok = False
        if ok:
            return device

        time.sleep(0.2)


def get_circuitpython_version(device):
    boot_contents = (Path(device.mount_point) / "boot_out.txt").read_text(encoding="utf-8")
    # Adafruit CircuitPython 7.2.5 on 2022-04-06; Adafruit Circuit Playground Bluefruit with nRF52840
    # Board ID:circuitplayground_bluefruit
    version = re.search(r"^Adafruit CircuitPython (\S+) on.*", boot_contents)
    # board_id = re.match(r'^Board ID:(\S+)')
    return version.group(1)



def content_flash(device: DeviceInfo):
    # Start by erasing filesystem
    try:
        # Check filesystem is clean
        mount_point = Path(device.mount_point)
        fs_contents = set(str(path.relative_to(mount_point)) for path in sorted(mount_point.rglob("*")))
        extras = fs_contents - EMPTY_FS_FILES - EMPTY_FS_IGNORE
        missing = EMPTY_FS_FILES - fs_contents - EMPTY_FS_FILES

        if extras or missing:
            logging.warning("Filesystem differences: extra=%s, missing%s", extras, missing)
            device = erase_filesystem(device)

        time.sleep(5)
        wait_for_device(device)

        # Check boot version
        cpy_version = get_circuitpython_version(device)
        if cpy_version < DESIRED_CPY_VERSION:
            logging.warning("Circuit Python version is %s", cpy_version)

            with serial.Serial(device.tty_device, timeout=5, exclusive=True) as serial_port:
                serial_port: serial.Serial
                acquire_repl(serial_port)
                logging.info("Entering bootloader to upgrade to %s", DESIRED_CPY_VERSION)
                serial_port.write(
                    b"import microcontroller\r" b"microcontroller.on_next_reset(microcontroller.RunMode.BOOTLOADER)\r"
                )

            logging.info("Waiting for device to restart")
            device = wait_for_device(device)

        logging.info("Copying content")
        copy_content(device)

        os.sync()
        logging.info("Done copying to %s", device)
        done_devices.add(device.serial_no)

        run_script(
            device,
            script=DONE_SCRIPT,
            description="Set LEDs",
        )

    except (serial.SerialException, OSError) as exc:
        logging.error("Exception: %s", str(exc), exc_info=exc)


def copy_content(device):
    for src in Path(SOURCE_CONTENT).rglob("*"):
        if src.stem == '.DS_Store':
            continue
        dst = device.mount_point / src.relative_to(SOURCE_CONTENT)
        if src.is_dir():
            logging.debug("- mkdir %s", dst)
            dst.mkdir(0o755, exist_ok=True)
            continue

        logging.debug("- %s", dst)
        shutil.copyfile(src=src, dst=dst)
        shutil.copymode(src=src, dst=dst)


def run_script(device, script, serial_port: Optional[serial.Serial] = None, timeout=15, description: str = ""):
    logging.info("Obtaining REPL")
    script = dedent(script).replace("\n", "\r").encode("utf-8")

    def get_port():
        if serial_port:
            return serial_port
        return serial.Serial(device.tty_device, timeout=timeout, exclusive=True)

    with get_port() as serial_port:
        serial_port: serial.Serial
        acquire_repl(serial_port)

        logging.info("Running script %s", description)
        for line in script.splitlines():
            logging.debug("Script: %s", line.decode('utf-8'))

        serial_port.write([5])
        log_serial_output(serial_port.readline())
        serial_port.write(script)
        serial_port.write([13])
        serial_port.flush()
        time.sleep(0.025)
        serial_port.write([4])
        serial_port.flush()
        with temporary_timeout(serial_port, 2):
            try:
                while serial_port.in_waiting:
                    log_serial_output(serial_port.readline(serial_port.in_waiting))
                    time.sleep(0.1)
            except OSError:
                pass


def log_serial_output(loggable: bytes):
    for line in loggable.decode('utf-8').splitlines():
        logging.debug("  << %s", line)


def erase_filesystem(device):
    try:
        run_script(
            device,
            script="""
                import storage
                storage.erase_filesystem()
                """,
            description="erase filesystem",
        )
        time.sleep(5)
    except serial.SerialException:
        pass

    device = wait_for_device(device)
    while device.mount_point is None:
        time.sleep(1)
        device = wait_for_device(device)
    return device


@contextmanager
def temporary_timeout(port: serial.Serial, timeout: int):
    cur_timeout = port.timeout
    port.timeout = timeout
    yield
    port.timeout = cur_timeout


def acquire_repl(serial_port):
    while True:
        serial_port.write(b'\r')
        with temporary_timeout(serial_port, 2):
            output = serial_port.read_until(b">>> ")
            if b">>>" in output:
                log_serial_output(output)
                logging.info("Found running REPL")
                return True
            else:
                logging.debug("Sending Ctrl-C")
                serial_port.write(b"\003\r\r")  # Ctrl-C LF
                if b">>>" in serial_port.read_until(b">>> "):
                    time.sleep(0.1)
                    return True


def main():
    tasks_flash = []
    tasks_boot = []
    for device in discover_devices():
        if device.serial_no in in_progress:
            continue

        logging.info("Discovered new device %s", device)
        in_progress.add(device.serial_no)

        if "BOOT" in device.mount_point:
            logging.info("Run boot flash on device %s", device)
            task = Thread(target=bootloader_flash, args=(device,), daemon=True, name=device.serial_no)
            tasks_boot.append(task)
            task.start()
        else:
            logging.info("Run content flash on device %s", device)
            task = Thread(target=content_flash, args=(device,), daemon=True, name=device.serial_no)
            tasks_flash.append(task)
            task.start()

        for source, tasks in (('content', tasks_flash), ('boot', tasks_boot)):
            for task in list(tasks):
                if not task.is_alive():
                    tasks.remove(task)
                    logging.info("Task done %s", task)
                    if task.name in in_progress:
                        in_progress.remove(task.name)
                        if source == 'content':
                            done_devices.add(task.name)
                        logging.info("Device %s done %s", task.name, source)
                    task.join()


        time.sleep(0.25)


if __name__ == "__main__":
    main()
