#!/usr/bin/env python3
import argparse
import inspect
import json
import logging
import os
import signal
import sys
import threading
from argparse import ArgumentDefaultsHelpFormatter
from functools import partial
from pathlib import Path
from subprocess import PIPE, Popen
from time import time
from typing import Any

import boto3
import botocore
from boto3.s3.transfer import TransferConfig

NOTICE = 25
logging.addLevelName(NOTICE, "NOTICE")


def notice(msg, *args, **kwargs):
    logging.log(NOTICE, msg, *args, **kwargs)


def _func_name() -> str:
    return inspect.stack()[1][3]  # caller function name


def _rmdir_recursive(path: Path) -> None:
    for fp in path.iterdir():
        fp.unlink()
    path.rmdir()


def _run_pipeline(cmd1: list[str], cmd2: list[str]) -> None:
    p1 = Popen(cmd1, stdout=PIPE)
    p2 = Popen(cmd2, stdin=p1.stdout)
    p1.stdout.close()  # Allow p1 to receive SIGPIPE if p2 exits.
    p2.communicate()
    if p2.returncode != 0:
        raise Exception("NonZeroReturnCode", p2.returncode, cmd1, cmd2)


class NoSuchKeyError(Exception):
    pass


# noinspection PyUnresolvedReferences
class S3Wrapper:
    def __init__(self, endpoint_url: str, bucket: str) -> None:
        s3_pool_size = 150
        # Default retry mode ('legacy') does not retry ConnectionClosedError,
        # which we've been experiencing often
        boto_config = botocore.config.Config(
            max_pool_connections=s3_pool_size,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 10, "mode": "standard"},
        )
        self._s3r = boto3.resource("s3", endpoint_url=endpoint_url, config=boto_config)
        self._s3c = self._s3r.meta.client
        self._bucket = bucket
        self._s3b = self._s3r.Bucket(self._bucket)
        self._s3b.create()
        self._tx_config = TransferConfig(
            max_concurrency=s3_pool_size,
            multipart_threshold=2**20,
            multipart_chunksize=2**20,
        )
        self._progress_interval = 120

    def get_from_json(self, key: str, **kwargs: Any) -> Any:
        obj = self._s3r.Object(self._bucket, key)
        try:
            body = obj.get()["Body"].read()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                if "default" in kwargs:
                    return kwargs["default"]
            raise NoSuchKeyError
        return json.loads(body)

    def put_as_json(self, state: dict[str, Any], key: str) -> None:
        body = json.dumps(state)
        self._s3c.put_object(Bucket=self._bucket, Key=key, Body=body)

    def download_file(self, key: str, path: str) -> None:
        self._s3c.download_file(self._bucket, key, path)

    def upload_file(self, path: str, key: str) -> None:
        label = "..." + path[-17:]
        size = Path(path).stat().st_size
        self._s3c.upload_file(
            path,
            self._bucket,
            key,
            Config=self._tx_config,
            Callback=ProgressMeter(label, size, self._progress_interval),
        )

    def delete_object(self, key: str) -> None:
        self._s3c.delete_object(Bucket=self._bucket, Key=key)

    def list_object_keys(self, prefix: str = "") -> list[str]:
        return [obj.key for obj in self._s3b.objects.filter(Prefix=prefix)]


class Readable:
    @staticmethod
    def size(size: float) -> str:
        if size < 10**3:
            return "%s B" % int(size)
        elif size < 10**6:
            return "%.2f KiB" % (size / 10**3)
        elif size < 10**9:
            return "%.2f MiB" % (size / 10**6)
        elif size < 10**12:
            return "%.2f GiB" % (size / 10**9)
        else:
            return "%.2f TiB" % (size / 10**12)

    @staticmethod
    def time(time_: float) -> str:
        time_ = int(round(time_))
        seconds = time_ % 60
        minutes = (time_ // 60) % 60
        hours = time_ // (60 * 60)
        if hours:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"


class ProgressMeter(object):
    # To simplify, assume this is hooked up to a single operation
    def __init__(self, label: str, size: int, update_interval: int = 10) -> None:
        self._label = label
        self._size = size
        self._count = 0
        self._first_time: float | None = None
        self._first_count: int | None = None
        self._update_interval = update_interval
        self._last_update_time: float | None = None
        self._last_update_count: int | None = None
        self._lock = threading.Lock()

    def __call__(self, num_bytes: int) -> None:
        with self._lock:
            now = time()
            if self._first_time is None:
                self._first_time = now
                self._first_count = num_bytes
                # trick to display initial stats earlier than self._update_interval
                self._last_update_time = now - self._update_interval + 10
                self._last_update_count = num_bytes
            self._count += num_bytes
            t_observed = now - self._first_time
            t_since_update = now - self._last_update_time

            rs = partial(Readable.size)
            rt = partial(Readable.time)
            if self._count == self._size:
                sys.stdout.write(
                    f"{self._label} {rs(self._size)} in ~{rt(t_observed)}\n"
                )
                sys.stdout.flush()
                return
            if t_since_update >= self._update_interval:
                percent = (self._count / self._size) * 100
                update_delta = self._count - self._last_update_count
                update_rate = (
                    update_delta / t_since_update
                )  # XXX why is this negative sometimes?
                average_rate = self._count / t_observed
                t_remaining_cur = (self._size - self._count) / update_rate
                sys.stdout.write(
                    f"{self._label: <20} {rt(t_observed): <7}  "
                    f"{rs(self._count): >10} / {rs(self._size)} {percent: 3.0f}%  "
                    f"{rs(update_rate)}/s {rs(average_rate)}/s  "
                    f"ETA: {rt(t_remaining_cur)}\n"
                )
                sys.stdout.flush()
                self._last_update_time = now
                self._last_update_count = self._count


def disassemble(s3w: S3Wrapper, work_dir: Path, part_size: int, dry_run: bool) -> None:
    my_name = _func_name()
    my_state_key = "disassemble.json"
    my_state = s3w.get_from_json(my_state_key, default={})
    origin = s3w.get_from_json("origin.json", default={})
    target = s3w.get_from_json("target.json")

    my_delivered = set(my_state).intersection(target)
    my_orphaned = set(my_state) - set(origin)
    my_unprocessed = set(origin) - set(my_state) - set(target)

    if dry_run:
        logging.info(f"Dry run: would have cleaned-up delivered {my_delivered}")
        logging.info(f"Dry run: would have cleaned-up orphaned {my_orphaned}")
        logging.info(f"Dry run: would have processed {my_unprocessed}")
        return

    # clean up processed data
    stats = []
    for filename in my_delivered.union(my_orphaned):
        logging.info(f"{my_name} cleaning up {Path(work_dir, filename)}")
        chunk_dir = Path(work_dir, filename)
        if chunk_dir.exists():
            _rmdir_recursive(chunk_dir)
        my_state.pop(filename)
        s3w.put_as_json(my_state, my_state_key)
        stats.append(filename)
    if stats:
        logging.info(f"{my_name} cleaned-up: {len(stats)} files")

    # do the disassembly
    for filename in my_unprocessed:
        logging.info(f"{my_name} compressing and splitting {filename}")
        chunk_dir = Path(work_dir, filename)
        if chunk_dir.exists():
            _rmdir_recursive(chunk_dir)
        chunk_dir.mkdir(parents=True)
        # Compressing with --threads=0 seems to use 4 cores
        zstd_cmd = ["zstd", "--threads=0", "--stdout", origin[filename]["path"]]
        split_cmd = ["split", "-b", str(part_size), "-", str(chunk_dir) + "/"]
        _run_pipeline(["nice", "-n", "19"] + zstd_cmd, split_cmd)
        my_state[filename] = [str(f) for f in chunk_dir.iterdir()]
        s3w.put_as_json(my_state, my_state_key)


def download(s3w: S3Wrapper, work_dir: Path, dry_run: bool) -> None:
    my_name = _func_name()
    my_state_key = "download.json"
    my_state = s3w.get_from_json(my_state_key, default={})
    src_state = s3w.get_from_json("upload.json", default={})
    origin = s3w.get_from_json("origin.json", default={})
    target = s3w.get_from_json("target.json")

    my_delivered = set(my_state).intersection(target)
    my_orphaned = set(my_state) - set(origin)
    my_unprocessed = set(src_state) - set(target) - my_orphaned

    if dry_run:
        logging.info(f"Dry run: would have cleaned-up delivered {my_delivered}")
        logging.info(f"Dry run: would have cleaned-up orphaned {my_orphaned}")
        logging.info(f"Dry run: would have downloaded {my_unprocessed}")
        return

    for filename in my_delivered.union(my_orphaned):
        logging.info(f"{my_name} cleaning up {Path(work_dir, filename)}")
        _rmdir_recursive(Path(work_dir, filename))
        my_state.pop(filename)
        s3w.put_as_json(my_state, my_state_key)

    for origin_path in my_unprocessed:
        part_keys = src_state[origin_path]
        my_state.setdefault(origin_path, [])
        chunks_dir = work_dir / Path(origin_path)
        chunks_dir.mkdir(parents=True, exist_ok=True)
        for part_key in part_keys:
            logging.info(f"Downloading {origin_path} {part_key}")
            dst_path = str(chunks_dir / Path(part_key).name)
            if dst_path in my_state[origin_path]:
                logging.warning(f"{part_key} already downloaded at {dst_path}")
                continue
            else:
                s3w.download_file(part_key, dst_path)
                my_state[origin_path].append(dst_path)
                s3w.put_as_json(my_state, my_state_key)


def reassemble(s3w: S3Wrapper, work_dir: Path, dst_dir: Path, dry_run: bool) -> None:
    my_name = _func_name()
    work_dir.mkdir(parents=True, exist_ok=True)
    dst_dir.mkdir(parents=True, exist_ok=True)
    src_state = s3w.get_from_json("download.json", default={})
    origin_state = s3w.get_from_json("origin.json")

    for origin_path, part_paths in src_state.items():
        logging.info(f"Processing {origin_path} from {len(part_paths)} parts")
        if not part_paths:
            logging.info(f"Skipping {origin_path} because no parts are available")
            continue
        output_path = Path(work_dir, Path(origin_path).name)
        target_path = dst_dir / output_path.name
        # We never want to overwrite, so early short-circuit to not do unnecessary work
        # (target path may exist because it was transferred out-of-band).
        if target_path.exists():
            notice(f"{target_path} already exists; skipping")
            continue
        if dry_run:
            logging.info(f"Skipping {part_paths} due to dry run")
            continue
        cat_cmd = ["cat"] + sorted(part_paths)
        # noinspection SpellCheckingInspection
        zstd_cmd = [
            "pzstd",
            "--quiet",
            "--force",
            "--decompress",
            "-o",
            str(output_path),
        ]
        try:
            _run_pipeline(cat_cmd, zstd_cmd)
        except Exception as e:
            logging.warning(f"Failed to reassemble {origin_path}")
            logging.warning(f"{e}")
            notice(f"File {origin_path} doesn't appear to be fully uploaded yet")
            continue
        origin_file = origin_state[origin_path]
        # noinspection SpellCheckingInspection
        os.utime(output_path, (origin_file["atime"], origin_file["mtime"]))
        output_path.chmod(0o444)
        # Path.rename on Linux silently overwrites, so there is a TOCTOU race here
        if target_path.exists():
            logging.error(f"{target_path} exists")
            continue
        else:
            output_path.rename(target_path)
            notice(f"{target_path} arrived at its final destination")


def refresh_terminus(
    s3w: S3Wrapper,
    root_dir: Path,
    excludes: list[str],
    my_state_key: str,
    verbose: bool = True,
) -> None:
    root_dir = root_dir.resolve()
    old_state = s3w.get_from_json(my_state_key, default={})
    state = {}
    relevant_files = [
        path
        for path in root_dir.iterdir()
        if path.is_file() and not sum(path.name.startswith(pref) for pref in excludes)
    ]
    for fp in relevant_files:
        filename = str(fp.relative_to(root_dir))
        if verbose and filename not in old_state:
            notice(f"Discovered new file {filename} in {root_dir}")
        # noinspection SpellCheckingInspection
        state[filename] = {
            "path": str(fp.resolve()),
            "size": fp.stat().st_size,
            "mtime": fp.stat().st_mtime,
            "atime": fp.stat().st_atime,
            "ts": time(),
        }
    s3w.put_as_json(state, my_state_key)


def show_status(s3w: S3Wrapper) -> None:
    # XXX handle missing state exceptions
    target = s3w.get_from_json("target.json")
    origin = s3w.get_from_json("origin.json")

    undelivered = [fn for fn in origin if fn not in target]
    present = [fn for fn in origin if fn in target]
    mismatched = [fn for fn in present if origin[fn]["size"] != target[fn]["size"]]

    print("Present:", len(present))
    print("Undelivered:", undelivered)
    print("Mismatched:", mismatched)


def upload(s3w: S3Wrapper, dry_run: bool) -> None:
    my_state_key = "upload.json"
    my_state = s3w.get_from_json(my_state_key, default={})
    src_state = s3w.get_from_json("disassemble.json", default={})
    origin = s3w.get_from_json("origin.json", default={})
    target = s3w.get_from_json("target.json")

    my_delivered = set(my_state).intersection(target)
    my_orphaned = set(my_state) - set(origin)
    my_unprocessed = set(src_state) - my_delivered - set(target) - my_orphaned
    uploaded_parts = set(s3w.list_object_keys(prefix="parts"))
    partially_uploaded = {
        filename
        for filename, parts in my_state.items()
        if uploaded_parts.intersection(parts)
    }
    unuploaded = my_unprocessed - partially_uploaded

    for filename in my_orphaned:
        logging.error(
            f"File {filename} has been uploaded but no longer present at origin"
        )

    if dry_run:
        logging.info(f"Dry run: would have cleaned-up delivered {my_delivered}")
        logging.info(f"Dry run: would have cleaned-up orphaned {my_orphaned}")
        logging.info(f"Dry run: would have started uploading {unuploaded}")
        logging.info(f"Dry run: would have resumed uploading {partially_uploaded}")
        return

    # clean up processed objects
    stats = []
    for filename in my_delivered.union(my_orphaned):
        for part_obj_key in my_state[filename]:
            is_orphan = filename in my_orphaned
            logging.info(
                f"Cleaning up {'orphan' if is_orphan else 'delivered'} object {part_obj_key}"
            )
            s3w.delete_object(part_obj_key)
        my_state.pop(filename)
        s3w.put_as_json(my_state, my_state_key)
        stats.append(filename)
    if stats:
        logging.info(f"Cleaned-up {len(stats)} part objects")

    # do the upload
    stats: list[str] = []
    total_uploaded = 0
    for filename in my_unprocessed:
        my_state.setdefault(filename, [])
        for part_path in src_state[filename]:
            part_obj_key = "parts" + part_path
            # check sanity of already uploaded "rogue" parts
            if part_obj_key in uploaded_parts:
                # is this a rogue object uploaded out-of-band?
                if part_obj_key not in my_state[filename]:
                    logging.error(
                        f"Uploaded object {part_obj_key} not in {my_state_key}."
                        f" Assuming it's intended and updating my state"
                    )
                    # must add the rogue object to state o/w it'll never be cleaned up
                    my_state[filename].append(part_obj_key)
                    s3w.put_as_json(my_state, my_state_key)
                logging.info(f"Object key {part_obj_key} already uploaded; skipping")
                continue

            logging.info(f"Uploading {part_path} as {part_obj_key}")
            time_start = time()
            s3w.upload_file(part_path, part_obj_key)
            upload_duration = time() - time_start
            file_size = Path(part_path).stat().st_size
            upload_rate = file_size / upload_duration
            notice(
                f"Uploaded {filename} part {part_path} at {Readable.size(upload_rate)}/s"
            )
            my_state[filename].append(part_obj_key)
            s3w.put_as_json(my_state, my_state_key)
            stats.append(filename)
            total_uploaded += file_size

    if stats:
        notice(f"Uploaded all {len(stats)} files, {Readable.size(total_uploaded)}")


def main() -> int:
    def __formatter(max_help_position: int, width: int = 90):
        return lambda prog: ArgumentDefaultsHelpFormatter(
            prog, max_help_position=max_help_position, width=width
        )

    def __abs_path(path: str) -> Path:
        return Path(path).resolve()

    parser = argparse.ArgumentParser(
        description="CTA Ingest. https://github.com/vbrik/cta-ingest",
        formatter_class=__formatter(27),
    )

    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        default="INFO",
        choices=["DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL"],
        help="application log level",
    )
    parser.add_argument(
        "--boto-log-level",
        metavar="LEVEL",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="log level for boto3/botocore/s3transfer",
    )
    parser.add_argument(
        "--log-file", metavar="PATH", help="write verbose log to file (unattended mode)"
    )

    subparsers = parser.add_subparsers(
        title="commands",
        dest="command",
        description='Use "%(prog)s <command> -h" or similar to get command help.',
    )
    subparsers.add_parser(
        "status",
        formatter_class=ArgumentDefaultsHelpFormatter,
        help="display status summary",
    )
    par_refresh_origin = subparsers.add_parser(
        "refresh_origin",
        formatter_class=__formatter(27),
        help="update the list of files currently in the origin directory",
    )
    par_refresh_origin.add_argument(
        "-x",
        dest="excludes",
        nargs="*",
        metavar="PREF",
        default=[],
        help="exclude files whose names start with PREF",
    )
    par_refresh_origin.add_argument(
        "path", metavar="ORIGIN_PATH", type=__abs_path, help="path to monitor"
    )

    par_refresh_target = subparsers.add_parser(
        "refresh_target",
        formatter_class=__formatter(27),
        help="update the list of files currently in the target directory",
    )
    par_refresh_target.add_argument(
        "path", metavar="PATH", type=__abs_path, help="path to monitor"
    )

    par_disassemble = subparsers.add_parser(
        "disassemble",
        formatter_class=ArgumentDefaultsHelpFormatter,
        help="prepare origin files for uploading to the S3 bucket",
    )
    par_disassemble.add_argument(
        "path",
        metavar="PATH",
        type=__abs_path,
        help="directory where to store file parts",
    )
    par_disassemble.add_argument(
        "--part-size-gb", metavar="GB", default=10.0, type=float, help="part size in GB"
    )
    par_disassemble.add_argument(
        "--dry-run", default=False, action="store_true", help="dry run"
    )

    par_upload = subparsers.add_parser(
        "upload",
        formatter_class=ArgumentDefaultsHelpFormatter,
        help="upload disassembled files to the S3 bucket",
    )
    par_upload.add_argument(
        "--timeout",
        metavar="SECONDS",
        type=int,
        help="terminate after this amount of time",
    )
    par_upload.add_argument(
        "--dry-run", default=False, action="store_true", help="dry run"
    )

    par_download = subparsers.add_parser(
        "download",
        formatter_class=ArgumentDefaultsHelpFormatter,
        help="download file parts from S3",
    )
    par_download.add_argument(
        "path",
        metavar="PATH",
        type=__abs_path,
        help="directory where to store file parts",
    )
    par_download.add_argument(
        "--dry-run", default=False, action="store_true", help="dry run"
    )

    par_reassemble = subparsers.add_parser(
        "reassemble",
        formatter_class=ArgumentDefaultsHelpFormatter,
        help="reassemble files from parts",
    )
    par_reassemble.add_argument(
        "dst_path",
        metavar="TARGET_PATH",
        type=__abs_path,
        help="final destination directory",
    )
    par_reassemble.add_argument(
        "--work-dir",
        metavar="WORK_PATH",
        type=__abs_path,
        required=True,
        help="temporary storage directory in the same file system as TARGET_PATH",
    )
    par_reassemble.add_argument(
        "--dry-run", default=False, action="store_true", help="dry run"
    )

    s3_grp = parser.add_argument_group(
        "S3 options",
        description="Note that S3 credential arguments are optional. "
        'See the "Configuring Credentials" section of boto3 library documentation for details.',
    )
    s3_grp.add_argument(
        "-u",
        "--s3-url",
        metavar="URL",
        default="https://rgw.icecube.wisc.edu",
        help="S3 endpoint URL",
    )
    s3_grp.add_argument(
        "-b", "--bucket", metavar="NAME", required=True, help="S3 bucket name"
    )
    s3_grp.add_argument("-a", dest="access_key_id", help="S3 access key id override")
    s3_grp.add_argument(
        "-s", dest="secret_access_key", help="S3 secret access key override"
    )

    args = parser.parse_args()

    verbose_level = logging.getLevelName(args.log_level)
    fmt = logging.Formatter("%(asctime)-23s %(levelname)s %(message)s")

    if args.log_file:
        fh = logging.FileHandler(args.log_file)
        fh.setLevel(verbose_level)
        fh.setFormatter(fmt)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(NOTICE)
        sh.setFormatter(fmt)
        logging.basicConfig(level=verbose_level, handlers=[fh, sh])
    else:
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(verbose_level)
        sh.setFormatter(fmt)
        logging.basicConfig(level=verbose_level, handlers=[sh])
    boto_level = logging.getLevelName(args.boto_log_level)
    for _lib in ("boto3", "botocore", "s3transfer", "urllib3"):
        logging.getLogger(_lib).setLevel(boto_level)

    args_dict = args.__dict__.copy()
    args_dict.pop("access_key_id")
    args_dict.pop("secret_access_key")
    logging.debug(f"Arguments (redacted): {args_dict}")

    if args.command is None:
        parser.print_help()
        parser.exit()

    s3w = S3Wrapper(args.s3_url, args.bucket)

    if args.command == "status":
        show_status(s3w)
    elif args.command == "refresh_origin":
        refresh_terminus(s3w, args.path, args.excludes, "origin.json", verbose=True)
    elif args.command == "refresh_target":
        refresh_terminus(s3w, args.path, [".*"], "target.json", verbose=False)
    elif args.command == "disassemble":
        disassemble(s3w, args.path, int(args.part_size_gb * 2**30), args.dry_run)
    elif args.command == "upload":
        if args.timeout:
            signal.alarm(args.timeout)
        upload(s3w, args.dry_run)
    elif args.command == "download":
        download(s3w, args.path, args.dry_run)
    elif args.command == "reassemble":
        reassemble(s3w, args.work_dir, args.dst_path, args.dry_run)

    return 0


if __name__ == "__main__":
    try:
        ret = main()
    except Exception:
        # log time of the exception
        logging.error("An exception occurred")
        raise
    sys.exit(ret)
