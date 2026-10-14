#!/usr/bin/env python3
"""Continue active local Codex root sessions after unresolved overload errors."""

import argparse
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time


@contextmanager
def database(path):
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()


def active(home, session_id):
    """A stale lock file is not an active writer."""
    try:
        with (home / 'thread-writer-locks' / (session_id + '.lock')).open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(lock, fcntl.LOCK_UN)
    except FileNotFoundError:
        pass
    return False


class Rollout:
    def __init__(self, path):
        self.path = Path(path)
        self.identity = None
        self.offset = 0
        self.error = None

    def refresh(self):
        with self.path.open('rb') as stream:
            stat = os.fstat(stream.fileno())
            identity = (stat.st_dev, stat.st_ino)
            if identity != self.identity or stat.st_size < self.offset:
                self.offset = 0
                self.error = None
                self.identity = identity
            stream.seek(self.offset)
            while True:
                line = stream.readline()
                if not line or not line.endswith(b'\n'):
                    break  # The writer may still be appending this line.
                item = json.loads(line)
                self.consume(item)
                self.offset = stream.tell()
        return self.error

    def consume(self, item):
        payload = item.get('payload', {})
        if item.get('type') == 'event_msg':
            kind = payload.get('type')
            if kind in ('task_started', 'turn_aborted', 'user_message'):
                self.error = None
            elif kind == 'task_complete':
                error = payload.get('error') or {}
                self.error = None
                if isinstance(error, dict) and error.get('codex_error_info') == 'server_overloaded':
                    timestamp = datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00')).timestamp()
                    self.error = (payload['turn_id'], item['timestamp'], timestamp)
        # Some versions record accepted user input without a user_message event.
        messages = payload.get('items', []) if item.get('type') == 'history_mutation' else [payload] if item.get('type') == 'response_item' else []
        if any(x.get('type') == 'message' and x.get('role') == 'user' for x in messages):
            self.error = None


class Monitor:
    def __init__(self, args):
        self.args = args
        self.home = args.codex_home.resolve()
        self.directory = self.home / 'overload-monitor'
        self.ledger_path = self.directory / 'sent.json'
        self.sent = json.loads(self.ledger_path.read_text()) if self.ledger_path.exists() else {}
        self.rollouts = {}

    def targets(self):
        with database(self.home / 'state_5.sqlite') as connection:
            rows = connection.execute('SELECT id, cwd, source, rollout_path FROM threads WHERE archived = 0').fetchall()
        targets = []
        for row in rows:
            if row['source'].startswith('{'):  # Internal and spawned agents are not direct-input roots.
                continue
            if self.args.session and row['id'] not in self.args.session:
                continue
            if self.args.cwd and Path(row['cwd']).resolve() != self.args.cwd.resolve():
                continue
            if active(self.home, row['id']):
                targets.append(row)
        return targets

    def queued(self, session_id):
        with database(self.home / 'queue_1.sqlite') as connection:
            return connection.execute('SELECT 1 FROM queued_items WHERE thread_id = ? LIMIT 1', (session_id,)).fetchone() is not None

    def check(self):
        targets = self.targets()
        live_ids = {row['id'] for row in targets}
        self.rollouts = {key: value for key, value in self.rollouts.items() if key in live_ids}
        actions = 0
        for row in targets:
            session_id = row['id']
            try:
                rollout = self.rollouts.get(session_id)
                if rollout is None or rollout.path != Path(row['rollout_path']):
                    rollout = self.rollouts[session_id] = Rollout(row['rollout_path'])
                error = rollout.refresh()
                if error is None:
                    continue
                key = list(error[:2])
                if self.sent.get(session_id) == key or time.time() - error[2] < self.args.delay:
                    continue
                if self.queued(session_id):
                    continue
                # Recheck immediately before sending; a manual/client retry cancels it.
                if rollout.refresh() != error or not active(self.home, session_id):
                    continue
                if self.args.dry_run:
                    print(f'would continue {session_id} turn={error[0]}', flush=True)
                else:
                    env = dict(os.environ, CODEX_HOME=str(self.home))
                    subprocess.run([self.args.codex, 'queue', '--thread', session_id, '--message', '继续'], env=env, check=True, capture_output=True, text=True, timeout=30)
                    self.sent[session_id] = key
                    temporary = self.ledger_path.with_suffix('.tmp')
                    temporary.write_text(json.dumps(self.sent) + '\n')
                    temporary.replace(self.ledger_path)
                    print(f'continued {session_id} turn={error[0]}', flush=True)
                actions += 1
            except (OSError, ValueError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
                # Exit on an uncertain send outcome rather than enqueue again.
                raise RuntimeError(f'{session_id}: {error}') from error
        return len(targets), actions


def duration(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError('must be a finite nonnegative number of seconds')
    return number


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument('--session', action='append', metavar='ID', help='session UUID; repeat to select several')
    selector.add_argument('--cwd', type=Path, help='exact working directory (not recursive)')
    selector.add_argument('--all', action='store_true', help='all active local root sessions')
    parser.add_argument('--delay', type=duration, default=200, help='seconds after overload before continuing (default: 200)')
    parser.add_argument('--poll', type=duration, default=600, help='check interval in seconds (default: 600)')
    parser.add_argument('--codex-home', type=Path, default=Path(os.environ.get('CODEX_HOME', '~/.codex')).expanduser())
    parser.add_argument('--codex', default='codex', help='Codex executable (default: codex)')
    parser.add_argument('--once', action='store_true', help='check once and exit')
    parser.add_argument('--dry-run', action='store_true', help='report eligible sends without sending')
    args = parser.parse_args(argv)
    if args.poll == 0:
        parser.error('--poll must be greater than zero')
    return args


def main(argv=None):
    args = arguments(argv)
    directory = args.codex_home.resolve() / 'overload-monitor'
    directory.mkdir(parents=True, exist_ok=True)
    try:
        with (directory / 'monitor.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('another monitor is running for this Codex home')
            monitor = Monitor(args)
            print(f'monitoring: delay={args.delay:g}s poll={args.poll:g}s dry_run={args.dry_run}', flush=True)
            while True:
                count, actions = monitor.check()
                if args.once:
                    print(f'checked {count} active sessions; {actions} actions', flush=True)
                    return 0
                time.sleep(args.poll)
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, sqlite3.Error, RuntimeError) as error:
        print(f'error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
