import errno
from io import StringIO
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from contextlib import contextmanager, redirect_stderr
from unittest.mock import patch

import monitor


class FakeMsvcrt:
    """Stands in for the `msvcrt` module so the Windows branch is testable anywhere."""

    LK_NBLCK = 1
    LK_UNLCK = 2

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def locking(self, descriptor, mode, size):
        self.calls.append((mode, size))
        if self.error is not None and mode == self.LK_NBLCK:
            raise self.error


@contextmanager
def sqlite_file(path):
    """Commit on success and always close; Windows keeps open files locked."""
    connection = sqlite3.connect(path)
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        (self.home / 'thread-writer-locks').mkdir()
        self.log = self.home / 'rollout.jsonl'
        self.log.touch()
        with sqlite_file(self.home / 'state_5.sqlite') as db:
            db.execute('CREATE TABLE threads (id, cwd, source, rollout_path, archived)')
            db.execute('INSERT INTO threads VALUES (?, ?, ?, ?, 0)', ('root', str(self.home), 'cli', str(self.log)))
        with sqlite_file(self.home / 'queue_1.sqlite') as db:
            db.execute('CREATE TABLE queued_items (thread_id)')
        self.lock_path = self.home / 'thread-writer-locks/root.lock'
        self.lock_context = None
        self.acquire_session_lock()
        self.addCleanup(self.release_session_lock)
        self.fake_codex = self.home / 'codex'
        (self.home / 'overload-monitor').mkdir()
        self.args = monitor.arguments(['--all', '--codex-home', str(self.home), '--codex', str(self.fake_codex), '--delay', '200'])
        # No test may reach a real Codex executable; record the argv instead.
        self.calls = []
        recorder = patch('monitor.subprocess.run', side_effect=self.record)
        recorder.start()
        self.addCleanup(recorder.stop)

    def record(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    def acquire_session_lock(self):
        self.lock_context = monitor.FileLock(self.lock_path, create=True)
        self.lock_context.__enter__()

    def release_session_lock(self):
        if self.lock_context is not None:
            self.lock_context.__exit__(None, None, None)
            self.lock_context = None

    def event(self, kind, turn='turn', overload=False, timestamp=None):
        payload = {'type': kind, 'turn_id': turn}
        if overload:
            payload['error'] = {'codex_error_info': 'server_overloaded'}
        item = {'type': 'event_msg', 'timestamp': timestamp or '2020-01-01T00:00:00Z', 'payload': payload}
        with self.log.open('a') as stream:
            stream.write(json.dumps(item) + '\n')

    def test_send_once_and_restart_deduplication(self):
        self.event('task_complete', overload=True)
        instance = monitor.Monitor(self.args)
        self.assertEqual(instance.check(), (1, 1))
        self.assertEqual(instance.check(), (1, 0))
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))
        self.assertEqual([argv for argv, _ in self.calls], [[str(self.fake_codex), 'queue', '--thread', 'root', '--message', monitor.MESSAGE]])
        self.assertEqual(self.calls[0][1]['encoding'], 'utf-8')
        self.event('task_started', turn='next')
        self.event('task_complete', turn='next', overload=True)
        self.assertEqual(instance.check(), (1, 1))

    def test_delay(self):
        self.event('task_complete', overload=True)
        instance = monitor.Monitor(self.args)
        timestamp = 1577836800
        with patch('monitor.time.time', return_value=timestamp + 199):
            self.assertEqual(instance.check(), (1, 0))
        with patch('monitor.time.time', return_value=timestamp + 200):
            self.assertEqual(instance.check(), (1, 1))

    def test_new_turn_normal_completion_and_abort_cancel(self):
        for kind in ('task_started', 'task_complete', 'turn_aborted', 'user_message'):
            with self.subTest(kind=kind):
                self.event('task_complete', overload=True)
                self.event(kind)
                self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))
        self.assertEqual(self.calls, [])

    def test_manual_input_cancels(self):
        self.event('task_complete', overload=True)
        with self.log.open('a') as stream:
            stream.write(json.dumps({'type': 'history_mutation', 'payload': {'items': [{'type': 'message', 'role': 'user'}]}}) + '\n')
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))
        self.assertEqual(self.calls, [])

    def test_existing_queue_suppresses_send(self):
        self.event('task_complete', overload=True)
        with sqlite_file(self.home / 'queue_1.sqlite') as db:
            db.execute('INSERT INTO queued_items VALUES (?)', ('root',))
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))
        self.assertEqual(self.calls, [])

    def test_stale_lock_and_subagent_excluded(self):
        self.release_session_lock()
        self.event('task_complete', overload=True)
        self.assertEqual(monitor.Monitor(self.args).check(), (0, 0))
        self.acquire_session_lock()
        with sqlite_file(self.home / 'state_5.sqlite') as db:
            db.execute('UPDATE threads SET source = ?', ('{"subagent": {}}',))
        self.assertEqual(monitor.Monitor(self.args).check(), (0, 0))
        self.assertEqual(self.calls, [])

    def test_selectors(self):
        for selector, expected in [(['--session', 'root'], 1), (['--session', 'other'], 0), (['--cwd', str(self.home)], 1), (['--cwd', str(self.home / 'child')], 0)]:
            with self.subTest(selector=selector):
                args = monitor.arguments(selector + ['--codex-home', str(self.home), '--dry-run'])
                self.assertEqual(len(monitor.Monitor(args).targets()), expected)

    def test_extended_length_cwd_matches_plain_path(self):
        recorded = '\\\\?\\' + str(self.home)
        with sqlite_file(self.home / 'state_5.sqlite') as db:
            db.execute('UPDATE threads SET cwd = ?', (recorded,))
        self.assertEqual(monitor.comparable(recorded), monitor.comparable(self.home))
        args = monitor.arguments(['--cwd', str(self.home), '--codex-home', str(self.home), '--dry-run'])
        self.assertEqual(len(monitor.Monitor(args).targets()), 1)

    def test_partial_line_then_manual_retry(self):
        self.event('task_complete', overload=True)
        rollout = monitor.Rollout(self.log)
        error = rollout.refresh()
        fragment = json.dumps({'type': 'event_msg', 'payload': {'type': 'task_started'}})
        with self.log.open('a') as stream:
            stream.write(fragment[:20])
        self.assertEqual(rollout.refresh(), error)
        with self.log.open('a') as stream:
            stream.write(fragment[20:] + '\n')
        self.assertIsNone(rollout.refresh())

    def test_final_recheck_cancels_manual_retry(self):
        self.event('task_complete', overload=True)
        instance = monitor.Monitor(self.args)
        def queued(session_id):
            self.event('task_started', turn='manual-retry')
            return False
        instance.queued = queued
        self.assertEqual(instance.check(), (1, 0))
        self.assertEqual(self.calls, [])

    def test_dry_run_does_not_mark_sent(self):
        self.event('task_complete', overload=True)
        self.args.dry_run = True
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 1))
        self.assertEqual(self.calls, [])
        self.assertFalse((self.home / 'overload-monitor/sent.json').exists())

    def test_queue_failure_stops_monitor(self):
        self.event('task_complete', overload=True)
        with patch('monitor.subprocess.run', side_effect=subprocess.CalledProcessError(1, 'codex')) as run:
            with self.assertRaises(RuntimeError):
                monitor.Monitor(self.args).check()
        self.assertEqual(run.call_count, 1)
        self.assertFalse((self.home / 'overload-monitor/sent.json').exists())

    def test_missing_rollout_stops_monitor(self):
        with sqlite_file(self.home / 'state_5.sqlite') as db:
            db.execute('UPDATE threads SET rollout_path = ?', (str(self.home / 'missing.jsonl'),))
        self.event('task_complete', overload=True)
        with self.assertRaises(RuntimeError):
            monitor.Monitor(self.args).check()

    def test_second_instance_cannot_send(self):
        self.event('task_complete', overload=True)
        noise = StringIO()
        with monitor.FileLock(self.home / 'overload-monitor/monitor.lock', create=True), redirect_stderr(noise):
            self.assertEqual(monitor.main(['--all', '--codex-home', str(self.home), '--codex', str(self.fake_codex), '--once']), 1)
        self.assertEqual(self.calls, [])
        self.assertIn('another monitor is running', noise.getvalue())

    def test_lock_detects_contention(self):
        path = self.home / 'contended.lock'
        with monitor.FileLock(path, create=True):
            with self.assertRaises(BlockingIOError):
                with monitor.FileLock(path):
                    pass
        with monitor.FileLock(path):
            pass

    def test_active_session_is_locked_and_missing_lock_is_not(self):
        self.assertTrue(monitor.active(self.home, 'root'))
        self.assertFalse(monitor.active(self.home, 'unknown'))
        (self.home / 'thread-writer-locks/other.lock').touch()
        self.assertFalse(monitor.active(self.home, 'other'))

    def test_msvcrt_lock_violation_means_contended(self):
        busy = PermissionError(errno.EACCES, 'Permission denied')
        fake = FakeMsvcrt(busy)
        with patch.object(monitor, 'IS_WINDOWS', True), patch.object(monitor, 'msvcrt', fake, create=True):
            with self.assertRaises(BlockingIOError):
                with monitor.FileLock(self.home / 'busy.lock', create=True):
                    pass
        self.assertEqual([mode for mode, _ in fake.calls], [fake.LK_NBLCK])

        held = FakeMsvcrt(busy)
        with patch.object(monitor, 'IS_WINDOWS', True), patch.object(monitor, 'msvcrt', held, create=True):
            self.assertTrue(monitor.active(self.home, 'root'))
        self.assertEqual([mode for mode, _ in held.calls], [held.LK_NBLCK])

    def test_other_lock_errors_propagate(self):
        fake = FakeMsvcrt(PermissionError(errno.EBADF, 'Bad file descriptor'))
        with patch.object(monitor, 'IS_WINDOWS', True), patch.object(monitor, 'msvcrt', fake, create=True):
            with self.assertRaises(PermissionError):
                with monitor.FileLock(self.home / 'broken.lock', create=True):
                    pass
        self.assertFalse(monitor.contended(FileNotFoundError(errno.ENOENT, 'missing')))

    def test_batch_shim_runs_through_cmd(self):
        shim = str(self.home / 'codex.cmd')
        with patch.object(monitor, 'IS_WINDOWS', True), patch.object(monitor.shutil, 'which', return_value=shim):
            self.assertEqual(monitor.Monitor(self.args).command('root'), ['cmd.exe', '/c', shim, 'queue', '--thread', 'root', '--message', monitor.MESSAGE])

    def test_executable_is_used_directly(self):
        program = str(self.home / 'codex.exe')
        with patch.object(monitor, 'IS_WINDOWS', True), patch.object(monitor.shutil, 'which', return_value=program):
            self.assertEqual(monitor.Monitor(self.args).command('root'), [program, 'queue', '--thread', 'root', '--message', monitor.MESSAGE])


if __name__ == '__main__':
    unittest.main()
