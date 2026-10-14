import fcntl
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import monitor


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        (self.home / 'thread-writer-locks').mkdir()
        self.log = self.home / 'rollout.jsonl'
        self.log.touch()
        with sqlite3.connect(self.home / 'state_5.sqlite') as db:
            db.execute('CREATE TABLE threads (id, cwd, source, rollout_path, archived)')
            db.execute('INSERT INTO threads VALUES (?, ?, ?, ?, 0)', ('root', str(self.home), 'cli', str(self.log)))
        with sqlite3.connect(self.home / 'queue_1.sqlite') as db:
            db.execute('CREATE TABLE queued_items (thread_id)')
        self.lock = (self.home / 'thread-writer-locks/root.lock').open('w')
        self.addCleanup(self.lock.close)
        fcntl.flock(self.lock, fcntl.LOCK_EX)
        self.fake_codex = self.home / 'codex'
        self.fake_codex.write_text('#!/usr/bin/env python3\nimport json, os, sys\nfrom pathlib import Path\nwith (Path(os.environ["CODEX_HOME"]) / "calls").open("a") as f: f.write(json.dumps(sys.argv[1:]) + "\\n")\n')
        self.fake_codex.chmod(0o755)
        (self.home / 'overload-monitor').mkdir()
        self.args = monitor.arguments(['--all', '--codex-home', str(self.home), '--codex', str(self.fake_codex), '--delay', '200'])

    def event(self, kind, turn='turn', overload=False, timestamp=None):
        payload = {'type': kind, 'turn_id': turn}
        if overload:
            payload['error'] = {'codex_error_info': 'server_overloaded'}
        item = {'type': 'event_msg', 'timestamp': timestamp or '2020-01-01T00:00:00Z', 'payload': payload}
        with self.log.open('a') as stream:
            stream.write(json.dumps(item) + '\n')

    def calls(self):
        path = self.home / 'calls'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_send_once_and_restart_deduplication(self):
        self.event('task_complete', overload=True)
        instance = monitor.Monitor(self.args)
        self.assertEqual(instance.check(), (1, 1))
        self.assertEqual(instance.check(), (1, 0))
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))
        self.assertEqual(self.calls(), [['queue', '--thread', 'root', '--message', '继续']])
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

    def test_manual_input_cancels(self):
        self.event('task_complete', overload=True)
        with self.log.open('a') as stream:
            stream.write(json.dumps({'type': 'history_mutation', 'payload': {'items': [{'type': 'message', 'role': 'user'}]}}) + '\n')
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))

    def test_existing_queue_suppresses_send(self):
        self.event('task_complete', overload=True)
        with sqlite3.connect(self.home / 'queue_1.sqlite') as db:
            db.execute('INSERT INTO queued_items VALUES (?)', ('root',))
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 0))

    def test_stale_lock_and_subagent_excluded(self):
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.event('task_complete', overload=True)
        self.assertEqual(monitor.Monitor(self.args).check(), (0, 0))
        fcntl.flock(self.lock, fcntl.LOCK_EX)
        with sqlite3.connect(self.home / 'state_5.sqlite') as db:
            db.execute('UPDATE threads SET source = ?', ('{"subagent": {}}',))
        self.assertEqual(monitor.Monitor(self.args).check(), (0, 0))

    def test_selectors(self):
        for selector, expected in [(['--session', 'root'], 1), (['--session', 'other'], 0), (['--cwd', str(self.home)], 1), (['--cwd', str(self.home / 'child')], 0)]:
            with self.subTest(selector=selector):
                args = monitor.arguments(selector + ['--codex-home', str(self.home), '--dry-run'])
                self.assertEqual(len(monitor.Monitor(args).targets()), expected)

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
        self.assertEqual(self.calls(), [])

    def test_dry_run_does_not_mark_sent(self):
        self.event('task_complete', overload=True)
        self.args.dry_run = True
        self.assertEqual(monitor.Monitor(self.args).check(), (1, 1))
        self.assertEqual(self.calls(), [])
        self.assertFalse((self.home / 'overload-monitor/sent.json').exists())

    def test_queue_failure_stops_monitor(self):
        self.event('task_complete', overload=True)
        self.fake_codex.write_text('#!/bin/sh\nexit 1\n')
        with self.assertRaises(RuntimeError):
            monitor.Monitor(self.args).check()
        self.assertFalse((self.home / 'overload-monitor/sent.json').exists())

    def test_second_instance_cannot_send(self):
        self.event('task_complete', overload=True)
        with (self.home / 'overload-monitor/monitor.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.assertEqual(monitor.main(['--all', '--codex-home', str(self.home), '--codex', str(self.fake_codex), '--once']), 1)
        self.assertEqual(self.calls(), [])


if __name__ == '__main__':
    unittest.main()
