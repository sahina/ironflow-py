from __future__ import annotations

import asyncio
import builtins
import io
import json
import logging
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from protobuf.wkt import Timestamp

from ironflow._gen.types_pb import Event
from ironflow._gen.worker_pb import JobAssignment, JobContext
from ironflow.serve._handler import handle
from ironflow.worker import CorrelationFormatter, StreamingWorker, function
from ironflow.worker._run import run_function
from ironflow.worker._step import ExecutionContext, RunInfo, StepError, StepTimeoutError
from ironflow.worker._streaming import _job_from_proto
from ironflow.worker._worker import _ActiveJob
from tests.worker.fake_engine import FakeEngine, make_job
from tests.worker.test_worker_jobs import worker_for
from tests.worker.test_worker_loop import run_until

TRACE = '1234567890abcdef1234567890abcdef'
CARRIER = {'traceparent': f'00-{TRACE}-1234567890abcdef-00'}
EVENT = {'id': 'ev', 'name': 'e', 'data': {}, 'timestamp': '2026-10-31T00:00:00Z'}


class Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def sink() -> Any:
    logger = logging.getLogger('ironflow.serve')
    previous = logger.handlers[:], logger.level, logger.propagate, logger.disabled
    capture = Capture()
    logger.handlers = [capture]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.disabled = False
    yield logger, capture
    logger.handlers, logger.level, logger.propagate, logger.disabled = previous


async def execute(mode: str, handler: Any, logger: logging.Logger, *, memo: bool = False) -> Any:
    fn = function(id='fn', name='Display name', triggers=[{'event': 'e'}])(handler)
    if mode == 'push':
        return await handle([fn], method='POST', path='/', headers=CARRIER, body=json.dumps({
            'run_id': 'run_1', 'function_id': 'fn', 'attempt': 3, 'event': EVENT,
            'steps': [{'id': 'run_1:work:0', 'output': 7}] if memo else [],
        }).encode())
    completed = [{'step_id': 'run_1:work:0', 'name': 'work', 'output': 7}] if memo else []
    if mode == 'stream':
        worker = StreamingWorker(functions=[fn], logger=logger)
        job = _job_from_proto(JobAssignment(
            job_id='j', run_id='run_1', function_id='fn', attempt=3,
            event=Event(id='ev', name='e', timestamp=Timestamp(seconds=1)),
            context=JobContext(**CARRIER), execution_seq=1, lease_token='t',
        ))
        job['completed_steps'] = completed
        await worker._execute(_ActiveJob(job=job, started_at=''))
        return list(worker._outbox)
    engine = FakeEngine()
    try:
        engine.enqueue(make_job(attempt=3, context=CARRIER, completed_steps=completed))
        worker = worker_for(engine, handler, logger=logger)
        await run_until(worker, lambda: engine.calls('terminal'))
        return engine.calls('terminal')
    finally:
        engine.shutdown()


@pytest.mark.parametrize('mode', ['push', 'poll', 'stream'])
@pytest.mark.parametrize('bridge', [False, True])
def test_native_line_and_application_bridge(mode: str, bridge: bool, sink: Any) -> None:
    logger, capture = sink
    text = io.StringIO()
    handler = logging.StreamHandler(text)
    handler.setFormatter(CorrelationFormatter())
    logger.addHandler(handler)
    provider = LoggerProvider(shutdown_on_exit=False)
    exporter = InMemoryLogRecordExporter()
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    if bridge:
        logger.addHandler(LoggingHandler(logger_provider=provider))

    async def callback(ctx: Any) -> int:
        def work() -> int:
            try:
                raise ValueError('explicit exception')
            except ValueError:
                ctx.logger.warning('value %s', 7, extra={
                    'app': 'retained', 'run_id': 'spoof', 'function_id': 'spoof',
                    'step_id': 'spoof', 'attempt': 'spoof', 'trace_id': 'spoof',
                }, exc_info=True)
            return 7
        await ctx.step.run('work', work)
        ctx.logger.debug('filtered')
        ctx.logger.info('outside')
        return 7

    try:
        asyncio.run(execute(mode, callback, logger))
        records = [r for r in capture.records if r.getMessage() == 'value 7']
        assert len(records) == 1
        record = records[0]
        assert (record.run_id, record.function_id, record.step_id, record.attempt, record.trace_id) == (
            'run_1', 'fn', 'run_1:work:0', 3, TRACE)
        assert record.app == 'retained' and record.exc_info[0] is ValueError
        assert f'value 7 run_id=run_1 function_id=fn step_id=run_1:work:0 attempt=3 trace_id={TRACE}' in text.getvalue()
        assert 'ValueError: explicit exception' in text.getvalue()
        outside = next(r for r in capture.records if r.getMessage() == 'outside')
        assert outside.step_id == ''
        assert not any(r.getMessage() == 'filtered' for r in capture.records)
        for diagnostic in (r for r in capture.records if r.getMessage() not in ('value 7', 'outside')):
            assert not hasattr(diagnostic, 'run_id')
        logs = exporter.get_finished_logs()
        if bridge:
            logs = [entry.log_record for entry in logs if entry.log_record.body == 'value 7']
            assert len(logs) == 1
            log = logs[0]
            assert log.trace_id == int(TRACE, 16) and log.span_id != 0 and log.trace_flags == 0
            assert log.attributes['trace_id'] == TRACE
            assert log.attributes['attempt'] == 3 and log.attributes['step_id'] == 'run_1:work:0'
            assert log.attributes['run_id'] == 'run_1' and log.attributes['function_id'] == 'fn'
            assert log.attributes['app'] == 'retained'
            assert log.severity_text == 'WARN'
        else:
            assert logs == ()
    finally:
        provider.shutdown()


@pytest.mark.parametrize('mode', ['push', 'poll', 'stream'])
def test_memoized_callback_emits_nothing(mode: str, sink: Any) -> None:
    logger, capture = sink
    async def callback(ctx: Any) -> int:
        return await ctx.step.run('work', lambda: ctx.logger.warning('must not run'))
    asyncio.run(execute(mode, callback, logger, memo=True))
    assert not any(r.getMessage() == 'must not run' for r in capture.records)


def run_callback(handler: Any, logger: logging.Logger, *, run_id: str = 'long-full-run', fn_id: str = 'fn', attempt: int = 2) -> Any:
    fn = function(id=fn_id, triggers=[{'event': 'e'}])(handler)
    return run_function(fn, raw_event=EVENT, upcasters=None, ctx=ExecutionContext(run_id, []),
                        run=RunInfo(id=run_id, function_id=fn_id, attempt=attempt), secrets={},
                        logger=logging.LoggerAdapter(logger, {}))


def test_nested_parallel_and_concurrent_run_identity(sink: Any) -> None:
    logger, capture = sink
    async def handler(ctx: Any) -> None:
        async def outer() -> None:
            ctx.logger.info('outer')
            await ctx.step.run('inner:escaped', lambda: ctx.logger.info('inner'))
            ctx.logger.info('restored')
        await ctx.step.run('outer', outer)
        async def branch(step: Any) -> None:
            async def body() -> None:
                await asyncio.sleep(0)
                ctx.logger.info('branch')
                await step.run('nested', lambda: ctx.logger.info('branch inner'))
                ctx.logger.info('branch restored')
            await step.run('work', body)
        await ctx.step.parallel('fork', [branch, branch])
        ctx.logger.info('outside')
    async def both() -> None:
        await asyncio.gather(run_callback(handler, logger, run_id='first-same-tail', fn_id='one', attempt=3),
                             run_callback(handler, logger, run_id='other-same-tail', fn_id='two', attempt=5))
    asyncio.run(both())
    for run_id, fn_id, attempt in [('first-same-tail', 'one', 3), ('other-same-tail', 'two', 5)]:
        records = [r for r in capture.records if r.run_id == run_id]
        assert len(records) == 10
        assert all(r.function_id == fn_id and r.attempt == attempt for r in records)
        assert [r.step_id for r in records[:3]] == [f'{run_id}:outer:0', f'{run_id}:inner\\:escaped:0', f'{run_id}:outer:0']
        assert {r.step_id for r in records if r.getMessage() == 'branch'} == {
            f'{run_id}:fork:0:work:0', f'{run_id}:fork:1:work:0'}
        assert {r.step_id for r in records if r.getMessage() == 'branch inner'} == {
            f'{run_id}:fork:0:nested:0', f'{run_id}:fork:1:nested:0'}
        assert {r.step_id for r in records if r.getMessage() == 'branch restored'} == {
            f'{run_id}:fork:0:work:0', f'{run_id}:fork:1:work:0'}
        assert records[-1].step_id == ''


@pytest.mark.parametrize('exit_kind', ['success', 'error', 'cancel', 'timeout'])
def test_detached_child_scope_expires_but_trace_remains(exit_kind: str, sink: Any) -> None:
    logger, capture = sink
    async def handler(ctx: Any) -> None:
        release = asyncio.Event()
        children = []
        async def detached() -> None:
            await release.wait()
            ctx.logger.info('late child')
        async def body() -> None:
            children.append(asyncio.create_task(detached()))
            ctx.logger.info('active')
            if exit_kind == 'error':
                raise ValueError('boom')
            if exit_kind == 'cancel':
                raise asyncio.CancelledError()
            if exit_kind == 'timeout':
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    await release.wait()
                    ctx.logger.info('late timed-out body')
        try:
            await ctx.step.run('work', body, timeout=0.01 if exit_kind == 'timeout' else None)
        except (StepError, StepTimeoutError, asyncio.CancelledError):
            pass
        ctx.logger.info('after')
        release.set()
        await asyncio.gather(*children)
        await asyncio.sleep(0)
    asyncio.run(execute('push', handler, logger))
    active = next(r for r in capture.records if r.getMessage() == 'active')
    assert active.step_id == 'run_1:work:0'
    late = [r for r in capture.records if r.getMessage().startswith('late')]
    assert len(late) == (2 if exit_kind == 'timeout' else 1)
    assert all(r.step_id == '' and r.trace_id == TRACE for r in late)
    assert next(r for r in capture.records if r.getMessage() == 'after').step_id == ''


def test_no_trace_api_and_fallback_respects_native_silence(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    original = builtins.__import__
    def no_trace(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == 'opentelemetry' or name.startswith('opentelemetry.'):
            raise ImportError('optional tracing absent')
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', no_trace)
    logger = logging.getLogger('application.no-handlers')
    logger.handlers = []
    logger.propagate = False
    logger.setLevel(logging.WARNING)
    async def handler(ctx: Any) -> None:
        await ctx.step.run('work', lambda: ctx.logger.warning('fallback %s', 'body'))
        ctx.logger.info('filtered')
    asyncio.run(run_callback(handler, logger))
    assert capsys.readouterr().err.strip() == (
        'WARNING fallback body run_id=long-full-run function_id=fn '
        'step_id=long-full-run:work:0 attempt=2 trace_id=')
    assert logger.handlers == []
    logger.addHandler(logging.NullHandler())
    asyncio.run(run_callback(handler, logger))
    assert capsys.readouterr().err == ''
    logger.handlers = []
    logger.disabled = True
    asyncio.run(run_callback(handler, logger))
    assert capsys.readouterr().err == ''
    logger.disabled = False
    logger.setLevel(logging.ERROR)
    asyncio.run(run_callback(handler, logger))
    assert capsys.readouterr().err == ''


def test_reachable_ancestor_and_propagation(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    parent = logging.getLogger('application.route')
    child = logging.getLogger('application.route.callback')
    capture = Capture()
    monkeypatch.setattr(parent, 'handlers', [capture])
    monkeypatch.setattr(parent, 'propagate', False)
    monkeypatch.setattr(child, 'handlers', [])
    monkeypatch.setattr(child, 'propagate', True)
    child.setLevel(logging.INFO)
    async def handler(ctx: Any) -> None:
        ctx.logger.info('owned route', extra={'app': 7})
    asyncio.run(run_callback(handler, child))
    assert len(capture.records) == 1 and capture.records[0].app == 7
    assert capsys.readouterr().err == ''
    monkeypatch.setattr(parent, 'handlers', [logging.NullHandler()])
    asyncio.run(run_callback(handler, child))
    assert capsys.readouterr().err == ''
    monkeypatch.setattr(child, 'propagate', False)
    asyncio.run(run_callback(handler, child))
    assert 'owned route run_id=long-full-run' in capsys.readouterr().err


@pytest.mark.parametrize('valid', [False, True])
def test_trace_validity_without_recording_provider(valid: bool, sink: Any) -> None:
    logger, capture = sink
    context = SpanContext(int(TRACE, 16) if valid else 0, 7, False, TraceFlags(0))
    async def callback(ctx: Any) -> None:
        with trace.use_span(NonRecordingSpan(context)):
            await ctx.step.run('work', lambda: ctx.logger.info('nonrecording'))
    asyncio.run(run_callback(callback, logger))
    assert capture.records[0].trace_id == (TRACE if valid else '')


def test_native_handler_exception_follows_callback_error_handling(sink: Any) -> None:
    logger, _ = sink
    class Broken(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise RuntimeError('application handler failed')
    logger.handlers = [Broken()]
    async def callback(ctx: Any) -> None:
        ctx.logger.warning('one attempt')
    result = asyncio.run(run_callback(callback, logger))
    assert result['status'] == 'failed' and result['error']['message'] == 'application handler failed'


def test_asgi_wrapper_keeps_push_hierarchy_and_default_attempt(sink: Any) -> None:
    from ironflow.serve import serve
    from tests.serve.test_asgi import body_chunks, drive, http
    logger, capture = sink
    @function(id='fn', triggers=[{'event': 'e'}])
    async def callback(ctx: Any) -> int:
        await ctx.step.run('work', lambda: ctx.logger.warning('asgi'))
        return 7
    sent = drive(serve([callback], signing_key=''), http(), body_chunks(json.dumps({
        'run_id': 'full-run', 'function_id': 'fn', 'event': EVENT,
    }).encode()))
    assert sent[0]['status'] == 200
    record = next(r for r in capture.records if r.getMessage() == 'asgi')
    assert (record.name, record.run_id, record.function_id, record.step_id, record.attempt) == (
        logger.name, 'full-run', 'fn', 'full-run:work:0', 1)


@pytest.mark.parametrize('mode', ['push', 'poll', 'stream'])
def test_bridge_intrinsic_span_matches_live_step(mode: str, sink: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON
    logger, _ = sink
    spans = InMemorySpanExporter()
    traces = TracerProvider(sampler=ALWAYS_ON)
    traces.add_span_processor(SimpleSpanProcessor(spans))
    monkeypatch.setattr(trace, '_TRACER_PROVIDER', traces)
    provider = LoggerProvider(shutdown_on_exit=False)
    logs = InMemoryLogRecordExporter()
    provider.add_log_record_processor(SimpleLogRecordProcessor(logs))
    logger.handlers = [LoggingHandler(logger_provider=provider)]
    async def callback(ctx: Any) -> None:
        await ctx.step.run('work', lambda: ctx.logger.info('step bridge'))
        ctx.logger.info('run bridge')
    try:
        asyncio.run(execute(mode, callback, logger))
        callbacks = [entry.log_record for entry in logs.get_finished_logs()
                     if entry.log_record.body in ('step bridge', 'run bridge')]
        assert len(callbacks) == 2
        records = {record.body: record for record in callbacks}
        assert set(records) == {'step bridge', 'run bridge'}
        for body, name in [('step bridge', 'step.run'), ('run bridge', 'process')]:
            span = next(s for s in spans.get_finished_spans() if s.name == name)
            record = records[body]
            assert (record.trace_id, record.span_id, record.trace_flags) == (
                span.context.trace_id, span.context.span_id, span.context.trace_flags)
            assert record.trace_flags == 1
            assert record.attributes['trace_id'] == TRACE
    finally:
        provider.shutdown()
        traces.shutdown()


def test_root_handler_is_application_owned(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    capture = Capture()
    root = logging.getLogger()
    logger = logging.getLogger('application.root-route')
    monkeypatch.setattr(root, 'handlers', [capture])
    monkeypatch.setattr(logger, 'handlers', [])
    monkeypatch.setattr(logger, 'propagate', True)
    logger.setLevel(logging.INFO)
    async def callback(ctx: Any) -> None:
        ctx.logger.info('root')
    asyncio.run(run_callback(callback, logger))
    assert len(capture.records) == 1 and capture.records[0].run_id == 'long-full-run'
    assert capsys.readouterr().err == ''
    assert root.handlers == [capture]


def test_import_and_base_execution_do_not_load_logs_sdk_or_open_network() -> None:
    import subprocess
    import sys
    import textwrap
    result = subprocess.run([sys.executable, '-c', textwrap.dedent('''
        import asyncio, logging, socket, sys
        def blocked(*args, **kwargs):
            raise AssertionError('unexpected SDK network activity')
        socket.socket.connect = blocked
        socket.create_connection = blocked
        from ironflow.worker import function
        from ironflow.worker._run import run_function
        from ironflow.worker._step import ExecutionContext, RunInfo
        @function(id='fn', triggers=[{'event': 'e'}])
        async def handler(ctx):
            await ctx.step.run('work', lambda: ctx.logger.warning('local'))
        logger = logging.getLogger('application')
        logger.addHandler(logging.NullHandler())
        result = asyncio.run(run_function(handler, raw_event={
            'id': 'e', 'name': 'e', 'timestamp': '2026-10-31T00:00:00Z', 'data': {}},
            upcasters=None, ctx=ExecutionContext('run', []),
            run=RunInfo(id='run', function_id='fn', attempt=1), secrets={},
            logger=logging.LoggerAdapter(logger, {})))
        assert result['status'] == 'completed'
        assert 'opentelemetry.sdk._logs' not in sys.modules
    ''')], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_sync_body_continuing_after_timeout_loses_only_step(sink: Any) -> None:
    import threading
    logger, capture = sink
    released, finished = threading.Event(), threading.Event()
    async def callback(ctx: Any) -> None:
        def body() -> None:
            ctx.logger.info('thread active')
            released.wait(2)
            ctx.logger.info('thread late')
            finished.set()
        try:
            await ctx.step.run('thread', body, timeout=0.02)
        except StepTimeoutError:
            released.set()
            assert await asyncio.to_thread(finished.wait, 2)
    asyncio.run(execute('push', callback, logger))
    active, late = [r for r in capture.records if r.getMessage().startswith('thread')]
    assert active.step_id == 'run_1:thread:0'
    assert late.step_id == '' and late.trace_id == TRACE


@pytest.mark.parametrize('mode', ['push', 'poll', 'stream'])
def test_application_null_handler_stays_silent(mode: str, sink: Any, capsys: Any) -> None:
    logger, capture = sink
    null = logging.NullHandler()
    logger.handlers = [null]
    async def callback(ctx: Any) -> None:
        await ctx.step.run('work', lambda: ctx.logger.warning('intentionally silent'))
    asyncio.run(execute(mode, callback, logger))
    assert logger.handlers == [null]
    assert capture.records == [] and capsys.readouterr().err == ''


def test_bound_extra_merges_with_call_extra_on_python310(sink: Any) -> None:
    logger, capture = sink
    @function(id='fn', triggers=[{'event': 'e'}])
    async def callback(ctx: Any) -> None:
        ctx.logger.info('merged', extra={'application': 'call', 'other': 9, 'attempt': 'spoof'})
    result = asyncio.run(run_function(callback, raw_event=EVENT, upcasters=None,
        ctx=ExecutionContext('r', []), run=RunInfo(id='r', function_id='fn', attempt=4), secrets={},
        logger=logging.LoggerAdapter(logger, {'bound': 7, 'application': 'bound', 'run_id': 'spoof'})))
    assert result['status'] == 'completed'
    record = capture.records[0]
    assert (record.bound, record.application, record.other, record.attempt, record.run_id) == (7, 'call', 9, 4, 'r')


def test_filter_and_global_disable_do_not_trigger_fallback(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    logger = logging.getLogger('application.filtered')
    logger.setLevel(logging.INFO)
    monkeypatch.setattr(logger, 'handlers', [])
    monkeypatch.setattr(logger, 'propagate', False)
    class Drop(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return False
    monkeypatch.setattr(logger, 'filters', [Drop()])
    async def callback(ctx: Any) -> None:
        ctx.logger.warning('silent')
    asyncio.run(run_callback(callback, logger))
    assert capsys.readouterr().err == ''
    monkeypatch.setattr(logger, 'filters', [])
    original_disable = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        asyncio.run(run_callback(callback, logger))
        assert capsys.readouterr().err == ''
    finally:
        logging.disable(original_disable)


def test_explicit_noop_bridge_does_not_enable_fallback(sink: Any, capsys: Any) -> None:
    from opentelemetry._logs import NoOpLoggerProvider
    logger, capture = sink
    logger.handlers = [LoggingHandler(logger_provider=NoOpLoggerProvider())]
    async def callback(ctx: Any) -> None:
        ctx.logger.warning('intentionally dropped')
    asyncio.run(run_callback(callback, logger))
    assert capture.records == [] and capsys.readouterr().err == ''


def test_nested_descendant_loses_step_after_ancestor_timeout(sink: Any) -> None:
    logger, capture = sink

    async def handler(ctx: Any) -> None:
        release = asyncio.Event()
        children: list[asyncio.Task[Any]] = []

        async def inner() -> None:
            ctx.logger.info('nested active')
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
                ctx.logger.info('nested late')

        async def outer() -> None:
            task = asyncio.create_task(ctx.step.run('inner', inner))
            children.append(task)
            await task

        try:
            with pytest.raises(StepTimeoutError):
                await ctx.step.run('outer', outer, timeout=0.01)
            ctx.logger.info('ancestor returned')
        finally:
            release.set()
            await asyncio.gather(*children)

    asyncio.run(execute('push', handler, logger))
    active = next(r for r in capture.records if r.getMessage() == 'nested active')
    late = next(r for r in capture.records if r.getMessage() == 'nested late')
    assert active.step_id == 'run_1:inner:0'
    assert next(r for r in capture.records if r.getMessage() == 'ancestor returned').step_id == ''
    assert late.step_id == ''
    assert late.trace_id == active.trace_id == TRACE
