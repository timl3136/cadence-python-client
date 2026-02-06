#!/usr/bin/env python3
import asyncio
from typing import List

import pytest
from cadence.api.v1.common_pb2 import Payload
from cadence.api.v1.history_pb2 import (
    DecisionTaskCompletedEventAttributes,
    DecisionTaskScheduledEventAttributes,
    DecisionTaskStartedEventAttributes,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionSignaledEventAttributes,
    WorkflowExecutionStartedEventAttributes,
)
from cadence._internal.workflow.workflow_engine import WorkflowEngine
from cadence import workflow
from cadence.data_converter import DefaultDataConverter
from cadence.workflow import WorkflowInfo, WorkflowDefinition, WorkflowDefinitionOptions


class TestWorkflow:
    @workflow.run
    async def echo(self, input_data):
        return f"echo: {input_data}"


# -- Signal workflow definitions used by tests --


class SyncSignalWorkflow:
    """Workflow with a sync signal handler that mutates state.

    The signal is delivered in the same input batch as the workflow start,
    so the sync handler modifies state in Phase 2 before run_once() in Phase 3.
    """

    def __init__(self):
        self.received_value: str = "none"

    @workflow.run
    async def run(self):
        return self.received_value

    @workflow.signal(name="set_value")
    def set_value(self, value: str):
        self.received_value = value


class AsyncSignalWorkflow:
    """Workflow with an async signal handler.

    Uses a Future so the workflow blocks until the async signal handler runs.
    Both the workflow task and the signal handler task execute during run_once():
    the workflow task creates and awaits the future first, then the signal handler
    task resolves it.
    """

    def __init__(self):
        self._signal_future: asyncio.Future | None = None

    @workflow.run
    async def run(self):
        loop = asyncio.get_event_loop()
        self._signal_future = loop.create_future()
        result = await self._signal_future
        return result

    @workflow.signal(name="set_value_async")
    async def set_value_async(self, value: str):
        if self._signal_future and not self._signal_future.done():
            self._signal_future.set_result(value)


class SignalWithPayloadWorkflow:
    """Workflow with a signal handler that accepts multiple typed parameters."""

    def __init__(self):
        self.name: str = ""
        self.count: int = 0

    @workflow.run
    async def run(self):
        return f"{self.name}:{self.count}"

    @workflow.signal(name="set_data")
    def set_data(self, name: str, count: int):
        self.name = name
        self.count = count


class NoSignalWorkflow:
    """Workflow with no signal handlers."""

    @workflow.run
    async def run(self):
        return "done"


class WaitForSignalWorkflow:
    """Workflow that blocks on a Future until a signal resolves it.

    Used to test signal handling across decision batches (replay scenario).
    """

    def __init__(self):
        self._signal_future: asyncio.Future | None = None

    @workflow.run
    async def run(self):
        loop = asyncio.get_event_loop()
        self._signal_future = loop.create_future()
        result = await self._signal_future
        return result

    @workflow.signal(name="complete")
    def complete(self, value: str):
        if self._signal_future and not self._signal_future.done():
            self._signal_future.set_result(value)


class NoParamSignalWorkflow:
    """Workflow with a signal handler that takes no arguments."""

    def __init__(self):
        self.signaled: bool = False

    @workflow.run
    async def run(self):
        return str(self.signaled)

    @workflow.signal(name="ping")
    def ping(self):
        self.signaled = True


class FailingSignalWorkflow:
    """Workflow whose signal handler raises an exception."""

    def __init__(self):
        self.value: str = "initial"

    @workflow.run
    async def run(self):
        return self.value

    @workflow.signal(name="bad_signal")
    def bad_signal(self, value: str):
        raise RuntimeError("signal handler exploded")


class TestWorkflowEngine:
    """Unit tests for WorkflowEngine."""

    @pytest.fixture
    def echo_workflow_definition(self) -> WorkflowDefinition:
        """Create a mock workflow definition."""
        workflow_opts = WorkflowDefinitionOptions(name="test_workflow")
        return WorkflowDefinition.wrap(TestWorkflow, workflow_opts)

    @pytest.fixture
    def simple_workflow_events(self) -> List[HistoryEvent]:
        return [
            HistoryEvent(
                event_id=1,
                workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                    input=Payload(data=b'"test-input"')
                ),
            ),
            HistoryEvent(
                event_id=2,
                decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
            ),
            HistoryEvent(
                event_id=3,
                decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                    scheduled_event_id=2
                ),
            ),
            HistoryEvent(
                event_id=4,
                decision_task_completed_event_attributes=DecisionTaskCompletedEventAttributes(
                    scheduled_event_id=2,
                ),
            ),
            HistoryEvent(
                event_id=5,
                workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
                    result=Payload(data=b'"echo: test-input"')
                ),
            ),
        ]

    def test_process_simple_workflow(
        self,
        echo_workflow_definition: WorkflowDefinition,
        simple_workflow_events: List[HistoryEvent],
    ):
        workflow_engine = create_workflow_engine(echo_workflow_definition)
        decision_result = workflow_engine.process_decision(simple_workflow_events[:3])
        assert len(decision_result.decisions) == 1
        assert decision_result.decisions[
            0
        ].complete_workflow_execution_decision_attributes.result == Payload(
            data=b'"echo: test-input"'
        )


class TestSignalHandling:
    """Unit tests for signal handling in WorkflowEngine."""

    def test_sync_signal_mutates_state(self):
        """Sync signal handler modifies workflow state before the workflow run method executes.

        The signal event is in the same input batch as the workflow start event,
        so the handler runs in Phase 2 before run_once() in Phase 3.
        """
        definition = WorkflowDefinition.wrap(
            SyncSignalWorkflow, WorkflowDefinitionOptions(name="sync_signal")
        )
        engine = create_workflow_engine(definition)

        events = _make_single_batch_signal_events(
            signal_name="set_value",
            signal_input=Payload(data=b'"hello"'),
        )

        result = engine.process_decision(events)

        assert len(result.decisions) == 1
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"hello"')
        )

    def test_async_signal_handler(self):
        """Async signal handler is scheduled as a task and resolves a Future the workflow awaits.

        Both workflow task and signal handler task run during run_once().
        The workflow task starts first (creates and awaits the Future), then the
        signal handler task runs and resolves the Future.
        """
        definition = WorkflowDefinition.wrap(
            AsyncSignalWorkflow, WorkflowDefinitionOptions(name="async_signal")
        )
        engine = create_workflow_engine(definition)

        events = _make_single_batch_signal_events(
            signal_name="set_value_async",
            signal_input=Payload(data=b'"async-hello"'),
        )

        result = engine.process_decision(events)

        assert len(result.decisions) == 1
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"async-hello"')
        )

    def test_signal_with_multiple_typed_params(self):
        """Signal handler with multiple typed parameters decodes payload correctly."""
        definition = WorkflowDefinition.wrap(
            SignalWithPayloadWorkflow,
            WorkflowDefinitionOptions(name="payload_signal"),
        )
        engine = create_workflow_engine(definition)

        # Two values encoded in whitespace-delimited JSON: "alice" 42
        events = _make_single_batch_signal_events(
            signal_name="set_data",
            signal_input=Payload(data=b'"alice" 42'),
        )

        result = engine.process_decision(events)

        assert len(result.decisions) == 1
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"alice:42"')
        )

    def test_unknown_signal_does_not_crash(self):
        """Receiving a signal with no registered handler logs a warning but does not crash."""
        definition = WorkflowDefinition.wrap(
            NoSignalWorkflow, WorkflowDefinitionOptions(name="no_signal")
        )
        engine = create_workflow_engine(definition)

        events = _make_single_batch_signal_events(
            signal_name="nonexistent_signal",
            signal_input=Payload(data=b'"ignored"'),
        )

        # Should complete without raising
        result = engine.process_decision(events)

        assert len(result.decisions) == 1
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"done"')
        )

    def test_signal_during_replay(self):
        """Signal handlers are invoked correctly when a signal arrives in a second decision batch.

        The first batch (replayed) starts the workflow, which blocks on a Future.
        The second batch delivers the signal, whose handler resolves the Future.
        """
        definition = WorkflowDefinition.wrap(
            WaitForSignalWorkflow,
            WorkflowDefinitionOptions(name="wait_signal"),
        )

        # First batch only: workflow starts, blocks on Future, does not complete
        engine1 = create_workflow_engine(definition)
        first_batch_events = [
            HistoryEvent(
                event_id=1,
                workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                    input=Payload(data=b""),
                ),
            ),
            HistoryEvent(
                event_id=2,
                decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
            ),
            HistoryEvent(
                event_id=3,
                decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                    scheduled_event_id=2,
                ),
            ),
        ]
        result1 = engine1.process_decision(first_batch_events)
        assert len(result1.decisions) == 0
        assert not engine1.is_done()

        # Full replay with signal in second batch
        engine2 = create_workflow_engine(definition)
        full_events = [
            # Replayed first batch
            HistoryEvent(
                event_id=1,
                workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                    input=Payload(data=b""),
                ),
            ),
            HistoryEvent(
                event_id=2,
                decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
            ),
            HistoryEvent(
                event_id=3,
                decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                    scheduled_event_id=2,
                ),
            ),
            HistoryEvent(
                event_id=4,
                decision_task_completed_event_attributes=DecisionTaskCompletedEventAttributes(
                    scheduled_event_id=2,
                ),
            ),
            # Signal arrives in second batch
            HistoryEvent(
                event_id=5,
                workflow_execution_signaled_event_attributes=WorkflowExecutionSignaledEventAttributes(
                    signal_name="complete",
                    input=Payload(data=b'"signal-result"'),
                ),
            ),
            HistoryEvent(
                event_id=6,
                decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
            ),
            HistoryEvent(
                event_id=7,
                decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                    scheduled_event_id=6,
                ),
            ),
        ]
        result2 = engine2.process_decision(full_events)

        assert len(result2.decisions) == 1
        assert (
            result2.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"signal-result"')
        )

    def test_signal_with_no_params(self):
        """Signal handler that takes no arguments works correctly."""
        definition = WorkflowDefinition.wrap(
            NoParamSignalWorkflow,
            WorkflowDefinitionOptions(name="no_param_signal"),
        )
        engine = create_workflow_engine(definition)

        events = _make_single_batch_signal_events(
            signal_name="ping",
            signal_input=Payload(data=b""),
        )

        result = engine.process_decision(events)

        assert len(result.decisions) == 1
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"True"')
        )

    def test_signal_after_workflow_completed_is_ignored(self):
        """Signals received after the workflow has completed are silently ignored.

        Matches Java client behaviour: ReplayDecider.handleWorkflowExecutionSignaled
        checks 'if (completed)' and rejects the signal.
        """
        definition = WorkflowDefinition.wrap(
            SyncSignalWorkflow, WorkflowDefinitionOptions(name="after_close")
        )
        engine = create_workflow_engine(definition)

        # First batch: workflow starts and completes immediately (no signal)
        first_events = [
            HistoryEvent(
                event_id=1,
                workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                    input=Payload(data=b""),
                ),
            ),
            HistoryEvent(
                event_id=2,
                decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
            ),
            HistoryEvent(
                event_id=3,
                decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                    scheduled_event_id=2,
                ),
            ),
            HistoryEvent(
                event_id=4,
                decision_task_completed_event_attributes=DecisionTaskCompletedEventAttributes(
                    scheduled_event_id=2,
                ),
            ),
            # Signal arrives AFTER workflow already completed in first batch
            HistoryEvent(
                event_id=5,
                workflow_execution_signaled_event_attributes=WorkflowExecutionSignaledEventAttributes(
                    signal_name="set_value",
                    input=Payload(data=b'"late-signal"'),
                ),
            ),
            HistoryEvent(
                event_id=6,
                decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
            ),
            HistoryEvent(
                event_id=7,
                decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                    scheduled_event_id=6,
                ),
            ),
        ]

        # Should complete without error; the late signal is ignored
        result = engine.process_decision(first_events)

        assert len(result.decisions) == 1
        # The result is the initial value "none" because the signal was ignored
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"none"')
        )

    def test_signal_handler_exception_does_not_crash_decision(self):
        """A signal handler that raises an exception is caught and logged.

        The decision task should continue processing. The workflow completes
        with its initial value since the handler never successfully mutated state.
        Matches Java client InvocationTargetException handling.
        """
        definition = WorkflowDefinition.wrap(
            FailingSignalWorkflow,
            WorkflowDefinitionOptions(name="failing_signal"),
        )
        engine = create_workflow_engine(definition)

        events = _make_single_batch_signal_events(
            signal_name="bad_signal",
            signal_input=Payload(data=b'"boom"'),
        )

        # Should NOT raise despite the signal handler throwing
        result = engine.process_decision(events)

        assert len(result.decisions) == 1
        # Workflow completed with initial value since handler failed
        assert (
            result.decisions[0]
            .complete_workflow_execution_decision_attributes.result
            == Payload(data=b'"initial"')
        )


def _make_single_batch_signal_events(
    signal_name: str,
    signal_input: Payload,
) -> List[HistoryEvent]:
    """Create a single-batch event sequence where signal and start are in the same input batch.

    Events 1 (started) and 2 (signaled) are both input events processed in Phase 2.
    The sync signal handler runs before run_once() in Phase 3.
    """
    return [
        HistoryEvent(
            event_id=1,
            workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                input=Payload(data=b""),
            ),
        ),
        HistoryEvent(
            event_id=2,
            workflow_execution_signaled_event_attributes=WorkflowExecutionSignaledEventAttributes(
                signal_name=signal_name,
                input=signal_input,
            ),
        ),
        HistoryEvent(
            event_id=3,
            decision_task_scheduled_event_attributes=DecisionTaskScheduledEventAttributes(),
        ),
        HistoryEvent(
            event_id=4,
            decision_task_started_event_attributes=DecisionTaskStartedEventAttributes(
                scheduled_event_id=3,
            ),
        ),
    ]


def create_workflow_engine(workflow_definition: WorkflowDefinition) -> WorkflowEngine:
    """Create workflow engine."""
    return WorkflowEngine(
        info=WorkflowInfo(
            workflow_type="test_workflow",
            workflow_domain="test-domain",
            workflow_id="test-workflow-id",
            workflow_run_id="test-run-id",
            workflow_task_list="test-task-list",
            data_converter=DefaultDataConverter(),
        ),
        workflow_definition=workflow_definition,
    )
