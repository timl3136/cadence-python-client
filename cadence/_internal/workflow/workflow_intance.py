import logging
from asyncio import CancelledError, InvalidStateError, Task
from typing import Optional, Type

from cadence._internal.workflow.deterministic_event_loop import DeterministicEventLoop
from cadence.api.v1.common_pb2 import Payload
from cadence.data_converter import DataConverter
from cadence.error import WorkflowFailure
from cadence.workflow import WorkflowDefinition

logger = logging.getLogger(__name__)


class WorkflowInstance:
    def __init__(
        self,
        loop: DeterministicEventLoop,
        workflow_definition: WorkflowDefinition,
        data_converter: DataConverter,
    ):
        self._loop = loop
        self._definition = workflow_definition
        self._data_converter = data_converter
        self._instance = workflow_definition.cls()  # construct a new workflow object
        self._task: Optional[Task] = None

    def start(self, payload: Payload):
        if self._task is None:
            run_method = self._definition.get_run_method(self._instance)
            # noinspection PyProtectedMember
            workflow_input = self._definition._run_signature.params_from_payload(
                self._data_converter, payload
            )
            self._task = self._loop.create_task(run_method(*workflow_input))

    def handle_signal(
        self, signal_name: str, payload: Payload, event_id: int
    ) -> None:
        """Handle an incoming signal by invoking the registered signal handler.

        Looks up the signal definition by name, decodes the payload using the
        data converter and parameter type hints, and invokes the handler on the
        workflow instance. Async handlers are scheduled as tasks on the
        deterministic event loop so they execute during run_once().

        Args:
            signal_name: The name of the signal to handle.
            payload: The encoded signal input payload.
            event_id: The history event ID (used for logging context).
        """
        # Guard: reject signals after workflow completion (matches Java client)
        if self.is_done():
            logger.warning(
                "Signal received after workflow is completed, ignoring",
                extra={"signal_name": signal_name, "event_id": event_id},
            )
            return

        signal_def = self._definition.signals.get(signal_name)
        if signal_def is None:
            logger.warning(
                "Received signal with no registered handler, ignoring",
                extra={"signal_name": signal_name, "event_id": event_id},
            )
            return

        # Decode payload using parameter type hints from the signal definition.
        # Deserialization errors are caught and logged rather than crashing the
        # decision (matches Java client DataConverterException handling).
        type_hints: list[Type | None] = [p.type_hint for p in signal_def.params]
        try:
            if type_hints:
                decoded_args = self._data_converter.from_data(payload, type_hints)
            else:
                decoded_args = []
        except Exception:
            logger.error(
                "Failed to deserialize signal payload, dropping signal",
                extra={"signal_name": signal_name, "event_id": event_id},
                exc_info=True,
            )
            return

        # Invoke the handler on the workflow instance.
        # signal_def._wrapped is the unbound class method, so we pass
        # self._instance as the first argument.
        # Handler invocation errors are caught and logged rather than crashing
        # the decision task (matches Java client InvocationTargetException handling).
        try:
            if signal_def.is_async:
                coro = signal_def(self._instance, *decoded_args)
                self._loop.create_task(coro)
            else:
                signal_def(self._instance, *decoded_args)
        except Exception:
            logger.error(
                "Signal handler raised an exception",
                extra={"signal_name": signal_name, "event_id": event_id},
                exc_info=True,
            )

    def run_once(self):
        self._loop.run_until_yield()

    def is_done(self) -> bool:
        return self._task is not None and self._task.done()

    # TODO: consider cache result to avoid multiple data conversions
    def get_result(self) -> Payload:
        if self._task is None:
            raise RuntimeError("Workflow is not started yet")
        try:
            result = self._task.result()
        except (CancelledError, InvalidStateError) as e:
            raise e
        except Exception as e:
            raise WorkflowFailure(f"Workflow failed: {e}") from e
        # TODO: handle result with multiple outputs
        return self._data_converter.to_data([result])
