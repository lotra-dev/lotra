from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from queue import Empty, Queue
from threading import Event, Thread
from time import monotonic
from typing import Any, cast, overload
from uuid import uuid4

from PySide6.QtCore import QPoint, QSize, Qt, QTimer, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QLabel,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from localrsa.brokers.account_selection import matching_accounts
from localrsa.brokers.base import BrokerAdapter, BrokerNotConfiguredError
from localrsa.brokers.connection import connect_broker_profile, session_recovery_required
from localrsa.brokers.registry import create_adapter
from localrsa.cloud import (
    AdminDraft,
    AdminPreviewLine,
    AdminPreviewSubmission,
    BrokerPreviewReason,
    PendingSignal,
    SignalExecutionOutcome,
)
from localrsa.config import load_settings
from localrsa.database.connection import connect
from localrsa.database.migrations import migrate
from localrsa.database.repositories import (
    BrokerLoginRepository,
    PortfolioOrderHistoryRepository,
    PortfolioProtectedTickerRepository,
    TrackedPlayRepository,
)
from localrsa.execution.batch_authorization import (
    BatchAuthorization,
    create_batch_authorization,
)
from localrsa.execution.executor import (
    ExecutionBlockedError,
    ExecutionUncertainError,
    MarketClosedError,
    OrderRejectedError,
    submit_reviewed_order,
)
from localrsa.execution.guard import (
    DatabaseOrderExecutionGuard,
    DuplicateExecutionTargetError,
    ExecutionGuardState,
)
from localrsa.execution.idempotency import idempotency_key
from localrsa.execution.portfolio_observation import TradePortfolioObservationWriter
from localrsa.login_identity import display_login_profiles, load_login_usernames
from localrsa.market_hours import (
    format_market_open,
    market_is_open,
    market_now,
    next_market_open,
    next_market_transition,
)
from localrsa.models.account import BrokerAccount, BrokerLoginProfile
from localrsa.models.broker import BrokerId
from localrsa.models.order import OrderAction, OrderPlan, OrderState, Quote
from localrsa.models.order_history import PortfolioOrderHistoryEntry
from localrsa.models.tracked_play import TrackedPlay
from localrsa.security.device_identity import anonymous_account_reference
from localrsa.ui.activity_log_page import ActivityLogEntry
from localrsa.ui.challenge_dialog import request_challenge
from localrsa.ui.custom_order_dialog import (
    CustomOrderDialog,
    CustomOrderSelection,
    CustomOrderTarget,
)
from localrsa.ui.design_system import (
    PageHeader,
    ToggleSelectionListWidget,
    configure_page,
    create_action_bar,
    mark_status,
)
from localrsa.ui.execution_dialog import ExecutionDialog
from localrsa.ui.task_progress import (
    AnimatedTaskProgressBar,
    TaskChallengeRequest,
    TaskProgressUpdate,
    request_task_challenge,
)

LOGGER = logging.getLogger(__name__)

# The cloud trade ledger (SignalExecutionOutcome / the worker's decimal parser)
# accepts at most 8 fractional digits. Order-cost math -- unit price times a
# fractional-share quantity -- routinely produces more, which made the worker
# reject the whole /complete call with HTTP 400. Round every amount to this
# quantum before it reaches the model.
_LEDGER_QUANTUM = Decimal("0.00000001")


@overload
def _ledger_amount(value: Decimal) -> Decimal: ...


@overload
def _ledger_amount(value: None) -> None: ...


def _ledger_amount(value: Decimal | None) -> Decimal | None:
    """Round a money value to the 8dp the cloud trade ledger accepts.

    ``SignalExecutionOutcome`` also serializes these fields as plain decimal
    strings, so the on-the-wire form never carries an exponent; this just
    keeps the value inside the model's ``decimal_places=8`` bound.
    """

    if value is None:
        return None
    return value.quantize(_LEDGER_QUANTUM, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class LiveOrderResult:
    submitted: int
    skipped: tuple[str, ...]
    popup_skips: tuple[str, ...]
    satisfied: int = 0
    failed: int = 0
    terminal_failures: int = 0
    uncertain: int = 0
    expired: int = 0
    market_closed: int = 0
    executions: tuple[SignalExecutionOutcome, ...] = ()
    retry_targets: tuple[CloudRetryTarget, ...] = ()
    skip_broker: BrokerId | None = None
    skip_broker_reason: str | None = None
    symbol_skip_reasons: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class LiveOrderBatch:
    profile: BrokerLoginProfile
    plans: tuple[OrderPlan, ...]


@dataclass(frozen=True)
class CloudRetryTarget:
    broker_id: BrokerId
    login_id: str
    account_ids: tuple[str, ...]


@dataclass(frozen=True)
class QueuedCustomOrder:
    queue_id: str
    selection: CustomOrderSelection
    created_at: datetime
    event_id: str | None = None
    expires_at: datetime | None = None
    reason: str = "Custom order"
    cloud_signal_id: str | None = None
    action: OrderAction = OrderAction.BUY
    sell_all: bool = False

    @property
    def account_order_count(self) -> int:
        return sum(len(target.account_ids) for target in self.selection.targets)

    @property
    def resolved_event_id(self) -> str:
        return self.event_id or f"custom-order:{self.queue_id}"


@dataclass(frozen=True)
class QueuedRemoteOrder:
    item_id: str
    symbol: str
    quantity: Decimal | None
    action: OrderAction
    sell_all: bool
    account_order_count: int
    login_count: int
    admin_draft: bool = False


@dataclass(frozen=True)
class PreparationCoverage:
    """Structured target outcomes used to make retry decisions."""

    ready_targets: int = 0
    confirmed_zero_targets: int = 0
    retryable_failed_targets: int = 0
    permanent_failed_targets: int = 0

    @property
    def fully_confirmed_zero(self) -> bool:
        return (
            self.ready_targets == 0
            and self.confirmed_zero_targets > 0
            and self.retryable_failed_targets == 0
            and self.permanent_failed_targets == 0
        )

    @property
    def terminal_without_orders(self) -> bool:
        return (
            self.ready_targets == 0
            and self.retryable_failed_targets == 0
            and (self.confirmed_zero_targets + self.permanent_failed_targets) > 0
        )


@dataclass(frozen=True)
class _SellAllPlanningResult:
    plans: tuple[OrderPlan, ...]
    permanent_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class _SellAllHolding:
    account: BrokerAccount
    quantity: Decimal
    average_cost: Decimal | None = None


@dataclass(frozen=True)
class _SellAllDiscoveryResult:
    holdings: tuple[_SellAllHolding, ...]
    permanent_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class OrderPreviewResult:
    batches: tuple[LiveOrderBatch, ...]
    queue_ids: frozenset[str]
    errors: tuple[str, ...]
    quotes: tuple[tuple[BrokerId, Quote], ...] = ()
    coverage: PreparationCoverage = field(default_factory=PreparationCoverage)
    retry_targets: tuple[CloudRetryTarget, ...] = ()
    cancelled: bool = False


@dataclass(frozen=True)
class CloudSignalClaimContext:
    signal: PendingSignal
    lease_token: str = field(repr=False)
    retry_targets: tuple[CloudRetryTarget, ...] = ()


@dataclass(frozen=True)
class CloudSignalPreparation:
    signal: PendingSignal
    lease_token: str = field(repr=False)
    batches: tuple[LiveOrderBatch, ...]
    errors: tuple[str, ...]
    coverage: PreparationCoverage = field(default_factory=PreparationCoverage)
    retry_targets: tuple[CloudRetryTarget, ...] = ()

    @property
    def plans(self) -> tuple[OrderPlan, ...]:
        return tuple(plan for batch in self.batches for plan in batch.plans)


@dataclass(frozen=True)
class CloudSignalExecutionResult:
    signal_id: str
    lease_token: str = field(repr=False)
    submitted: int
    satisfied: int
    failed: int
    uncertain: int
    expired: int
    retryable_failures: int = 0
    permanent_failures: int = 0
    executions: tuple[SignalExecutionOutcome, ...] = ()
    retry_targets: tuple[CloudRetryTarget, ...] = ()


class _PermanentOrderPreparationError(ExecutionBlockedError):
    """A known-safe preparation failure that cannot improve on an automatic retry."""


class _OrderPreviewCancelledError(RuntimeError):
    pass


@dataclass(frozen=True)
class AdminDraftPreviewResult:
    draft_id: str
    submission: AdminPreviewSubmission


PreviewQueueItem = TaskProgressUpdate | TaskChallengeRequest | OrderPreviewResult
LiveOrderQueueItem = TaskProgressUpdate | TaskChallengeRequest | LiveOrderResult


class SignalListenerState(StrEnum):
    UNAVAILABLE = "unavailable"
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


class ExecutePage(QWidget):
    signal_listener_start_requested = Signal()
    signal_listener_stop_requested = Signal()
    cloud_signal_prepared = Signal(object)
    cloud_signal_finished = Signal(object)
    admin_preview_ready = Signal(object)
    admin_preview_deferred = Signal(object, str)
    admin_preview_skipped = Signal(object)
    cloud_work_finished = Signal()
    market_state_changed = Signal(bool)
    activity_logged = Signal(object)

    def __init__(self, *, market_clock: Callable[[], datetime] | None = None) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        configure_page(self, layout)
        layout.addWidget(PageHeader("Orders"))

        signal_bar, signal_controls = create_action_bar()
        self.signal_listener_status = QLabel()
        self.signal_listener_status.setObjectName("mutedLabel")
        self.signal_listener_status.setTextFormat(Qt.TextFormat.RichText)
        self.signal_start_button = QPushButton("Start")
        self.signal_start_button.setObjectName("blueButton")
        self.signal_start_button.setToolTip(
            "Request active Cloudflare orders that have not been executed for this user."
        )
        self.signal_stop_button = QPushButton("Stop")
        self.signal_stop_button.setObjectName("dangerButton")
        self.signal_stop_button.setToolTip(
            "Stop requesting new cloud orders without interrupting an order already submitting."
        )
        self.signal_start_button.clicked.connect(self.request_signal_listener_start)
        self.signal_stop_button.clicked.connect(self.request_signal_listener_stop)
        signal_controls.addWidget(self.signal_listener_status, 1)
        signal_controls.addWidget(self.signal_start_button)
        signal_controls.addWidget(self.signal_stop_button)
        layout.addWidget(signal_bar)

        action_bar, controls = create_action_bar()
        self.review_button = QPushButton("Review Selected Orders")
        self.review_button.setObjectName("primaryButton")
        self.custom_order_button = QPushButton("Add Order")
        self.clear_button = QPushButton("Clear Queue")
        self.clear_button.setObjectName("dangerButton")
        self.review_button.clicked.connect(self.review_selected_orders)
        self.custom_order_button.clicked.connect(self.add_custom_order)
        self.clear_button.clicked.connect(self.clear_orders)
        controls.addWidget(self.review_button)
        controls.addWidget(self.custom_order_button)
        controls.addStretch(1)
        controls.addWidget(self.clear_button)
        layout.addWidget(action_bar)

        self.status_label = mark_status(QLabel("0 orders queued."))
        layout.addWidget(self.status_label)
        self.task_progress = AnimatedTaskProgressBar()
        layout.addWidget(self.task_progress)

        self.order_list = ToggleSelectionListWidget()
        self.order_list.setObjectName("orderList")
        self.order_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.order_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.order_list.itemSelectionChanged.connect(self._update_review_state)
        self.order_list.itemDoubleClicked.connect(self._edit_list_item)
        self.order_list.customContextMenuRequested.connect(self._open_order_context_menu)
        layout.addWidget(self.order_list, 1)

        self.queued_orders: list[QueuedCustomOrder] = []
        self._remote_orders: dict[str, QueuedRemoteOrder] = {}
        self._preview_queue: Queue[PreviewQueueItem] | None = None
        self._preview_cancel: Event | None = None
        self._preview_timer = QTimer(self)
        self._preview_timer.timeout.connect(self._poll_preview_progress)
        self._order_queue: Queue[LiveOrderQueueItem] | None = None
        self._order_timer = QTimer(self)
        self._order_timer.timeout.connect(self._poll_order_progress)
        self._active_order_ids: set[str] = set()
        self._preview_context: CloudSignalClaimContext | AdminDraft | None = None
        self._cloud_order_context: CloudSignalPreparation | None = None
        self._market_clock = market_clock or market_now
        self._market_open = market_is_open(self._market_clock())
        self._market_timer = QTimer(self)
        self._market_timer.setSingleShot(True)
        self._market_timer.timeout.connect(self._refresh_market_state)
        self._signal_listener_state = SignalListenerState.UNAVAILABLE
        self._signal_listener_error: str | None = None
        self._render_signal_listener_state()
        self.reload_orders()
        self._schedule_market_refresh()

    @property
    def signal_listener_state(self) -> SignalListenerState:
        return self._signal_listener_state

    @property
    def cloud_work_busy(self) -> bool:
        return self._preview_queue is not None or self._order_queue is not None

    @property
    def market_open(self) -> bool:
        return market_is_open(self._market_clock())

    @property
    def next_market_open_text(self) -> str:
        return format_market_open(next_market_open(self._market_clock()))

    def set_signal_listener_available(self, available: bool) -> None:
        if available and self._signal_listener_state is SignalListenerState.UNAVAILABLE:
            self.set_signal_listener_state(SignalListenerState.STOPPED)
        elif not available:
            self.set_signal_listener_state(SignalListenerState.UNAVAILABLE)

    def set_signal_listener_state(
        self,
        state: SignalListenerState,
        *,
        error: str | None = None,
    ) -> None:
        self._signal_listener_state = state
        self._signal_listener_error = error if state is SignalListenerState.ERROR else None
        self._render_signal_listener_state()

    def request_signal_listener_start(self) -> None:
        if self._signal_listener_state not in {
            SignalListenerState.STOPPED,
            SignalListenerState.ERROR,
        }:
            return
        self.set_signal_listener_state(SignalListenerState.STARTING)
        self.signal_listener_start_requested.emit()

    def request_signal_listener_stop(self) -> None:
        if self._signal_listener_state not in {
            SignalListenerState.STARTING,
            SignalListenerState.RUNNING,
        }:
            return
        self.set_signal_listener_state(SignalListenerState.STOPPING)
        self.signal_listener_stop_requested.emit()

    def _render_signal_listener_state(self) -> None:
        messages = {
            SignalListenerState.UNAVAILABLE: (
                'Lotra Software <span style="color:#df4b4b;font-weight:600;">'
                "Sign in required</span>"
            ),
            SignalListenerState.STOPPED: (
                'Lotra Software <span style="color:#df4b4b;font-weight:600;">Stopped</span>'
            ),
            SignalListenerState.STARTING: (
                'Lotra Software <span style="color:#d4a017;font-weight:600;">Starting</span>'
            ),
            SignalListenerState.RUNNING: (
                'Lotra Software <span style="color:#35c96f;font-weight:600;">Running</span>'
            ),
            SignalListenerState.STOPPING: (
                'Lotra Software <span style="color:#d4a017;font-weight:600;">' "Stopping...</span>"
            ),
            SignalListenerState.ERROR: (
                'Lotra Software <span style="color:#df4b4b;font-weight:600;">Stopped</span>'
            ),
        }
        self.signal_listener_status.setText(messages[self._signal_listener_state])
        self.signal_start_button.setEnabled(
            self._signal_listener_state in {SignalListenerState.STOPPED, SignalListenerState.ERROR}
        )
        self.signal_stop_button.setEnabled(
            self._signal_listener_state
            in {SignalListenerState.STARTING, SignalListenerState.RUNNING}
        )

    def reload_orders(self) -> None:
        selected_ids = {order.queue_id for order in self._selected_orders()}
        self.order_list.clear()
        for order in self.queued_orders:
            selection = order.selection
            login_count = len(selection.targets)
            account_count = order.account_order_count
            item = QListWidgetItem(
                f"{selection.symbol}\n"
                f"{order.action.value.upper()} {selection.quantity.normalize()} per account  —  "
                f"{account_count} account order{'s' if account_count != 1 else ''}  —  "
                f"{login_count} broker login{'s' if login_count != 1 else ''}\n"
                "Live price fetched before confirmation"
            )
            item.setData(Qt.ItemDataRole.UserRole, order.queue_id)
            item.setSizeHint(QSize(0, 78))
            item.setToolTip("Double-click to edit. Right-click to edit or remove.")
            self.order_list.addItem(item)
            if order.queue_id in selected_ids:
                item.setSelected(True)
        for remote_order in self._remote_orders.values():
            quantity = (
                "ALL"
                if remote_order.sell_all
                else str((remote_order.quantity or Decimal("1")).normalize())
            )
            queue_status = (
                "Awaiting broker review"
                if remote_order.admin_draft
                else "Received signal queued for execution"
            )
            item = QListWidgetItem(
                f"{remote_order.symbol}\n"
                f"{remote_order.action.value.upper()} {quantity} per account  —  "
                f"{remote_order.account_order_count} account order"
                f"{'s' if remote_order.account_order_count != 1 else ''}  —  "
                f"{remote_order.login_count} broker login"
                f"{'s' if remote_order.login_count != 1 else ''}\n"
                f"{queue_status}"
            )
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsSelectable)
            item.setSizeHint(QSize(0, 78))
            self.order_list.addItem(item)
        count = len(self.queued_orders) + len(self._remote_orders)
        self.status_label.setText(f"{count} order{'s' if count != 1 else ''} queued.")
        self.clear_button.setVisible(bool(self.queued_orders))
        self._update_review_state()

    def queue_cloud_signal(self, signal: PendingSignal) -> None:
        self._queue_remote_order(
            item_id=f"signal:{signal.signal_id}",
            symbol=signal.symbol,
            quantity=signal.quantity,
            action=signal.action,
            sell_all=signal.sell_all,
            broker=signal.broker,
            brokers=signal.brokers,
            excluded_brokers=signal.excluded_brokers,
            admin_draft=False,
        )

    def queue_admin_draft(self, draft: AdminDraft) -> None:
        self._queue_remote_order(
            item_id=f"draft:{draft.draft_id}",
            symbol=draft.symbol,
            quantity=draft.quantity,
            action=draft.action,
            sell_all=draft.sell_all,
            broker=draft.broker,
            brokers=draft.brokers,
            excluded_brokers=draft.excluded_brokers,
            admin_draft=True,
        )

    def remove_cloud_signal(self, signal_id: str) -> None:
        self._remove_remote_order(f"signal:{signal_id}")

    def remove_admin_draft(self, draft_id: str) -> None:
        self._remove_remote_order(f"draft:{draft_id}")

    def clear_remote_orders(self) -> None:
        if self._remote_orders:
            self._remote_orders.clear()
            self.reload_orders()

    def _queue_remote_order(
        self,
        *,
        item_id: str,
        symbol: str,
        quantity: Decimal | None,
        action: OrderAction,
        sell_all: bool,
        broker: BrokerId | None,
        brokers: tuple[BrokerId, ...],
        excluded_brokers: tuple[BrokerId, ...],
        admin_draft: bool,
    ) -> None:
        if item_id in self._remote_orders:
            return
        selection = self._cloud_selection(
            symbol,
            quantity or Decimal("1"),
            broker=broker,
            brokers=brokers,
            excluded_brokers=excluded_brokers,
        )
        targets = () if selection is None else selection.targets
        self._remote_orders[item_id] = QueuedRemoteOrder(
            item_id=item_id,
            symbol=symbol,
            quantity=quantity,
            action=action,
            sell_all=sell_all,
            account_order_count=sum(len(target.account_ids) for target in targets),
            login_count=len(targets),
            admin_draft=admin_draft,
        )
        self.reload_orders()
        if not self.market_open:
            self.status_label.setText(f"{symbol} signal queued until {self.next_market_open_text}.")

    def _remove_remote_order(self, item_id: str) -> None:
        if self._remote_orders.pop(item_id, None) is not None:
            self.reload_orders()

    def _update_review_state(self) -> None:
        idle = self._preview_queue is None and self._order_queue is None
        self.review_button.setEnabled(idle and self.market_open and bool(self._selected_orders()))

    def add_custom_order(self) -> None:
        profiles = self._enabled_login_profiles()
        if not profiles:
            QMessageBox.information(self, "No Logins", "Add and enable a broker login first.")
            return
        dialog = CustomOrderDialog(
            profiles,
            load_settings(),
            usernames=self._login_usernames(profiles),
        )
        if int(dialog.exec()) != int(QDialog.DialogCode.Accepted):
            return
        selection = dialog.selection()
        order = QueuedCustomOrder(
            queue_id=uuid4().hex,
            selection=selection,
            created_at=datetime.now(UTC),
        )
        self.queued_orders.append(order)
        self.reload_orders()
        self._select_order(order.queue_id)
        if self.market_open:
            self.status_label.setText(
                f"Added {selection.symbol}. Select orders, then review to fetch live prices."
            )
        else:
            self.status_label.setText(
                f"Added {selection.symbol}; queued until {self.next_market_open_text}."
            )

    def clear_orders(self) -> None:
        if not self.queued_orders:
            return
        answer = QMessageBox.question(
            self,
            "Clear Order Queue",
            "Remove every queued order? No broker orders will be changed.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        count = len(self.queued_orders)
        self.queued_orders.clear()
        self.reload_orders()
        self.status_label.setText(f"Removed {count} queued order{'s' if count != 1 else ''}.")

    def _selected_orders(self) -> list[QueuedCustomOrder]:
        selected_ids = {
            str(item.data(Qt.ItemDataRole.UserRole)) for item in self.order_list.selectedItems()
        }
        return [order for order in self.queued_orders if order.queue_id in selected_ids]

    def _select_order(self, queue_id: str) -> None:
        for index in range(self.order_list.count()):
            item = self.order_list.item(index)
            if str(item.data(Qt.ItemDataRole.UserRole)) == queue_id:
                item.setSelected(True)
                self.order_list.scrollToItem(item)
                break

    def _order_for_item(self, item: QListWidgetItem | None) -> QueuedCustomOrder | None:
        if item is None:
            return None
        queue_id = str(item.data(Qt.ItemDataRole.UserRole))
        return next((order for order in self.queued_orders if order.queue_id == queue_id), None)

    def _open_order_context_menu(self, position: QPoint) -> None:
        item = self.order_list.itemAt(position)
        order = self._order_for_item(item)
        if order is None:
            return
        menu = QMenu(self.order_list)
        edit_action = menu.addAction("Edit Order")
        delete_action = menu.addAction("Remove Order")
        selected = menu.exec(self.order_list.viewport().mapToGlobal(position))
        if selected is edit_action:
            self._edit_order(order)
        elif selected is delete_action:
            self._delete_order(order)

    def _edit_list_item(self, item: QListWidgetItem, _column: int = 0) -> None:
        order = self._order_for_item(item)
        if order is not None:
            self._edit_order(order)

    def _edit_order(self, order: QueuedCustomOrder) -> None:
        profiles = self._enabled_login_profiles()
        if not profiles:
            QMessageBox.information(self, "No Logins", "Add and enable a broker login first.")
            return
        dialog = CustomOrderDialog(
            profiles,
            load_settings(),
            initial=order.selection,
            action_label="Save Order",
            usernames=self._login_usernames(profiles),
        )
        if int(dialog.exec()) != int(QDialog.DialogCode.Accepted):
            return
        replacement = QueuedCustomOrder(
            queue_id=order.queue_id,
            selection=dialog.selection(),
            created_at=order.created_at,
        )
        self.queued_orders[self.queued_orders.index(order)] = replacement
        self.reload_orders()
        self._select_order(replacement.queue_id)
        self.status_label.setText(f"Updated {replacement.selection.symbol}.")

    def _delete_order(self, order: QueuedCustomOrder) -> None:
        answer = QMessageBox.question(
            self,
            "Remove Order",
            f"Remove {order.selection.symbol} from the order queue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        self.queued_orders.remove(order)
        self.reload_orders()
        self.status_label.setText(f"Removed {order.selection.symbol}.")

    def review_selected_orders(self) -> None:
        orders = self._selected_orders()
        if not orders:
            return
        if self._preview_queue is not None or self._order_queue is not None:
            return
        if not self.market_open:
            self.status_label.setText(
                f"Selected orders are queued until {self.next_market_open_text}."
            )
            return
        self._begin_order_preview(tuple(orders), context=None)

    def prepare_cloud_signal(self, context: CloudSignalClaimContext) -> bool:
        if self.cloud_work_busy:
            return False
        signal = context.signal
        if not signal.is_active_at(datetime.now(UTC)):
            self.cloud_signal_prepared.emit(
                CloudSignalPreparation(
                    signal=signal,
                    lease_token=context.lease_token,
                    batches=(),
                    errors=("The signal deadline has expired.",),
                )
            )
            self.cloud_work_finished.emit()
            return True
        selection = self._cloud_selection(
            signal.symbol,
            signal.quantity or Decimal("1"),
            broker=signal.broker,
            brokers=signal.brokers,
            excluded_brokers=signal.excluded_brokers,
        )
        if selection is None:
            self.cloud_signal_prepared.emit(
                CloudSignalPreparation(
                    signal=signal,
                    lease_token=context.lease_token,
                    batches=(),
                    errors=("No enabled broker logins are configured.",),
                    coverage=PreparationCoverage(retryable_failed_targets=1),
                )
            )
            self.cloud_work_finished.emit()
            return True
        if context.retry_targets:
            selection = _restrict_cloud_selection(selection, context.retry_targets)
            if not selection.targets:
                self.cloud_signal_prepared.emit(
                    CloudSignalPreparation(
                        signal=signal,
                        lease_token=context.lease_token,
                        batches=(),
                        errors=("The failed broker targets are no longer enabled.",),
                        coverage=PreparationCoverage(permanent_failed_targets=1),
                    )
                )
                self.cloud_work_finished.emit()
                return True
        order = QueuedCustomOrder(
            queue_id=signal.signal_id,
            selection=selection,
            created_at=signal.created_at,
            event_id=f"cloud-signal:{signal.signal_id}",
            expires_at=signal.expires_at,
            reason="Lotra Software signal",
            cloud_signal_id=signal.signal_id,
            action=signal.action,
            sell_all=signal.sell_all,
        )
        self._begin_order_preview((order,), context=context)
        return True

    def prepare_admin_draft(self, draft: AdminDraft) -> bool:
        if self.cloud_work_busy:
            return False
        if not draft.is_active_at(datetime.now(UTC)):
            self.status_label.setText("The pending Discord signal has expired.")
            self.cloud_work_finished.emit()
            return True
        selection = self._cloud_selection(
            draft.symbol,
            draft.quantity or Decimal("1"),
            broker=draft.broker,
            brokers=draft.brokers,
            excluded_brokers=draft.excluded_brokers,
        )
        if selection is None:
            deferred_message = "The Discord signal needs at least one enabled broker login."
            self.status_label.setText(deferred_message)
            self.admin_preview_deferred.emit(draft, deferred_message)
            self.cloud_work_finished.emit()
            return True
        order = QueuedCustomOrder(
            queue_id=draft.draft_id,
            selection=selection,
            created_at=draft.requested_at,
            event_id=f"cloud-draft:{draft.draft_id}",
            expires_at=draft.expires_at,
            reason="Discord admin review",
            cloud_signal_id=draft.draft_id,
            action=draft.action,
            sell_all=draft.sell_all,
        )
        self._begin_order_preview((order,), context=draft)
        return True

    def submit_cloud_signal(
        self,
        preparation: CloudSignalPreparation,
        *,
        authorization_expires_at: datetime,
    ) -> bool:
        if self.cloud_work_busy or not preparation.batches or not self.market_open:
            return False
        now = datetime.now(UTC)
        expires_at = min(preparation.signal.expires_at, authorization_expires_at)
        if expires_at <= now:
            return False
        authorization = create_batch_authorization(
            list(preparation.plans),
            now=now,
        )
        self._cloud_order_context = preparation
        self.status_label.setText("Authorization verified; submitting cloud signal...")
        self._submit_order_batches(
            preparation.batches,
            queued_order_ids=set(),
            authorization=authorization,
        )
        return True

    def _begin_order_preview(
        self,
        orders: tuple[QueuedCustomOrder, ...],
        *,
        context: CloudSignalClaimContext | AdminDraft | None,
    ) -> None:
        target_count = sum(len(order.selection.targets) for order in orders)
        self._preview_context = context
        self._set_busy(True)
        self.status_label.setText("Checking brokers for symbol availability and live prices...")
        self.task_progress.start_task("Fetching live prices...", total=target_count)
        self._preview_queue = Queue()
        self._preview_cancel = Event()
        Thread(
            target=_collect_order_previews,
            args=(self._preview_queue, orders, self._preview_cancel),
            daemon=True,
            name="lotra-order-preview",
        ).start()
        self._preview_timer.start(75)

    def cancel_cloud_preview(self) -> bool:
        if self._preview_cancel is None or not isinstance(
            self._preview_context, CloudSignalClaimContext | AdminDraft
        ):
            return False
        self._preview_cancel.set()
        self.task_progress.finish_task()
        self.status_label.setText("Signal receiving stopped; canceling broker check...")
        return True

    def cancel_ignored_sell_preview(self) -> bool:
        context = self._preview_context
        if (
            not isinstance(context, CloudSignalClaimContext)
            or context.signal.action is not OrderAction.SELL
        ):
            return False
        return self.cancel_cloud_preview()

    def _cloud_selection(
        self,
        symbol: str,
        quantity: Decimal,
        *,
        broker: BrokerId | None = None,
        brokers: tuple[BrokerId, ...] = (),
        excluded_brokers: tuple[BrokerId, ...] = (),
    ) -> CustomOrderSelection | None:
        return _cloud_selection_for_profiles(
            self._enabled_login_profiles(),
            symbol=symbol,
            quantity=quantity,
            broker=broker,
            brokers=brokers,
            excluded_brokers=excluded_brokers,
        )

    def _poll_preview_progress(self) -> None:
        queue = self._preview_queue
        if queue is None:
            return
        try:
            while True:
                item = queue.get_nowait()
                if isinstance(item, TaskProgressUpdate):
                    if self._preview_cancel is not None and self._preview_cancel.is_set():
                        continue
                    self.status_label.setText(item.message)
                    self.task_progress.update_task(
                        item.message,
                        current=item.current,
                        total=item.total,
                        eta_seconds=item.eta_seconds,
                        reset_eta_sample=item.reset_eta_sample,
                    )
                    continue
                if isinstance(item, TaskChallengeRequest):
                    if self._preview_cancel is not None and self._preview_cancel.is_set():
                        item.response = None
                        item.completed.set()
                        continue
                    self.task_progress.pause_eta()
                    try:
                        item.response = request_challenge(
                            self,
                            broker_name=item.login_label,
                            login_label=item.login_label,
                            prompt=item.prompt,
                        )
                    finally:
                        self.task_progress.resume_eta()
                        item.completed.set()
                    continue
                self._preview_timer.stop()
                self._preview_queue = None
                self._preview_cancel = None
                context = self._preview_context
                self._preview_context = None
                self.task_progress.finish_task()
                self._set_busy(False)
                if item.cancelled:
                    if isinstance(context, CloudSignalClaimContext):
                        self.cloud_signal_prepared.emit(
                            CloudSignalPreparation(
                                signal=context.signal,
                                lease_token=context.lease_token,
                                batches=(),
                                errors=("Broker check canceled.",),
                                coverage=PreparationCoverage(retryable_failed_targets=1),
                            )
                        )
                    elif isinstance(context, AdminDraft):
                        self.admin_preview_deferred.emit(context, "Broker check canceled.")
                    self.status_label.setText("Signal receiving stopped; broker check canceled.")
                    self.cloud_work_finished.emit()
                    return
                if isinstance(context, CloudSignalClaimContext):
                    preparation = CloudSignalPreparation(
                        signal=context.signal,
                        lease_token=context.lease_token,
                        batches=item.batches,
                        errors=item.errors,
                        coverage=item.coverage,
                        retry_targets=item.retry_targets,
                    )
                    if item.errors:
                        # A broker that fails the availability check (can't get
                        # a quote, can't resolve an account, ...) never reaches
                        # order submission, so it wouldn't otherwise appear in
                        # the submit-phase Activity Log entry. Record it here so
                        # a broker that's silently skipped at the check stage is
                        # still visible -- without interrupting processing.
                        self.activity_logged.emit(
                            ActivityLogEntry(
                                title=(
                                    f"{context.signal.symbol}: "
                                    f"{len(item.errors)} broker(s) skipped at symbol check"
                                ),
                                details=tuple(item.errors),
                                level="warning",
                            )
                        )
                    self.cloud_signal_prepared.emit(preparation)
                    if not item.batches:
                        self.status_label.setText(_cloud_preparation_status(item))
                        self.cloud_work_finished.emit()
                    else:
                        self.status_label.setText(
                            "Cloud signal prices are ready; verifying authorization..."
                        )
                    return
                if isinstance(context, AdminDraft):
                    if item.coverage.fully_confirmed_zero:
                        self.admin_preview_skipped.emit(context)
                        self.status_label.setText(
                            "Skipped this Discord signal because no enabled account holds it."
                        )
                        self.cloud_work_finished.emit()
                        return
                    try:
                        preview = _admin_preview_from_result(context, item)
                    except Exception:
                        LOGGER.exception(
                            "Unable to finalize Discord admin preview: draft=%s",
                            context.draft_id,
                        )
                        deferred_message = "Discord signal preview could not be finalized."
                        self.status_label.setText(deferred_message)
                        self.admin_preview_deferred.emit(context, deferred_message)
                        self.cloud_work_finished.emit()
                        return
                    if preview is not None:
                        self.admin_preview_ready.emit(preview)
                        self.status_label.setText(
                            "Discord signal price is ready; submitting automatically."
                        )
                    else:
                        deferred_message = "No broker price was available for the Discord signal."
                        self.status_label.setText(deferred_message)
                        self.admin_preview_deferred.emit(context, deferred_message)
                    self.cloud_work_finished.emit()
                    return
                if item.errors:
                    self.activity_logged.emit(
                        ActivityLogEntry(
                            title=f"{len(item.errors)} broker(s) skipped at symbol check",
                            details=tuple(item.errors),
                            level="warning",
                        )
                    )
                if not item.batches:
                    self.status_label.setText("No selected orders were ready for review.")
                    return
                if not self.market_open:
                    self.status_label.setText(
                        f"Orders remain queued until {self.next_market_open_text}."
                    )
                    return
                plans = [plan for batch in item.batches for plan in batch.plans]
                review = ExecutionDialog(plans=plans)
                if int(review.exec()) != int(QDialog.DialogCode.Accepted):
                    self.status_label.setText("Order review cancelled; queued orders were kept.")
                    return
                authorization = create_batch_authorization(plans)
                self._submit_order_batches(
                    item.batches,
                    queued_order_ids=set(item.queue_ids),
                    authorization=authorization,
                )
                return
        except Empty:
            return

    def _submit_order_batches(
        self,
        batches: tuple[LiveOrderBatch, ...],
        *,
        queued_order_ids: set[str],
        authorization: BatchAuthorization,
    ) -> None:
        total_orders = sum(len(batch.plans) for batch in batches)
        self._set_busy(True)
        self._active_order_ids = set(queued_order_ids)
        self.task_progress.start_task("Submitting reviewed orders...", total=total_orders)
        self._order_queue = Queue()
        Thread(
            target=_collect_live_order_batches,
            args=(self._order_queue, batches, authorization),
            daemon=True,
            name="lotra-order-submit",
        ).start()
        self._order_timer.start(75)

    def _poll_order_progress(self) -> None:
        queue = self._order_queue
        if queue is None:
            return
        try:
            while True:
                item = queue.get_nowait()
                if isinstance(item, TaskProgressUpdate):
                    self.status_label.setText(item.message)
                    self.task_progress.update_task(
                        item.message,
                        current=item.current,
                        total=item.total,
                        eta_seconds=item.eta_seconds,
                        reset_eta_sample=item.reset_eta_sample,
                    )
                    continue
                if isinstance(item, TaskChallengeRequest):
                    self.task_progress.pause_eta()
                    try:
                        item.response = request_challenge(
                            self,
                            broker_name=item.login_label,
                            login_label=item.login_label,
                            prompt=item.prompt,
                        )
                    finally:
                        self.task_progress.resume_eta()
                        item.completed.set()
                    continue
                self._order_timer.stop()
                self._order_queue = None
                self.task_progress.finish_task()
                self._set_busy(False)
                cloud_context = self._cloud_order_context
                self._cloud_order_context = None
                if self._active_order_ids and item.market_closed == 0:
                    self.queued_orders = [
                        order
                        for order in self.queued_orders
                        if order.queue_id not in self._active_order_ids
                    ]
                    self._active_order_ids.clear()
                self.reload_orders()
                self.status_label.setText(
                    f"{item.submitted} submitted; {len(item.skipped)} not submitted."
                )
                if item.market_closed:
                    self.status_label.setText(
                        f"Market closed; remaining orders are queued until "
                        f"{self.next_market_open_text}."
                    )
                if cloud_context is not None:
                    # Record the outcome to the Activity Log instead of
                    # interrupting with a popup, so processing proceeds to the
                    # next signal automatically. `item.skipped` (not
                    # popup_skips) is used deliberately: it includes the benign
                    # guardrail skips too -- "already holds a position",
                    # duplicate-order blocks -- so the log fully explains why
                    # each account did or didn't submit.
                    self._log_cloud_signal_outcome(cloud_context.signal.symbol, item)
                    execution_result = CloudSignalExecutionResult(
                        signal_id=cloud_context.signal.signal_id,
                        lease_token=cloud_context.lease_token,
                        submitted=item.submitted,
                        satisfied=item.satisfied,
                        failed=item.failed,
                        uncertain=item.uncertain,
                        expired=item.expired,
                        retryable_failures=(cloud_context.coverage.retryable_failed_targets),
                        permanent_failures=(
                            cloud_context.coverage.permanent_failed_targets + item.terminal_failures
                        ),
                        executions=item.executions,
                        retry_targets=_merge_cloud_retry_targets(
                            cloud_context.retry_targets + item.retry_targets
                        ),
                    )
                    # cloud_work_finished MUST fire even if a cloud_signal_finished
                    # slot raises -- otherwise _schedule_cloud_work never runs and
                    # every remaining signal freezes behind this one.
                    try:
                        self.cloud_signal_finished.emit(execution_result)
                    except Exception:
                        LOGGER.exception(
                            "cloud_signal_finished handler failed for signal %s",
                            cloud_context.signal.signal_id,
                        )
                    self.cloud_work_finished.emit()
                    return
                if item.submitted or item.skipped:
                    self._log_cloud_signal_outcome(None, item)
                return
        except Empty:
            return

    def _log_cloud_signal_outcome(self, symbol: str | None, item: LiveOrderResult) -> None:
        """Record an order run's outcome to the Activity Log (no popup)."""
        label = f"{symbol}: " if symbol else "Manual order: "
        title = f"{label}{item.submitted} submitted, {len(item.skipped)} not submitted"
        level = "info" if not item.skipped else "warning"
        self.activity_logged.emit(
            ActivityLogEntry(title=title, details=tuple(item.skipped), level=level)
        )

    def _set_busy(self, busy: bool) -> None:
        self.custom_order_button.setEnabled(not busy)
        self.clear_button.setEnabled(not busy and bool(self.queued_orders))
        self.order_list.setEnabled(not busy)
        self.review_button.setEnabled(
            not busy and self.market_open and bool(self._selected_orders())
        )

    def _refresh_market_state(self) -> None:
        was_open = self._market_open
        self._market_open = self.market_open
        self._update_review_state()
        if self._market_open and not was_open and self.queued_orders:
            self.status_label.setText("Market open; queued orders are ready for review.")
        if self._market_open != was_open:
            self.market_state_changed.emit(self._market_open)
        self._schedule_market_refresh()

    def _schedule_market_refresh(self) -> None:
        now = self._market_clock()
        delay = max(
            1_000,
            min(
                int((next_market_transition(now) - now).total_seconds() * 1_000) + 1_000,
                2_147_483_647,
            ),
        )
        self._market_timer.start(delay)

    def _enabled_login_profiles(self) -> list[BrokerLoginProfile]:
        with connect() as connection:
            migrate(connection)
            settings = load_settings()
            profiles = [
                profile
                for profile in BrokerLoginRepository(connection).list_profiles()
                if profile.enabled and settings.broker_enabled(profile.broker_id)
            ]
        return display_login_profiles(profiles)

    @staticmethod
    def _login_usernames(profiles: list[BrokerLoginProfile]) -> dict[str, str]:
        return load_login_usernames(profiles)

    def challenge_code(self, broker_id: object, login_label: str, prompt: str) -> str | None:
        del broker_id
        return request_challenge(
            self,
            broker_name=login_label,
            login_label=login_label,
            prompt=prompt,
        )


# Chase and SoFi get priority over the other brokers: their targets are
# checked first (in _collect_order_previews) and their batches submit first
# (in _group_live_order_batches), Chase before SoFi, ahead of everyone else.
# This only changes *which broker goes first* within the existing "check the
# whole signal, authorize it as one unit, then submit" pipeline -- it does
# not change that pipeline's structure, since splitting Chase/SoFi out to
# submit before the rest are even checked would mean giving them a separate,
# smaller batch authorization than the rest of the signal, and that wasn't
# changed without being able to verify it against the authorization and
# execution-guard invariants the shared BatchAuthorization currently relies
# on covering the whole signal at once.
_EXECUTION_BROKER_ORDER: tuple[BrokerId, ...] = (
    BrokerId.CHASE,
    BrokerId.SOFI,
    *(broker_id for broker_id in BrokerId if broker_id not in {BrokerId.CHASE, BrokerId.SOFI}),
)


def _collect_order_previews(
    queue: Queue[PreviewQueueItem],
    orders: tuple[QueuedCustomOrder, ...],
    cancel_event: Event | None = None,
) -> None:
    batches: list[LiveOrderBatch] = []
    quotes: list[tuple[BrokerId, Quote]] = []
    prepared_ids: set[str] = set()
    errors: list[str] = []
    ready_targets = 0
    confirmed_zero_targets = 0
    retryable_failed_targets = 0
    permanent_failed_targets = 0
    retry_targets: list[CloudRetryTarget] = []
    sell_all_accounts: dict[
        tuple[str, BrokerId, str],
        BrokerLoginProfile,
    ] = {}
    broker_orders: dict[
        BrokerId,
        list[tuple[QueuedCustomOrder, tuple[CustomOrderTarget, ...]]],
    ] = {}
    for order in orders:
        targets_by_broker: dict[BrokerId, list[CustomOrderTarget]] = {}
        for target in order.selection.targets:
            targets_by_broker.setdefault(target.profile.broker_id, []).append(target)
        for broker_id, broker_targets in targets_by_broker.items():
            broker_orders.setdefault(broker_id, []).append((order, tuple(broker_targets)))

    total_targets = sum(
        len(targets) for items in broker_orders.values() for _order, targets in items
    )
    completed = 0
    cancelled = False
    settings = load_settings()
    for broker_id in _EXECUTION_BROKER_ORDER:
        if cancel_event is not None and cancel_event.is_set():
            cancelled = True
            break
        work = broker_orders.get(broker_id)
        if not work:
            continue
        for order, order_targets in work:
            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                break
            for target in order_targets:
                adapter: BrokerAdapter | None = None
                candidate_plans: tuple[OrderPlan, ...] = ()
                permanent_plan_errors: tuple[str, ...] = ()
                plans: tuple[OrderPlan, ...] = ()
                discovery: _SellAllDiscoveryResult | None = None
                quote: Quote | None = None
                try:
                    if cancel_event is not None and cancel_event.is_set():
                        raise _OrderPreviewCancelledError
                    if order.expires_at is not None and datetime.now(UTC) >= order.expires_at:
                        raise _PermanentOrderPreparationError(
                            "order authorization deadline has expired"
                        )

                    def _connect(
                        inner_queue: Queue[Any],
                        _profile: BrokerLoginProfile = target.profile,
                        _completed: int = completed,
                    ) -> BrokerAdapter:
                        return _connect_order_adapter(
                            inner_queue,
                            _profile,
                            current=_completed,
                            total=total_targets,
                        )

                    adapter = _connect_with_deadline(
                        queue,
                        _connect,
                        label=target.profile.label,
                    )
                    queue.put(
                        TaskProgressUpdate(
                            message=(
                                f"Checking {order.selection.symbol} on "
                                f"{target.profile.label}..."
                            ),
                            current=completed,
                            total=total_targets,
                        )
                    )
                    if cancel_event is not None and cancel_event.is_set():
                        raise _OrderPreviewCancelledError
                    if order.sell_all:
                        discovery = _discover_sell_all_holdings(
                            adapter=adapter,
                            order=order,
                            target=target,
                        )
                    if discovery is None or discovery.holdings or order.cloud_signal_id is None:
                        try:
                            quote = adapter.get_quote(order.selection.symbol)
                        except Exception as exc:
                            if cancel_event is not None and cancel_event.is_set():
                                raise _OrderPreviewCancelledError from exc
                            if not session_recovery_required(broker_id, exc):
                                raise
                            with suppress(Exception):
                                adapter.disconnect()
                            adapter = None
                            queue.put(
                                TaskProgressUpdate(
                                    message=(
                                        f"{target.profile.label} session expired; signing in "
                                        "automatically..."
                                    ),
                                    current=completed,
                                    total=total_targets,
                                )
                            )

                            def _reconnect(
                                inner_queue: Queue[Any],
                                _profile: BrokerLoginProfile = target.profile,
                            ) -> BrokerAdapter:
                                return _create_connected_order_adapter(
                                    inner_queue,
                                    _profile,
                                    force_interactive_login=True,
                                )

                            adapter = _connect_with_deadline(
                                queue,
                                _reconnect,
                                label=target.profile.label,
                            )
                            if order.sell_all:
                                discovery = _discover_sell_all_holdings(
                                    adapter=adapter,
                                    order=order,
                                    target=target,
                                )
                            if (
                                discovery is None
                                or discovery.holdings
                                or order.cloud_signal_id is None
                            ):
                                quote = adapter.get_quote(order.selection.symbol)
                    if cancel_event is not None and cancel_event.is_set():
                        raise _OrderPreviewCancelledError
                    if quote is not None:
                        quotes.append((broker_id, quote))
                    cost = (
                        quote.price * order.selection.quantity
                        if quote is not None
                        else Decimal("0")
                    )
                    if (
                        order.action is OrderAction.BUY
                        and quote is not None
                        and cost > settings.maximum_cost_per_order
                    ):
                        raise ExecutionBlockedError(
                            f"live cost {_money(cost)} exceeds the per-order limit "
                            f"of {_money(settings.maximum_cost_per_order)}"
                        )
                    event_id = order.resolved_event_id
                    if order.sell_all:
                        if discovery is None:
                            raise RuntimeError("sell-all position discovery was not initialized")
                        planning = (
                            _build_sell_all_order_plans(
                                adapter=adapter,
                                order=order,
                                target=target,
                                quote=quote,
                                discovery=discovery,
                            )
                            if quote is not None
                            else _SellAllPlanningResult(
                                plans=(),
                                permanent_errors=discovery.permanent_errors,
                            )
                        )
                        candidate_plans = planning.plans
                        permanent_plan_errors = planning.permanent_errors
                        if permanent_plan_errors:
                            permanent_failed_targets += 1
                            scope = "some accounts skipped" if candidate_plans else "broker skipped"
                            errors.append(
                                f"{order.selection.symbol} — {target.profile.label}: {scope}; "
                                + "; ".join(permanent_plan_errors)
                            )
                        plans = _reserve_sell_all_accounts(
                            candidate_plans,
                            profile=target.profile,
                            seen=sell_all_accounts,
                        )
                    else:
                        if quote is None:
                            raise RuntimeError("order quote was not loaded")
                        if target.profile.broker_id is BrokerId.WELLSTRADE:
                            account_ids, permanent_plan_errors = _resolve_enabled_account_ids(
                                adapter=adapter,
                                target=target,
                                action=order.action,
                            )
                        else:
                            account_ids = target.account_ids
                        plans = tuple(
                            _build_custom_order_plan(
                                event_id=event_id,
                                profile=target.profile,
                                account_id=account_id,
                                quote=quote,
                                quantity=order.selection.quantity,
                                action=order.action,
                                expires_at=order.expires_at,
                                reason=order.reason,
                            )
                            for account_id in account_ids
                        )
                        if permanent_plan_errors:
                            permanent_failed_targets += 1
                            scope = "some accounts skipped" if plans else "broker skipped"
                            errors.append(
                                f"{order.selection.symbol} - {target.profile.label}: {scope}; "
                                + "; ".join(permanent_plan_errors)
                            )
                except _OrderPreviewCancelledError:
                    cancelled = True
                except _PermanentOrderPreparationError as exc:
                    if cancel_event is not None and cancel_event.is_set():
                        cancelled = True
                    else:
                        permanent_failed_targets += 1
                        errors.append(
                            f"{order.selection.symbol} — {target.profile.label}: "
                            f"broker skipped; {exc}"
                        )
                except Exception as exc:
                    if cancel_event is not None and cancel_event.is_set():
                        cancelled = True
                    else:
                        if _is_current_signal_terminal_failure(broker_id, str(exc)):
                            permanent_failed_targets += 1
                        else:
                            retryable_failed_targets += 1
                            retry_targets.append(
                                CloudRetryTarget(
                                    broker_id=broker_id,
                                    login_id=target.profile.login_id,
                                    account_ids=target.account_ids,
                                )
                            )
                        errors.append(
                            f"{order.selection.symbol} — {target.profile.label}: "
                            f"broker skipped; {exc}"
                        )
                else:
                    if order.sell_all:
                        if candidate_plans:
                            ready_targets += 1
                        elif not permanent_plan_errors:
                            confirmed_zero_targets += 1
                    if plans:
                        batches.append(LiveOrderBatch(profile=target.profile, plans=plans))
                    prepared_ids.add(order.queue_id)
                finally:
                    if adapter is not None:
                        with suppress(Exception):
                            adapter.disconnect()
                    completed += 1
                    queue.put(
                        TaskProgressUpdate(
                            message=f"Checked {completed} of {total_targets} broker targets",
                            current=completed,
                            total=total_targets,
                        )
                    )
                if cancelled:
                    break
            if cancelled:
                break
        if cancelled:
            break
    if cancelled:
        batches.clear()
        quotes.clear()
        prepared_ids.clear()
        errors.clear()
        retry_targets.clear()
    queue.put(
        OrderPreviewResult(
            batches=_group_live_order_batches(tuple(batches)),
            queue_ids=frozenset(prepared_ids),
            errors=tuple(errors),
            quotes=tuple(quotes),
            coverage=PreparationCoverage(
                ready_targets=ready_targets,
                confirmed_zero_targets=confirmed_zero_targets,
                retryable_failed_targets=retryable_failed_targets,
                permanent_failed_targets=permanent_failed_targets,
            ),
            retry_targets=_merge_cloud_retry_targets(tuple(retry_targets)),
            cancelled=cancelled,
        )
    )


def _build_sell_all_order_plans(
    *,
    adapter: BrokerAdapter,
    order: QueuedCustomOrder,
    target: CustomOrderTarget,
    quote: Quote | None,
    discovery: _SellAllDiscoveryResult | None = None,
) -> _SellAllPlanningResult:
    if order.action is not OrderAction.SELL or not order.sell_all:
        raise ExecutionBlockedError("sell-all planning requires a sell-all order")
    if quote is None:
        raise ExecutionBlockedError("sell-all planning requires a live quote")
    resolved = discovery or _discover_sell_all_holdings(
        adapter=adapter,
        order=order,
        target=target,
    )
    plans = tuple(
        _build_custom_order_plan(
            event_id=order.resolved_event_id,
            profile=target.profile,
            account_id=holding.account.account_id,
            quote=quote,
            quantity=holding.quantity,
            action=OrderAction.SELL,
            cost_basis=(
                holding.average_cost * holding.quantity
                if holding.average_cost is not None and holding.average_cost > 0
                else None
            ),
            expires_at=order.expires_at,
            reason=order.reason,
        )
        for holding in resolved.holdings
    )
    return _SellAllPlanningResult(
        plans=plans,
        permanent_errors=resolved.permanent_errors,
    )


def _discover_sell_all_holdings(
    *,
    adapter: BrokerAdapter,
    order: QueuedCustomOrder,
    target: CustomOrderTarget,
) -> _SellAllDiscoveryResult:
    if order.action is not OrderAction.SELL or not order.sell_all:
        raise ExecutionBlockedError("sell-all discovery requires a sell-all order")
    accounts = _discover_broker_accounts(adapter)
    if not accounts:
        raise ExecutionBlockedError("broker returned no enabled accounts after login")
    holdings: list[_SellAllHolding] = []
    permanent_errors: list[str] = []
    resolved_account_ids: set[str] = set()
    for selector in target.account_ids:
        account = _matching_broker_account(
            accounts,
            selector=selector,
            profile=target.profile,
        )
        if account is None:
            permanent_errors.append(
                f"enabled account ending in {_account_selector_suffix(selector)} "
                "is no longer available"
            )
            continue
        if account.account_id in resolved_account_ids:
            continue
        resolved_account_ids.add(account.account_id)
        matching_positions = [
            position
            for position in adapter.get_positions(account.account_id)
            if position.symbol.upper() == order.selection.symbol.upper()
        ]
        quantity = sum((position.quantity for position in matching_positions), start=Decimal("0"))
        costed_quantity = sum(
            (
                position.quantity
                for position in matching_positions
                if position.average_cost is not None and position.average_cost > 0
            ),
            start=Decimal("0"),
        )
        known_cost = sum(
            (
                position.quantity * position.average_cost
                for position in matching_positions
                if position.average_cost is not None and position.average_cost > 0
            ),
            start=Decimal("0"),
        )
        average_cost = (
            known_cost / quantity if costed_quantity == quantity and quantity > 0 else None
        )
        if not quantity.is_finite() or quantity < 0:
            raise ExecutionBlockedError(
                f"broker returned an invalid {order.selection.symbol} position quantity "
                f"for account {account.masked_account_id}"
            )
        if quantity == 0:
            continue
        if not account.tradable and not account.closing_only:
            permanent_errors.append(
                f"account {account.masked_account_id} holds {quantity} "
                f"{order.selection.symbol} shares but is not eligible for liquidation"
            )
            continue
        if (
            quantity != quantity.to_integral_value()
            and not adapter.metadata.fractional_sell_supported
        ):
            permanent_errors.append(
                f"{adapter.metadata.display_name} does not support fractional-share "
                f"liquidation in account {account.masked_account_id}"
            )
            continue
        holdings.append(
            _SellAllHolding(account=account, quantity=quantity, average_cost=average_cost)
        )
    return _SellAllDiscoveryResult(
        holdings=tuple(holdings),
        permanent_errors=tuple(permanent_errors),
    )


def _account_selector_suffix(selector: str) -> str:
    compact = "".join(character for character in selector if character.isalnum())
    return compact[-4:] or "unknown"


def _resolve_enabled_account_ids(
    *,
    adapter: BrokerAdapter,
    target: CustomOrderTarget,
    action: OrderAction,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    accounts = _discover_broker_accounts(adapter)
    if not accounts:
        raise ExecutionBlockedError("broker returned no enabled accounts after login")
    resolved: list[str] = []
    errors: list[str] = []
    for selector in target.account_ids:
        account = _matching_broker_account(
            accounts,
            selector=selector,
            profile=target.profile,
        )
        suffix = _account_selector_suffix(selector)
        if account is None:
            errors.append(f"enabled account ending in {suffix} is no longer available")
            continue
        eligible = account.tradable or (action is OrderAction.SELL and account.closing_only)
        if not eligible:
            errors.append(f"account ending in {suffix} is not eligible for this order")
            continue
        if account.account_id not in resolved:
            resolved.append(account.account_id)
    return tuple(resolved), tuple(errors)


def _reserve_sell_all_accounts(
    plans: tuple[OrderPlan, ...],
    *,
    profile: BrokerLoginProfile,
    seen: dict[tuple[str, BrokerId, str], BrokerLoginProfile],
) -> tuple[OrderPlan, ...]:
    reserved: list[OrderPlan] = []
    for plan in plans:
        key = (plan.event_id, plan.broker_id, plan.account_id)
        if key in seen:
            continue
        seen[key] = profile
        reserved.append(plan)
    return tuple(reserved)


def _cloud_selection_for_profiles(
    profiles: list[BrokerLoginProfile],
    *,
    symbol: str,
    quantity: Decimal,
    broker: BrokerId | None,
    brokers: tuple[BrokerId, ...] = (),
    excluded_brokers: tuple[BrokerId, ...] = (),
) -> CustomOrderSelection | None:
    included = frozenset(brokers or (() if broker is None else (broker,)))
    excluded = frozenset(excluded_brokers)
    targets = tuple(
        CustomOrderTarget(profile=profile, account_ids=profile.account_selectors)
        for profile in profiles
        if profile.account_selectors
        and (not included or profile.broker_id in included)
        and profile.broker_id not in excluded
    )
    if not targets:
        return None
    return CustomOrderSelection(targets=targets, symbol=symbol, quantity=quantity)


def _challenge_handler_for_queue(
    queue: Queue[PreviewQueueItem] | Queue[LiveOrderQueueItem],
) -> Any:
    return lambda broker_id, login_label, prompt: request_task_challenge(
        cast(Queue[Any], queue),
        broker_id=broker_id,
        login_label=login_label,
        prompt=prompt,
    )


# Same ceiling as _LIVE_ORDER_BATCH_TIMEOUT_SECONDS below, kept as its own
# literal so this stays defined before that constant appears later in the
# file (Python only needs a name to exist by call time, not by def time, but
# a plain `X = Y` at module scope runs top-to-bottom like any other statement).
_PREVIEW_CONNECT_TIMEOUT_SECONDS = 90.0


def _connect_with_deadline(
    queue: Queue[PreviewQueueItem] | Queue[LiveOrderQueueItem],
    connect: Callable[[Queue[Any]], BrokerAdapter],
    *,
    label: str,
) -> BrokerAdapter:
    """Bound an otherwise-unbounded broker connection attempt during preview.

    `connect_broker_profile` has no internal timeout, so a broker whose
    session never responds blocks the caller forever -- Chase, SoFi, and
    WellsTrade are the realistic case, since they need a real browser-
    automation session, unlike Fennel/Public/Robinhood/Aries's
    lighter-weight API sessions. The per-target `except Exception` around
    every caller of this function already turns a *raised* connection error
    into a retryable failure and moves on to the next target; a hang never
    raises anything, so it bypasses that handling completely and blocks the
    whole preview forever. Because `cloud_work_busy` stays true for as long as
    the preview is running, that one stuck broker doesn't just fail its own
    orders -- it permanently blocks every other queued signal too.

    The live-order-submission side already tolerates exactly this failure
    mode via `_run_live_order_batch_with_watchdog`'s thread-and-deadline
    pattern; this mirrors it for preview/preflight connections, which had no
    equivalent. `TaskProgressUpdate` and `TaskChallengeRequest` items are
    relayed to the real queue as they arrive (so a 2FA prompt still reaches
    the user normally) and extend the deadline the same way the submission
    watchdog does -- a challenge substantially, since answering one is a
    legitimate reason a connection takes a while, not a hang.
    """
    inner: Queue[TaskProgressUpdate | TaskChallengeRequest | BrokerAdapter | BaseException] = (
        Queue()
    )

    def attempt() -> None:
        try:
            inner.put(connect(cast(Queue[Any], inner)))
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread below
            inner.put(exc)

    worker = Thread(target=attempt, daemon=True, name="lotra-preview-connect")
    worker.start()
    deadline = monotonic() + _PREVIEW_CONNECT_TIMEOUT_SECONDS
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        try:
            item = inner.get(timeout=min(remaining, 0.25))
        except Empty:
            continue
        if isinstance(item, TaskProgressUpdate):
            queue.put(item)
            deadline = monotonic() + _PREVIEW_CONNECT_TIMEOUT_SECONDS
            continue
        if isinstance(item, TaskChallengeRequest):
            queue.put(item)
            # request_task_challenge has its own 10-minute user-response cap.
            deadline = max(deadline, monotonic() + 600.0)
            continue
        if isinstance(item, BaseException):
            raise item
        return item
    # Timed out: the connection attempt is still stuck, and since Python
    # threads cannot be forcibly cancelled it stays stuck in the background.
    # If it eventually succeeds there, the adapter it built is simply never
    # used -- this target has already moved on to being reported as failed.
    raise TimeoutError(
        f"{label} did not respond within "
        f"{_watchdog_duration_text(_PREVIEW_CONNECT_TIMEOUT_SECONDS)}"
    )


def _create_connected_order_adapter(
    queue: Queue[PreviewQueueItem] | Queue[LiveOrderQueueItem],
    profile: BrokerLoginProfile,
    *,
    force_interactive_login: bool,
) -> BrokerAdapter:
    adapter = _create_order_adapter(
        queue,
        profile,
        force_interactive_login=force_interactive_login,
    )
    try:
        _connect_and_probe_order_adapter(adapter, profile)
    except Exception:
        with suppress(Exception):
            adapter.disconnect()
        raise
    return adapter


def _connect_order_adapter(
    queue: Queue[PreviewQueueItem] | Queue[LiveOrderQueueItem],
    profile: BrokerLoginProfile,
    *,
    current: int,
    total: int,
) -> BrokerAdapter:
    queue.put(
        TaskProgressUpdate(
            message=f"Connecting to {profile.label}...",
            current=current,
            total=total,
        )
    )

    def on_recovery() -> None:
        queue.put(
            TaskProgressUpdate(
                message=f"{profile.label} session expired; signing in automatically...",
                current=current,
                total=total,
            )
        )

    return connect_broker_profile(
        profile,
        create_adapter=lambda interactive: _create_order_adapter(
            queue,
            profile,
            force_interactive_login=interactive,
        ),
        connect_adapter=lambda adapter, _interactive: _connect_and_probe_order_adapter(
            adapter,
            profile,
        ),
        on_recovery=on_recovery,
    )


def _create_order_adapter(
    queue: Queue[PreviewQueueItem] | Queue[LiveOrderQueueItem],
    profile: BrokerLoginProfile,
    *,
    force_interactive_login: bool,
) -> BrokerAdapter:
    return create_adapter(
        profile.broker_id,
        login_profile=profile,
        challenge_handler=_challenge_handler_for_queue(queue),
        force_interactive_login=force_interactive_login,
    )


def _connect_and_probe_order_adapter(
    adapter: BrokerAdapter,
    profile: BrokerLoginProfile,
) -> None:
    adapter.connect()
    accounts = _discover_broker_accounts(adapter)
    if not accounts:
        raise BrokerNotConfiguredError(
            f"{profile.label} session expired or returned no accounts after login"
        )
    cast(Any, adapter)._lotra_discovered_accounts = tuple(accounts)


class _BatchProgressQueue:
    def __init__(
        self,
        output: Queue[LiveOrderQueueItem],
        *,
        completed_before_batch: int,
        total_orders: int,
    ) -> None:
        self.output = output
        self.completed_before_batch = completed_before_batch
        self.total_orders = total_orders
        self.result: LiveOrderResult | None = None

    def put(self, item: LiveOrderQueueItem) -> None:
        if isinstance(item, LiveOrderResult):
            self.result = item
            return
        if isinstance(item, TaskProgressUpdate):
            current = self.completed_before_batch + item.current
            message = item.message
            if message.startswith("Processed "):
                message = f"Processed {current} of {self.total_orders} orders"
            self.output.put(
                TaskProgressUpdate(
                    message=message,
                    current=current,
                    total=self.total_orders,
                    eta_seconds=item.eta_seconds,
                    reset_eta_sample=item.reset_eta_sample,
                )
            )
            return
        self.output.put(item)


def _login_scoped_reason(login_label: str, reason: str) -> str:
    clean = " ".join(reason.split()).strip()
    lowered = clean.casefold()
    label = login_label.casefold()
    if lowered == label or lowered.startswith(f"{label}:") or lowered.startswith(f"{label} "):
        return clean
    return f"{login_label}: {clean}"


# How long a broker login may remain idle inside _collect_live_orders before
# the rest of the signal moves on. Each progress update starts a fresh window;
# this avoids cutting off a slow but healthy multi-account run while still
# recovering when one broker call never returns.
_LIVE_ORDER_BATCH_TIMEOUT_SECONDS = 90.0

# Overall ceiling on how long one signal's full batch loop (every broker
# login it targets, one at a time) is allowed to run before the caller stops
# attempting any further logins and lets the signal complete anyway. Past this
# point, every not-yet-attempted login is reported as a retryable failure.
_LIVE_ORDER_TOTAL_TIMEOUT_SECONDS = 780.0


def _run_live_order_batch_with_watchdog(
    queue: Queue[LiveOrderQueueItem],
    profile: BrokerLoginProfile,
    plans: tuple[OrderPlan, ...],
    authorization: BatchAuthorization | None,
) -> None:
    # _collect_live_orders writes into its own private queue rather than the
    # caller's `queue` directly. That is what lets this function walk away
    # from a hung login cleanly: once the deadline below passes we simply
    # stop reading `inner`, so nothing the orphaned thread does afterwards
    # (a stale "Connecting..." update, or a challenge dialog for a login the
    # rest of the app has already moved past) ever reaches the real queue.
    inner: Queue[LiveOrderQueueItem] = Queue()
    worker = Thread(
        target=_collect_live_orders,
        args=(inner, profile, plans, authorization),
        daemon=True,
        name="lotra-order-submit-login",
    )
    worker.start()
    deadline = monotonic() + _LIVE_ORDER_BATCH_TIMEOUT_SECONDS
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        try:
            item = inner.get(timeout=min(remaining, 0.25))
        except Empty:
            continue
        queue.put(item)
        if isinstance(item, TaskProgressUpdate):
            deadline = monotonic() + _LIVE_ORDER_BATCH_TIMEOUT_SECONDS
        elif isinstance(item, TaskChallengeRequest):
            # request_task_challenge has its own 10-minute user-response cap.
            deadline = max(deadline, monotonic() + 600.0)
        if isinstance(item, LiveOrderResult):
            return
    # Timed out: _collect_live_orders is still stuck, and since Python
    # threads cannot be forcibly cancelled it stays stuck in the background.
    # Report every plan on this login as a retryable failure and let the
    # caller move on to the next batch -- and, ultimately, the next signal --
    # instead of waiting on it forever.
    reason = (
        f"{profile.label} did not respond within "
        f"{_watchdog_duration_text(_LIVE_ORDER_BATCH_TIMEOUT_SECONDS)}; skipped for now."
    )
    queue.put(
        LiveOrderResult(
            submitted=0,
            skipped=(reason,),
            popup_skips=(reason,),
            failed=len(plans),
            retry_targets=(
                CloudRetryTarget(
                    broker_id=profile.broker_id,
                    login_id=profile.login_id,
                    account_ids=tuple(dict.fromkeys(plan.account_id for plan in plans)),
                ),
            ),
        )
    )


def _collect_live_order_batches(
    queue: Queue[LiveOrderQueueItem],
    batches: tuple[LiveOrderBatch, ...],
    authorization: BatchAuthorization | None = None,
) -> None:
    batches = _group_live_order_batches(batches)
    submitted = 0
    skipped: list[str] = []
    popup_skips: list[str] = []
    satisfied = 0
    failed = 0
    terminal_failures = 0
    uncertain = 0
    expired = 0
    market_closed = 0
    executions: list[SignalExecutionOutcome] = []
    retry_targets: list[CloudRetryTarget] = []
    skipped_brokers: set[BrokerId] = set()
    skip_broker_reasons: dict[BrokerId, str] = {}
    symbol_skip_reasons: dict[tuple[BrokerId, str], str] = {}
    completed = 0
    total_orders = sum(len(batch.plans) for batch in batches)
    deadline = monotonic() + _LIVE_ORDER_TOTAL_TIMEOUT_SECONDS
    for batch in batches:
        remaining_plans: list[OrderPlan] = []
        for plan in batch.plans:
            reason = symbol_skip_reasons.get((batch.profile.broker_id, plan.symbol.upper()))
            if reason is None:
                remaining_plans.append(plan)
                continue
            skipped.append(
                _login_scoped_reason(
                    batch.profile.label, f"{plan.account_id} {plan.symbol}: {reason}"
                )
            )
            terminal_failures += 1
            completed += 1
            queue.put(
                TaskProgressUpdate(
                    message=f"Processed {completed} of {total_orders} orders",
                    current=completed,
                    total=total_orders,
                )
            )
        if not remaining_plans:
            continue
        batch = LiveOrderBatch(profile=batch.profile, plans=tuple(remaining_plans))
        if batch.profile.broker_id in skipped_brokers:
            reason = skip_broker_reasons.get(
                batch.profile.broker_id,
                "Chase reported a stock unavailable to trade; remaining Chase orders "
                "were skipped for this signal.",
            )
            for offset, plan in enumerate(batch.plans, start=1):
                label = f"{plan.account_id} {plan.symbol}: {reason}"
                skipped.append(label)
                popup_skips.append(label)
                terminal_failures += 1
                queue.put(
                    TaskProgressUpdate(
                        message=f"Processed {completed + offset} of {total_orders} orders",
                        current=completed + offset,
                        total=total_orders,
                    )
                )
            completed += len(batch.plans)
            continue
        if monotonic() >= deadline:
            reason = (
                f"{batch.profile.label} was not reached before this "
                "signal's processing time ran out; it will be retried."
            )
            skipped.append(reason)
            popup_skips.append(reason)
            failed += len(batch.plans)
            completed += len(batch.plans)
            retry_targets.append(
                CloudRetryTarget(
                    broker_id=batch.profile.broker_id,
                    login_id=batch.profile.login_id,
                    account_ids=tuple(dict.fromkeys(plan.account_id for plan in batch.plans)),
                )
            )
            continue
        proxy = _BatchProgressQueue(
            queue,
            completed_before_batch=completed,
            total_orders=total_orders,
        )
        _run_live_order_batch_with_watchdog(
            cast(Queue[LiveOrderQueueItem], proxy),
            batch.profile,
            batch.plans,
            authorization,
        )
        result = proxy.result
        if result is not None:
            submitted += result.submitted
            satisfied += result.satisfied
            failed += result.failed
            terminal_failures += result.terminal_failures
            uncertain += result.uncertain
            expired += result.expired
            market_closed += result.market_closed
            executions.extend(result.executions)
            retry_targets.extend(result.retry_targets)
            for symbol, reason in result.symbol_skip_reasons:
                symbol_skip_reasons[(batch.profile.broker_id, symbol)] = reason
            if result.skip_broker is not None:
                skipped_brokers.add(result.skip_broker)
                if result.skip_broker_reason:
                    skip_broker_reasons[result.skip_broker] = result.skip_broker_reason
            skipped.extend(
                _login_scoped_reason(batch.profile.label, reason) for reason in result.skipped
            )
            popup_skips.extend(
                _login_scoped_reason(batch.profile.label, reason) for reason in result.popup_skips
            )
        completed += len(batch.plans)
    queue.put(
        LiveOrderResult(
            submitted=submitted,
            skipped=tuple(skipped),
            popup_skips=tuple(popup_skips),
            satisfied=satisfied,
            failed=failed,
            terminal_failures=terminal_failures,
            uncertain=uncertain,
            expired=expired,
            market_closed=market_closed,
            executions=tuple(executions),
            retry_targets=_merge_cloud_retry_targets(tuple(retry_targets)),
        )
    )


def _group_live_order_batches(
    batches: tuple[LiveOrderBatch, ...],
) -> tuple[LiveOrderBatch, ...]:
    grouped: dict[tuple[BrokerId, str], list[OrderPlan]] = {}
    profiles: dict[tuple[BrokerId, str], BrokerLoginProfile] = {}
    first_seen: dict[tuple[BrokerId, str], int] = {}
    for index, batch in enumerate(batches):
        key = (batch.profile.broker_id, batch.profile.login_id)
        profiles.setdefault(key, batch.profile)
        first_seen.setdefault(key, index)
        grouped.setdefault(key, []).extend(batch.plans)
    broker_order = {broker_id: index for index, broker_id in enumerate(_EXECUTION_BROKER_ORDER)}
    ordered_keys = sorted(grouped, key=lambda key: (broker_order[key[0]], first_seen[key]))
    return tuple(
        LiveOrderBatch(profile=profiles[key], plans=tuple(grouped[key])) for key in ordered_keys
    )


def _merge_cloud_retry_targets(
    targets: tuple[CloudRetryTarget, ...],
) -> tuple[CloudRetryTarget, ...]:
    merged: dict[tuple[BrokerId, str], list[str]] = {}
    for target in targets:
        account_ids = merged.setdefault((target.broker_id, target.login_id), [])
        account_ids.extend(item for item in target.account_ids if item not in account_ids)
    return tuple(
        CloudRetryTarget(broker_id=key[0], login_id=key[1], account_ids=tuple(account_ids))
        for key, account_ids in merged.items()
        if account_ids
    )


def _restrict_cloud_selection(
    selection: CustomOrderSelection,
    retry_targets: tuple[CloudRetryTarget, ...],
) -> CustomOrderSelection:
    allowed = {
        (target.broker_id, target.login_id): set(target.account_ids) for target in retry_targets
    }
    targets = tuple(
        CustomOrderTarget(
            profile=target.profile,
            account_ids=tuple(
                account_id
                for account_id in target.account_ids
                if account_id
                in allowed.get((target.profile.broker_id, target.profile.login_id), set())
            ),
        )
        for target in selection.targets
        if any(
            account_id in allowed.get((target.profile.broker_id, target.profile.login_id), set())
            for account_id in target.account_ids
        )
    )
    return CustomOrderSelection(
        targets=targets,
        symbol=selection.symbol,
        quantity=selection.quantity,
    )


def _collect_live_orders(
    queue: Queue[LiveOrderQueueItem],
    profile: BrokerLoginProfile,
    plans: tuple[OrderPlan, ...],
    authorization: BatchAuthorization | None = None,
) -> None:
    approved_plans = plans
    submitted = 0
    satisfied = 0
    failed = 0
    terminal_failures = 0
    uncertain = 0
    expired = 0
    market_closed = 0
    executions: list[SignalExecutionOutcome] = []
    retryable_account_ids: list[str] = []
    non_retryable_account_ids: set[str] = set()
    skipped: list[str] = []
    popup_skips: list[str] = []
    skip_broker: BrokerId | None = None
    skip_broker_reason: str | None = None
    symbol_skip_reasons: dict[str, str] = {}
    adapter: BrokerAdapter | None = None
    try:
        queue.put(
            TaskProgressUpdate(
                message=f"Connecting to {profile.label}...",
                current=0,
                total=len(plans),
            )
        )
        adapter = _connect_order_adapter(
            queue,
            profile,
            current=0,
            total=len(plans),
        )
        plans = _resolve_account_plans(adapter, profile, plans)
        if authorization is not None:
            for approved, resolved in zip(approved_plans, plans, strict=True):
                authorization.allow_account_resolution(approved, resolved)
        with connect() as connection:
            migrate(connection)
            observation_writer = TradePortfolioObservationWriter(connection)
            settings = load_settings()
            execution_guard = DatabaseOrderExecutionGuard(
                connection,
                login_id=profile.login_id,
            )
            has_sell_plans = any(plan.action is OrderAction.SELL for plan in plans)
            protected_tickers = (
                PortfolioProtectedTickerRepository(connection).list_all()
                if has_sell_plans
                else set()
            )
            tracked_plays = TrackedPlayRepository(connection) if has_sell_plans else None
            for index, plan in enumerate(plans, start=1):
                approved_plan = approved_plans[index - 1]
                tracked: TrackedPlay | None = None
                queue.put(
                    TaskProgressUpdate(
                        message=(
                            f"Submitting {plan.symbol} to {profile.label} " f"{plan.account_id}..."
                        ),
                        current=index - 1,
                        total=len(plans),
                    )
                )
                try:
                    if plan.symbol.upper() in symbol_skip_reasons:
                        raise OrderRejectedError(symbol_skip_reasons[plan.symbol.upper()])
                    if plan.action is OrderAction.SELL:
                        if plan.symbol.upper() in protected_tickers:
                            raise ExecutionBlockedError("ticker is protected from liquidation")
                        if tracked_plays is None:
                            raise RuntimeError("sell plan tracking was not initialized")
                        tracked = tracked_plays.find_position(
                            broker_id=profile.broker_id,
                            login_id=profile.login_id,
                            account_id=plan.account_id,
                            symbol=plan.symbol,
                        )
                        if tracked is not None and tracked.protects_position:
                            raise ExecutionBlockedError(
                                "position is protected until its round-up is complete"
                            )
                    order = submit_reviewed_order(
                        adapter=adapter,
                        plan=plan,
                        settings=settings,
                        authorization=authorization,
                        execution_guard=execution_guard,
                        observation_sink=lambda observation: observation_writer.persist(
                            observation,
                            profile=profile,
                        ),
                    )
                except ExecutionUncertainError as exc:
                    uncertain += 1
                    non_retryable_account_ids.add(approved_plan.account_id)
                    reason = str(exc)
                    label = f"{plan.account_id} {plan.symbol}: {reason}"
                    skipped.append(label)
                    popup_skips.append(label)
                except MarketClosedError as exc:
                    failed += 1
                    market_closed += 1
                    retryable_account_ids.append(approved_plan.account_id)
                    reason = str(exc)
                    label = f"{plan.account_id} {plan.symbol}: {reason}"
                    skipped.append(label)
                    popup_skips.append(label)
                except ExecutionBlockedError as exc:
                    reason = str(exc)
                    label = f"{plan.account_id} {plan.symbol}: {reason}"
                    skipped.append(label)
                    cause = exc.__cause__
                    if isinstance(cause, DuplicateExecutionTargetError):
                        if cause.state is ExecutionGuardState.SUBMITTED:
                            satisfied += 1
                        else:
                            uncertain += 1
                        non_retryable_account_ids.add(approved_plan.account_id)
                    elif "already holds" in reason.lower():
                        satisfied += 1
                        non_retryable_account_ids.add(approved_plan.account_id)
                    elif "deadline" in reason.lower() or "expired" in reason.lower():
                        expired += 1
                        non_retryable_account_ids.add(approved_plan.account_id)
                    elif isinstance(exc, OrderRejectedError) and not _reason_is_login_related(
                        reason
                    ):
                        # The broker positively rejected this order (preflight,
                        # preview, or the submission). Re-running the signal
                        # would just hit the same rejection and loop, so treat
                        # it as terminal unless a fresh sign-in could clear it.
                        terminal_failures += 1
                        non_retryable_account_ids.add(approved_plan.account_id)
                        if (
                            profile.broker_id is BrokerId.SOFI
                            and _is_sofi_stock_unavailable_rejection(reason, plan.symbol)
                        ):
                            symbol_skip_reasons.setdefault(
                                plan.symbol.upper(),
                                f"SoFi cannot trade {plan.symbol}; remaining SoFi orders for "
                                f"this ticker were skipped for this run. Reason: {reason}",
                            )
                        if (
                            profile.broker_id is BrokerId.CHASE
                            and _is_chase_stock_unavailable_rejection(reason)
                        ):
                            skip_broker = BrokerId.CHASE
                            skip_broker_reason = (
                                "Chase reported a stock unavailable to trade; remaining "
                                "Chase orders were skipped for this signal."
                            )
                    elif _is_current_signal_terminal_failure(profile.broker_id, reason):
                        terminal_failures += 1
                        non_retryable_account_ids.add(approved_plan.account_id)
                    else:
                        failed += 1
                        retryable_account_ids.append(approved_plan.account_id)
                    if "already holds" not in reason.lower() and not isinstance(
                        cause, DuplicateExecutionTargetError
                    ):
                        popup_skips.append(label)
                except Exception as exc:
                    failed += 1
                    retryable_account_ids.append(approved_plan.account_id)
                    reason = str(exc)
                    label = f"{plan.account_id} {plan.symbol}: {reason}"
                    skipped.append(label)
                    popup_skips.append(label)
                else:
                    submitted += 1
                    non_retryable_account_ids.add(approved_plan.account_id)
                    history_repository = PortfolioOrderHistoryRepository(connection)
                    requested_cost_basis = plan.cost_basis
                    if plan.action is OrderAction.SELL and tracked is not None:
                        tracked_basis = tracked.submitted_quantity * tracked.submitted_price
                        current_quantity = tracked.last_quantity or plan.quantity
                        if (
                            tracked_basis > 0
                            and current_quantity > 0
                            and plan.quantity <= current_quantity
                        ):
                            requested_cost_basis = tracked_basis * plan.quantity / current_quantity
                    if plan.action is OrderAction.SELL and requested_cost_basis is None:
                        requested_cost_basis = history_repository.local_sale_cost_basis(
                            broker_id=profile.broker_id,
                            login_id=profile.login_id,
                            account_id=plan.account_id,
                            symbol=plan.symbol,
                            current_quantity=plan.quantity,
                            sold_quantity=plan.quantity,
                        )
                    filled_quantity = order.filled_quantity
                    if (
                        order.state in {OrderState.FILLED, OrderState.RECONCILED}
                        and filled_quantity <= 0
                    ):
                        filled_quantity = order.quantity
                    confirmed_fill = (
                        order.state
                        in {
                            OrderState.PARTIALLY_FILLED,
                            OrderState.FILLED,
                            OrderState.RECONCILED,
                        }
                        and filled_quantity > 0
                        and order.average_fill_price is not None
                    )
                    history_quantity = filled_quantity if confirmed_fill else order.quantity
                    unit_price = order.average_fill_price or plan.quote.price
                    gross_amount = unit_price * history_quantity
                    cost_basis = requested_cost_basis
                    if cost_basis is not None and history_quantity != plan.quantity:
                        cost_basis = cost_basis * history_quantity / plan.quantity
                    recorded_cost_basis = (
                        gross_amount if plan.action is OrderAction.BUY else cost_basis
                    )
                    realized_pnl = (
                        gross_amount - cost_basis
                        if (
                            confirmed_fill
                            and plan.action is OrderAction.SELL
                            and cost_basis is not None
                        )
                        else None
                    )
                    history_repository.record(
                        PortfolioOrderHistoryEntry(
                            event_id=plan.event_id,
                            broker_id=profile.broker_id,
                            login_id=profile.login_id,
                            login_label=profile.label,
                            account_id=plan.account_id,
                            broker_order_id=order.broker_order_id,
                            symbol=order.symbol,
                            action=plan.action,
                            quantity=history_quantity,
                            unit_price=unit_price,
                            gross_amount=gross_amount,
                            cost_basis=recorded_cost_basis,
                            realized_pnl=realized_pnl,
                            state=order.state,
                            executed_at=order.created_at or datetime.now(UTC),
                        )
                    )
                    if confirmed_fill:
                        fill_price = cast(Decimal, order.average_fill_price)
                        filled_basis = cost_basis
                        executions.append(
                            SignalExecutionOutcome(
                                execution_id=hashlib.sha256(
                                    (
                                        f"{profile.broker_id.value}\0{profile.login_id}\0"
                                        f"{plan.account_id}\0{order.broker_order_id}"
                                    ).encode()
                                ).hexdigest(),
                                broker=profile.broker_id.value,
                                account_ref=anonymous_account_reference(
                                    profile.broker_id,
                                    profile.login_id,
                                    plan.account_id,
                                ),
                                symbol=order.symbol,
                                action=plan.action.value,
                                state=order.state.value,
                                filled_quantity=_ledger_amount(filled_quantity),
                                fill_price=_ledger_amount(fill_price),
                                cost_basis=_ledger_amount(
                                    fill_price * filled_quantity
                                    if plan.action is OrderAction.BUY
                                    else filled_basis
                                ),
                                executed_at=order.created_at or datetime.now(UTC),
                            )
                        )
                    elif order.state is OrderState.SUBMITTED:
                        estimated_profit = (
                            plan.quote.price * order.quantity - cost_basis
                            if plan.action is OrderAction.SELL and cost_basis is not None
                            else None
                        )
                        executions.append(
                            SignalExecutionOutcome(
                                execution_id=hashlib.sha256(
                                    (
                                        f"{profile.broker_id.value}\0{profile.login_id}\0"
                                        f"{plan.account_id}\0{order.broker_order_id}"
                                    ).encode()
                                ).hexdigest(),
                                broker=profile.broker_id.value,
                                account_ref=anonymous_account_reference(
                                    profile.broker_id,
                                    profile.login_id,
                                    plan.account_id,
                                ),
                                symbol=order.symbol,
                                action=plan.action.value,
                                state=order.state.value,
                                filled_quantity=Decimal("0"),
                                fill_price=Decimal("0"),
                                cost_basis=_ledger_amount(
                                    plan.quote.price * order.quantity
                                    if plan.action is OrderAction.BUY
                                    else cost_basis
                                ),
                                estimated_profit=_ledger_amount(estimated_profit),
                                executed_at=order.created_at or datetime.now(UTC),
                            )
                        )
                finally:
                    queue.put(
                        TaskProgressUpdate(
                            message=f"Processed {index} of {len(plans)} orders",
                            current=index,
                            total=len(plans),
                        )
                    )
                if skip_broker is not None:
                    for offset, (remaining_plan, remaining_approved) in enumerate(
                        zip(plans[index:], approved_plans[index:], strict=True),
                        start=1,
                    ):
                        remaining_label = (
                            f"{remaining_plan.account_id} {remaining_plan.symbol}: "
                            f"{skip_broker_reason}"
                        )
                        skipped.append(remaining_label)
                        popup_skips.append(remaining_label)
                        terminal_failures += 1
                        non_retryable_account_ids.add(remaining_approved.account_id)
                        queue.put(
                            TaskProgressUpdate(
                                message=(f"Processed {index + offset} of {len(plans)} orders"),
                                current=index + offset,
                                total=len(plans),
                            )
                        )
                    break
    except Exception as exc:
        reason = f"{profile.label} connection failed: {exc}"
        skipped.append(reason)
        popup_skips.append(reason)
        processed = submitted + satisfied + failed + terminal_failures + uncertain + expired
        failed += max(0, len(plans) - processed)
        retryable_account_ids.extend(
            plan.account_id
            for plan in approved_plans
            if plan.account_id not in non_retryable_account_ids
            and plan.account_id not in retryable_account_ids
        )
    finally:
        if adapter is not None:
            with suppress(Exception):
                adapter.disconnect()
        queue.put(
            LiveOrderResult(
                submitted=submitted,
                skipped=tuple(skipped),
                popup_skips=tuple(popup_skips),
                satisfied=satisfied,
                failed=failed,
                terminal_failures=terminal_failures,
                uncertain=uncertain,
                expired=expired,
                market_closed=market_closed,
                executions=tuple(executions),
                retry_targets=(
                    (
                        CloudRetryTarget(
                            broker_id=profile.broker_id,
                            login_id=profile.login_id,
                            account_ids=tuple(dict.fromkeys(retryable_account_ids)),
                        ),
                    )
                    if retryable_account_ids
                    else ()
                ),
                skip_broker=skip_broker,
                skip_broker_reason=skip_broker_reason,
                symbol_skip_reasons=tuple(symbol_skip_reasons.items()),
            )
        )


def _resolve_account_plans(
    adapter: BrokerAdapter,
    profile: BrokerLoginProfile,
    plans: tuple[OrderPlan, ...],
) -> tuple[OrderPlan, ...]:
    accounts = _discover_broker_accounts(adapter)
    if not accounts:
        raise ExecutionBlockedError(f"{profile.label} returned no enabled accounts after login")
    resolved: list[OrderPlan] = []
    for plan in plans:
        account = _matching_broker_account(accounts, selector=plan.account_id, profile=profile)
        if account is None:
            raise ExecutionBlockedError(
                f"{profile.label} could not resolve enabled account ending in "
                f"{_account_selector_suffix(plan.account_id)}"
            )
        actual_account_id = account.account_id
        resolved.append(
            plan.model_copy(
                update={
                    "account_id": actual_account_id,
                    "idempotency_key": idempotency_key(
                        event_id=plan.event_id,
                        broker_id=plan.broker_id,
                        account_id=actual_account_id,
                        action=plan.action,
                        quantity=plan.quantity,
                        login_id=profile.login_id,
                    ),
                }
            )
        )
    return tuple(resolved)


def _matching_broker_account(
    accounts: list[BrokerAccount],
    *,
    selector: str,
    profile: BrokerLoginProfile,
) -> BrokerAccount | None:
    del profile
    matches = matching_accounts(accounts, selector)
    if len(matches) > 1:
        raise ExecutionBlockedError(
            f"account ending in {_account_selector_suffix(selector)} is ambiguous"
        )
    return matches[0] if matches else None


def _discover_broker_accounts(adapter: BrokerAdapter) -> list[BrokerAccount]:
    cached = getattr(adapter, "_lotra_discovered_accounts", None)
    if cached is not None:
        with suppress(AttributeError):
            delattr(adapter, "_lotra_discovered_accounts")
        return list(cast(tuple[BrokerAccount, ...], cached))
    discover = getattr(adapter, "discover_accounts", None)
    if callable(discover):
        return cast(list[BrokerAccount], discover())
    return adapter.list_accounts()


def _build_custom_order_plan(
    *,
    event_id: str,
    profile: BrokerLoginProfile,
    account_id: str,
    quote: Quote,
    quantity: Decimal,
    action: OrderAction = OrderAction.BUY,
    cost_basis: Decimal | None = None,
    expires_at: datetime | None = None,
    reason: str = "Custom order",
) -> OrderPlan:
    key = idempotency_key(
        event_id=event_id,
        broker_id=profile.broker_id,
        account_id=account_id,
        action=action,
        quantity=quantity,
        login_id=profile.login_id,
    )
    return OrderPlan(
        event_id=event_id,
        broker_id=profile.broker_id,
        account_id=account_id,
        symbol=quote.symbol,
        action=action,
        quantity=quantity,
        quote=quote,
        estimated_cost=quote.price * quantity,
        cost_basis=cost_basis,
        idempotency_key=key,
        reason=reason,
        expires_at=expires_at,
    )


def _admin_preview_from_result(
    draft: AdminDraft,
    result: OrderPreviewResult,
) -> AdminDraftPreviewResult | None:
    plans_by_broker: dict[BrokerId, list[OrderPlan]] = {}
    for batch in result.batches:
        plans_by_broker.setdefault(batch.profile.broker_id, []).extend(batch.plans)
    errors_by_broker: dict[BrokerId, str] = {}
    for error in result.errors:
        lowered = error.lower()
        broker_id = next(
            (broker for broker in BrokerId if broker.value.lower() in lowered),
            None,
        )
        if broker_id is not None:
            errors_by_broker.setdefault(broker_id, error)
    quote_by_broker = {
        broker_id: quote
        for broker_id, quote in result.quotes
        if quote.symbol.upper() == draft.symbol.upper()
    }
    broker_ids = sorted(
        set(plans_by_broker)
        | set(errors_by_broker)
        | (set(quote_by_broker) if draft.sell_all else set()),
        key=lambda broker: list(BrokerId).index(broker),
    )
    if not broker_ids:
        return None
    lines: list[AdminPreviewLine] = []
    quoted_at = datetime.now(UTC)
    for broker_id in broker_ids:
        plans = plans_by_broker.get(broker_id, [])
        if plans:
            lines.append(
                AdminPreviewLine(
                    broker=broker_id.value.title(),
                    account_count=len(plans),
                    available=True,
                    unit_price=_preview_decimal(plans[0].quote.price, places=6),
                )
            )
            continue
        lines.append(
            AdminPreviewLine(
                broker=broker_id.value.title(),
                account_count=0,
                available=False,
                reason=_generic_broker_preview_reason(
                    errors_by_broker.get(
                        broker_id,
                        "No eligible account currently holds this ticker",
                    )
                ),
            )
        )
    reference_price = next(
        (line.unit_price for line in lines if line.unit_price is not None),
        None,
    )
    if reference_price is None and draft.sell_all:
        reference_price = next(
            (_preview_decimal(quote.price, places=6) for quote in quote_by_broker.values()),
            None,
        )
    if reference_price is None:
        return None
    cash_in_lieu_value = (
        _preview_decimal(
            sum(
                (plan.estimated_cost for plans in plans_by_broker.values() for plan in plans),
                start=Decimal("0"),
            ),
            places=8,
        )
        if draft.cash_in_lieu
        else None
    )
    if draft.cash_in_lieu and (cash_in_lieu_value is None or cash_in_lieu_value <= 0):
        return None
    return AdminDraftPreviewResult(
        draft_id=draft.draft_id,
        submission=AdminPreviewSubmission(
            quoted_at=quoted_at,
            unit_price=reference_price,
            cash_in_lieu_value=cash_in_lieu_value,
            brokers=tuple(lines),
        ),
    )


def _preview_decimal(value: Decimal, *, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def _generic_broker_preview_reason(error: str) -> BrokerPreviewReason:
    lowered = error.lower()
    if "no eligible account" in lowered and ("hold" in lowered or "position" in lowered):
        return BrokerPreviewReason.NO_POSITION
    if "deadline" in lowered or "expired order" in lowered:
        return BrokerPreviewReason.DEADLINE_EXPIRED
    if "session" in lowered and "expired" in lowered:
        return BrokerPreviewReason.SESSION_EXPIRED
    if "login" in lowered or "sign in" in lowered:
        return BrokerPreviewReason.LOGIN_REQUIRED
    if any(marker in lowered for marker in ("symbol", "ticker", "could not find")):
        return BrokerPreviewReason.TICKER_UNAVAILABLE
    if "quote" in lowered or "price" in lowered:
        return BrokerPreviewReason.PRICE_UNAVAILABLE
    if "no enabled account" in lowered:
        return BrokerPreviewReason.NO_ENABLED_ACCOUNTS
    return BrokerPreviewReason.BROKER_CHECK_FAILED


def _cloud_preparation_status(result: OrderPreviewResult) -> str:
    if result.coverage.terminal_without_orders:
        if result.coverage.permanent_failed_targets:
            return "Broker restrictions excluded this signal; no order was submitted."
        return "No matching positions were found; no order was needed."
    reasons = tuple(
        dict.fromkeys(_generic_broker_preview_reason(error).value for error in result.errors)
    )
    detail = f" ({', '.join(reasons[:3])})" if reasons else ""
    return f"Broker checks could not finish{detail}; the signal will retry."


# Markers that mean a broker rejection was caused by a login/session problem
# rather than the order itself. These stay retryable: the signal is re-run
# after the broker signs in again. Everything else that the broker positively
# rejects is terminal for the signal.
_LOGIN_RELATED_FAILURE_MARKERS = (
    "log in",
    "login",
    "logged in",
    "sign in",
    "sign-in",
    "signed out",
    "session expired",
    "session has expired",
    "session is expired",
    "session is no longer",
    "session was not",
    "reconnect to authenticate",
    "authenticate again",
    "authentication failed",
    "reauthenticate",
    "re-authenticate",
    "not connected",
    "is not stored",
    "not configured",
    "credentials",
    "verify you are human",
)


def _reason_is_login_related(reason: str) -> bool:
    lowered = reason.casefold()
    return any(marker in lowered for marker in _LOGIN_RELATED_FAILURE_MARKERS)


def _is_chase_stock_unavailable_rejection(reason: str) -> bool:
    normalized = " ".join(reason.casefold().replace("_", " ").replace("-", " ").split())
    has_security = any(
        marker in normalized for marker in ("stock", "security", "symbol", "instrument", "equity")
    )
    has_unavailable = any(
        marker in normalized
        for marker in (
            "unavailable to trade",
            "not available to trade",
            "unavailable for trading",
            "not available for trading",
            "not eligible to trade",
            "not eligible for trading",
            "cannot be traded",
            "can't be traded",
            "not tradable",
        )
    )
    return has_security and has_unavailable


def _is_sofi_stock_unavailable_rejection(reason: str, symbol: str) -> bool:
    # Only a security-wide rejection can be shared across SoFi accounts.
    # Buying power, account restrictions and session failures are account-specific.
    normalized = " ".join(reason.casefold().split())
    return f"security 'equity-{symbol.casefold()}' cannot be traded" in normalized


def _watchdog_duration_text(seconds: float) -> str:
    if seconds >= 60:
        minutes = max(1, int(round(seconds / 60)))
        return f"{minutes} minute(s)"
    return f"{max(1, int(round(seconds)))} second(s)"


def _is_current_signal_terminal_failure(_broker_id: BrokerId, reason: str) -> bool:
    lowered = reason.lower()
    return "corporate action" in lowered or "corporate-action" in lowered


def _money(value: Decimal) -> str:
    return f"${value:,.2f}"
