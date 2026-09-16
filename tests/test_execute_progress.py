from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from queue import Queue
from threading import Event
from time import sleep
from types import SimpleNamespace

import pytest
from localrsa.brokers.base import BrokerNotConfiguredError
from localrsa.cloud.models import AdminDraft, BrokerPreviewReason, PendingSignal
from localrsa.config import LOCALRSA_DATA_DIR_ENV, AppSettings
from localrsa.execution import executor
from localrsa.models.account import BrokerAccount, BrokerLoginProfile, Position
from localrsa.models.broker import BrokerId
from localrsa.models.order import BrokerOrder, OrderAction, OrderState, Quote
from localrsa.ui import execute_page
from localrsa.ui.activity_log_page import ActivityLogEntry
from localrsa.ui.custom_order_dialog import (
    CustomOrderDialog,
    CustomOrderSelection,
    CustomOrderTarget,
)
from localrsa.ui.execute_page import (
    CloudRetryTarget,
    CloudSignalClaimContext,
    CloudSignalPreparation,
    LiveOrderBatch,
    LiveOrderQueueItem,
    LiveOrderResult,
    OrderPreviewResult,
    PreparationCoverage,
    PreviewQueueItem,
    SignalListenerState,
    _group_live_order_batches,
    _restrict_cloud_selection,
)
from localrsa.ui.task_progress import TaskProgressUpdate
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QAbstractItemView, QDialog, QDialogButtonBox, QMessageBox


@pytest.fixture(autouse=True)
def _market_is_open(monkeypatch: pytest.MonkeyPatch) -> None:
    def clock() -> datetime:
        return datetime(2026, 7, 28, 15, 0, tzinfo=UTC)

    monkeypatch.setattr(execute_page, "market_now", clock)
    monkeypatch.setattr(executor, "market_now", clock)


def test_account_resolution_skips_stale_selector_without_skipping_login() -> None:
    profile = BrokerLoginProfile(
        broker_id=BrokerId.WELLSTRADE,
        login_id="wells-login",
        label="WellsTrade",
        account_selectors=("8363", "7267"),
    )
    account = BrokerAccount(
        broker_id=BrokerId.WELLSTRADE,
        login_id=profile.login_id,
        account_id="7267",
        masked_account_id="7267",
        account_type="brokerage",
        tradable=True,
    )
    adapter = SimpleNamespace(_lotra_discovered_accounts=(account,))

    resolved, errors = execute_page._resolve_enabled_account_ids(
        adapter=adapter,
        target=CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),
        action=OrderAction.BUY,
    )

    assert resolved == ("7267",)
    assert errors == ("enabled account ending in 8363 is no longer available",)


class _OrderAdapter:
    metadata = SimpleNamespace(
        display_name="Test broker",
        fractional_sell_supported=True,
    )

    def __init__(self) -> None:
        self.connected = False
        self.disconnected = False
        self.quote_calls = 0

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.disconnected = True

    def list_accounts(self) -> list[BrokerAccount]:
        return [
            BrokerAccount(
                broker_id=BrokerId.PUBLIC,
                login_id="public-login",
                account_id="public-login-1",
                masked_account_id="public-login-1",
                account_type="brokerage",
                tradable=True,
            )
        ]

    def get_quote(self, symbol: str) -> Quote:
        self.quote_calls += 1
        return Quote(symbol=symbol, price=Decimal("1.25"), as_of=datetime.now(UTC))

    def get_positions(self, _account_id: str) -> list[object]:
        return []

    def preflight(self, _request: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(accepted=True, reason=None, estimated_cost=None)

    def peek_portfolio_observation(self, _account_id: str) -> None:
        return None

    def place_order(self, request: object) -> BrokerOrder:
        return BrokerOrder(
            broker_id=BrokerId.PUBLIC,
            account_id=request.account_id,
            broker_order_id="order-1",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            action=OrderAction.BUY,
            quantity=request.quantity,
            state=OrderState.SUBMITTED,
            created_at=datetime.now(UTC),
            average_fill_price=Decimal("1.10"),
        )


def _profile(
    broker_id: BrokerId = BrokerId.PUBLIC,
    login_id: str = "public-login",
) -> BrokerLoginProfile:
    return BrokerLoginProfile(
        broker_id=broker_id,
        login_id=login_id,
        label=broker_id.value.title(),
        account_selectors=(f"{login_id}-1",),
    )


def test_custom_order_plan_contains_live_cost_without_gain() -> None:
    profile = _profile()
    quote = Quote(symbol="MANL", price=Decimal("2.50"), as_of=datetime.now(UTC))
    plan = execute_page._build_custom_order_plan(
        event_id="custom-order:test",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=quote,
        quantity=Decimal("2"),
    )
    assert plan.estimated_cost == Decimal("5.00")
    assert "estimated_gross_gain" not in type(plan).model_fields
    assert plan.reason == "Custom order"


def test_custom_order_defaults_to_all_enabled_logins_and_has_no_price_field(qtbot) -> None:  # type: ignore[no-untyped-def]
    profiles = [
        _profile().model_copy(update={"account_selectors": ("public-1", "public-2")}),
        _profile(BrokerId.PUBLIC, "public-login-2"),
        _profile(BrokerId.ROBINHOOD, "robinhood-login"),
    ]
    dialog = CustomOrderDialog(
        profiles,
        AppSettings(),
        usernames={
            "public-login": "first@example.com",
            "public-login-2": "second@example.com",
            "robinhood-login": "third@example.com",
        },
    )
    qtbot.addWidget(dialog)
    selection = dialog.selection()
    buttons = dialog.findChild(QDialogButtonBox)
    assert [target.profile.login_id for target in selection.targets] == [
        "public-login",
        "public-login-2",
        "robinhood-login",
    ]
    public = dialog.account_tree.topLevelItem(0)
    assert public.text(0) == "Public (2 logins)"
    assert public.isExpanded()
    assert public.child(0).text(0) == "first@example.com (2 accounts)"
    assert not public.child(0).isExpanded()
    public.child(0).setExpanded(True)
    assert public.child(0).child(0).text(0) == "Account ending in lic1"
    public.child(0).setCheckState(0, Qt.CheckState.Unchecked)
    assert [target.profile.login_id for target in dialog.selection().targets] == [
        "public-login-2",
        "robinhood-login",
    ]
    assert not hasattr(dialog, "reference_price")
    assert buttons is not None
    assert buttons.button(QDialogButtonBox.StandardButton.Ok).text() == "Add to Orders"


def test_add_order_loads_raw_usernames_once_for_enabled_profiles(
    qtbot,  # type: ignore[no-untyped-def]
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOCALRSA_DATA_DIR_ENV, str(tmp_path / "data"))
    profile = _profile()
    loads: list[list[str]] = []
    dialog_usernames: list[dict[str, str]] = []

    def load_usernames(profiles: list[BrokerLoginProfile]) -> dict[str, str]:
        loads.append([item.login_id for item in profiles])
        return {profile.login_id: "full-username@example.com"}

    class _RejectedDialog:
        def __init__(
            self,
            _profiles: list[BrokerLoginProfile],
            _settings: AppSettings,
            *,
            usernames: dict[str, str],
        ) -> None:
            dialog_usernames.append(usernames)

        def exec(self) -> int:
            return int(QDialog.DialogCode.Rejected)

    monkeypatch.setattr(execute_page, "load_login_usernames", load_usernames)
    monkeypatch.setattr(execute_page, "CustomOrderDialog", _RejectedDialog)
    page = execute_page.ExecutePage()
    qtbot.addWidget(page)
    monkeypatch.setattr(page, "_enabled_login_profiles", lambda: [profile])

    page.add_custom_order()

    assert loads == [[profile.login_id]]
    assert dialog_usernames == [{profile.login_id: "full-username@example.com"}]


def test_cloud_signal_start_and_stop_controls_require_an_available_session(qtbot) -> None:  # type: ignore[no-untyped-def]
    page = execute_page.ExecutePage()
    qtbot.addWidget(page)

    assert page.signal_listener_state is SignalListenerState.UNAVAILABLE
    assert not page.signal_start_button.isEnabled()
    assert not page.signal_stop_button.isEnabled()

    page.set_signal_listener_available(True)
    assert page.signal_listener_state is SignalListenerState.STOPPED
    assert page.signal_start_button.isEnabled()
    assert "#df4b4b" in page.signal_listener_status.text()

    starts: list[bool] = []
    page.signal_listener_start_requested.connect(lambda: starts.append(True))
    page.signal_start_button.click()
    assert starts == [True]
    assert page.signal_listener_state is SignalListenerState.STARTING
    assert page.signal_stop_button.isEnabled()

    page.set_signal_listener_state(SignalListenerState.RUNNING)
    assert "Lotra Software" in page.signal_listener_status.text()
    assert "#35c96f" in page.signal_listener_status.text()
    stops: list[bool] = []
    page.signal_listener_stop_requested.connect(lambda: stops.append(True))
    page.signal_stop_button.click()
    assert stops == [True]
    assert page.signal_listener_state is SignalListenerState.STOPPING
    assert "#d4a017" in page.signal_listener_status.text()
    assert "current request" not in page.signal_listener_status.text()
    assert not page.signal_start_button.isEnabled()
    assert not page.signal_stop_button.isEnabled()


def test_add_order_creates_a_selectable_card_without_a_checkbox_column(
    qtbot,  # type: ignore[no-untyped-def]
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOCALRSA_DATA_DIR_ENV, str(tmp_path / "data"))
    profile = _profile()
    selection = CustomOrderSelection(
        targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
        symbol="MANL",
        quantity=Decimal("1"),
    )

    class _AcceptedDialog:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def exec(self) -> int:
            return int(QDialog.DialogCode.Accepted)

        def selection(self) -> CustomOrderSelection:
            return selection

    monkeypatch.setattr(execute_page, "CustomOrderDialog", _AcceptedDialog)
    page = execute_page.ExecutePage()
    qtbot.addWidget(page)
    monkeypatch.setattr(page, "_enabled_login_profiles", lambda: [profile])
    cloud_events: list[object] = []
    page.cloud_signal_prepared.connect(cloud_events.append)
    page.admin_preview_ready.connect(cloud_events.append)
    page.add_custom_order()
    assert len(page.queued_orders) == 1
    assert page.order_list.count() == 1
    assert page.order_list.selectionMode() == QAbstractItemView.SelectionMode.ExtendedSelection
    assert "MANL" in page.order_list.item(0).text()
    assert page.review_button.isEnabled()
    assert cloud_events == []


def test_manual_order_added_after_hours_stays_queued_until_open(
    qtbot,  # type: ignore[no-untyped-def]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = [datetime(2026, 7, 28, 21, 0, tzinfo=UTC)]
    profile = _profile()
    selection = CustomOrderSelection(
        targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
        symbol="LATE",
        quantity=Decimal("1"),
    )

    class _AcceptedDialog:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def exec(self) -> int:
            return int(QDialog.DialogCode.Accepted)

        def selection(self) -> CustomOrderSelection:
            return selection

    monkeypatch.setattr(execute_page, "CustomOrderDialog", _AcceptedDialog)
    page = execute_page.ExecutePage(market_clock=lambda: current[0])
    qtbot.addWidget(page)
    monkeypatch.setattr(page, "_enabled_login_profiles", lambda: [profile])

    page.add_custom_order()

    assert len(page.queued_orders) == 1
    assert not page.review_button.isEnabled()
    assert "queued until" in page.status_label.text()

    current[0] = datetime(2026, 7, 29, 14, 0, tzinfo=UTC)
    page._refresh_market_state()

    assert page.review_button.isEnabled()
    assert "ready for review" in page.status_label.text()


def test_preview_worker_fetches_live_quote_before_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    profile = _profile()
    selection = CustomOrderSelection(
        targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
        symbol="LIVE",
        quantity=Decimal("2"),
    )
    queued = execute_page.QueuedCustomOrder(
        queue_id="queue-1",
        selection=selection,
        created_at=datetime.now(UTC),
    )
    adapter = _OrderAdapter()
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    queue: Queue[PreviewQueueItem] = Queue()
    execute_page._collect_order_previews(queue, (queued,))
    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert adapter.connected
    assert adapter.disconnected
    assert result.errors == ()
    assert result.batches[0].plans[0].quote.price == Decimal("1.25")
    assert result.batches[0].plans[0].estimated_cost == Decimal("2.50")


def test_preview_cancellation_stops_before_quote_and_disconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.ROBINHOOD, "robinhood-login")
    queued = execute_page.QueuedCustomOrder(
        queue_id="cancel-preview",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="STOP",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
    )
    cancel = Event()

    class _CancelingAdapter(_OrderAdapter):
        def connect(self) -> None:
            super().connect()
            cancel.set()

    adapter = _CancelingAdapter()
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,), cancel)

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.cancelled
    assert result.batches == ()
    assert adapter.quote_calls == 0
    assert adapter.disconnected


def test_cloud_preview_preserves_stable_event_and_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    expires_at = datetime.now(UTC) + timedelta(days=1)
    queued = execute_page.QueuedCustomOrder(
        queue_id="signal-1",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="CLOUD",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        event_id="cloud-signal:signal-1",
        expires_at=expires_at,
        reason="Lotra Software signal",
        cloud_signal_id="signal-1",
    )
    adapter = _OrderAdapter()
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    plan = result.batches[0].plans[0]
    assert plan.event_id == "cloud-signal:signal-1"
    assert plan.expires_at == expires_at
    assert plan.reason == "Lotra Software signal"


def test_cloud_sell_preview_preserves_sell_action_and_ignores_buy_cost_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-signal-1",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="SELLME",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        event_id="cloud-signal:sell-signal-1",
        action=OrderAction.SELL,
    )
    adapter = _OrderAdapter()
    monkeypatch.setattr(
        adapter,
        "get_quote",
        lambda symbol: Quote(
            symbol=symbol,
            price=Decimal("500"),
            as_of=datetime.now(UTC),
        ),
    )
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    monkeypatch.setattr(
        execute_page,
        "load_settings",
        lambda: AppSettings(maximum_cost_per_order=Decimal("25")),
    )
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.errors == ()
    assert result.batches[0].plans[0].action is OrderAction.SELL


def test_cloud_sell_all_resolves_every_fractional_account_quantity_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile().model_copy(
        update={"account_selectors": ("account-1", "account-2", "account-3")}
    )
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-signal",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="FRAC",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        event_id="cloud-signal:sell-all-signal",
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _FractionalPositionAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=profile.broker_id,
                    login_id=profile.login_id,
                    account_id=account_id,
                    masked_account_id=account_id,
                    account_type="brokerage",
                    tradable=True,
                )
                for account_id in profile.account_selectors
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            quantities = {
                "account-1": (Decimal("0.1"), Decimal("0.23")),
                "account-2": (Decimal("1.000001"),),
                "account-3": (),
            }
            return [
                Position(
                    broker_id=profile.broker_id,
                    account_id=account_id,
                    symbol="FRAC",
                    quantity=quantity,
                )
                for quantity in quantities[account_id]
            ]

    adapter = _FractionalPositionAdapter()
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    plans = tuple(plan for batch in result.batches for plan in batch.plans)
    assert result.errors == ()
    assert {plan.account_id: plan.quantity for plan in plans} == {
        "account-1": Decimal("0.33"),
        "account-2": Decimal("1.000001"),
    }
    assert all(plan.action is OrderAction.SELL for plan in plans)
    assert all(plan.estimated_cost == plan.quote.price * plan.quantity for plan in plans)


def test_cloud_sell_all_without_a_position_does_not_request_a_quote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-no-position",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="EDBL",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        cloud_signal_id="sell-all-no-position",
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _NoPositionAdapter(_OrderAdapter):
        def get_quote(self, _symbol: str) -> Quote:
            raise AssertionError("a zero-position target must not request a quote")

    adapter = _NoPositionAdapter()
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.batches == ()
    assert result.errors == ()
    assert result.coverage.fully_confirmed_zero
    assert result.coverage.confirmed_zero_targets == 1


def test_cloud_sell_all_corporate_action_is_terminal_during_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.FENNEL, "fennel-login")
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-corporate-action",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="GNPX",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        cloud_signal_id="sell-all-corporate-action",
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _CorporateActionAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=BrokerId.FENNEL,
                    login_id=profile.login_id,
                    account_id=profile.account_selectors[0],
                    masked_account_id="fennel-1",
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=BrokerId.FENNEL,
                    account_id=account_id,
                    symbol="GNPX",
                    quantity=Decimal("1"),
                )
            ]

        def get_quote(self, _symbol: str) -> Quote:
            raise executor.ExecutionBlockedError(
                "GNPX is temporarily paused for a corporate action"
            )

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: _CorporateActionAdapter(),
    )
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.batches == ()
    assert result.coverage.permanent_failed_targets == 1
    assert result.coverage.retryable_failed_targets == 0


def test_cloud_sell_all_connects_every_login_for_the_same_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _profile(BrokerId.PUBLIC, "public-one")
    second = _profile(BrokerId.PUBLIC, "public-two")
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-logins",
        selection=CustomOrderSelection(
            targets=(
                CustomOrderTarget(profile=first, account_ids=first.account_selectors),
                CustomOrderTarget(profile=second, account_ids=second.account_selectors),
            ),
            symbol="MULTI",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _LoginPositionAdapter(_OrderAdapter):
        def __init__(self, profile: BrokerLoginProfile, quantity: Decimal) -> None:
            super().__init__()
            self.profile = profile
            self.quantity = quantity

        def list_accounts(self) -> list[BrokerAccount]:
            account_id = self.profile.account_selectors[0]
            return [
                BrokerAccount(
                    broker_id=self.profile.broker_id,
                    login_id=self.profile.login_id,
                    account_id=account_id,
                    masked_account_id=account_id,
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=self.profile.broker_id,
                    account_id=account_id,
                    symbol="MULTI",
                    quantity=self.quantity,
                )
            ]

    adapters = {
        first.login_id: _LoginPositionAdapter(first, Decimal("0.33")),
        second.login_id: _LoginPositionAdapter(second, Decimal("0.44")),
    }

    def create(_broker_id: BrokerId, **kwargs: object) -> _LoginPositionAdapter:
        profile = kwargs["login_profile"]
        assert isinstance(profile, BrokerLoginProfile)
        return adapters[profile.login_id]

    monkeypatch.setattr(execute_page, "create_adapter", create)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.errors == ()
    assert {batch.profile.login_id: batch.plans[0].quantity for batch in result.batches} == {
        "public-one": Decimal("0.33"),
        "public-two": Decimal("0.44"),
    }
    assert all(adapter.connected for adapter in adapters.values())
    assert all(adapter.disconnected for adapter in adapters.values())


def test_cloud_sell_all_broken_first_login_does_not_block_healthy_same_broker_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = _profile(BrokerId.PUBLIC, "public-broken")
    healthy = _profile(BrokerId.PUBLIC, "public-healthy")
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-isolated-logins",
        selection=CustomOrderSelection(
            targets=(
                CustomOrderTarget(profile=broken, account_ids=broken.account_selectors),
                CustomOrderTarget(profile=healthy, account_ids=healthy.account_selectors),
            ),
            symbol="ISOL",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _BrokenAdapter(_OrderAdapter):
        def connect(self) -> None:
            raise BrokerNotConfiguredError("temporary login unavailable")

    class _HealthyAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=healthy.broker_id,
                    login_id=healthy.login_id,
                    account_id=healthy.account_selectors[0],
                    masked_account_id="xxxx5678",
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=healthy.broker_id,
                    account_id=account_id,
                    symbol="ISOL",
                    quantity=Decimal("0.33"),
                )
            ]

    adapters: dict[str, _OrderAdapter] = {
        broken.login_id: _BrokenAdapter(),
        healthy.login_id: _HealthyAdapter(),
    }

    def create(_broker_id: BrokerId, **kwargs: object) -> _OrderAdapter:
        profile = kwargs["login_profile"]
        assert isinstance(profile, BrokerLoginProfile)
        return adapters[profile.login_id]

    monkeypatch.setattr(execute_page, "create_adapter", create)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    plans = tuple(plan for batch in result.batches for plan in batch.plans)
    assert len(plans) == 1
    assert plans[0].account_id == healthy.account_selectors[0]
    assert plans[0].quantity == Decimal("0.33")
    assert result.coverage.ready_targets == 1
    assert result.coverage.retryable_failed_targets == 1
    assert result.retry_targets == (
        CloudRetryTarget(
            broker_id=broken.broker_id,
            login_id=broken.login_id,
            account_ids=broken.account_selectors,
        ),
    )
    assert len(result.errors) == 1
    assert broken.label in result.errors[0]
    assert adapters[healthy.login_id].connected
    assert adapters[healthy.login_id].disconnected


def test_cloud_sell_all_deduplicates_same_real_account_across_login_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _profile(BrokerId.PUBLIC, "public-one").model_copy(
        update={"account_selectors": ("6789",), "label": "Public One"}
    )
    second = _profile(BrokerId.PUBLIC, "public-two").model_copy(
        update={"account_selectors": ("6789",), "label": "Public Two"}
    )
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-duplicate-account",
        selection=CustomOrderSelection(
            targets=(
                CustomOrderTarget(profile=first, account_ids=first.account_selectors),
                CustomOrderTarget(profile=second, account_ids=second.account_selectors),
            ),
            symbol="DUPL",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        event_id="cloud-signal:duplicate-account",
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _DuplicateAccountAdapter(_OrderAdapter):
        def __init__(self, profile: BrokerLoginProfile) -> None:
            super().__init__()
            self.profile = profile

        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=self.profile.broker_id,
                    login_id=self.profile.login_id,
                    account_id="canonical-public-account",
                    masked_account_id="xxxxx6789",
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            assert account_id == "canonical-public-account"
            return [
                Position(
                    broker_id=self.profile.broker_id,
                    account_id=account_id,
                    symbol="DUPL",
                    quantity=Decimal("0.33"),
                )
            ]

    adapters = {
        first.login_id: _DuplicateAccountAdapter(first),
        second.login_id: _DuplicateAccountAdapter(second),
    }

    def create(_broker_id: BrokerId, **kwargs: object) -> _DuplicateAccountAdapter:
        profile = kwargs["login_profile"]
        assert isinstance(profile, BrokerLoginProfile)
        return adapters[profile.login_id]

    monkeypatch.setattr(execute_page, "create_adapter", create)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    plans = tuple(plan for batch in result.batches for plan in batch.plans)
    assert len(plans) == 1
    assert plans[0].account_id == "canonical-public-account"
    assert result.errors == ()
    assert result.coverage.ready_targets == 2
    assert result.coverage.retryable_failed_targets == 0


def test_cloud_sell_all_reports_unsupported_chase_fractional_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.CHASE, "chase-login")
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-chase-fractional",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="FRAC",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _ChaseFractionalAdapter(_OrderAdapter):
        metadata = SimpleNamespace(
            display_name="Chase",
            fractional_sell_supported=False,
        )

        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=BrokerId.CHASE,
                    login_id=profile.login_id,
                    account_id=profile.account_selectors[0],
                    masked_account_id="xxxx1234",
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=BrokerId.CHASE,
                    account_id=account_id,
                    symbol="FRAC",
                    quantity=Decimal("0.33"),
                )
            ]

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: _ChaseFractionalAdapter(),
    )
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.batches == ()
    assert any(
        "Chase does not support fractional-share liquidation" in error for error in result.errors
    )
    assert result.coverage.confirmed_zero_targets == 0
    assert result.coverage.retryable_failed_targets == 0
    assert result.coverage.permanent_failed_targets == 1
    assert not result.coverage.fully_confirmed_zero
    assert result.coverage.terminal_without_orders


def test_cloud_sell_all_restricted_account_with_position_is_not_confirmed_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.PUBLIC, "public-restricted")
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-restricted",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="LOCK",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _RestrictedAccountAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=profile.broker_id,
                    login_id=profile.login_id,
                    account_id=profile.account_selectors[0],
                    masked_account_id="xxxx1234",
                    account_type="brokerage",
                    tradable=False,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=profile.broker_id,
                    account_id=account_id,
                    symbol="LOCK",
                    quantity=Decimal("0.33"),
                )
            ]

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: _RestrictedAccountAdapter(),
    )
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert result.batches == ()
    assert result.coverage.confirmed_zero_targets == 0
    assert result.coverage.retryable_failed_targets == 0
    assert result.coverage.permanent_failed_targets == 1
    assert result.coverage.terminal_without_orders
    assert any("not eligible for liquidation" in error for error in result.errors)


def test_cloud_sell_all_keeps_eligible_account_when_same_login_has_permanent_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.CHASE, "chase-mixed").model_copy(
        update={"account_selectors": ("whole-account", "fractional-account")}
    )
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-mixed-capability",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="MIXC",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _MixedCapabilityAdapter(_OrderAdapter):
        metadata = SimpleNamespace(
            display_name="Chase",
            fractional_sell_supported=False,
        )

        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=profile.broker_id,
                    login_id=profile.login_id,
                    account_id=account_id,
                    masked_account_id=account_id,
                    account_type="brokerage",
                    tradable=True,
                )
                for account_id in profile.account_selectors
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            quantity = Decimal("1") if account_id == "whole-account" else Decimal("0.33")
            return [
                Position(
                    broker_id=profile.broker_id,
                    account_id=account_id,
                    symbol="MIXC",
                    quantity=quantity,
                )
            ]

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: _MixedCapabilityAdapter(),
    )
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    plans = tuple(plan for batch in result.batches for plan in batch.plans)
    assert [(plan.account_id, plan.quantity) for plan in plans] == [("whole-account", Decimal("1"))]
    assert result.coverage.ready_targets == 1
    assert result.coverage.confirmed_zero_targets == 0
    assert result.coverage.retryable_failed_targets == 0
    assert result.coverage.permanent_failed_targets == 1
    assert any("fractional-account" in error for error in result.errors)


def test_cloud_sell_all_allows_robinhood_closing_only_account() -> None:
    profile = _profile(BrokerId.ROBINHOOD, "robinhood-login")
    order = execute_page.QueuedCustomOrder(
        queue_id="sell-all-closing-only",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="CLOSE",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _ClosingOnlyAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=BrokerId.ROBINHOOD,
                    login_id=profile.login_id,
                    account_id=profile.account_selectors[0],
                    masked_account_id="xxxx1234",
                    account_type="brokerage",
                    tradable=False,
                    closing_only=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=BrokerId.ROBINHOOD,
                    account_id=account_id,
                    symbol="CLOSE",
                    quantity=Decimal("0.33"),
                )
            ]

    result = execute_page._build_sell_all_order_plans(
        adapter=_ClosingOnlyAdapter(),
        order=order,
        target=order.selection.targets[0],
        quote=Quote(
            symbol="CLOSE",
            price=Decimal("2"),
            as_of=datetime.now(UTC),
        ),
    )

    assert result.plans[0].quantity == Decimal("0.33")
    assert result.permanent_errors == ()


def test_cloud_sell_broker_scope_filters_targets_without_changing_quantity() -> None:
    public = _profile(BrokerId.PUBLIC, "public-login")
    robinhood = _profile(BrokerId.ROBINHOOD, "robinhood-login")

    selection = execute_page._cloud_selection_for_profiles(
        [public, robinhood],
        symbol="SCOPE",
        quantity=Decimal("1"),
        broker=BrokerId.ROBINHOOD,
    )

    assert selection is not None
    assert [target.profile.broker_id for target in selection.targets] == [BrokerId.ROBINHOOD]
    assert selection.quantity == Decimal("1")


def test_cloud_multi_broker_scope_and_buy_exclusions_filter_targets() -> None:
    public = _profile(BrokerId.PUBLIC, "public-login")
    robinhood = _profile(BrokerId.ROBINHOOD, "robinhood-login")
    sofi = _profile(BrokerId.SOFI, "sofi-login")

    selected = execute_page._cloud_selection_for_profiles(
        [public, robinhood, sofi],
        symbol="SCOPE",
        quantity=Decimal("1"),
        broker=None,
        brokers=(BrokerId.PUBLIC, BrokerId.ROBINHOOD),
    )
    excluded = execute_page._cloud_selection_for_profiles(
        [public, robinhood, sofi],
        symbol="SCOPE",
        quantity=Decimal("1"),
        broker=None,
        excluded_brokers=(BrokerId.SOFI, BrokerId.ROBINHOOD),
    )

    assert selected is not None
    assert [target.profile.broker_id for target in selected.targets] == [
        BrokerId.PUBLIC,
        BrokerId.ROBINHOOD,
    ]
    assert excluded is not None
    assert [target.profile.broker_id for target in excluded.targets] == [BrokerId.PUBLIC]


def test_admin_sell_all_preview_skips_without_a_local_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    queued = execute_page.QueuedCustomOrder(
        queue_id="sell-all-draft",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="REMOTE",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        cloud_signal_id="sell-all-draft",
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _NoPositionAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=profile.broker_id,
                    login_id=profile.login_id,
                    account_id=profile.account_selectors[0],
                    masked_account_id="***1",
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, _account_id: str) -> list[Position]:
            return []

        def get_quote(self, _symbol: str) -> Quote:
            raise AssertionError("a zero-position draft must not request a quote")

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: _NoPositionAdapter(),
    )
    queue: Queue[PreviewQueueItem] = Queue()
    execute_page._collect_order_previews(queue, (queued,))
    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    draft = AdminDraft.model_validate(
        {
            "id": "sell-all-draft",
            "action": "SELL",
            "symbol": "REMOTE",
            "sell_all": True,
            "deadline_date": "2026-07-25",
            "created_at": "2026-07-24T12:00:00Z",
            "expires_at": "2026-07-31T12:00:00Z",
            "status": "awaiting_preview",
            "preview": None,
        }
    )

    preview = execute_page._admin_preview_from_result(draft, result)

    assert result.batches == ()
    assert result.quotes == ()
    assert result.coverage.fully_confirmed_zero
    assert result.coverage.confirmed_zero_targets == 1
    assert result.coverage.retryable_failed_targets == 0
    assert preview is None


def test_admin_confirmed_zero_emits_per_user_skip(qtbot) -> None:  # type: ignore[no-untyped-def]
    page = execute_page.ExecutePage()
    qtbot.addWidget(page)
    draft = AdminDraft.model_validate(
        {
            "id": "sell-all-draft",
            "action": "SELL",
            "symbol": "MSGY",
            "sell_all": True,
            "deadline_date": "2099-01-02",
            "created_at": "2026-07-24T12:00:00Z",
            "expires_at": "2099-01-03T12:00:00Z",
            "status": "awaiting_preview",
        }
    )
    skipped: list[object] = []
    deferred: list[object] = []
    page.admin_preview_skipped.connect(skipped.append)
    page.admin_preview_deferred.connect(lambda *_args: deferred.append(object()))
    queue: Queue[PreviewQueueItem] = Queue()
    queue.put(
        OrderPreviewResult(
            batches=(),
            queue_ids=frozenset({draft.draft_id}),
            errors=(),
            coverage=PreparationCoverage(confirmed_zero_targets=1),
        )
    )
    page._preview_queue = queue
    page._preview_cancel = Event()
    page._preview_context = draft

    page._poll_preview_progress()

    assert skipped == [draft]
    assert deferred == []


def test_cash_in_lieu_preview_uses_fractional_position_value() -> None:
    profile = _profile()
    quote = Quote(symbol="WETO", price=Decimal("5"), as_of=datetime.now(UTC))
    plan = execute_page._build_custom_order_plan(
        event_id="cash-in-lieu:test",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=quote,
        quantity=Decimal("0.2"),
        action=OrderAction.SELL,
    )
    draft = AdminDraft.model_validate(
        {
            "id": "cash-in-lieu-test",
            "action": "SELL",
            "symbol": "WETO",
            "sell_all": True,
            "cash_in_lieu": True,
            "deadline_date": "2026-07-31",
            "created_at": "2026-07-30T12:00:00Z",
            "expires_at": "2026-08-01T12:00:00Z",
            "status": "awaiting_preview",
            "preview": None,
        }
    )

    preview = execute_page._admin_preview_from_result(
        draft,
        OrderPreviewResult(
            batches=(LiveOrderBatch(profile=profile, plans=(plan,)),),
            queue_ids=frozenset({"cash-in-lieu-test"}),
            errors=(),
        ),
    )

    assert preview is not None
    assert preview.submission.unit_price == Decimal("5")
    assert preview.submission.cash_in_lieu_value == Decimal("1.0")
    assert (
        execute_page._admin_preview_from_result(
            draft,
            OrderPreviewResult(
                batches=(),
                queue_ids=frozenset({"cash-in-lieu-test"}),
                errors=(),
                quotes=((BrokerId.PUBLIC, quote),),
            ),
        )
        is None
    )


def test_cash_in_lieu_preview_rounds_fractional_aggregate_to_model_precision() -> None:
    profile = _profile()
    quote = Quote(
        symbol="LEXX",
        price=Decimal("0.4099"),
        as_of=datetime.now(UTC) - timedelta(hours=2),
    )
    quantities = (
        Decimal("0.06666700"),
        Decimal("0.06666700"),
        Decimal("0.06666700"),
        Decimal("0.06666"),
        Decimal("0.06666"),
    )
    plans = tuple(
        execute_page._build_custom_order_plan(
            event_id=f"cash-in-lieu:lexx:{index}",
            profile=profile,
            account_id=f"account-{index}",
            quote=quote,
            quantity=quantity,
            action=OrderAction.SELL,
        )
        for index, quantity in enumerate(quantities)
    )
    draft = AdminDraft.model_validate(
        {
            "id": "cash-in-lieu-lexx",
            "action": "SELL",
            "symbol": "LEXX",
            "sell_all": True,
            "cash_in_lieu": True,
            "deadline_date": "2026-08-06",
            "created_at": "2026-08-05T12:00:00Z",
            "expires_at": "2026-08-07T12:00:00Z",
            "status": "awaiting_preview",
            "preview": None,
        }
    )

    preview_started_at = datetime.now(UTC)
    preview = execute_page._admin_preview_from_result(
        draft,
        OrderPreviewResult(
            batches=(LiveOrderBatch(profile=profile, plans=plans),),
            queue_ids=frozenset({"cash-in-lieu-lexx"}),
            errors=(),
        ),
    )

    assert preview is not None
    assert preview.submission.cash_in_lieu_value == Decimal("0.13662828")
    assert preview.submission.quoted_at >= preview_started_at


def test_sell_all_preview_structures_partial_ready_and_retryable_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = _profile(BrokerId.PUBLIC, "public-ready")
    unavailable = _profile(BrokerId.PUBLIC, "public-unavailable")
    queued = execute_page.QueuedCustomOrder(
        queue_id="partial-sell-all",
        selection=CustomOrderSelection(
            targets=(
                CustomOrderTarget(profile=ready, account_ids=ready.account_selectors),
                CustomOrderTarget(
                    profile=unavailable,
                    account_ids=unavailable.account_selectors,
                ),
            ),
            symbol="PART",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
        event_id="cloud-signal:partial-sell-all",
        action=OrderAction.SELL,
        sell_all=True,
    )

    class _ReadyAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            account_id = ready.account_selectors[0]
            return [
                BrokerAccount(
                    broker_id=ready.broker_id,
                    login_id=ready.login_id,
                    account_id=account_id,
                    masked_account_id=account_id,
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=ready.broker_id,
                    account_id=account_id,
                    symbol="PART",
                    quantity=Decimal("0.33"),
                )
            ]

    class _UnavailableAdapter(_OrderAdapter):
        def connect(self) -> None:
            raise BrokerNotConfiguredError("temporary login unavailable")

    adapters = {
        ready.login_id: _ReadyAdapter(),
        unavailable.login_id: _UnavailableAdapter(),
    }

    def create(_broker_id: BrokerId, **kwargs: object) -> _OrderAdapter:
        profile = kwargs["login_profile"]
        assert isinstance(profile, BrokerLoginProfile)
        return adapters[profile.login_id]

    monkeypatch.setattr(execute_page, "create_adapter", create)
    queue: Queue[PreviewQueueItem] = Queue()

    execute_page._collect_order_previews(queue, (queued,))

    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))
    assert len(result.batches) == 1
    assert result.batches[0].plans[0].quantity == Decimal("0.33")
    assert result.coverage.ready_targets == 1
    assert result.coverage.confirmed_zero_targets == 0
    assert result.coverage.retryable_failed_targets == 1
    assert result.retry_targets == (
        CloudRetryTarget(
            broker_id=unavailable.broker_id,
            login_id=unavailable.login_id,
            account_ids=unavailable.account_selectors,
        ),
    )
    assert len(result.errors) == 1


def test_admin_preview_never_uploads_raw_broker_error_text() -> None:
    draft = AdminDraft.model_validate(
        {
            "id": "draft-1",
            "action": "BUY",
            "symbol": "SAFE",
            "shares": 1,
            "deadline_date": "2026-07-24",
            "created_at": "2026-07-18T12:00:00Z",
            "expires_at": "2026-07-25T04:00:00Z",
            "status": "awaiting_preview",
            "preview": None,
        }
    )
    raw_error = "public login user@example.com account 123456789 session expired"
    preview = execute_page._admin_preview_from_result(
        draft,
        OrderPreviewResult(batches=(), queue_ids=frozenset(), errors=(raw_error,)),
    )

    assert preview is None
    assert execute_page._generic_broker_preview_reason(raw_error) is (
        BrokerPreviewReason.SESSION_EXPIRED
    )


def test_preview_checks_each_login_per_broker_then_expands_to_all_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _profile(BrokerId.PUBLIC, "public-one").model_copy(
        update={"account_selectors": ("account-1", "account-2")}
    )
    second = _profile(BrokerId.PUBLIC, "public-two").model_copy(
        update={"account_selectors": ("account-3",)}
    )
    selection = CustomOrderSelection(
        targets=(
            CustomOrderTarget(profile=first, account_ids=first.account_selectors),
            CustomOrderTarget(profile=second, account_ids=second.account_selectors),
        ),
        symbol="ONE",
        quantity=Decimal("1"),
    )
    queued = execute_page.QueuedCustomOrder(
        queue_id="queue-one-broker",
        selection=selection,
        created_at=datetime.now(UTC),
    )
    adapter = _OrderAdapter()
    profiles_used: list[str] = []

    def create(_broker_id: BrokerId, **kwargs: object) -> _OrderAdapter:
        profiles_used.append(kwargs["login_profile"].login_id)  # type: ignore[union-attr]
        return adapter

    monkeypatch.setattr(execute_page, "create_adapter", create)
    queue: Queue[PreviewQueueItem] = Queue()
    execute_page._collect_order_previews(queue, (queued,))
    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))

    assert profiles_used == ["public-one", "public-two"]
    assert adapter.quote_calls == 2
    assert {plan.account_id for batch in result.batches for plan in batch.plans} == {
        "account-1",
        "account-2",
        "account-3",
    }


def test_cloud_retry_selection_keeps_only_failed_accounts() -> None:
    first = _profile(BrokerId.PUBLIC, "public-one").model_copy(
        update={"account_selectors": ("account-1", "account-2")}
    )
    second = _profile(BrokerId.ROBINHOOD, "robinhood-one")
    selection = CustomOrderSelection(
        targets=(
            CustomOrderTarget(profile=first, account_ids=first.account_selectors),
            CustomOrderTarget(profile=second, account_ids=second.account_selectors),
        ),
        symbol="ONE",
        quantity=Decimal("1"),
    )

    retry = _restrict_cloud_selection(
        selection,
        (
            CloudRetryTarget(
                broker_id=first.broker_id,
                login_id=first.login_id,
                account_ids=("account-2",),
            ),
        ),
    )

    assert len(retry.targets) == 1
    assert retry.targets[0].profile == first
    assert retry.targets[0].account_ids == ("account-2",)


def test_preview_skips_unavailable_broker_but_keeps_available_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public = _profile(BrokerId.PUBLIC, "public-login")
    robinhood = _profile(BrokerId.ROBINHOOD, "robinhood-login")
    selection = CustomOrderSelection(
        targets=(
            CustomOrderTarget(profile=public, account_ids=public.account_selectors),
            CustomOrderTarget(profile=robinhood, account_ids=robinhood.account_selectors),
        ),
        symbol="MIXED",
        quantity=Decimal("1"),
    )
    queued = execute_page.QueuedCustomOrder(
        queue_id="queue-mixed",
        selection=selection,
        created_at=datetime.now(UTC),
    )
    available = _OrderAdapter()

    class _UnavailableAdapter(_OrderAdapter):
        def connect(self) -> None:
            raise BrokerNotConfiguredError("trading support is unavailable")

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda broker_id, **_kwargs: (
            available if broker_id is BrokerId.PUBLIC else _UnavailableAdapter()
        ),
    )
    queue: Queue[PreviewQueueItem] = Queue()
    execute_page._collect_order_previews(queue, (queued,))
    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))

    assert [batch.profile.broker_id for batch in result.batches] == [BrokerId.PUBLIC]
    assert result.queue_ids == {"queue-mixed"}
    assert len(result.errors) == 1
    assert "robinhood" in result.errors[0].casefold()
    assert result.coverage.retryable_failed_targets == 1


def test_preview_automatically_retries_expired_headless_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.CHASE, "chase-login")
    queued = execute_page.QueuedCustomOrder(
        queue_id="queue-relogin",
        selection=CustomOrderSelection(
            targets=(CustomOrderTarget(profile=profile, account_ids=profile.account_selectors),),
            symbol="LOGIN",
            quantity=Decimal("1"),
        ),
        created_at=datetime.now(UTC),
    )
    recovered = _OrderAdapter()
    force_modes: list[bool] = []

    class _ExpiredAdapter(_OrderAdapter):
        def connect(self) -> None:
            raise BrokerNotConfiguredError("Chase session is expired")

    def create(_broker_id: BrokerId, **kwargs: object) -> _OrderAdapter:
        force = bool(kwargs.get("force_interactive_login"))
        force_modes.append(force)
        return recovered if force else _ExpiredAdapter()

    monkeypatch.setattr(execute_page, "create_adapter", create)
    queue: Queue[PreviewQueueItem] = Queue()
    execute_page._collect_order_previews(queue, (queued,))
    result = next(item for item in queue.queue if isinstance(item, OrderPreviewResult))

    assert force_modes == [False, True]
    assert result.errors == ()
    assert result.batches[0].profile.broker_id is BrokerId.CHASE


def test_live_order_worker_records_history_and_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    plan = execute_page._build_custom_order_plan(
        event_id="custom-order:test",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=Quote(symbol="MANL", price=Decimal("1.25"), as_of=datetime.now(UTC)),
        quantity=Decimal("1"),
    )
    adapter = _OrderAdapter()
    history: list[object] = []
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    monkeypatch.setattr(execute_page, "connect", lambda: nullcontext(None))
    monkeypatch.setattr(execute_page, "migrate", lambda _connection: None)
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    monkeypatch.setattr(
        execute_page,
        "DatabaseOrderExecutionGuard",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        execute_page,
        "TradePortfolioObservationWriter",
        lambda _connection: SimpleNamespace(persist=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        execute_page,
        "PortfolioOrderHistoryRepository",
        lambda _connection: SimpleNamespace(record=history.append),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_orders(queue, profile, (plan,))
    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    progress = [item for item in queue.queue if isinstance(item, TaskProgressUpdate)]
    assert result.submitted == 1
    assert len(result.executions) == 1
    assert result.executions[0].state == "SUBMITTED"
    assert result.executions[0].filled_quantity == 0
    assert history[0].unit_price == Decimal("1.10")
    assert progress[-1].current == progress[-1].total == 1


def test_live_order_connection_failure_returns_account_scoped_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile().model_copy(update={"account_selectors": ("account-1", "account-2")})
    plans = tuple(
        execute_page._build_custom_order_plan(
            event_id="cloud-signal:retry",
            profile=profile,
            account_id=account_id,
            quote=Quote(symbol="RETRY", price=Decimal("1.25"), as_of=datetime.now(UTC)),
            quantity=Decimal("1"),
        )
        for account_id in profile.account_selectors
    )

    class _UnavailableAdapter(_OrderAdapter):
        def connect(self) -> None:
            raise BrokerNotConfiguredError("temporary login unavailable")

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: _UnavailableAdapter(),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()

    execute_page._collect_live_orders(queue, profile, plans)

    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    assert result.failed == 2
    assert result.retry_targets == (
        CloudRetryTarget(
            broker_id=profile.broker_id,
            login_id=profile.login_id,
            account_ids=profile.account_selectors,
        ),
    )


def test_live_order_batch_watchdog_gives_up_on_a_hung_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    plans = (
        execute_page._build_custom_order_plan(
            event_id="custom-order:hang",
            profile=profile,
            account_id=profile.account_selectors[0],
            quote=Quote(symbol="HANG", price=Decimal("1.25"), as_of=datetime.now(UTC)),
            quantity=Decimal("1"),
        ),
    )
    started = Event()

    def hang(
        _queue: object, _profile: object, _plans: object, _authorization: object = None
    ) -> None:
        started.set()
        Event().wait()  # never set: simulates a call with no timeout of its own

    monkeypatch.setattr(execute_page, "_collect_live_orders", hang)
    monkeypatch.setattr(execute_page, "_LIVE_ORDER_BATCH_TIMEOUT_SECONDS", 0.05)

    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._run_live_order_batch_with_watchdog(queue, profile, plans, None)

    assert started.is_set()
    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    assert result.failed == 1
    assert result.submitted == 0
    assert result.retry_targets == (
        CloudRetryTarget(
            broker_id=profile.broker_id,
            login_id=profile.login_id,
            account_ids=profile.account_selectors,
        ),
    )
    assert "did not respond" in result.skipped[0]


def test_live_order_batch_watchdog_resets_after_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    plans = (
        execute_page._build_custom_order_plan(
            event_id="custom-order:slow-progress",
            profile=profile,
            account_id=profile.account_selectors[0],
            quote=Quote(symbol="SLOW", price=Decimal("1.25"), as_of=datetime.now(UTC)),
            quantity=Decimal("1"),
        ),
    )

    def slow_after_progress(
        queue: Queue[LiveOrderQueueItem],
        _profile: BrokerLoginProfile,
        _plans: tuple[object, ...],
        _authorization: object = None,
    ) -> None:
        sleep(0.04)
        queue.put(TaskProgressUpdate(message="Processed 1 of 1 orders", current=1, total=1))
        sleep(0.04)
        queue.put(LiveOrderResult(submitted=1, skipped=(), popup_skips=()))

    monkeypatch.setattr(execute_page, "_collect_live_orders", slow_after_progress)
    monkeypatch.setattr(execute_page, "_LIVE_ORDER_BATCH_TIMEOUT_SECONDS", 0.05)

    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._run_live_order_batch_with_watchdog(queue, profile, plans, None)

    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    assert result.submitted == 1
    assert result.failed == 0


def test_collect_live_order_batches_continues_past_a_hung_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stuck = _profile(BrokerId.CHASE, "chase-login")
    healthy = _profile()
    now = datetime.now(UTC)

    def batch(profile: BrokerLoginProfile, symbol: str) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=(
                execute_page._build_custom_order_plan(
                    event_id=f"custom-order:{symbol}",
                    profile=profile,
                    account_id=profile.account_selectors[0],
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=now),
                    quantity=Decimal("1"),
                ),
            ),
        )

    real_collect_live_orders = execute_page._collect_live_orders

    def maybe_hang(
        queue: Queue[LiveOrderQueueItem],
        profile: BrokerLoginProfile,
        plans: object,
        authorization: object = None,
    ) -> None:
        if profile.login_id == stuck.login_id:
            Event().wait()  # never set: this login never responds
            return
        real_collect_live_orders(queue, profile, plans, authorization)

    monkeypatch.setattr(execute_page, "_collect_live_orders", maybe_hang)
    monkeypatch.setattr(execute_page, "_LIVE_ORDER_BATCH_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: _OrderAdapter())
    monkeypatch.setattr(execute_page, "connect", lambda: nullcontext(None))
    monkeypatch.setattr(execute_page, "migrate", lambda _connection: None)
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    monkeypatch.setattr(execute_page, "DatabaseOrderExecutionGuard", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        execute_page,
        "TradePortfolioObservationWriter",
        lambda _connection: SimpleNamespace(persist=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        execute_page,
        "PortfolioOrderHistoryRepository",
        lambda _connection: SimpleNamespace(record=lambda *_args, **_kwargs: None),
    )

    queue: Queue[LiveOrderQueueItem] = Queue()
    # The whole point under test: this call must return in bounded time even
    # though the Chase login never responds -- it must not hang forever, and
    # it must still process the healthy Robinhood login behind it.
    execute_page._collect_live_order_batches(queue, (batch(stuck, "S1"), batch(healthy, "H1")))

    result = next(item for item in reversed(queue.queue) if isinstance(item, LiveOrderResult))
    assert result.submitted == 1
    assert result.failed == 1
    assert result.retry_targets == (
        CloudRetryTarget(
            broker_id=stuck.broker_id,
            login_id=stuck.login_id,
            account_ids=stuck.account_selectors,
        ),
    )


def test_collect_live_order_batches_skips_remaining_chase_after_unavailable_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chase_one = _profile(BrokerId.CHASE, "chase-one")
    chase_two = _profile(BrokerId.CHASE, "chase-two")
    healthy = _profile()
    now = datetime.now(UTC)

    def batch(profile: BrokerLoginProfile, symbol: str) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=(
                execute_page._build_custom_order_plan(
                    event_id=f"custom-order:{symbol}",
                    profile=profile,
                    account_id=profile.account_selectors[0],
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=now),
                    quantity=Decimal("1"),
                ),
            ),
        )

    calls: list[str] = []

    def collect(
        queue: Queue[LiveOrderQueueItem],
        profile: BrokerLoginProfile,
        plans: tuple[object, ...],
        _authorization: object = None,
    ) -> None:
        calls.append(profile.login_id)
        if profile.broker_id is BrokerId.CHASE and profile.login_id == chase_one.login_id:
            queue.put(
                LiveOrderResult(
                    submitted=0,
                    skipped=("TE: stock unavailable to trade",),
                    popup_skips=("TE: stock unavailable to trade",),
                    terminal_failures=1,
                    skip_broker=BrokerId.CHASE,
                    skip_broker_reason=(
                        "Chase reported a stock unavailable to trade; remaining Chase orders "
                        "were skipped for this signal."
                    ),
                )
            )
        else:
            queue.put(LiveOrderResult(submitted=len(plans), skipped=(), popup_skips=()))

    monkeypatch.setattr(execute_page, "_collect_live_orders", collect)
    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_order_batches(
        queue,
        (batch(healthy, "H1"), batch(chase_one, "C1"), batch(chase_two, "C2")),
    )

    result = next(item for item in reversed(queue.queue) if isinstance(item, LiveOrderResult))
    assert calls == [chase_one.login_id, healthy.login_id]
    assert result.submitted == 1
    assert result.terminal_failures == 2
    assert result.retry_targets == ()
    assert any("C2" in reason and "remaining Chase orders" in reason for reason in result.skipped)


def test_chase_unavailable_to_trade_detection_is_limited_to_security_rejections() -> None:
    assert execute_page._is_chase_stock_unavailable_rejection(
        "Chase order rejected: Security TEST is unavailable to trade"
    )
    assert not execute_page._is_chase_stock_unavailable_rejection(
        "Chase order rejected: account is unavailable to trade"
    )


def test_sofi_security_skip_is_terminal_even_after_batch_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    first = _profile(BrokerId.SOFI, "sofi-first")
    mixed = _profile(BrokerId.SOFI, "sofi-mixed")
    last = _profile(BrokerId.SOFI, "sofi-last")
    healthy = _profile()
    reason = "SoFi cannot trade NCT; remaining SoFi orders for this ticker were skipped."

    def batch(profile: BrokerLoginProfile, symbols: tuple[str, ...]) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=tuple(
                execute_page._build_custom_order_plan(
                    event_id=f"test:{symbol}",
                    profile=profile,
                    account_id=f"{profile.login_id}-{symbol}",
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=datetime.now(UTC)),
                    quantity=Decimal("1"),
                )
                for symbol in symbols
            ),
        )

    def collect(queue: object, profile: BrokerLoginProfile, *_args: object) -> None:
        assert profile.login_id == first.login_id
        queue.put(
            LiveOrderResult(
                submitted=0,
                skipped=(reason,),
                popup_skips=(),
                terminal_failures=1,
                symbol_skip_reasons=(("NCT", reason),),
            )
        )
        now[0] = 20.0

    monkeypatch.setattr(execute_page, "monotonic", lambda: now[0])
    monkeypatch.setattr(execute_page, "_LIVE_ORDER_TOTAL_TIMEOUT_SECONDS", 10.0)
    monkeypatch.setattr(execute_page, "_run_live_order_batch_with_watchdog", collect)
    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_order_batches(
        queue,
        (
            batch(first, ("NCT",)),
            batch(mixed, ("NCT", "IBO")),
            batch(last, ("NCT",)),
            batch(healthy, ("NCT",)),
        ),
    )
    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    assert result.terminal_failures == 3
    assert result.failed == 2
    assert {(target.login_id, target.account_ids) for target in result.retry_targets} == {
        (mixed.login_id, (f"{mixed.login_id}-IBO",)),
        (healthy.login_id, (f"{healthy.login_id}-NCT",)),
    }


def test_collect_live_order_batches_stops_starting_new_logins_past_the_total_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _profile(BrokerId.CHASE, "chase-login")
    second = _profile(BrokerId.ARIES, "aries-login")
    third = _profile(BrokerId.FENNEL, "fennel-login")
    now = datetime.now(UTC)

    def batch(profile: BrokerLoginProfile, symbol: str) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=(
                execute_page._build_custom_order_plan(
                    event_id=f"custom-order:{symbol}",
                    profile=profile,
                    account_id=profile.account_selectors[0],
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=now),
                    quantity=Decimal("1"),
                ),
            ),
        )

    def slow_but_healthy(
        queue: Queue[LiveOrderQueueItem],
        profile: BrokerLoginProfile,
        plans: object,
        authorization: object = None,
    ) -> None:
        # Real elapsed time, not mocked: the deadline check in
        # _collect_live_order_batches reads the real clock, so the first
        # login has to actually take longer than the deadline for it to have
        # passed by the time the second login is considered.
        sleep(0.25)
        queue.put(LiveOrderResult(submitted=1, skipped=(), popup_skips=()))

    monkeypatch.setattr(execute_page, "_collect_live_orders", slow_but_healthy)
    monkeypatch.setattr(execute_page, "_LIVE_ORDER_TOTAL_TIMEOUT_SECONDS", 0.15)
    monkeypatch.setattr(execute_page, "_LIVE_ORDER_BATCH_TIMEOUT_SECONDS", 60.0)

    queue: Queue[LiveOrderQueueItem] = Queue()
    # Chase sorts first under _EXECUTION_BROKER_ORDER, so it is the one login
    # that gets a real attempt before the deadline is judged to have passed.
    execute_page._collect_live_order_batches(
        queue, (batch(third, "T1"), batch(second, "S1"), batch(first, "F1"))
    )

    result = next(item for item in reversed(queue.queue) if isinstance(item, LiveOrderResult))
    assert result.submitted == 1
    assert result.failed == 2
    assert {target.login_id for target in result.retry_targets} == {
        second.login_id,
        third.login_id,
    }
    assert any("processing time ran out" in reason for reason in result.skipped)


def test_cloud_signal_completion_logs_failure_reasons_without_a_popup(
    qtbot,  # type: ignore[no-untyped-def]
) -> None:
    page = execute_page.ExecutePage()
    qtbot.addWidget(page)
    now = datetime.now(UTC)
    signal = PendingSignal(
        id="signal-1",
        action=OrderAction.SELL,
        symbol="AAPL",
        sell_all=True,
        deadline_date=date.today(),
        created_at=now,
        expires_at=now + timedelta(days=7),
        status="active",
    )
    preparation = CloudSignalPreparation(
        signal=signal,
        lease_token="l" * 48,
        batches=(),
        errors=(),
    )
    page._cloud_order_context = preparation
    page._order_queue = Queue()
    page._order_queue.put(
        LiveOrderResult(
            submitted=1,
            skipped=(
                "account-1 AAPL: insufficient balance",
                "account-2 AAPL: broker already holds a position in this symbol",
            ),
            popup_skips=("account-1 AAPL: insufficient balance",),
            failed=1,
        )
    )

    finished: list[object] = []
    logged: list[object] = []
    page.cloud_signal_finished.connect(finished.append)
    page.activity_logged.connect(logged.append)
    page._poll_order_progress()

    # No popup: processing proceeds automatically (cloud_signal_finished
    # fired), and the outcome is recorded to the Activity Log instead.
    assert len(finished) == 1
    assert page.findChildren(QMessageBox) == []
    assert len(logged) == 1
    entry = logged[0]
    assert isinstance(entry, ActivityLogEntry)
    assert entry.level == "warning"
    assert "AAPL" in entry.title
    # Uses item.skipped, not popup_skips: the benign "already holds" guardrail
    # skip is included so the log fully explains every account.
    assert any("insufficient balance" in detail for detail in entry.details)
    assert any("already holds a position" in detail for detail in entry.details)


def test_cloud_signal_check_logs_silently_skipped_brokers_without_a_popup(
    qtbot,  # type: ignore[no-untyped-def]
) -> None:
    page = execute_page.ExecutePage()
    qtbot.addWidget(page)
    now = datetime.now(UTC)
    signal = PendingSignal(
        id="signal-1",
        action=OrderAction.SELL,
        symbol="ARIP",
        sell_all=True,
        deadline_date=date.today(),
        created_at=now,
        expires_at=now + timedelta(days=7),
        status="active",
    )
    context = CloudSignalClaimContext(signal=signal, lease_token="l" * 48)
    healthy_profile = _profile()
    plan = execute_page._build_custom_order_plan(
        event_id="cloud-signal:1",
        profile=healthy_profile,
        account_id=healthy_profile.account_selectors[0],
        quote=Quote(symbol="ARIP", price=Decimal("1"), as_of=now),
        quantity=Decimal("1"),
    )
    queue: Queue[PreviewQueueItem] = Queue()
    queue.put(
        OrderPreviewResult(
            batches=(LiveOrderBatch(profile=healthy_profile, plans=(plan,)),),
            queue_ids=frozenset(),
            errors=(
                "ARIP — Aries(arip-1): broker skipped; "
                "Aries quote did not include a usable price",
            ),
            coverage=PreparationCoverage(ready_targets=1, retryable_failed_targets=1),
        )
    )
    page._preview_queue = queue
    page._preview_cancel = Event()
    page._preview_context = context

    prepared: list[object] = []
    logged: list[object] = []
    page.cloud_signal_prepared.connect(prepared.append)
    page.activity_logged.connect(logged.append)
    page._poll_preview_progress()

    # No popup: cloud_signal_prepared fired and the skip is logged instead.
    assert len(prepared) == 1
    assert page.findChildren(QMessageBox) == []
    assert len(logged) == 1
    entry = logged[0]
    assert isinstance(entry, ActivityLogEntry)
    assert entry.level == "warning"
    assert any("Aries" in detail for detail in entry.details)
    assert any("did not include a usable price" in detail for detail in entry.details)
    assert page.status_label.text() == "Cloud signal prices are ready; verifying authorization..."


def test_fennel_corporate_action_is_terminal_only_for_current_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(BrokerId.FENNEL, "fennel-login")
    plan = execute_page._build_custom_order_plan(
        event_id="cloud-signal:gnpx",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=Quote(symbol="GNPX", price=Decimal("6.30"), as_of=datetime.now(UTC)),
        quantity=Decimal("1"),
        action=OrderAction.SELL,
    )

    class CorporateActionAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            return [
                BrokerAccount(
                    broker_id=BrokerId.FENNEL,
                    login_id=profile.login_id,
                    account_id=profile.account_selectors[0],
                    masked_account_id="fennel-1",
                    account_type="brokerage",
                    tradable=True,
                )
            ]

        def get_positions(self, account_id: str) -> list[Position]:
            return [
                Position(
                    broker_id=BrokerId.FENNEL,
                    account_id=account_id,
                    symbol="GNPX",
                    quantity=Decimal("1"),
                )
            ]

        def preflight(self, _request: object, **_kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(
                accepted=False,
                reason="Fennel does not allow sell for GNPX: undergoing corporate action",
            )

    monkeypatch.setattr(
        execute_page,
        "create_adapter",
        lambda *_args, **_kwargs: CorporateActionAdapter(),
    )
    monkeypatch.setattr(execute_page, "connect", lambda: nullcontext(None))
    monkeypatch.setattr(execute_page, "migrate", lambda _connection: None)
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    monkeypatch.setattr(
        execute_page,
        "DatabaseOrderExecutionGuard",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        execute_page,
        "TradePortfolioObservationWriter",
        lambda _connection: SimpleNamespace(persist=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        execute_page,
        "PortfolioProtectedTickerRepository",
        lambda _connection: SimpleNamespace(list_all=lambda: set()),
    )
    monkeypatch.setattr(
        execute_page,
        "TrackedPlayRepository",
        lambda _connection: SimpleNamespace(find_position=lambda **_kwargs: None),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()

    execute_page._collect_live_orders(queue, profile, (plan,))

    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    assert result.submitted == 0
    assert result.failed == 0
    assert result.terminal_failures == 1


def _rejecting_adapter(reason: str) -> type[_OrderAdapter]:
    class RejectingAdapter(_OrderAdapter):
        def preflight(self, _request: object, **_kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(accepted=False, reason=reason, estimated_cost=None)

    return RejectingAdapter


def _run_single_buy_order(
    monkeypatch: pytest.MonkeyPatch,
    adapter_cls: type[_OrderAdapter],
) -> LiveOrderResult:
    profile = _profile()
    plan = execute_page._build_custom_order_plan(
        event_id="cloud-signal:reject",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=Quote(symbol="MANL", price=Decimal("1.25"), as_of=datetime.now(UTC)),
        quantity=Decimal("1"),
        action=OrderAction.BUY,
    )
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_a, **_k: adapter_cls())
    monkeypatch.setattr(execute_page, "connect", lambda: nullcontext(None))
    monkeypatch.setattr(execute_page, "migrate", lambda _connection: None)
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    monkeypatch.setattr(execute_page, "DatabaseOrderExecutionGuard", lambda *_a, **_k: None)
    monkeypatch.setattr(
        execute_page,
        "TradePortfolioObservationWriter",
        lambda _connection: SimpleNamespace(persist=lambda *_a, **_k: None),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_orders(queue, profile, (plan,))
    return next(item for item in queue.queue if isinstance(item, LiveOrderResult))


def test_non_login_order_rejection_is_terminal_and_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run_single_buy_order(
        monkeypatch,
        _rejecting_adapter(
            "Aries preview rejected the order: the account is not permitted to "
            "trade this security"
        ),
    )

    assert result.submitted == 0
    assert result.failed == 0
    assert result.terminal_failures == 1
    assert result.retry_targets == ()


@pytest.mark.parametrize(
    ("reason", "sofi_attempts", "submitted", "terminal"),
    [
        ("SoFi rejected the order (HTTP 400): Security 'EQUITY-NCT' cannot be traded", 1, 3, 4),
        ("SoFi rejected the order (HTTP 400): insufficient buying power", 4, 6, 1),
    ],
)
def test_sofi_rejection_continues_other_tickers_and_brokers(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    sofi_attempts: int,
    submitted: int,
    terminal: int,
) -> None:
    from localrsa.brokers.base import DefinitiveOrderRejectionError

    first = _profile(BrokerId.SOFI, "sofi-first")
    second = _profile(BrokerId.SOFI, "sofi-second")
    third = _profile(BrokerId.SOFI, "sofi-third")
    healthy = _profile()
    calls: list[tuple[BrokerId, str, str]] = []
    connections: list[str] = []

    class Adapter(_OrderAdapter):
        def place_order(self, request: object) -> BrokerOrder:
            calls.append((request.broker_id, request.account_id, request.symbol))
            if request.broker_id is BrokerId.SOFI and request.symbol == "NCT":
                attempts = sum(b is BrokerId.SOFI and s == "NCT" for b, _, s in calls)
                if attempts == 1:
                    raise DefinitiveOrderRejectionError(reason)
            return (
                super()
                .place_order(request)
                .model_copy(
                    update={
                        "broker_id": request.broker_id,
                        "broker_order_id": f"{request.account_id}-{request.symbol}",
                    }
                )
            )

    def connect_adapter(_queue: object, profile: BrokerLoginProfile, **_kwargs: object) -> Adapter:
        connections.append(profile.login_id)
        return Adapter()

    monkeypatch.setattr(execute_page, "_connect_order_adapter", connect_adapter)
    monkeypatch.setattr(execute_page, "_resolve_account_plans", lambda _a, _p, plans: plans)
    monkeypatch.setattr(execute_page, "DatabaseOrderExecutionGuard", lambda *_a, **_k: None)

    def batch(profile: BrokerLoginProfile, pairs: tuple[tuple[str, str], ...]) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=tuple(
                execute_page._build_custom_order_plan(
                    event_id=f"test:{symbol}",
                    profile=profile,
                    account_id=account,
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=datetime.now(UTC)),
                    quantity=Decimal("1"),
                )
                for account, symbol in pairs
            ),
        )

    batches = (
        batch(first, (("S1", "NCT"), ("S2", "NCT"), ("S1", "IBO"))),
        batch(second, (("S3", "NCT"), ("S3", "IBO"))),
        batch(third, (("S4", "NCT"),)),
        batch(healthy, (("P1", "NCT"),)),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_order_batches(queue, batches)
    result = next(item for item in reversed(queue.queue) if isinstance(item, LiveOrderResult))
    assert sum(b is BrokerId.SOFI and s == "NCT" for b, _, s in calls) == sofi_attempts
    assert result.submitted == submitted
    assert result.terminal_failures == terminal
    assert result.failed == result.uncertain == 0
    assert result.retry_targets == ()
    assert any(reason in detail for detail in result.skipped)
    assert calls[-1] == (BrokerId.PUBLIC, "P1", "NCT")
    progress = [item for item in queue.queue if isinstance(item, TaskProgressUpdate)]
    assert progress[-1].current == progress[-1].total == 7
    assert (third.login_id in connections) == (sofi_attempts > 1)

    # The skip is local to this run, never a permanent broker/ticker blacklist.
    calls.clear()
    next_queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_order_batches(next_queue, batches)
    assert (BrokerId.SOFI, "S1", "NCT") in calls


def test_login_related_order_rejection_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run_single_buy_order(
        monkeypatch,
        _rejecting_adapter("Public preflight rejected the order: please sign in again"),
    )

    assert result.submitted == 0
    assert result.terminal_failures == 0
    assert result.failed == 1
    assert result.retry_targets != ()


def test_order_batches_group_by_login_in_broker_order() -> None:
    public = _profile()
    robinhood = _profile(BrokerId.ROBINHOOD, "robinhood-login")
    now = datetime.now(UTC)

    def batch(profile: BrokerLoginProfile, symbol: str) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=(
                execute_page._build_custom_order_plan(
                    event_id=f"custom-order:{symbol}",
                    profile=profile,
                    account_id=profile.account_selectors[0],
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=now),
                    quantity=Decimal("1"),
                ),
            ),
        )

    grouped = _group_live_order_batches(
        (batch(robinhood, "R1"), batch(public, "P1"), batch(robinhood, "R2"))
    )
    assert [item.profile.broker_id for item in grouped] == [
        BrokerId.PUBLIC,
        BrokerId.ROBINHOOD,
    ]
    assert [plan.symbol for plan in grouped[1].plans] == ["R1", "R2"]


def test_chase_and_sofi_batches_submit_before_every_other_broker() -> None:
    aries = _profile(BrokerId.ARIES, "aries-login")
    sofi = _profile(BrokerId.SOFI, "sofi-login")
    chase = _profile(BrokerId.CHASE, "chase-login")
    robinhood = _profile(BrokerId.ROBINHOOD, "robinhood-login")
    now = datetime.now(UTC)

    def batch(profile: BrokerLoginProfile, symbol: str) -> LiveOrderBatch:
        return LiveOrderBatch(
            profile=profile,
            plans=(
                execute_page._build_custom_order_plan(
                    event_id=f"custom-order:{symbol}",
                    profile=profile,
                    account_id=profile.account_selectors[0],
                    quote=Quote(symbol=symbol, price=Decimal("1"), as_of=now),
                    quantity=Decimal("1"),
                ),
            ),
        )

    # Deliberately fed in an order that would otherwise reflect plain
    # BrokerId enum order (Aries, Robinhood, Chase, SoFi) to prove the
    # priority reorder -- not just first-seen order -- is what's applied.
    grouped = _group_live_order_batches(
        (batch(aries, "A1"), batch(robinhood, "R1"), batch(chase, "C1"), batch(sofi, "S1"))
    )

    assert [item.profile.broker_id for item in grouped] == [
        BrokerId.CHASE,
        BrokerId.SOFI,
        BrokerId.ARIES,
        BrokerId.ROBINHOOD,
    ]


def test_execution_broker_order_is_chase_then_sofi_then_the_rest_unchanged() -> None:
    order = execute_page._EXECUTION_BROKER_ORDER
    assert order[0] is BrokerId.CHASE
    assert order[1] is BrokerId.SOFI
    assert set(order) == set(BrokerId)
    assert len(order) == len(BrokerId)
    remaining_original = [b for b in BrokerId if b not in {BrokerId.CHASE, BrokerId.SOFI}]
    assert list(order[2:]) == remaining_original


def test_batch_progress_proxy_preserves_eta_metadata() -> None:
    output: Queue[execute_page.LiveOrderQueueItem] = Queue()
    proxy = execute_page._BatchProgressQueue(
        output,
        completed_before_batch=3,
        total_orders=8,
    )

    proxy.put(
        TaskProgressUpdate(
            message="Processed 1 of 2 orders",
            current=1,
            total=2,
            eta_seconds=9.0,
            reset_eta_sample=True,
        )
    )

    forwarded = output.get_nowait()
    assert isinstance(forwarded, TaskProgressUpdate)
    assert forwarded.current == 4
    assert forwarded.total == 8
    assert forwarded.eta_seconds == 9.0
    assert forwarded.reset_eta_sample is True


def test_login_scoped_reason_does_not_duplicate_login_identity() -> None:
    label = "WellsTrade(huss)"

    assert execute_page._login_scoped_reason(label, "connection failed") == (
        "WellsTrade(huss): connection failed"
    )
    assert (
        execute_page._login_scoped_reason(
            label,
            "WellsTrade(huss) connection failed: unavailable",
        )
        == "WellsTrade(huss) connection failed: unavailable"
    )


def test_ledger_amount_rounds_to_eight_places_the_cloud_ledger_accepts() -> None:
    import re

    from localrsa.cloud.models import SignalExecutionOutcome

    worker_decimal = re.compile(r"^(?:0|[1-9]\d{0,11})(?:\.\d{1,8})?$")

    # unit price x fractional-share quantity -- the shape that produced the
    # 18-decimal cost_basis the worker rejected with HTTP 400.
    raw = Decimal("2.47") * Decimal("4.04858299595141700404858299")
    quantized = execute_page._ledger_amount(raw)

    assert quantized is not None
    assert -quantized.as_tuple().exponent <= 8
    assert execute_page._ledger_amount(None) is None

    for amount in (raw, Decimal("0"), Decimal("1000.00"), Decimal("0.000000123")):
        outcome = SignalExecutionOutcome(
            execution_id="order-1",
            broker="fennel",
            account_ref="a" * 24,
            symbol="SFWL",
            action="BUY",
            state="SUBMITTED",
            filled_quantity=Decimal("0"),
            fill_price=Decimal("0"),
            cost_basis=execute_page._ledger_amount(amount),
            executed_at=datetime.now(UTC),
        )
        serialized = outcome.model_dump(mode="json")["cost_basis"]
        assert worker_decimal.match(serialized), serialized


def test_shared_broker_order_id_has_distinct_account_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    plan = execute_page._build_custom_order_plan(
        event_id="custom-order:test",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=Quote(symbol="MANL", price=Decimal("1.25"), as_of=datetime.now(UTC)),
        quantity=Decimal("1"),
    )

    class TwoAccountAdapter(_OrderAdapter):
        def list_accounts(self) -> list[BrokerAccount]:
            first = super().list_accounts()[0]
            return [
                first,
                first.model_copy(
                    update={"account_id": "second-account", "masked_account_id": "second-account"}
                ),
            ]

    adapter = TwoAccountAdapter()
    history: list[object] = []
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    monkeypatch.setattr(execute_page, "connect", lambda: nullcontext(None))
    monkeypatch.setattr(execute_page, "migrate", lambda _connection: None)
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    monkeypatch.setattr(
        execute_page,
        "DatabaseOrderExecutionGuard",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        execute_page,
        "TradePortfolioObservationWriter",
        lambda _connection: SimpleNamespace(persist=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        execute_page,
        "PortfolioOrderHistoryRepository",
        lambda _connection: SimpleNamespace(record=history.append),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()
    execute_page._collect_live_orders(
        queue,
        profile,
        (
            plan,
            plan.model_copy(update={"account_id": "second-account", "idempotency_key": "second"}),
        ),
    )
    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    progress = [item for item in queue.queue if isinstance(item, TaskProgressUpdate)]
    assert result.submitted == 2
    assert len(result.executions) == 2
    assert len({e.execution_id for e in result.executions}) == 2
    from localrsa.cloud.models import SignalCompletionRequest

    SignalCompletionRequest(lease_token="l" * 48, result="executed", executions=result.executions)
    assert result.executions[0].state == "SUBMITTED"
    assert result.executions[0].filled_quantity == 0
    assert history[0].unit_price == Decimal("1.10")
    assert progress[-1].current == progress[-1].total == 2


def test_unresolved_submission_is_terminal_without_retry_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile()
    plan = execute_page._build_custom_order_plan(
        event_id="custom-order:test",
        profile=profile,
        account_id=profile.account_selectors[0],
        quote=Quote(symbol="MANL", price=Decimal("1.25"), as_of=datetime.now(UTC)),
        quantity=Decimal("1"),
    )
    adapter = _OrderAdapter()
    history: list[object] = []
    monkeypatch.setattr(execute_page, "create_adapter", lambda *_args, **_kwargs: adapter)
    monkeypatch.setattr(execute_page, "connect", lambda: nullcontext(None))
    monkeypatch.setattr(execute_page, "migrate", lambda _connection: None)
    monkeypatch.setattr(execute_page, "load_settings", AppSettings)
    monkeypatch.setattr(
        execute_page,
        "DatabaseOrderExecutionGuard",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        execute_page,
        "TradePortfolioObservationWriter",
        lambda _connection: SimpleNamespace(persist=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        execute_page,
        "PortfolioOrderHistoryRepository",
        lambda _connection: SimpleNamespace(record=history.append),
    )
    queue: Queue[LiveOrderQueueItem] = Queue()

    def unresolved(**kwargs: object) -> None:
        raise executor.ExecutionUncertainError("a prior exact matching order is still uncertain")

    monkeypatch.setattr(execute_page, "submit_reviewed_order", unresolved)
    execute_page._collect_live_orders(queue, profile, (plan,))
    result = next(item for item in queue.queue if isinstance(item, LiveOrderResult))
    progress = [item for item in queue.queue if isinstance(item, TaskProgressUpdate)]
    assert result.submitted == 0
    assert result.uncertain == 1
    assert result.failed == 0
    assert result.retry_targets == ()
    assert result.executions == ()
    assert history == []
    assert progress[-1].current == progress[-1].total == 1
