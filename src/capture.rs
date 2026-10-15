use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    future::{Future, pending},
    rc::Rc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use input_capture::{
    CaptureError, CaptureEvent, CaptureHandle, DeviceGrab, InputCapture, InputCaptureError,
    Position,
};
use input_event::{Event, KeyboardEvent, scancode};
use lan_mouse_proto::ProtoEvent;
use local_channel::mpsc::{Receiver, Sender, channel};
use tokio::{
    process::Command,
    task::{JoinHandle, spawn_local},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{config::KeyHook, connect::LanMouseConnection};

pub(crate) struct Capture {
    cancellation_token: CancellationToken,
    request_tx: Sender<CaptureRequest>,
    task: JoinHandle<()>,
    event_rx: Receiver<ICaptureEvent>,
}

pub(crate) enum ICaptureEvent {
    /// a client was entered
    CaptureBegin(CaptureHandle),
    /// capture disabled
    CaptureDisabled,
    /// capture disabled
    CaptureEnabled,
    /// A (new) client was entered.
    /// In contrast to [`ICaptureEvent::CaptureBegin`] this
    /// event is only triggered when the capture was
    /// explicitly released in the meantime by
    /// either the remote client leaving its device region,
    /// a new device entering the screen or the release bind.
    ClientEntered(u64),
    /// The previously active client was left, i.e. capture
    /// was released for the given handle. Mirrors
    /// [`ICaptureEvent::ClientEntered`] for the leave side
    /// and fires on every release path (release-bind chord,
    /// remote `Leave`, explicit `Release` request, send
    /// failure, or destroy of the active capture).
    ClientLeft(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CaptureType {
    /// a normal input capture
    Default,
    /// A capture only interested in [`CaptureEvent::Begin`] events.
    /// The capture is released immediately, if there is no
    /// Default capture at the same position.
    EnterOnly,
}

#[derive(Clone, Debug)]
enum CaptureRequest {
    /// capture must release the mouse
    Release,
    /// add a capture client
    Create(CaptureHandle, Position, CaptureType),
    /// destory a capture client
    Destroy(CaptureHandle),
    /// reenable input capture
    Reenable,
    /// set release bind
    SetReleaseBind(Vec<scancode::Linux>),
    /// set captured key hooks
    SetKeyHooks(Vec<KeyHook>),
    /// set (or clear) the client standing in for the local screen
    SetStandIn(Option<StandIn>),
    /// grab the local input devices and forward them to the stand-in
    EnterStandIn,
}

/// A client that takes the local screen's place: while set, a `Leave`
/// from a remote client hands input to the next remote client instead of
/// returning it to the local screen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StandIn {
    handle: CaptureHandle,
    pos: Position,
}

/// What to do when the client we are forwarding to reports a `Leave`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaveAction {
    /// return input to the local screen
    Release,
    /// keep forwarding to the same client
    Stay,
    /// forward to another client without releasing the local grab
    HandOff(CaptureHandle),
}

impl Capture {
    pub(crate) fn new(
        backend: Option<input_capture::Backend>,
        conn: LanMouseConnection,
        release_bind: Vec<scancode::Linux>,
        key_hooks: Vec<KeyHook>,
    ) -> Self {
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let cancellation_token = CancellationToken::new();
        let capture_task = CaptureTask {
            active_client: None,
            grab: None,
            backend,
            cancellation_token: cancellation_token.clone(),
            captures: Default::default(),
            conn,
            device_grab: None,
            event_tx,
            key_hooks: KeyHookState::new(key_hooks),
            request_rx,
            release_bind: Rc::new(RefCell::new(release_bind)),
            stand_in: None,
            state: Default::default(),
        };
        let task = spawn_local(capture_task.run());
        Self {
            cancellation_token,
            request_tx,
            task,
            event_rx,
        }
    }

    pub(crate) fn reenable(&self) {
        self.request_tx
            .send(CaptureRequest::Reenable)
            .expect("channel closed");
    }

    pub(crate) async fn terminate(&mut self) {
        self.cancellation_token.cancel();
        log::debug!("terminating capture");
        if let Err(e) = (&mut self.task).await {
            log::warn!("{e}");
        }
    }

    pub(crate) fn create(
        &self,
        handle: CaptureHandle,
        pos: lan_mouse_ipc::Position,
        capture_type: CaptureType,
    ) {
        let pos = to_capture_pos(pos);
        self.request_tx
            .send(CaptureRequest::Create(handle, pos, capture_type))
            .expect("channel closed");
    }

    pub(crate) fn destroy(&self, handle: CaptureHandle) {
        self.request_tx
            .send(CaptureRequest::Destroy(handle))
            .expect("channel closed");
    }

    pub(crate) fn release(&self) {
        self.request_tx
            .send(CaptureRequest::Release)
            .expect("channel closed");
    }

    pub(crate) async fn event(&mut self) -> ICaptureEvent {
        self.event_rx.recv().await.expect("channel closed")
    }

    pub(crate) fn set_release_bind(&mut self, bind: Vec<scancode::Linux>) {
        let _ = self.request_tx.send(CaptureRequest::SetReleaseBind(bind));
    }

    pub(crate) fn set_key_hooks(&mut self, hooks: Vec<KeyHook>) {
        let _ = self.request_tx.send(CaptureRequest::SetKeyHooks(hooks));
    }

    pub(crate) fn set_stand_in(&self, stand_in: Option<(CaptureHandle, lan_mouse_ipc::Position)>) {
        let stand_in = stand_in.map(|(handle, pos)| StandIn {
            handle,
            pos: to_capture_pos(pos),
        });
        let _ = self.request_tx.send(CaptureRequest::SetStandIn(stand_in));
    }

    pub(crate) fn enter_stand_in(&self) {
        let _ = self.request_tx.send(CaptureRequest::EnterStandIn);
    }
}

/// debounce a statement `$st`, i.e. the statement is executed only if the
/// time since the previous execution is at least `$dur`.
/// `$prev` is used to keep track of this timestamp
macro_rules! debounce {
    ($prev:ident, $dur:expr, $st:stmt) => {
        let exec = match $prev.get() {
            None => true,
            Some(instant) if instant.elapsed() > $dur => true,
            _ => false,
        };
        if exec {
            $prev.replace(Some(Instant::now()));
            $st
        }
    };
}

struct CaptureTask {
    /// the client input is currently forwarded to
    active_client: Option<CaptureHandle>,
    /// the capture whose barrier holds the local grab; differs from
    /// `active_client` after a stand-in handoff
    grab: Option<CaptureHandle>,
    backend: Option<input_capture::Backend>,
    cancellation_token: CancellationToken,
    captures: Vec<(CaptureHandle, Position, CaptureType)>,
    conn: LanMouseConnection,
    /// keyboards and pointers grabbed while a stand-in replaces the local
    /// screen; takes the place of a barrier's grab
    device_grab: Option<DeviceGrab>,
    event_tx: Sender<ICaptureEvent>,
    key_hooks: KeyHookState,
    release_bind: Rc<RefCell<Vec<scancode::Linux>>>,
    request_rx: Receiver<CaptureRequest>,
    stand_in: Option<StandIn>,
    state: State,
}

impl CaptureTask {
    fn add_capture(&mut self, handle: CaptureHandle, pos: Position, capture_type: CaptureType) {
        self.captures.push((handle, pos, capture_type));
    }

    fn remove_capture(&mut self, handle: CaptureHandle) {
        self.captures.retain(|&(h, ..)| handle != h);
    }

    fn is_default_capture_at(&self, pos: Position) -> bool {
        self.captures
            .iter()
            .any(|&(_, p, t)| p == pos && t == CaptureType::Default)
    }

    fn get_pos(&self, handle: CaptureHandle) -> Position {
        self.captures
            .iter()
            .find(|(h, ..)| *h == handle)
            .expect("no such capture")
            .1
    }

    /// position of a forwarding target, which may be the stand-in client
    /// rather than a capture barrier
    fn client_pos(&self, handle: CaptureHandle) -> Option<Position> {
        self.captures
            .iter()
            .find(|&&(h, _, t)| h == handle && t == CaptureType::Default)
            .map(|&(_, p, _)| p)
            .or(self.stand_in.filter(|s| s.handle == handle).map(|s| s.pos))
    }

    fn get_type(&self, handle: CaptureHandle) -> CaptureType {
        self.captures
            .iter()
            .find(|(h, ..)| *h == handle)
            .expect("no such capture")
            .2
    }

    async fn run(mut self) {
        loop {
            if let Err(e) = self.do_capture().await {
                log::warn!("input capture exited: {e}");
            }
            loop {
                tokio::select! {
                    r = self.request_rx.recv() => match r.expect("channel closed") {
                        CaptureRequest::Reenable => break,
                        CaptureRequest::Create(h, p, t) => self.add_capture(h, p, t),
                        CaptureRequest::Destroy(h) => self.remove_capture(h),
                        CaptureRequest::Release => { /* nothing to do */ }
                        CaptureRequest::SetReleaseBind(bind) => {
                            self.release_bind.borrow_mut().clone_from(&bind);
                        }
                        CaptureRequest::SetKeyHooks(hooks) => self.key_hooks.replace(hooks),
                        CaptureRequest::SetStandIn(stand_in) => self.stand_in = stand_in,
                        CaptureRequest::EnterStandIn => {
                            log::warn!("cannot enter the stand-in: input capture is disabled");
                        }
                    },
                    _ = self.cancellation_token.cancelled() => return,
                }
            }
        }
    }

    async fn do_capture(&mut self) -> Result<(), InputCaptureError> {
        /* allow cancelling capture request */
        let mut capture = tokio::select! {
            r = InputCapture::new(self.backend) => r?,
            _ = self.cancellation_token.cancelled() => return Ok(()),
        };

        let _capture_guard = DropGuard::new(
            self.event_tx.clone(),
            ICaptureEvent::CaptureEnabled,
            ICaptureEvent::CaptureDisabled,
        );

        /* create barriers for active clients */
        let r = self.create_captures(&mut capture).await;
        if let Err(e) = r {
            capture.terminate().await?;
            return Err(e.into());
        }

        let session_result = self.do_capture_session(&mut capture).await;

        // Release remote and local held-key state even when the capture session
        // ended because of an error or service cancellation.
        let release_result = self.release_capture(&mut capture).await;

        // FIXME replace with async drop when stabilized
        let terminate_result = capture.terminate().await;

        session_result?;
        release_result?;
        terminate_result?;
        Ok(())
    }

    async fn create_captures(&mut self, capture: &mut InputCapture) -> Result<(), CaptureError> {
        let captures = self.captures.clone();
        for (handle, pos, _type) in captures {
            tokio::select! {
                r = capture.create(handle, pos) => r?,
                _ = self.cancellation_token.cancelled() => return Ok(()),
            }
        }
        Ok(())
    }

    async fn do_capture_session(
        &mut self,
        capture: &mut InputCapture,
    ) -> Result<(), InputCaptureError> {
        loop {
            tokio::select! {
                event = capture.next() => match event {
                    Some(event) => self.handle_capture_event(capture, event?).await?,
                    None => return Ok(()),
                },
                event = next_grabbed(&mut self.device_grab) => match event {
                    Some(event) => self.handle_grabbed_event(capture, event).await?,
                    None => {
                        log::warn!("releasing capture: grabbed input devices are gone");
                        self.release_capture(capture).await?;
                    }
                },
                (handle, event) = self.conn.recv() => {
                    if let Some(active) = self.active_client {
                        if handle != active {
                            // we only care about events coming from the client we are currently connected to
                            // only `Ack` and `Leave` are relevant
                            continue
                        }
                    }

                    match event {
                        // connection acknowlegded => set state to Sending
                        ProtoEvent::Ack(_) => {
                            log::info!("client {handle} acknowledged the connection!");
                            self.state = State::Sending;
                        }
                        // client disconnected
                        ProtoEvent::Leave(_) => {
                            match leave_action(self.stand_in, &self.captures, handle) {
                                LeaveAction::Release => {
                                    log::info!("releasing capture: left remote client device region");
                                    self.release_capture(capture).await?;
                                }
                                LeaveAction::Stay => {
                                    log::debug!("stand-in client {handle} left, but has no neighbor");
                                }
                                LeaveAction::HandOff(next) => {
                                    self.hand_off(capture, handle, next).await;
                                }
                            }
                        },
                        _ => {}
                    }
                },
                e = self.request_rx.recv() => match e.expect("channel closed") {
                    CaptureRequest::Reenable => { /* already active */ },
                    CaptureRequest::Release => self.release_capture(capture).await?,
                    CaptureRequest::Create(h, p, t) => {
                        self.add_capture(h, p, t);
                        capture.create(h, p).await?;
                    }
                    CaptureRequest::Destroy(h) => {
                        // If the capture we're tearing down is the
                        // currently-active one, treat this as a
                        // release for hook purposes. The release_capture
                        // path also clears active_client and flushes
                        // pressed-key state to the peer; without this,
                        // `cli deactivate` (or a hostname change
                        // re-creating the client) would skip leave_hook.
                        // The same applies to the barrier holding the
                        // grab after a stand-in handoff.
                        if self.active_client == Some(h) || self.grab == Some(h) {
                            self.release_capture(capture).await?;
                        }
                        self.remove_capture(h);
                        capture.destroy(h).await?;
                    }
                    CaptureRequest::SetReleaseBind(bind) => {
                        self.release_bind.borrow_mut().clone_from(&bind);
                    }
                    CaptureRequest::SetKeyHooks(hooks) => self.key_hooks.replace(hooks),
                    CaptureRequest::SetStandIn(stand_in) => self.set_stand_in(capture, stand_in).await?,
                    CaptureRequest::EnterStandIn => self.enter_stand_in(capture).await?,
                },
                _ = self.cancellation_token.cancelled() => break,
            }
        }
        Ok(())
    }

    async fn handle_capture_event(
        &mut self,
        capture: &mut InputCapture,
        event: (CaptureHandle, CaptureEvent),
    ) -> Result<(), CaptureError> {
        let (handle, event) = event;
        log::trace!("({handle}): {event:?}");

        // A destroyed barrier may still have events queued in the backend.
        if !self.captures.iter().any(|(h, ..)| *h == handle) {
            log::trace!("dropping event for destroyed capture {handle}");
            return Ok(());
        }

        // Input reaches barriers only through the compositor, which a
        // device grab cuts off; anything still queued is stale.
        if self.device_grab.is_some() {
            log::trace!("dropping event for capture {handle} during device grab");
            return Ok(());
        }

        if !accepts_routed_event(self.grab, handle, event) {
            log::trace!("dropping input event for inactive capture {handle}");
            return Ok(());
        }

        if let CaptureEvent::Input(event) = event {
            capture.track_event(event);
        }

        if capture.keys_pressed(&self.release_bind.borrow()) {
            log::info!("releasing capture: release-bind pressed");
            return self.release_capture(capture).await;
        }

        if event == CaptureEvent::Begin {
            self.event_tx
                .send(ICaptureEvent::CaptureBegin(handle))
                .expect("channel closed");
        }

        // enter only capture (for incoming connections)
        if self.get_type(handle) == CaptureType::EnterOnly {
            // if there is no active outgoing connection at the current capture,
            // we release the capture
            if !self.is_default_capture_at(self.get_pos(handle)) {
                log::info!("releasing capture: no active client at this position");
                capture.release().await?;
            }
            // we dont care about events from incoming handles except for releasing the capture
            return Ok(());
        }

        // activated a new client
        if event == CaptureEvent::Begin && Some(handle) != self.grab {
            self.state = State::WaitingForAck;
            self.grab.replace(handle);
            self.active_client.replace(handle);
            self.event_tx
                .send(ICaptureEvent::ClientEntered(handle))
                .expect("channel closed");
        }

        // after a stand-in handoff, input grabbed by one barrier is
        // forwarded to a different client
        let target = self.active_client.unwrap_or(handle);
        self.forward(capture, target, event).await
    }

    async fn handle_grabbed_event(
        &mut self,
        capture: &mut InputCapture,
        event: Event,
    ) -> Result<(), CaptureError> {
        log::trace!("(grab): {event:?}");
        capture.track_event(event);
        if capture.keys_pressed(&self.release_bind.borrow()) {
            log::info!("releasing capture: release-bind pressed");
            return self.release_capture(capture).await;
        }
        match self.active_client {
            Some(target) => {
                self.forward(capture, target, CaptureEvent::Input(event))
                    .await
            }
            None => Ok(()),
        }
    }

    async fn forward(
        &mut self,
        capture: &mut InputCapture,
        target: CaptureHandle,
        event: CaptureEvent,
    ) -> Result<(), CaptureError> {
        let Some(target_pos) = self.client_pos(target) else {
            log::warn!("releasing capture: client {target} has no position");
            return self.release_capture(capture).await;
        };
        let opposite_pos = to_proto_pos(target_pos.opposite());

        let (event, hook_dispatch) = match event {
            CaptureEvent::Begin => (ProtoEvent::Enter(opposite_pos), HookDispatch::default()),
            CaptureEvent::Input(e) => match self.state {
                // connection not acknowledged, repeat `Enter` event
                State::WaitingForAck => (
                    ProtoEvent::Enter(opposite_pos),
                    self.key_hooks.dispatch(e, false),
                ),
                State::Sending => (ProtoEvent::Input(e), self.key_hooks.dispatch(e, true)),
            },
        };

        let send_result =
            send_with_hooks(hook_dispatch, self.conn.send(event, target), run_key_hook).await;

        if let Err(e) = send_result {
            // The local screen is replaced, so an unreachable neighbor
            // returns input to the stand-in rather than to the local screen.
            if let Some(stand_in) = send_failure_fallback(self.stand_in, target) {
                log::warn!("client {target} is unreachable ({e}), returning to stand-in");
                self.hand_off(capture, target, stand_in).await;
                return Ok(());
            }
            const DUR: Duration = Duration::from_millis(500);
            debounce!(PREV_LOG, DUR, log::warn!("releasing capture: {e}"));
            // Funnel through release_capture so the leave_hook
            // fires and active_client is cleared (without this the
            // active_client field would stay stale until the next
            // Begin from a different handle).
            self.release_capture(capture).await?;
        }
        Ok(())
    }

    async fn release_capture(&mut self, capture: &mut InputCapture) -> Result<(), CaptureError> {
        let active_client = self.active_client.take();
        self.grab = None;
        // dropping the grab returns the devices to the compositor
        self.device_grab = None;
        self.state = State::WaitingForAck;
        let pressed_keys = capture.take_pressed_keys();

        // Stop capture before awaiting network or command IO so queued events
        // cannot restart hooks or be forwarded after this terminal transition.
        let release_result = capture.release().await;
        let mut release_commands = Vec::new();

        // If we have an active client, notify them we're leaving.
        if let Some(handle) = active_client {
            release_commands = self.leave_client(handle, pressed_keys).await;
        }

        release_commands.extend(self.key_hooks.drain_releases());
        for command in release_commands {
            run_key_hook("release", command).await;
        }
        release_result
    }

    /// Stops forwarding to `handle`: flushes held keys and modifiers to the
    /// peer and sends `Leave`. Returns release hooks of the flushed keys.
    async fn leave_client(
        &mut self,
        handle: CaptureHandle,
        pressed_keys: HashSet<scancode::Linux>,
    ) -> Vec<HookCommand> {
        let mut release_commands = Vec::new();
        // Surface the leave to the service layer so it can fire
        // the per-client leave_hook. Sent before the network
        // teardown below so we never race against the peer
        // disappearing.
        self.event_tx
            .send(ICaptureEvent::ClientLeft(handle))
            .expect("channel closed");
        // Synthesize key-up events for every key still held in the
        // capture's pressed_keys set BEFORE sending Leave. Without
        // this, pressing the release-bind chord (typically all four
        // modifiers) leaves the peer with phantom held modifiers:
        // the down events were forwarded while capture was active,
        // but the matching up events arrive after the local tap
        // flips to passthrough and never reach the peer. The peer
        // then runs every subsequent keystroke through those held
        // mods until its watchdog times out (1+ s) or our Leave
        // arrives — and Leave can be lost over UDP/DTLS.
        for key in pressed_keys {
            let key_up = ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Key {
                time: 0,
                key: key as u32,
                state: 0,
            }));
            if let Err(e) = self.conn.send(key_up, handle).await {
                log::warn!("failed to send key-up to client {handle}: {e}");
            }
            if let Some(command) = self.key_hooks.take_release(key) {
                release_commands.push(command);
            }
        }
        // Reset the modifier mask too. The peer's input-emulation
        // layer keeps a separate XKB-style modifier state that's
        // updated by KeyboardEvent::Modifiers, distinct from the
        // pressed_keys set drained above. Without this, an
        // already-locked CapsLock would survive the release.
        let mods_zero = ProtoEvent::Input(Event::Keyboard(KeyboardEvent::Modifiers {
            depressed: 0,
            latched: 0,
            locked: 0,
            group: 0,
        }));
        if let Err(e) = self.conn.send(mods_zero, handle).await {
            log::warn!("failed to reset modifiers on client {handle}: {e}");
        }

        log::info!("sending Leave event to client {handle}");
        if let Err(e) = self.conn.send(ProtoEvent::Leave(0), handle).await {
            log::warn!("failed to send Leave to client {handle}: {e}");
        }
        release_commands
    }

    /// Moves forwarding from `from` to `to` while keeping the local grab,
    /// so input never returns to the (replaced) local screen.
    async fn hand_off(
        &mut self,
        capture: &mut InputCapture,
        from: CaptureHandle,
        to: CaptureHandle,
    ) {
        let Some(pos) = self.client_pos(to) else {
            log::warn!("cannot hand off to client {to}: no position");
            return;
        };
        log::info!("handing off from client {from} to client {to}");
        let pressed_keys = capture.take_pressed_keys();
        let release_commands = self.leave_client(from, pressed_keys).await;
        self.enter_client(to, pos).await;
        for command in release_commands {
            run_key_hook("release", command).await;
        }
    }

    /// Starts forwarding to `handle`, which borders this device at `pos`.
    async fn enter_client(&mut self, handle: CaptureHandle, pos: Position) {
        self.active_client = Some(handle);
        self.state = State::WaitingForAck;
        self.event_tx
            .send(ICaptureEvent::ClientEntered(handle))
            .expect("channel closed");
        // Further input repeats `Enter` until the peer acknowledges it.
        let enter = ProtoEvent::Enter(to_proto_pos(pos.opposite()));
        if let Err(e) = self.conn.send(enter, handle).await {
            log::debug!("failed to send Enter to client {handle}: {e}");
        }
    }

    async fn set_stand_in(
        &mut self,
        capture: &mut InputCapture,
        stand_in: Option<StandIn>,
    ) -> Result<(), CaptureError> {
        let previous = std::mem::replace(&mut self.stand_in, stand_in);
        if let Some(previous) = previous {
            let still_stand_in = stand_in.is_some_and(|s| s.handle == previous.handle);
            let forwarding = self.active_client == Some(previous.handle);
            if !still_stand_in && (forwarding || self.device_grab.is_some()) {
                log::info!(
                    "releasing capture: client {} no longer stands in",
                    previous.handle
                );
                self.release_capture(capture).await?;
            }
        }
        if let Some(stand_in) = stand_in {
            // Connect ahead of the first handoff: a send that fails because
            // the connection is still being set up releases the capture.
            let _ = self.conn.send(ProtoEvent::Ping, stand_in.handle).await;
        }
        Ok(())
    }

    /// Takes the local keyboards and pointers from the compositor and enters
    /// the stand-in, so input reaches it without crossing an edge and keeps
    /// flowing while the local display is gone.
    async fn enter_stand_in(&mut self, capture: &mut InputCapture) -> Result<(), CaptureError> {
        let Some(stand_in) = self.stand_in else {
            log::warn!("cannot enter the stand-in: stand-in mode is off");
            return Ok(());
        };
        if self.device_grab.is_some() {
            return Ok(());
        }
        // A failed send to the stand-in releases the capture, so grab only
        // once the stand-in is connected.
        if !self.wait_for_connection(stand_in.handle).await {
            log::warn!(
                "cannot enter the stand-in: client {} is unreachable",
                stand_in.handle
            );
            return Ok(());
        }
        let device_grab = match DeviceGrab::new().await {
            Ok(device_grab) => device_grab,
            Err(e) => {
                log::warn!("stand-in is reachable only across edges: {e}");
                return Ok(());
            }
        };
        match self.active_client {
            Some(active) if active == stand_in.handle => {}
            Some(active) => self.hand_off(capture, active, stand_in.handle).await,
            None => self.enter_client(stand_in.handle, stand_in.pos).await,
        }
        // the device grab replaces a barrier's grab
        self.grab = None;
        capture.release().await?;
        self.device_grab = Some(device_grab);
        Ok(())
    }

    async fn wait_for_connection(&self, handle: CaptureHandle) -> bool {
        const ATTEMPTS: u32 = 20;
        for attempt in 1..=ATTEMPTS {
            if self.conn.send(ProtoEvent::Ping, handle).await.is_ok() {
                return true;
            }
            if attempt < ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        false
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HookCommand {
    command: String,
    timeout: Duration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct HookDispatch {
    before_forward: Option<HookCommand>,
    after_forward: Option<HookCommand>,
}

#[derive(Clone, Debug, Default)]
struct HookCommands {
    press: Option<HookCommand>,
    release: Option<HookCommand>,
}

impl From<KeyHook> for HookCommands {
    fn from(hook: KeyHook) -> Self {
        let timeout = Duration::from_millis(hook.timeout_ms);
        Self {
            press: hook.press.map(|command| HookCommand { command, timeout }),
            release: hook.release.map(|command| HookCommand { command, timeout }),
        }
    }
}

#[derive(Debug, Default)]
struct KeyHookState {
    configured: HashMap<scancode::Linux, HookCommands>,
    held: HashMap<scancode::Linux, Option<HookCommand>>,
    pressed: HashSet<scancode::Linux>,
}

impl KeyHookState {
    fn new(hooks: Vec<KeyHook>) -> Self {
        let mut state = Self::default();
        state.replace(hooks);
        state
    }

    fn replace(&mut self, hooks: Vec<KeyHook>) {
        self.configured = hooks
            .into_iter()
            .map(|hook| (hook.key, HookCommands::from(hook)))
            .collect();
    }

    fn dispatch(&mut self, event: Event, allow_press: bool) -> HookDispatch {
        let Event::Keyboard(KeyboardEvent::Key { key, state, .. }) = event else {
            return HookDispatch::default();
        };
        let Ok(key) = scancode::Linux::try_from(key) else {
            return HookDispatch::default();
        };

        match state {
            1 if self.pressed.insert(key) && allow_press => {
                let Some(hook) = self.configured.get(&key) else {
                    return HookDispatch::default();
                };
                self.held.insert(key, hook.release.clone());
                HookDispatch {
                    before_forward: hook.press.clone(),
                    after_forward: None,
                }
            }
            0 => {
                self.pressed.remove(&key);
                HookDispatch {
                    before_forward: None,
                    after_forward: self.held.remove(&key).flatten(),
                }
            }
            _ => HookDispatch::default(),
        }
    }

    fn take_release(&mut self, key: scancode::Linux) -> Option<HookCommand> {
        self.held.remove(&key).flatten()
    }

    fn drain_releases(&mut self) -> Vec<HookCommand> {
        self.pressed.clear();
        self.held
            .drain()
            .filter_map(|(_, command)| command)
            .collect()
    }
}

async fn run_key_hook(phase: &str, hook: HookCommand) {
    let mut command = Command::new("sh");
    command.arg("-c").arg(&hook.command).kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            log::warn!(
                "could not spawn captured key {phase} hook {:?}: {e}",
                hook.command
            );
            return;
        }
    };

    match timeout(hook.timeout, child.wait()).await {
        Ok(Ok(status)) if status.success() => {}
        Ok(Ok(status)) => log::warn!(
            "captured key {phase} hook {:?} exited with {status}",
            hook.command
        ),
        Ok(Err(e)) => log::warn!(
            "failed waiting for captured key {phase} hook {:?}: {e}",
            hook.command
        ),
        Err(_) => {
            log::warn!(
                "captured key {phase} hook {:?} timed out after {:?}",
                hook.command,
                hook.timeout
            );
            if let Err(e) = child.kill().await {
                log::warn!(
                    "failed to kill timed-out captured key {phase} hook {:?}: {e}",
                    hook.command
                );
            }
        }
    }
}

async fn send_with_hooks<E, SendFuture, RunHook, HookFuture>(
    dispatch: HookDispatch,
    send: SendFuture,
    mut run_hook: RunHook,
) -> Result<(), E>
where
    SendFuture: Future<Output = Result<(), E>>,
    RunHook: FnMut(&'static str, HookCommand) -> HookFuture,
    HookFuture: Future<Output = ()>,
{
    if let Some(command) = dispatch.before_forward {
        run_hook("press", command).await;
    }

    let send_result = send.await;

    if let Some(command) = dispatch.after_forward {
        run_hook("release", command).await;
    }

    send_result
}

#[cfg(test)]
mod tests {
    use super::{
        CaptureType, HookCommand, HookDispatch, KeyHookState, LeaveAction, StandIn,
        accepts_routed_event, leave_action, send_failure_fallback, send_with_hooks,
    };
    use crate::config::KeyHook;
    use input_capture::{CaptureEvent, Position};
    use input_event::{Event, KeyboardEvent, scancode::Linux};
    use std::{cell::RefCell, rc::Rc, time::Duration};

    fn key_event(key: Linux, state: u8) -> Event {
        Event::Keyboard(KeyboardEvent::Key {
            time: 0,
            key: key as u32,
            state,
        })
    }

    fn right_alt_hook() -> KeyHook {
        KeyHook {
            key: Linux::KeyRightalt,
            press: Some("duck-start".to_owned()),
            release: Some("duck-stop".to_owned()),
            timeout_ms: 250,
        }
    }

    fn command(value: &str) -> Option<HookCommand> {
        Some(HookCommand {
            command: value.to_owned(),
            timeout: Duration::from_millis(250),
        })
    }

    #[test]
    fn dispatches_press_before_forward_and_release_after_forward() {
        let mut state = KeyHookState::new(vec![right_alt_hook()]);

        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 1), true),
            HookDispatch {
                before_forward: command("duck-start"),
                after_forward: None,
            }
        );
        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 0), true),
            HookDispatch {
                before_forward: None,
                after_forward: command("duck-stop"),
            }
        );
    }

    #[test]
    fn suppresses_repeat_hooks_until_final_release() {
        let mut state = KeyHookState::new(vec![right_alt_hook()]);

        assert!(
            state
                .dispatch(key_event(Linux::KeyRightalt, 1), true)
                .before_forward
                .is_some()
        );
        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 1), true),
            HookDispatch::default()
        );
        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 2), true),
            HookDispatch::default()
        );
        assert_eq!(
            state
                .dispatch(key_event(Linux::KeyRightalt, 0), true)
                .after_forward,
            command("duck-stop")
        );
        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 0), true),
            HookDispatch::default()
        );
    }

    #[test]
    fn drains_held_release_hook_exactly_once() {
        let mut state = KeyHookState::new(vec![right_alt_hook()]);
        state.dispatch(key_event(Linux::KeyRightalt, 1), true);

        assert_eq!(state.drain_releases(), vec![command("duck-stop").unwrap()]);
        assert!(state.drain_releases().is_empty());
    }

    #[test]
    fn held_release_uses_configuration_from_press_time() {
        let mut state = KeyHookState::new(vec![right_alt_hook()]);
        state.dispatch(key_event(Linux::KeyRightalt, 1), true);
        state.replace(Vec::new());

        assert_eq!(
            state
                .dispatch(key_event(Linux::KeyRightalt, 0), true)
                .after_forward,
            command("duck-stop")
        );
    }

    #[test]
    fn does_not_press_hook_when_key_was_already_down_before_hooks_were_allowed() {
        let mut state = KeyHookState::new(vec![right_alt_hook()]);

        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 1), false),
            HookDispatch::default()
        );
        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 1), true),
            HookDispatch::default()
        );
        assert_eq!(
            state.dispatch(key_event(Linux::KeyRightalt, 0), true),
            HookDispatch::default()
        );
    }

    #[tokio::test]
    async fn executes_press_then_send_then_release() {
        let actions = Rc::new(RefCell::new(Vec::new()));
        let send_actions = actions.clone();
        let send = async move {
            send_actions.borrow_mut().push("send");
            Ok::<(), ()>(())
        };
        let hook_actions = actions.clone();

        send_with_hooks(
            HookDispatch {
                before_forward: command("duck-start"),
                after_forward: command("duck-stop"),
            },
            send,
            move |phase, _| {
                let hook_actions = hook_actions.clone();
                async move { hook_actions.borrow_mut().push(phase) }
            },
        )
        .await
        .expect("send should succeed");

        assert_eq!(&*actions.borrow(), &["press", "send", "release"]);
    }

    #[test]
    fn rejects_queued_input_after_capture_release() {
        let event = CaptureEvent::Input(key_event(Linux::KeyRightalt, 1));

        assert!(!accepts_routed_event(None, 7, event));
        assert!(!accepts_routed_event(Some(8), 7, event));
        assert!(accepts_routed_event(Some(7), 7, event));
        assert!(accepts_routed_event(None, 7, CaptureEvent::Begin));
    }

    const LEFT_NEIGHBOR: u64 = 1;
    const STAND_IN: u64 = 2;
    const STAND_IN_RIGHT: StandIn = StandIn {
        handle: STAND_IN,
        pos: Position::Right,
    };

    #[test]
    fn leave_releases_without_stand_in() {
        let captures = [(LEFT_NEIGHBOR, Position::Left, CaptureType::Default)];
        assert_eq!(
            leave_action(None, &captures, LEFT_NEIGHBOR),
            LeaveAction::Release
        );
    }

    #[test]
    fn stand_in_and_opposite_neighbor_hand_off_to_each_other() {
        let captures = [
            (LEFT_NEIGHBOR, Position::Left, CaptureType::Default),
            (u64::MAX, Position::Left, CaptureType::EnterOnly),
        ];
        assert_eq!(
            leave_action(Some(STAND_IN_RIGHT), &captures, STAND_IN),
            LeaveAction::HandOff(LEFT_NEIGHBOR)
        );
        assert_eq!(
            leave_action(Some(STAND_IN_RIGHT), &captures, LEFT_NEIGHBOR),
            LeaveAction::HandOff(STAND_IN)
        );
    }

    #[test]
    fn stand_in_without_opposite_neighbor_stays() {
        let captures = [
            (LEFT_NEIGHBOR, Position::Top, CaptureType::Default),
            (u64::MAX, Position::Left, CaptureType::EnterOnly),
        ];
        assert_eq!(
            leave_action(Some(STAND_IN_RIGHT), &captures, STAND_IN),
            LeaveAction::Stay
        );
        assert_eq!(
            leave_action(Some(STAND_IN_RIGHT), &captures, LEFT_NEIGHBOR),
            LeaveAction::HandOff(STAND_IN)
        );
    }

    #[test]
    fn failed_send_falls_back_to_stand_in_only_from_other_clients() {
        assert_eq!(send_failure_fallback(None, LEFT_NEIGHBOR), None);
        assert_eq!(
            send_failure_fallback(Some(STAND_IN_RIGHT), LEFT_NEIGHBOR),
            Some(STAND_IN)
        );
        assert_eq!(send_failure_fallback(Some(STAND_IN_RIGHT), STAND_IN), None);
    }
}

async fn next_grabbed(device_grab: &mut Option<DeviceGrab>) -> Option<Event> {
    match device_grab {
        Some(device_grab) => device_grab.next().await,
        None => pending().await,
    }
}

thread_local! {
    static PREV_LOG: Cell<Option<Instant>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum State {
    #[default]
    WaitingForAck,
    Sending,
}

fn to_capture_pos(pos: lan_mouse_ipc::Position) -> input_capture::Position {
    match pos {
        lan_mouse_ipc::Position::Left => input_capture::Position::Left,
        lan_mouse_ipc::Position::Right => input_capture::Position::Right,
        lan_mouse_ipc::Position::Top => input_capture::Position::Top,
        lan_mouse_ipc::Position::Bottom => input_capture::Position::Bottom,
    }
}

fn to_proto_pos(pos: input_capture::Position) -> lan_mouse_proto::Position {
    match pos {
        input_capture::Position::Left => lan_mouse_proto::Position::Left,
        input_capture::Position::Right => lan_mouse_proto::Position::Right,
        input_capture::Position::Top => lan_mouse_proto::Position::Top,
        input_capture::Position::Bottom => lan_mouse_proto::Position::Bottom,
    }
}

fn accepts_routed_event(
    active_client: Option<CaptureHandle>,
    handle: CaptureHandle,
    event: CaptureEvent,
) -> bool {
    !matches!(event, CaptureEvent::Input(_)) || active_client == Some(handle)
}

/// Decides where input goes when `from` reports a `Leave`. Each remote
/// returns input only across the edge facing this device, so with a stand-in
/// the stand-in hands off to the client on that side of the local screen and
/// every other client hands off back to the stand-in.
fn leave_action(
    stand_in: Option<StandIn>,
    captures: &[(CaptureHandle, Position, CaptureType)],
    from: CaptureHandle,
) -> LeaveAction {
    let Some(stand_in) = stand_in else {
        return LeaveAction::Release;
    };
    if from != stand_in.handle {
        return LeaveAction::HandOff(stand_in.handle);
    }
    captures
        .iter()
        .find(|&&(h, p, t)| {
            h != stand_in.handle && p == stand_in.pos.opposite() && t == CaptureType::Default
        })
        .map_or(LeaveAction::Stay, |&(h, ..)| LeaveAction::HandOff(h))
}

/// Where to forward after sending to `target` failed: the stand-in, unless
/// `target` is the stand-in itself or no stand-in is set.
fn send_failure_fallback(
    stand_in: Option<StandIn>,
    target: CaptureHandle,
) -> Option<CaptureHandle> {
    stand_in
        .map(|s| s.handle)
        .filter(|&handle| handle != target)
}

struct DropGuard<T> {
    tx: Sender<T>,
    on_drop: Option<T>,
}

impl<T> DropGuard<T> {
    fn new(tx: Sender<T>, on_new: T, on_drop: T) -> Self {
        tx.send(on_new).expect("channel closed");
        let on_drop = Some(on_drop);
        Self { tx, on_drop }
    }
}

impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.tx
            .send(self.on_drop.take().expect("item"))
            .expect("channel closed");
    }
}
