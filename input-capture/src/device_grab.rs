//! Exclusive capture of keyboards and pointers through evdev (`EVIOCGRAB`).
//!
//! Unlike the edge-barrier backends, a device grab does not depend on the
//! compositor or on any output: while grabbed, the devices deliver their
//! events only to Lan Mouse. Only devices the user can open are grabbed, so
//! udev rules decide which devices take part.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant, UNIX_EPOCH},
};

use evdev::{Device, EventType, InputEvent, KeyCode, RelativeAxisCode};
use futures::{
    StreamExt,
    future::ready,
    stream::{self, SelectAll},
};
use futures_core::Stream;
use input_event::{Event, KeyboardEvent, PointerEvent};

use crate::CaptureError;

/// How long to wait for held keys to be released before grabbing anyway.
/// A key held across the grab would stay pressed for the compositor.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

/// Keyboards and pointers grabbed for exclusive use. Dropping it releases
/// the grab.
pub struct DeviceGrab {
    events: SelectAll<Pin<Box<dyn Stream<Item = Event>>>>,
}

impl DeviceGrab {
    /// Grabs every keyboard and pointer that can be opened, after waiting
    /// for held keys to be released.
    pub async fn new() -> Result<Self, CaptureError> {
        let mut devices: Vec<Device> = evdev::enumerate()
            .map(|(_, device)| device)
            .filter(is_grab_candidate)
            .collect();
        if devices.is_empty() {
            return Err(CaptureError::NoGrabbableDevices);
        }

        let start = Instant::now();
        while devices.iter().any(has_pressed_keys) {
            if start.elapsed() > RELEASE_TIMEOUT {
                log::warn!("grabbing input devices while keys are still held");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mut events = SelectAll::new();
        for mut device in devices.drain(..) {
            let name = device.name().unwrap_or("unnamed device").to_owned();
            if let Err(e) = device.grab() {
                log::warn!("failed to grab {name}: {e}");
                continue;
            }
            let translator = Translator::new(&device);
            match device.into_event_stream() {
                Ok(stream) => {
                    log::info!("grabbed input device: {name}");
                    events.push(device_events(stream, name, translator));
                }
                Err(e) => log::warn!("failed to read {name}: {e}"),
            }
        }
        if events.is_empty() {
            return Err(CaptureError::NoGrabbableDevices);
        }
        Ok(Self { events })
    }
}

impl Stream for DeviceGrab {
    type Item = Event;

    /// Ends once every grabbed device is gone.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.events.poll_next_unpin(cx)
    }
}

/// A physical keyboard or pointer. Devices without a physical path are
/// virtual (uinput), such as the output of input emulation tools.
fn is_grab_candidate(device: &Device) -> bool {
    if device.physical_path().is_none_or(str::is_empty) {
        return false;
    }
    let keyboard = device
        .supported_keys()
        .is_some_and(|keys| keys.contains(KeyCode::KEY_A));
    let pointer = device.supported_relative_axes().is_some_and(|axes| {
        axes.contains(RelativeAxisCode::REL_X) && axes.contains(RelativeAxisCode::REL_Y)
    });
    keyboard || pointer
}

fn has_pressed_keys(device: &Device) -> bool {
    device
        .get_key_state()
        .is_ok_and(|keys| keys.iter().next().is_some())
}

fn device_events(
    stream: evdev::EventStream,
    name: String,
    mut translator: Translator,
) -> Pin<Box<dyn Stream<Item = Event>>> {
    let events = stream
        .take_while(move |event| {
            if let Err(e) = event {
                log::warn!("lost grabbed input device {name}: {e}");
            }
            ready(event.is_ok())
        })
        .flat_map(move |event| {
            let event = event.expect("errors end the stream");
            stream::iter(translator.translate(raw(&event)))
        });
    Box::pin(events)
}

#[derive(Clone, Copy, Debug)]
struct RawEvent {
    time: u32,
    kind: u16,
    code: u16,
    value: i32,
}

fn raw(event: &InputEvent) -> RawEvent {
    let millis = event
        .timestamp()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    RawEvent {
        // wraps like the 32-bit millisecond timestamps of Wayland
        time: millis as u32,
        kind: event.event_type().0,
        code: event.code(),
        value: event.value(),
    }
}

/// Turns one device's evdev events into Lan Mouse events. Relative motion
/// and scrolling are summed up per `SYN_REPORT` frame.
#[derive(Debug, Default)]
struct Translator {
    /// the device reports high-resolution (120 per detent) scroll events,
    /// which duplicate its low-resolution ones
    hi_res_wheel: bool,
    hi_res_hwheel: bool,
    dx: i32,
    dy: i32,
    /// vertical and horizontal scroll in 120ths of a detent
    wheel: i32,
    hwheel: i32,
}

impl Translator {
    fn new(device: &Device) -> Self {
        let axes = device.supported_relative_axes();
        let supports = |axis| axes.is_some_and(|axes| axes.contains(axis));
        Self {
            hi_res_wheel: supports(RelativeAxisCode::REL_WHEEL_HI_RES),
            hi_res_hwheel: supports(RelativeAxisCode::REL_HWHEEL_HI_RES),
            ..Default::default()
        }
    }

    fn translate(&mut self, event: RawEvent) -> Vec<Event> {
        match EventType(event.kind) {
            EventType::KEY => self.key(event).into_iter().collect(),
            EventType::RELATIVE => {
                self.relative(event);
                vec![]
            }
            EventType::SYNCHRONIZATION if event.code == SYN_REPORT => self.flush(event.time),
            _ => vec![],
        }
    }

    fn key(&self, event: RawEvent) -> Option<Event> {
        // Autorepeat (2) is left to the receiving side, which repeats
        // held keys itself.
        let state = match event.value {
            0 | 1 => event.value as u8,
            _ => return None,
        };
        let code = event.code;
        Some(if (BTN_MISC..KEY_OK).contains(&code) {
            Event::Pointer(PointerEvent::Button {
                time: event.time,
                button: u32::from(code),
                state: u32::from(state),
            })
        } else {
            Event::Keyboard(KeyboardEvent::Key {
                time: event.time,
                key: u32::from(code),
                state,
            })
        })
    }

    fn relative(&mut self, event: RawEvent) {
        let value = event.value;
        match RelativeAxisCode(event.code) {
            RelativeAxisCode::REL_X => self.dx += value,
            RelativeAxisCode::REL_Y => self.dy += value,
            // evdev scrolls up for positive values, Wayland down
            RelativeAxisCode::REL_WHEEL_HI_RES => self.wheel -= value,
            RelativeAxisCode::REL_WHEEL if !self.hi_res_wheel => self.wheel -= value * 120,
            RelativeAxisCode::REL_HWHEEL_HI_RES => self.hwheel += value,
            RelativeAxisCode::REL_HWHEEL if !self.hi_res_hwheel => self.hwheel += value * 120,
            _ => {}
        }
    }

    fn flush(&mut self, time: u32) -> Vec<Event> {
        let mut events = vec![];
        if self.dx != 0 || self.dy != 0 {
            events.push(Event::Pointer(PointerEvent::Motion {
                time,
                dx: f64::from(self.dx),
                dy: f64::from(self.dy),
            }));
        }
        for (axis, value) in [(AXIS_VERTICAL, self.wheel), (AXIS_HORIZONTAL, self.hwheel)] {
            if value != 0 {
                events.push(Event::Pointer(PointerEvent::AxisDiscrete120 {
                    axis,
                    value,
                }));
            }
        }
        (self.dx, self.dy, self.wheel, self.hwheel) = (0, 0, 0, 0);
        events
    }
}

const SYN_REPORT: u16 = 0;
/// first and one past the last pointer button code
const BTN_MISC: u16 = 0x100;
const KEY_OK: u16 = 0x160;
/// `wl_pointer` axis numbers
const AXIS_VERTICAL: u8 = 0;
const AXIS_HORIZONTAL: u8 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: EventType, code: u16, value: i32) -> RawEvent {
        RawEvent {
            time: 7,
            kind: kind.0,
            code,
            value,
        }
    }

    fn syn() -> RawEvent {
        event(EventType::SYNCHRONIZATION, SYN_REPORT, 0)
    }

    fn rel(axis: RelativeAxisCode, value: i32) -> RawEvent {
        event(EventType::RELATIVE, axis.0, value)
    }

    #[test]
    fn keys_and_buttons_skip_autorepeat() {
        let mut t = Translator::default();
        let key_a = KeyCode::KEY_A.0;
        assert_eq!(
            t.translate(event(EventType::KEY, key_a, 1)),
            vec![Event::Keyboard(KeyboardEvent::Key {
                time: 7,
                key: u32::from(key_a),
                state: 1
            })]
        );
        assert_eq!(t.translate(event(EventType::KEY, key_a, 2)), vec![]);
        let left = KeyCode::BTN_LEFT.0;
        assert_eq!(
            t.translate(event(EventType::KEY, left, 0)),
            vec![Event::Pointer(PointerEvent::Button {
                time: 7,
                button: u32::from(left),
                state: 0
            })]
        );
    }

    #[test]
    fn motion_is_summed_per_frame() {
        let mut t = Translator::default();
        assert_eq!(t.translate(rel(RelativeAxisCode::REL_X, 3)), vec![]);
        assert_eq!(t.translate(rel(RelativeAxisCode::REL_Y, -2)), vec![]);
        assert_eq!(t.translate(rel(RelativeAxisCode::REL_X, 1)), vec![]);
        assert_eq!(
            t.translate(syn()),
            vec![Event::Pointer(PointerEvent::Motion {
                time: 7,
                dx: 4.0,
                dy: -2.0
            })]
        );
        assert_eq!(t.translate(syn()), vec![]);
    }

    #[test]
    fn hi_res_scroll_replaces_low_res_and_follows_wayland_direction() {
        let mut t = Translator {
            hi_res_wheel: true,
            ..Default::default()
        };
        t.translate(rel(RelativeAxisCode::REL_WHEEL, 1));
        t.translate(rel(RelativeAxisCode::REL_WHEEL_HI_RES, 120));
        t.translate(rel(RelativeAxisCode::REL_HWHEEL, 1));
        assert_eq!(
            t.translate(syn()),
            vec![
                Event::Pointer(PointerEvent::AxisDiscrete120 {
                    axis: AXIS_VERTICAL,
                    value: -120
                }),
                Event::Pointer(PointerEvent::AxisDiscrete120 {
                    axis: AXIS_HORIZONTAL,
                    value: 120
                }),
            ]
        );
    }
}
