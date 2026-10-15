//! Device grabs need evdev, which is only available on Linux.

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use input_event::Event;

use crate::CaptureError;

pub struct DeviceGrab(Infallible);

impl DeviceGrab {
    pub async fn new() -> Result<Self, CaptureError> {
        Err(CaptureError::DeviceGrabUnsupported)
    }
}

impl Stream for DeviceGrab {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Event>> {
        match self.0 {}
    }
}
