use std::{
    fmt::{Debug, Error, Formatter},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};

/// TODO
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DeviceEventType {
    /// Device has been removed.
    DeviceRemoved,
    /// Special case for loopback device removal: loopback devices are not
    /// real SPDK bdevs but bdev aliases. Deleting an alias won't make
    /// SPDK send a proper bdev remove event.
    LoopbackRemoved,
    /// Device has been resized.
    DeviceResized,
    /// TODO
    MediaManagement,
    /// TODO
    AdminCommandCompletionFailed,
}

/// TODO
pub trait DeviceEventListener {
    /// TODO
    fn handle_device_event(
        self: Pin<&mut Self>,
        evt: DeviceEventType,
        dev_name: &str,
    );

    /// TODO
    fn get_listener_name(&self) -> String {
        "unnamed device event listener".to_string()
    }
}

/// TODO
struct ListenerPtr(*mut dyn DeviceEventListener);

unsafe impl Send for ListenerPtr {}

/// TODO
struct SinkInner {
    cell: Mutex<ListenerPtr>,
}

impl SinkInner {
    /// TODO
    fn new(lst: Pin<&mut dyn DeviceEventListener>) -> Self {
        let p = unsafe { Pin::into_inner_unchecked(lst) };
        let p = unsafe {
            std::mem::transmute::<
                &mut dyn DeviceEventListener,
                &'static mut dyn DeviceEventListener,
            >(p)
        };

        Self {
            cell: Mutex::new(ListenerPtr(p)),
        }
    }

    /// TODO
    fn dispatch_event(&self, evt: DeviceEventType, dev_name: &str) {
        unsafe {
            let mut cell = self.cell.lock().unwrap();
            Pin::new_unchecked(&mut *cell.0).handle_device_event(evt, dev_name);
        }
    }

    /// TODO
    fn get_listener_name(&self) -> String {
        unsafe {
            let cell = self.cell.lock().unwrap();
            (*cell.0).get_listener_name()
        }
    }
}

/// A reference for a device event listener.
/// This object behaves like a reference counted reference to dispatcher's inner
/// representation of event listener instance.
#[derive(Clone)]
pub struct DeviceEventSink {
    inner: Arc<SinkInner>,
}

impl Debug for DeviceEventSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        // TODO
        write!(f, "--device event sink--")
    }
}

impl DeviceEventSink {
    /// TODO
    pub fn new(lst: Pin<&mut dyn DeviceEventListener>) -> Self {
        Self {
            inner: Arc::new(SinkInner::new(lst)),
        }
    }

    /// Consumes a event listener reference and returns a weak listener
    /// reference.
    fn into_weak(self) -> Weak<SinkInner> {
        Arc::downgrade(&self.inner)
    }

    /// TODO
    pub fn get_listener_name(&self) -> String {
        self.inner.get_listener_name()
    }
}

/// TODO
#[derive(Default)]
pub struct DeviceEventDispatcher {
    listeners: Mutex<Vec<Weak<SinkInner>>>,
}

impl DeviceEventDispatcher {
    /// Creates a new instance of device event dispatcher.
    pub fn new() -> Self {
        Default::default()
    }

    /// Adds a new event listener reference.
    /// The client code must retain a clone for the added reference in order to
    /// keep events coming. As long as the client code drops the last
    /// reference to an event listener, the dispatcher stops delivering events
    /// to it.
    ///
    /// # Arguments
    ///
    /// * `listener`: Reference to an event listener.
    pub fn add_listener(&self, listener: DeviceEventSink) {
        self.listeners.lock().unwrap().push(listener.into_weak());
        self.purge();
    }

    /// Dispatches an event to all registered listeners.
    /// Returns the number of listeners notified about target event.
    pub fn dispatch_event(&self, evt: DeviceEventType, dev_name: &str) -> u64 {
        let mut listeners = Vec::new();

        // To avoid potential deadlocks we never call the listeners with the
        // mutex held, just find all suitable listeners and save them
        // for further invocation.
        self.listeners.lock().unwrap().iter_mut().for_each(|dst| {
            if let Some(p) = dst.upgrade() {
                listeners.push(Arc::clone(&p));
            }
        });

        // Invoke all listeners once the mutex is dropped.
        let notified = {
            if !listeners.is_empty() {
                for l in &listeners {
                    l.dispatch_event(evt, dev_name);
                }
                listeners.len() as u64
            } else {
                0
            }
        };
        self.purge();
        notified
    }

    /// Returns the number of registered listeners.
    pub fn count(&self) -> usize {
        self.listeners.lock().unwrap().iter().fold(0, |acc, x| {
            if x.strong_count() > 0 {
                acc + 1
            } else {
                acc
            }
        })
    }

    /// Removes all dropped listeners.
    fn purge(&self) {
        self.listeners
            .lock()
            .unwrap()
            .retain(|x| x.strong_count() > 0);
    }
}
