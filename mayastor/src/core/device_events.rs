use std::{cell::RefCell, rc::Rc};

/// TODO
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DeviceEventType {
    /// TODO
    DeviceRemoved,

    /// TODO
    DeviceResized,

    /// TODO
    MediaManagement,

    /// TODO
    AdminCommandCompletionFailed,
}

/// TODO
pub trait DeviceEventHandler {
    /// TODO
    fn handle_device_event(&mut self, evt: DeviceEventType, dev_name: &str);
}

/// TODO
trait DeviceEventTarget {
    /// TODO
    fn dispatch(&mut self, evt: DeviceEventType, dev_name: &str);
}

/// TODO
struct DeviceEventCallbackTarget<'a> {
    /// TODO
    callback: Box<dyn Fn(DeviceEventType, &str) + 'a>,
}

impl DeviceEventTarget for DeviceEventCallbackTarget<'_> {
    fn dispatch(&mut self, evt: DeviceEventType, dev_name: &str) {
        (self.callback)(evt, dev_name);
    }
}

/// TODO
struct DeviceEventHandlerTarget<'a> {
    /// TODO
    dst: &'a mut dyn DeviceEventHandler,
}

impl DeviceEventTarget for DeviceEventHandlerTarget<'_> {
    fn dispatch(&mut self, event: DeviceEventType, device_name: &str) {
        self.dst.handle_device_event(event, device_name);
    }
}

/// TODO
#[derive(Clone)]
pub struct DeviceEventListener {
    tgt: Rc<RefCell<dyn DeviceEventTarget>>,
}

unsafe impl Send for DeviceEventListener {}
unsafe impl Sync for DeviceEventListener {}

impl From<&mut dyn DeviceEventHandler> for DeviceEventListener {
    fn from(dst: &mut dyn DeviceEventHandler) -> Self {
        // Extend destination's lifetime to match the static lifetime of
        // listener container(s).
        // TODO: describe why it is safe to do
        let dst = unsafe {
            std::mem::transmute::<_, &'static mut dyn DeviceEventHandler>(dst)
        };

        Self::new(Rc::new(RefCell::new(DeviceEventHandlerTarget {
            dst,
        })))
    }
}

impl DeviceEventListener {
    /// TODO
    /// Note: is it possible to have it as From<> implementation?
    pub fn from_fn(callback: fn(DeviceEventType, &str)) -> Self {
        // // Extend destination's lifetime to match the static lifetime of
        // // listeners.
        // // TODO: describe why it is safe to do
        // let callback = unsafe {
        //     std::mem::transmute::<_, &'static dyn Fn(DeviceEventType, &str)>(
        //         callback,
        //     )
        // };

        Self::new(Rc::new(RefCell::new(DeviceEventCallbackTarget {
            callback: Box::new(callback),
        })))
    }

    /// TODO
    fn new(tgt: Rc<RefCell<dyn DeviceEventTarget>>) -> Self {
        Self {
            tgt,
        }
    }

    /// TODO
    pub fn notify(&mut self, evt: DeviceEventType, dev_name: &str) {
        self.tgt.borrow_mut().dispatch(evt, dev_name)
    }
}
