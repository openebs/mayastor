extern crate once_cell;
use std::cell::UnsafeCell;

/// TODO
pub trait Singleton {
    fn get_or_init() -> &'static mut Self;
}

/// TODO
pub struct SingletonCell<T>
where
    T: Default,
{
    cell: UnsafeCell<T>,
}

unsafe impl<T> Sync for SingletonCell<T> where T: Default {}
unsafe impl<T> Send for SingletonCell<T> where T: Default {}

impl<T> SingletonCell<T>
where
    T: Default,
{
    /// TODO
    pub fn new() -> Self {
        Self {
            cell: UnsafeCell::new(T::default()),
        }
    }

    /// TODO
    pub fn get(&self) -> &'static mut T {
        unsafe { &mut *self.cell.get() }
    }
}

/// TODO
macro_rules! singleton {
    ($id:ident) => {
        impl Singleton for $id {
            fn get_or_init() -> &'static mut Self {
                type Store = SingletonCell<$id>;
                type CellType = once_cell::sync::OnceCell<Store>;
                static INST: CellType = CellType::new();
                INST.get_or_init(|| Store::new()).get()
            }
        }
    };
}
