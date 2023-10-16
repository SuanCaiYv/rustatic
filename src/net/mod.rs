use std::os::fd::{AsFd, BorrowedFd, RawFd};

pub(crate) mod server;
pub(self) mod download;

#[derive(Clone, Copy)]
pub(self) struct UnsafeFD {
    fd: RawFd,
}

impl AsFd for UnsafeFD {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}