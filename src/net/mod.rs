use std::os::fd::{AsFd, BorrowedFd, RawFd};

pub(crate) mod server;
pub(self) mod download;
pub(self) mod upload;

#[derive(Clone, Copy)]
pub(super) struct UnsafeFD {
    fd: RawFd,
}

impl AsFd for UnsafeFD {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}