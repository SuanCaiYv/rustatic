use std::{
    future::Future,
    os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd},
};

use tokio::net::TcpStream;

pub(self) mod download;
pub(crate) mod server;
pub(self) mod upload;
pub(self) mod rename;
pub(self) mod delete;

#[derive(Clone, Copy)]
pub(super) struct UnsafeFD {
    fd: RawFd,
}

impl AsFd for UnsafeFD {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}

#[derive(Clone, Copy)]
pub(super) struct DirectStreamWriter<'a> {
    pub(super) stream: &'a TcpStream,
}

impl<'a> Future for DirectStreamWriter<'a> {
    type Output = anyhow::Result<UnsafeFD>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.stream.poll_write_ready(cx) {
            std::task::Poll::Ready(Ok(_)) => {
                let fd = self.stream.as_raw_fd();
                std::task::Poll::Ready(Ok(UnsafeFD { fd }))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e.into())),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
