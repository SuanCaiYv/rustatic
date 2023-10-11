## A static file server write in Rust

with the goal of fast, we choose zero-copying technology in Linux with `sendfile` of `mmap`, also the `splice` is included for test.

we support plaintext transfer and 'tls' transfer, the 'tls' is not as same as the normal in https of ftps, it required encrypt first before storage on disk; not encrypt in-place.

## Usage

## Limit

the transmission not support UDP, case the UDP protocol need user-space work to reach reliable transmission, and this violates the goal of zero-copying.
