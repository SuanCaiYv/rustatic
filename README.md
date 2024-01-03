## A static file server write in Rust

With the goal of fast, we choose zero-copying technology in Linux with `sendfile` or `mmap` also, the `splice` is included for test.

We support plaintext transfer and 'tls' transfer, the 'tls' is not as same as the normal in https or ftps, it required encrypt first before storage on disk; not encrypt in-place. This is usually done on client.

## Usage

## Implementation
About download operation, we choose `sendfile` + `mmap` to compatible with event-loop model and zero-copying.

 - First, select file partition that need to send, using `mmap` to load content into page cache in kernel space, this is a block call.

 - Notify event-loop thread to call `sendfile` to send the content.

 - Update partition of the file.

 - Loop for this until all content sent.

By test, using async context, such as `Tokio`, the writeable state of socket is inaccurate sometimes, and this is talked in tokio document. So we may choose `mio` or `poll` to implement the event-loop.

`sendfile` will copy from kernel space to socket buffer unless the machine support `DMA Scatter/Gather Copy`, this is implemented by hardware. But `splice` support completely zero-copying, why we not using it for now? the discussion over [StackOverflow](https://stackoverflow.com/a/25265665) gives the answer: using splice takes more concerns into development, and setting buffer size for cache data in kernel space is hard to achieve. Also, there are some bugs for using. although `sendfile` itself is a wrapper over `splice`, but linux do some additional works to make it easy for use, so that is the answer we choosing it.

Using `mmap` to load file into memory will raising memory usage. So after every call to `mmap`, call `munmap` to notify kernel that we don't need this map anymore, also `madvise` is called with `M_DONTNEED` flag setted to drop momory immediately. But this also acts as a advise for kernel.

## Limit

the transmission not support UDP, case the UDP protocol need user-space work to reach reliable transmission, and this violates the goal of zero-copying.
