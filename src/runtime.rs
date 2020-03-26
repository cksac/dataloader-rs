// runtime-async-std
#[cfg(feature = "runtime-async-std")]
pub type Arc<T> = async_std::sync::Arc<T>;

#[cfg(feature = "runtime-async-std")]
pub type Mutex<T> = async_std::sync::Mutex<T>;

#[cfg(feature = "runtime-async-std")]
pub use async_std::task::yield_now;

// runtime-tokio
#[cfg(feature = "runtime-tokio")]
pub type Arc<T> = std::sync::Arc<T>;

#[cfg(feature = "runtime-tokio")]
pub type Mutex<T> = tokio::sync::Mutex<T>;

#[cfg(feature = "runtime-tokio")]
pub use tokio::task::yield_now;
