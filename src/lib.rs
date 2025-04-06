//! This library is an asynchronous Redis distributed read-write lock based on redis-rs aio::ConnectionLike. It supports the following features:
//!
//! 1. Read-write mutual exclusion: Only one write lock or multiple read locks can exist at the same time.
//! 2. Passive release: When the lock fails to be unlocked due to network or abnormal exit, the lock will be automatically released after the specified timeout.
//! 3. Automatic extension: After the lock is successfully locked, the tokio thread will be started to automatically extend the lock time until the lock is actively released. (If the program exits abnormally and the lock is not actively released, the automatic extension will also be terminated and the lock will automatically expire and be released).
//!
//! Examples
//!
//! 1. General usage
//! ```rust
//! use rds_lock::{Locker, Mode};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let cli = redis::Client::open("redis://127.0.0.1:6379/0")?;
//!     let con = redis::aio::ConnectionManager::new(cli).await?;
//!     let key = "lock_key".into();
//!
//!     // Lock key with default write mode, and
//!     // the passive timeout will be automatically extend
//!     // util this lock unlocked.
//!     let w_unlock = Locker::new(con.clone()).lock(key.clone()).await?;
//!
//!     // Do something with lock guard.
//!     for x in 1..10 {
//!         println!("{}", x);
//!     }
//!
//!     // When key is locked in write mode, the other write or read lock should fail.
//!     assert!(Locker::new(con.clone()).mode(&Mode::W).lock(key.clone()).await.is_err());
//!     assert!(Locker::new(con.clone()).mode(&Mode::R).lock(key.clone()).await.is_err());
//!
//!     // Explicit unlock is required.
//!     // In most cases you should ignore unlock errors.
//!     let _ = w_unlock.await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! 2. Closure usage
//! ```rust
//! use rds_lock::{Locker, Mode};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let cli = redis::Client::open("redis://127.0.0.1:6379/0")?;
//!     let con = redis::aio::ConnectionManager::new(cli).await?;
//!     let key = "lock_key".into();
//!
//!     // Do something in closure with lock guard, no explicit unlock needed.
//!     // If the lock is successful, lock_exec finally returns
//!     // the return value of the closure.
//!     let r = Locker::new(con.clone()).lock_exec(key.clone(), async {
//!         for x in 1..10 {
//!             println!("{}", x);
//!         }
//!         Ok(())
//!     }).await;
//!
//!     r
//! }
//! ```
pub mod lua_script;

use anyhow::{Result, anyhow};
use derive_more::Display;
use redis::Script;
use redis::aio::ConnectionLike;
use std::any::Any;
use tokio::select;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

/// Locker provides an easy-to-use lock builder and bundles the function of
/// automatic passive timeout extend.
pub struct Locker<T: ConnectionLike> {
    /// Redis connection.
    conn: T,

    /// Lock mode: read or write.
    mode: Mode,

    /// Milliseconds of timeout.
    to: u64,

    /// Milliseconds of retry interval.
    rty_iv: u64,

    /// Milliseconds of retry timout, negative means retry until success.
    rty_to: i64,

    /// Milliseconds of extend interval.
    ext_iv: u64,
}

impl<T: ConnectionLike + Send + Clone + 'static> Locker<T> {
    pub fn new(conn: T) -> Self {
        Self {
            conn,
            mode: Mode::W,
            to: 3000,
            rty_iv: 100,
            rty_to: 1000,
            ext_iv: 1000,
        }
    }

    pub fn mode(mut self, mode: &Mode) -> Self {
        self.mode = mode.clone();
        self
    }

    pub fn to(mut self, ms: u64) -> Self {
        self.to = ms;
        self
    }

    pub fn rty_int(mut self, ms: u64) -> Self {
        self.rty_iv = ms;
        self
    }

    pub fn rty_to(mut self, ms: i64) -> Self {
        self.rty_to = ms;
        self
    }

    pub fn ext_int(mut self, ms: u64) -> Self {
        self.ext_iv = ms;
        self
    }

    /// Lock the specified key, and spawned an async task to automatically extend
    /// the passive timeout of the lock.
    /// It returns an unlock future used to stop the extent task and do unlock.
    pub async fn lock(mut self, key: String) -> Result<impl Future<Output = Result<()>>> {
        let id = lock(
            &mut self.conn,
            &self.mode,
            &key,
            self.to,
            self.rty_iv,
            self.rty_to,
        )
        .await?;

        let mut conn_c = self.conn.clone();
        let mode_c = self.mode.clone();
        let key_c = key.clone();
        let id_c = id.clone();

        let ext_cancel_t = CancellationToken::new();
        let ext_cancel_r = ext_cancel_t.clone();
        let ext_itv = Duration::from_millis(self.ext_iv);

        let ext_handle = tokio::spawn(async move {
            let mut ext_ac = self.ext_iv;
            loop {
                select! {
                    _ = ext_cancel_r.cancelled() => break,
                    _ = sleep(ext_itv) => {
                        if extend(&mut conn_c, &mode_c, &key_c, &id_c, self.to).await.is_err() {
                            ext_ac += self.ext_iv;
                            if ext_ac > self.to {
                                panic!("Extend failed")
                            }
                        }
                        ext_ac = self.ext_iv;
                    }
                }
            }
        });

        let unlock = async move {
            ext_cancel_t.cancel();
            ext_handle.await?;
            unlock(&mut self.conn, &self.mode, &key, &id).await
        };

        Ok(unlock)
    }

    /// Executed the specified closure function with lock guard of the specified key.
    /// It returns result of the closure, and result of unlock will be ignored.
    /// Todo: Use macros to achieve the same functionality
    pub async fn lock_exec<V: Any, F: Future<Output = Result<V>>>(
        self,
        key: String,
        f: F,
    ) -> Result<V> {
        let unlock = self.lock(key.to_string()).await?;
        let r = f.await;
        let _ = unlock.await;
        r
    }
}

/// Lock mode
#[derive(Clone, Display)]
pub enum Mode {
    /// Read mode
    R,

    /// Write mode
    W,
}

/// Set a lock with the specified key, returns the lock id.
/// If the key is exclusive with the other locks, it will be retried util timed out.
pub async fn lock<T: ConnectionLike>(
    // Redis async connection.
    conn: &mut T,
    // Lock mode: read or write.
    mode: &Mode,
    // Key of the lock.
    key: &str,
    // Passive timeout milliseconds.
    to: u64,
    // Retry interval milliseconds.
    rty_iv: u64,
    // Retry timeout milliseconds, negative value means it will be retried until succeeds.
    rty_to: i64,
) -> Result<String> {
    let id = uuid::Uuid::new_v4().to_string();

    log::info!("LCK<{}> [BG] {} {}", mode, key, id);

    let script = match mode {
        Mode::R => Script::new(lua_script::R_LOCK),
        Mode::W => Script::new(lua_script::W_LOCK),
    };

    select! {
        r = async move {
            loop {
                if let 1 = script.key(key).arg(&id).arg(to).invoke_async(conn).await? {
                    log::info!("LCK<{}> [OK] {} {}", mode, key, id);
                    break Ok(id);
                }
                sleep(Duration::from_millis(rty_iv)).await
            }
        } => r,
        Some(v) = async move {
            match rty_to {
                0.. => {
                    sleep(Duration::from_millis(rty_to as u64)).await;
                    log::info!("LCK<{}> [TO] {}", mode, key);
                    Some(Err(anyhow!("Timed out")))
                }
                _ => None,
            }
        } => v,
    }
}

/// Extend passive timeout duration of the specified lock with its key and id.
/// It returns a not found error when the key or id not matched or the lock has been unlocked.
/// Note: For an expired read lock, if there are other valid read locks,
/// it can still be extended successfully.
pub async fn extend<T: ConnectionLike>(
    // Redis async connection.
    conn: &mut T,
    // Lock mode: read or write.
    mode: &Mode,
    // Key of the lock.
    key: &str,
    // Lock id.
    id: &str,
    // Passive timeout milliseconds.
    to: u64,
) -> Result<()> {
    log::info!("EXT<{}> [BG] {} {}", mode, key, id);

    let script = match mode {
        Mode::R => Script::new(lua_script::R_EXTEND),
        Mode::W => Script::new(lua_script::W_EXTEND),
    };

    match script.key(key).arg(id).arg(to).invoke_async(conn).await? {
        1 => {
            log::info!("EXT<{}> [OK] {} {}", mode, key, id);
            Ok(())
        }
        _ => {
            log::info!("EXT<{}> [NF] {} {}", mode, key, id);
            Err(anyhow!("Not found"))
        }
    }
}

/// Unlock the specified lock with its key and id.
/// It returns a not found error when the key or id not matched or the lock has been unlocked.
/// Note: For an expired read lock, if there are other valid read locks,
/// it can still be unlocked successfully.
pub async fn unlock<T: ConnectionLike>(
    // Redis async connection.
    conn: &mut T,
    // Lock mode: read or write.
    mode: &Mode,
    // Key of the lock.
    key: &str,
    // Lock id.
    id: &str,
) -> Result<()> {
    log::info!("UCK<{}> [BG] {} {}", mode, key, id);

    let script = match mode {
        Mode::R => Script::new(lua_script::R_UNLOCK),
        Mode::W => Script::new(lua_script::W_UNLOCK),
    };

    match script.key(key).arg(id).invoke_async(conn).await? {
        1 => {
            log::info!("UCK<{}> [OK] {} {}", mode, key, id);
            Ok(())
        }
        _ => {
            log::info!("UCK<{}> [NF] {} {}", mode, key, id);
            Err(anyhow!("Not found"))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::stream::{self, StreamExt};
    use std::iter;

    #[tokio::test]
    async fn test_lock_exclusive() {
        simple_logger::init_with_level(log::Level::Info).unwrap();

        let url = "redis://:c6bfb872-49f6-48bc-858d-2aca0c020702@127.0.0.1:8003/0";
        let cli = redis::Client::open(url).unwrap();
        let cfg = redis::aio::ConnectionManagerConfig::new().set_max_delay(1000);
        let con = redis::aio::ConnectionManager::new_with_config(cli, cfg)
            .await
            .unwrap();
        let key = String::from("test:lock_key");

        log::info!("Should lock read-locks concurrently...");
        let r_unlocks: Vec<_> = stream::iter(
            iter::repeat_with(|| {
                let con = con.clone();
                let key = key.clone();
                tokio::spawn(async move { Locker::new(con).mode(&Mode::R).lock(key).await })
            })
            .take(10)
            .collect::<Vec<_>>(),
        )
        .then(|lck_handle| async move { lck_handle.await.unwrap() })
        .map(|lck_result| lck_result.unwrap())
        .collect()
        .await;

        log::info!("Should extend read-locks automatically...");
        sleep(Duration::from_secs(5)).await;

        log::info!("Should not lock write-lock when read-lock exists...");
        assert!(
            Locker::new(con.clone())
                .mode(&Mode::W)
                .lock(key.clone())
                .await
                .is_err()
        );

        log::info!("Should unlock read-locks...");
        stream::iter(r_unlocks)
            .for_each(|unlock| async {
                unlock.await.unwrap();
            })
            .await;

        log::info!("Should lock write-lock...");
        let w_unlock = Locker::new(con.clone())
            .mode(&Mode::W)
            .lock(key.clone())
            .await
            .unwrap();

        log::info!("Should extend write-lock automatically...");
        sleep(Duration::from_secs(5)).await;

        log::info!("Should not lock write-lock when write-lock exists...");
        assert!(
            Locker::new(con.clone())
                .mode(&Mode::W)
                .lock(key.clone())
                .await
                .is_err()
        );

        log::info!("Should not lock read-lock when write-lock exists...");
        assert!(
            Locker::new(con.clone())
                .mode(&Mode::R)
                .lock(key.clone())
                .await
                .is_err()
        );

        log::info!("Should unlock write-lock...");
        w_unlock.await.unwrap();
    }
    #[tokio::test]
    async fn test_lock_exec() {
        simple_logger::init_with_level(log::Level::Info).unwrap();

        let url = "redis://:c6bfb872-49f6-48bc-858d-2aca0c020702@127.0.0.1:8003/0";
        let cli = redis::Client::open(url).unwrap();
        let cfg = redis::aio::ConnectionManagerConfig::new().set_max_delay(1000);
        let con = redis::aio::ConnectionManager::new_with_config(cli, cfg)
            .await
            .unwrap();
        let key = String::from("test:lock_key");

        log::info!("Should exec with lock guard, and return the closure returned...");

        let r = Locker::new(con)
            .mode(&Mode::W)
            .lock_exec(key, async {
                sleep(Duration::from_secs(5)).await;
                Ok(1)
            })
            .await
            .unwrap();

        assert_eq!(r, 1);
    }
}
