//! A simple and easy-to-use asynchronous redis distributed read-write lock implementation based on tokio and redis-rs.
//!
//! It supports the following features:
//!
//! 1. Read-write mutual exclusion: Only one write lock or multiple read locks can exist at the same time.
//! 2. Passive release: When the lock fails to be unlocked due to network or abnormal exit, the lock will be automatically released after the specified timeout.
//! 3. Automatic extension: After the lock is successfully locked, a future will be spawned to automatically extend the lock passive timeout until the lock is actively released. (If the program exits abnormally and the lock is not actively released, the automatic extension will also be terminated and the lock will automatically expire and be released).
//!
//! # Examples
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
//!     // When the key is locked in write mode, the other write or read lock should fail.
//!     assert!(Locker::new(con.clone()).mode(&Mode::W).lock(key.clone()).await.is_err());
//!     assert!(Locker::new(con.clone()).mode(&Mode::R).lock(key.clone()).await.is_err());
//!
//!     // Explicit unlock is required.
//!     // In most cases you should ignore the unlock result.
//!     let _ = w_unlock.await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! 2. Future closure usage
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
//!
//! 3. Custom execution parameters
//! ```rust
//! use rds_lock::{Locker, Mode};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<i32> {
//!     let cli = redis::Client::open("redis://127.0.0.1:6379/0")?;
//!     let con = redis::aio::ConnectionManager::new(cli).await?;
//!     let key = "lock_key".into();
//!     
//!     let locker = Locker::new(con)
//!         .mode(&Mode::R) // Set lock mode, default write-mode.
//!         .to(2000)       // Set milliseconds of lock passive timeout, default 3000.
//!         .rty_int(200)   // Set milliseconds of retry lock interval milliseconds, default 100.
//!         .rty_to(1500)   // Set milliseconds of retry lock timeout milliseconds, default 1000, if set to -1, means retry never timed out.
//!         .ext_int(800);  // Set milliseconds of extend lock interval, default 1000.
//!
//!     // Do something with lock guard
//!     locker.lock_exec(key, async {
//!         let mut r = 0;
//!         for x in 1..10 {
//!             r += x;
//!         }
//!         Ok(r)
//!    })
//! }
//! ```
pub mod lua_script;

use anyhow::{Result, anyhow};
use redis::Script;
use redis::aio::ConnectionLike;
use std::any::Any;
use tokio::time::{Duration, sleep};
use tokio::{select, spawn, sync::oneshot};

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

        let (ext_tx, mut ext_rx) = oneshot::channel();
        let ext_iv = Duration::from_millis(self.ext_iv);

        let ext = spawn(async move {
            let mut ext_ac = self.ext_iv;
            loop {
                select! {
                    _ = &mut ext_rx => break,
                    _ = sleep(ext_iv) => {
                        if extend(&mut conn_c, &mode_c, &key_c, &id_c, self.to).await.is_ok() {
                            ext_ac += self.ext_iv;
                            if ext_ac > self.to {
                            panic!("Failed to extend lock")
                            }
                        }
                        ext_ac = self.ext_iv;
                    }
                }
            }
        });

        let unlock = async move {
            if ext_tx.send(()).is_err() {
                panic!("Failed to stop lock extension");
            }
            ext.await?;
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
#[derive(Clone)]
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

    let script = match mode {
        Mode::R => Script::new(lua_script::R_LOCK),
        Mode::W => Script::new(lua_script::W_LOCK),
    };

    select! {
        r = async move {
            loop {
                if let 1 = script.key(key).arg(&id).arg(to).invoke_async(conn).await? {
                    break Ok(id);
                }
                sleep(Duration::from_millis(rty_iv)).await
            }
        } => r,
        Some(v) = async move {
            match rty_to {
                0.. => {
                    sleep(Duration::from_millis(rty_to as u64)).await;
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
    let script = match mode {
        Mode::R => Script::new(lua_script::R_EXTEND),
        Mode::W => Script::new(lua_script::W_EXTEND),
    };

    match script.key(key).arg(id).arg(to).invoke_async(conn).await? {
        1 => Ok(()),
        _ => Err(anyhow!("Not found")),
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
    let script = match mode {
        Mode::R => Script::new(lua_script::R_UNLOCK),
        Mode::W => Script::new(lua_script::W_UNLOCK),
    };

    match script.key(key).arg(id).invoke_async(conn).await? {
        1 => Ok(()),
        _ => Err(anyhow!("Not found")),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::iter;

    #[tokio::test]
    async fn test_lock_exclusive() {
        let url = "redis://:c6bfb872-49f6-48bc-858d-2aca0c020702@127.0.0.1:8003/0";
        let cli = redis::Client::open(url).unwrap();
        let cfg = redis::aio::ConnectionManagerConfig::new().set_max_delay(1000);
        let con = redis::aio::ConnectionManager::new_with_config(cli, cfg)
            .await
            .unwrap();
        let key = String::from("test:lock_key");

        // Should concurrently lock key multiple times with read-mode.
        let r_locks: Vec<_> = iter::repeat_with(|| {
            let con = con.clone();
            let key = key.clone();
            spawn(async move { Locker::new(con).mode(&Mode::R).lock(key).await })
        })
        .take(10)
        .collect();

        // Wait for lock future completed and collect unlock futures.
        let mut r_unlocks = Vec::new();
        for r_lock in r_locks {
            r_unlocks.push(r_lock.await.unwrap().unwrap());
        }

        // Should automatically extend the passive timeout of lock in read-mode.
        sleep(Duration::from_secs(5)).await;

        // Should not lock key with write-mode when it has been locked with read-mode.
        assert!(
            Locker::new(con.clone())
                .mode(&Mode::W)
                .lock(key.clone())
                .await
                .is_err()
        );

        // Should unlock key locked in read-mode.
        for r_unlock in r_unlocks {
            r_unlock.await.unwrap();
        }

        // Should lock key with write-mode.
        let w_unlock = Locker::new(con.clone())
            .mode(&Mode::W)
            .lock(key.clone())
            .await
            .unwrap();

        // Should automatically extend the passive timeout of lock in write-mode.
        sleep(Duration::from_secs(5)).await;

        // Should not lock key with write-mode when it has been locked with write-mode.
        assert!(
            Locker::new(con.clone())
                .mode(&Mode::W)
                .lock(key.clone())
                .await
                .is_err()
        );

        // Should not lock key with read-mode when it has been locked with write-mode.
        assert!(
            Locker::new(con.clone())
                .mode(&Mode::R)
                .lock(key.clone())
                .await
                .is_err()
        );

        // Should unlock key locked in write-mode.
        w_unlock.await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_exec() {
        let url = "redis://:c6bfb872-49f6-48bc-858d-2aca0c020702@127.0.0.1:8003/0";
        let cli = redis::Client::open(url).unwrap();
        let cfg = redis::aio::ConnectionManagerConfig::new().set_max_delay(1000);
        let con = redis::aio::ConnectionManager::new_with_config(cli, cfg)
            .await
            .unwrap();
        let key = String::from("test:lock_key_exec");

        // Should exec with lock guard, and return the closure returned.
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
