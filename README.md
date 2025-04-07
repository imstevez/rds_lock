# rds_lock

A simple and easy-to-use asynchronous redis distributed read-write lock implementation based on tokio and redis-rs.

It supports the following features:

1. Read-write mutual exclusion: Only one write lock or multiple read locks can exist at the same time.
2. Passive release: When the lock fails to be unlocked due to network or abnormal exit, the lock will be automatically
   released after the specified timeout.
3. Automatic extension: After the lock is successfully locked, a Future will be spawned to automatically extend the lock
   time until the lock is actively released. (If the program exits abnormally and the lock is not actively released, the
   automatic extension will also be terminated and the lock will automatically expire and be released).

## Examples

### 1. General usage

```rust
use rds_lock::{Locker, Mode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = redis::Client::open("redis://127.0.0.1:6379")?;
    let con = redis::aio::ConnectionManager::new(cli).await?;
    let key = "lock_key".into();

    // Lock key with default write mode, and 
    // the passive timeout will be automatically extend 
    // util this lock unlocked.
    let w_unlock = Locker::new(con.clone()).lock(key.clone()).await?;

    // Do something with lock guard.
    for x in 1..10 {
        println!("{}", x);
    }

    // When key is locked in write mode, the other write or read lock should fail.
    assert!(Locker::new(con.clone()).mode(&Mode::W).lock(key.clone()).await.is_err());
    assert!(Locker::new(con.clone()).mode(&Mode::R).lock(key.clone()).await.is_err());

    // Explicit unlock is required.
    // In most cases you should ignore unlock errors.
    let _ = w_unlock.await?;

    Ok(())
}
 ```

### 2. Future closure usage

```rust
use rds_lock::{Locker, Mode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = redis::Client::open("redis://127.0.0.1:6379")?;
    let con = redis::aio::ConnectionManager::new(cli).await?;
    let key = "lock_key".into();

    // Do something in closure with lock guard, no explicit unlock needed.
    // If the lock is successful, lock_exec finally returns 
    // the return value of the closure.
    Locker::new(con.clone()).lock_exec(key.clone(), async {
        for x in 1..10 {
            println!("{}", x);
        }
        Ok(())
    }).await
}
```

### 3. Custom execution parameters

```rust
use rds_lock::{Locker, Mode};
//!
#[tokio::main]
async fn main() -> anyhow::Result<i32> {
    let cli = redis::Client::open("redis://127.0.0.1:6379/0")?;
    let con = redis::aio::ConnectionManager::new(cli).await?;
    let key = "lock_key".into();

    let locker = Locker::new(con)
        .mode(&Mode::R) // Set lock mode, default write-mode.
        .to(2000)       // Set milliseconds of lock passive timeout, default 3000.
        .rty_int(200)   // Set milliseconds of retry lock interval milliseconds, default 100.
        .rty_to(1500)   // Set milliseconds of retry lock timeout milliseconds, default 1000, if set to -1, means retry never timed out.
        .ext_int(800);  // Set milliseconds of extend lock interval, default 1000.

    // Do something with lock guard
    locker.lock_exec(key, async {
        let mut r = 0;
        for x in 1..10 {
            r += x;
        }
        Ok(r)
    })
}
```
