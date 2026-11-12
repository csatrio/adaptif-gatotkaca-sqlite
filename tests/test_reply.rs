// ============================================================================
// Import Definition
// ============================================================================

use adaptif_gatotkaca_sqlite::reply::*;

use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
    time::{Duration, Instant},
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Counts how many times it was dropped.
struct GkTestCounted {
    drops: Arc<AtomicUsize>,
}

impl Drop for GkTestCounted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn counted() -> (GkTestCounted, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (
        GkTestCounted {
            drops: Arc::clone(&drops),
        },
        drops,
    )
}

/// Waker that unparks a thread and counts wakeups.
struct GkTestThreadWaker {
    thread: Thread,
    wakes: AtomicUsize,
}

impl Wake for GkTestThreadWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.thread.unpark();
    }
}

fn thread_waker() -> (Arc<GkTestThreadWaker>, Waker) {
    let inner = Arc::new(GkTestThreadWaker {
        thread: thread::current(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&inner));
    (inner, waker)
}

/// Tiny executor: polls until ready, parking between polls, with a hard time bound.
fn block_on<F: Future>(future: F, limit: Duration) -> F::Output {
    let (_inner, waker) = thread_waker();
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let deadline = Instant::now() + limit;
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
        let now = Instant::now();
        assert!(now < deadline, "block_on exceeded {limit:?}");
        thread::park_timeout(deadline - now);
    }
}

fn after<F: FnOnce() + Send + 'static>(delay: Duration, f: F) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        thread::sleep(delay);
        f();
    })
}

/// Runs `f` on a thread and fails the test if it does not finish within `limit`.
fn bounded<F: FnOnce() + Send + 'static>(limit: Duration, f: F) {
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(limit)
        .unwrap_or_else(|_| panic!("test body exceeded {limit:?}"));
    handle.join().expect("test body panicked");
}

const LIMIT: Duration = Duration::from_secs(10);

// ============================================================================
// Tests
// ============================================================================

#[test]
fn error_codes_and_display() {
    assert_ne!(GkReplyError::DISCONNECTED, GkReplyError::TIMEOUT);
    assert!(
        GkReplyError::DISCONNECTED
            .message()
            .contains("disconnected")
    );
    assert!(GkReplyError::TIMEOUT.message().contains("timed out"));
    assert_eq!(
        GkReplyError::TIMEOUT.to_string(),
        GkReplyError::TIMEOUT.message()
    );
    let boxed: Box<dyn std::error::Error> = Box::new(GkReplyError::DISCONNECTED);
    assert!(!boxed.to_string().is_empty());
    assert_eq!(std::mem::size_of::<GkReplyError>(), 1);
}

#[test]
fn types_are_send() {
    fn assert_send<T: Send>() {}
    assert_send::<GkReplySender<Vec<u8>>>();
    assert_send::<GkReplyReceiver<Vec<u8>>>();
    assert_send::<GkReplyReceiver<Box<dyn Any + Send>>>();
}

#[test]
fn value_sent_before_wait() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u64>();
        tx.send(7);
        assert!(rx.is_ready());
        assert_eq!(rx.wait(), Ok(7));
    });
}

#[test]
fn value_sent_while_parked() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<String>();
        let h = after(Duration::from_millis(20), move || tx.send("hi".to_string()));
        let start = Instant::now();
        assert_eq!(rx.wait().as_deref(), Ok("hi"));
        assert!(start.elapsed() >= Duration::from_millis(10));
        h.join().unwrap();
    });
}

#[test]
fn sender_dropped_before_wait() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u32>();
        drop(tx);
        assert!(rx.is_ready());
        assert_eq!(rx.wait(), Err(GkReplyError::DISCONNECTED));
    });
}

#[test]
fn sender_dropped_while_parked() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u32>();
        let h = after(Duration::from_millis(20), move || drop(tx));
        assert_eq!(rx.wait(), Err(GkReplyError::DISCONNECTED));
        h.join().unwrap();
    });
}

#[test]
fn wait_timeout_gets_value_in_time() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u8>();
        let h = after(Duration::from_millis(10), move || tx.send(9));
        assert_eq!(rx.wait_timeout(Duration::from_secs(5)), Ok(9));
        h.join().unwrap();
    });
}

#[test]
fn wait_timeout_zero_and_disconnect() {
    bounded(LIMIT, || {
        let (_tx, rx) = reply_channel::<u8>();
        assert_eq!(rx.wait_timeout(Duration::ZERO), Err(GkReplyError::TIMEOUT));
        let (tx, rx) = reply_channel::<u8>();
        drop(tx);
        assert_eq!(
            rx.wait_timeout(Duration::ZERO),
            Err(GkReplyError::DISCONNECTED)
        );
        let (tx, rx) = reply_channel::<u8>();
        let h = after(Duration::from_millis(10), move || drop(tx));
        assert_eq!(
            rx.wait_timeout(Duration::from_secs(5)),
            Err(GkReplyError::DISCONNECTED)
        );
        h.join().unwrap();
    });
}

#[test]
fn wait_timeout_then_late_send_drops_value_once() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<GkTestCounted>();
        let (value, drops) = counted();
        let h = after(Duration::from_millis(80), move || tx.send(value));
        let start = Instant::now();
        let res = rx.wait_timeout(Duration::from_millis(20));
        assert!(matches!(res, Err(GkReplyError::TIMEOUT)));
        assert!(start.elapsed() < Duration::from_millis(70));
        h.join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn wait_timeout_then_late_sender_drop_is_quiet() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u8>();
        let h = after(Duration::from_millis(50), move || drop(tx));
        assert_eq!(
            rx.wait_timeout(Duration::from_millis(10)),
            Err(GkReplyError::TIMEOUT)
        );
        h.join().unwrap();
    });
}

#[test]
fn try_take_states() {
    bounded(LIMIT, || {
        let (tx, mut rx) = reply_channel::<u32>();
        assert!(!rx.is_ready());
        assert_eq!(rx.try_take(), Ok(None));
        assert_eq!(rx.try_take(), Ok(None));
        tx.send(5);
        assert!(rx.is_ready());
        assert_eq!(rx.try_take(), Ok(Some(5)));
        assert!(!rx.is_ready());
        assert_eq!(rx.try_take(), Err(GkReplyError::DISCONNECTED));

        let (tx, mut rx) = reply_channel::<u32>();
        assert_eq!(rx.try_take(), Ok(None));
        drop(tx);
        assert_eq!(rx.try_take(), Err(GkReplyError::DISCONNECTED));
        assert_eq!(rx.try_take(), Err(GkReplyError::DISCONNECTED));
    });
}

#[test]
fn future_ready_immediately() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u32>();
        tx.send(1);
        assert_eq!(block_on(rx, LIMIT), Ok(1));
        let (tx, rx) = reply_channel::<u32>();
        drop(tx);
        assert_eq!(block_on(rx, LIMIT), Err(GkReplyError::DISCONNECTED));
    });
}

#[test]
fn future_value_after_first_pending() {
    bounded(LIMIT, || {
        let (tx, mut rx) = reply_channel::<u32>();
        let (inner, waker) = thread_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        // Polling again with the same waker stays pending and keeps one registration.
        assert!(Pin::new(&mut rx).poll(&mut cx).is_pending());
        let h = after(Duration::from_millis(20), move || tx.send(11));
        let deadline = Instant::now() + LIMIT;
        while inner.wakes.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "never woken");
            thread::park_timeout(Duration::from_millis(50));
        }
        assert_eq!(inner.wakes.load(Ordering::SeqCst), 1);
        assert_eq!(Pin::new(&mut rx).poll(&mut cx), Poll::Ready(Ok(11)));
        // Polling a finished future reports a disconnect instead of misbehaving.
        assert_eq!(
            Pin::new(&mut rx).poll(&mut cx),
            Poll::Ready(Err(GkReplyError::DISCONNECTED))
        );
        h.join().unwrap();
    });
}

#[test]
fn future_sender_dropped_after_pending_wakes() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<u32>();
        let h = after(Duration::from_millis(20), move || drop(tx));
        assert_eq!(block_on(rx, LIMIT), Err(GkReplyError::DISCONNECTED));
        h.join().unwrap();
    });
}

#[test]
fn future_repoll_with_different_waker() {
    bounded(LIMIT, || {
        let (tx, mut rx) = reply_channel::<u32>();
        let (inner_a, waker_a) = thread_waker();
        let (inner_b, waker_b) = thread_waker();
        assert!(!waker_a.will_wake(&waker_b));
        assert!(
            Pin::new(&mut rx)
                .poll(&mut Context::from_waker(&waker_a))
                .is_pending()
        );
        assert!(
            Pin::new(&mut rx)
                .poll(&mut Context::from_waker(&waker_b))
                .is_pending()
        );
        // The old waker was released: only the strong counts of the live handles remain.
        assert_eq!(Arc::strong_count(&inner_a), 2); // inner_a + waker_a
        assert_eq!(Arc::strong_count(&inner_b), 3); // inner_b + waker_b + registered clone
        tx.send(3);
        assert_eq!(inner_a.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(inner_b.wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            Pin::new(&mut rx).poll(&mut Context::from_waker(&waker_a)),
            Poll::Ready(Ok(3))
        );
    });
}

#[test]
fn future_after_wait_style_registration_and_thread_slot() {
    bounded(LIMIT, || {
        // Poll registers a task waker, then the receiver is consumed by wait_timeout (which
        // must reclaim the slot and park by thread).
        let (tx, mut rx) = reply_channel::<u32>();
        let (inner, waker) = thread_waker();
        assert!(
            Pin::new(&mut rx)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let h = after(Duration::from_millis(20), move || tx.send(8));
        assert_eq!(rx.wait_timeout(Duration::from_secs(5)), Ok(8));
        assert_eq!(inner.wakes.load(Ordering::SeqCst), 0);
        h.join().unwrap();
    });
}

#[test]
fn receiver_dropped_before_send_drops_value_once() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<GkTestCounted>();
        drop(rx);
        let (value, drops) = counted();
        tx.send(value);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn receiver_dropped_after_send_drops_value_once() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<GkTestCounted>();
        let (value, drops) = counted();
        tx.send(value);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(rx);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn received_value_is_not_double_dropped() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<GkTestCounted>();
        let (value, drops) = counted();
        tx.send(value);
        let got = rx.wait().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(got);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let (tx, mut rx) = reply_channel::<GkTestCounted>();
        let (value, drops) = counted();
        tx.send(value);
        let got = rx.try_take().unwrap().unwrap();
        drop(rx);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(got);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn sender_dropped_after_receiver() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<GkTestCounted>();
        drop(rx);
        drop(tx);
    });
}

#[test]
fn registered_waker_released_when_receiver_dropped() {
    bounded(LIMIT, || {
        let (tx, mut rx) = reply_channel::<u8>();
        let (inner, waker) = thread_waker();
        assert!(
            Pin::new(&mut rx)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(Arc::strong_count(&inner), 3);
        drop(rx);
        assert_eq!(Arc::strong_count(&inner), 2);
        tx.send(1);
        assert_eq!(inner.wakes.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn receiver_dropped_while_other_thread_sends() {
    bounded(LIMIT, || {
        for _ in 0..2_000 {
            let (tx, rx) = reply_channel::<GkTestCounted>();
            let (value, drops) = counted();
            let h = thread::spawn(move || tx.send(value));
            drop(rx);
            h.join().unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    });
}

#[test]
fn payload_types() {
    bounded(LIMIT, || {
        let (tx, rx) = reply_channel::<Box<dyn Any + Send>>();
        tx.send(Box::new(42_u32));
        let any = rx.wait().unwrap();
        assert_eq!(*any.downcast::<u32>().unwrap(), 42);

        let (tx, rx) = reply_channel::<Vec<u8>>();
        let h = after(Duration::from_millis(5), move || tx.send(vec![1, 2, 3]));
        assert_eq!(rx.wait().unwrap(), vec![1, 2, 3]);
        h.join().unwrap();

        let (tx, rx) = reply_channel::<()>();
        tx.send(());
        assert_eq!(rx.wait(), Ok(()));
        let (tx, rx) = reply_channel::<()>();
        drop(tx);
        assert_eq!(rx.wait(), Err(GkReplyError::DISCONNECTED));
    });
}

/// 4 requester threads x 5_000 pairs; every 4th mode uses a different receive style, every
/// other reply is delayed (park path), every 7th sender is dropped unsent.
#[test]
fn stress_four_threads() {
    const THREADS: usize = 4;
    const ITERS: usize = 5_000;
    let (done_tx, done_rx) = mpsc::channel();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let done_tx = done_tx.clone();
        handles.push(thread::spawn(move || {
            let (req_tx, req_rx) = mpsc::channel::<(usize, GkReplySender<usize>)>();
            let responder = thread::spawn(move || {
                while let Ok((i, reply)) = req_rx.recv() {
                    // BRANCH: drop unsent / delay (park path) / immediate (spin path)
                    if i % 7 == 0 {
                        drop(reply);
                    } else {
                        if i % 2 == 1 {
                            let until = Instant::now() + Duration::from_micros(60);
                            while Instant::now() < until {
                                std::hint::spin_loop();
                            }
                        }
                        reply.send(i * 2);
                    }
                }
            });
            for i in 0..ITERS {
                let (tx, rx) = reply_channel::<usize>();
                req_tx.send((i, tx)).unwrap();
                let res = match i % 4 {
                    0 | 1 => rx.wait(),
                    2 => block_on(rx, Duration::from_secs(10)),
                    _ => rx.wait_timeout(Duration::from_secs(10)),
                };
                // BRANCH: expected disconnect or echo
                if i % 7 == 0 {
                    assert_eq!(res, Err(GkReplyError::DISCONNECTED), "thread {t} iter {i}");
                } else {
                    assert_eq!(res, Ok(i * 2), "thread {t} iter {i}");
                }
            }
            drop(req_tx);
            responder.join().unwrap();
            done_tx.send(()).unwrap();
        }));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    for _ in 0..THREADS {
        let left = deadline.saturating_duration_since(Instant::now());
        done_rx
            .recv_timeout(left)
            .expect("stress threads did not finish in time");
    }
    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// Benchmark (run with --ignored --nocapture)
// ============================================================================

#[test]
#[ignore]
fn bench_round_trip() {
    const N: u64 = 1_000_000;
    const M: u64 = 200_000;

    let start = Instant::now();
    let mut sum = 0_u64;
    for i in 0..N {
        let (tx, rx) = reply_channel::<u64>();
        tx.send(i);
        sum += rx.wait().unwrap();
    }
    let ns = start.elapsed().as_nanos() as f64 / N as f64;
    println!("reply_channel same-thread create+send+wait: {ns:.1} ns (sum {sum})");

    #[cfg(feature = "crossbeam-channel")]
    {
        let start = Instant::now();
        let mut sum = 0_u64;
        for i in 0..N {
            let (tx, rx) = adaptif_gatotkaca_sqlite::crossbeam_channel::bounded::<u64>(1);
            tx.send(i).unwrap();
            sum += rx.recv().unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / N as f64;
        println!("crossbeam bounded(1) same-thread create+send+recv: {ns:.1} ns (sum {sum})");
    }

    // Two threads: requests travel over a std mpsc channel (the same for both variants), the
    // responder replies on the per-request reply channel.
    let (req_tx, req_rx) = mpsc::channel::<GkReplySender<u64>>();
    let responder = thread::spawn(move || {
        let mut i = 0_u64;
        while let Ok(reply) = req_rx.recv() {
            reply.send(i);
            i += 1;
        }
    });
    let start = Instant::now();
    for _ in 0..M {
        let (tx, rx) = reply_channel::<u64>();
        req_tx.send(tx).unwrap();
        rx.wait().unwrap();
    }
    let ns = start.elapsed().as_nanos() as f64 / M as f64;
    println!("reply_channel two-thread ping-pong (mpsc request transport): {ns:.1} ns");
    drop(req_tx);
    responder.join().unwrap();

    #[cfg(feature = "crossbeam-channel")]
    {
        use adaptif_gatotkaca_sqlite::crossbeam_channel::{Sender, bounded};
        let (req_tx, req_rx) = mpsc::channel::<Sender<u64>>();
        let responder = thread::spawn(move || {
            let mut i = 0_u64;
            while let Ok(reply) = req_rx.recv() {
                reply.send(i).unwrap();
                i += 1;
            }
        });
        let start = Instant::now();
        for _ in 0..M {
            let (tx, rx) = bounded::<u64>(1);
            req_tx.send(tx).unwrap();
            rx.recv().unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / M as f64;
        println!("crossbeam bounded(1) two-thread ping-pong (mpsc request transport): {ns:.1} ns");
        drop(req_tx);
        responder.join().unwrap();
    }
}
