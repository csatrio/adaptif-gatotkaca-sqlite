// ============================================================================
// Import Definition
// ============================================================================

use adaptif_gatotkaca_sqlite::queue::{
    GkChannelKind, GkQueueError, GkWriterQueueConfig, GkWriterRx, GkWriterTx, writer_queue,
};
use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

// Every test below has an upper bound: receives use `recv_timeout` with `LIMIT`, and joins go through `finish_within`.

const LIMIT: Duration = Duration::from_secs(20);

// ============================================================================
// Helpers
// ============================================================================

fn kinds() -> Vec<GkChannelKind> {
    #[allow(unused_mut)]
    let mut kinds = vec![GkChannelKind::Treiber];
    #[cfg(feature = "crossbeam-channel")]
    kinds.push(GkChannelKind::Crossbeam);
    kinds
}

fn queue<T: Send + 'static>(
    kind: GkChannelKind,
    capacity: usize,
) -> (GkWriterTx<T>, GkWriterRx<T>) {
    writer_queue(
        &GkWriterQueueConfig::default()
            .with_kind(kind)
            .with_capacity(capacity),
    )
    .expect("queue is created")
}

/// Receives one message, failing the test after `LIMIT`.
fn recv_limited<T: Send + 'static>(rx: &mut GkWriterRx<T>) -> T {
    rx.recv_timeout(LIMIT).expect("a message arrives in time")
}

/// Runs `work` on its own thread and fails the test if it does not finish within `LIMIT`.
fn finish_within<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> R {
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let _ = done_tx.send(work());
    });
    let result = done_rx
        .recv_timeout(LIMIT)
        .expect("the work finished within the time bound");
    handle.join().expect("worker thread did not panic");
    result
}

fn join_all(handles: Vec<thread::JoinHandle<()>>) {
    for handle in handles {
        handle.join().expect("producer thread did not panic");
    }
}

/// A payload of about the size of the real writer message.
#[derive(Clone, Debug)]
struct GkPayload {
    producer: u32,
    seq: u64,
    pad: [u8; 184],
}

impl GkPayload {
    fn new(producer: u32, seq: u64) -> Self {
        Self {
            producer,
            seq,
            pad: [0xAB; 184],
        }
    }
}

/// Counts its own drops.
struct GkDropCounted {
    data: Vec<u8>,
    name: String,
    drops: Arc<AtomicUsize>,
}

impl GkDropCounted {
    fn new(n: usize, drops: &Arc<AtomicUsize>) -> Self {
        Self {
            data: vec![n as u8; 64 + n % 64],
            name: format!("message-{n}"),
            drops: drops.clone(),
        }
    }
}

impl Drop for GkDropCounted {
    fn drop(&mut self) {
        assert!(!self.name.is_empty() && !self.data.is_empty());
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

// ============================================================================
// Behavior
// ============================================================================

fn single_producer_order(kind: GkChannelKind) {
    const COUNT: u64 = 10_000;
    let (tx, mut rx) = queue::<u64>(kind, 256);
    let producer = thread::spawn(move || {
        for seq in 0..COUNT {
            tx.send(seq).expect("receiver is alive");
        }
    });
    for expected in 0..COUNT {
        assert_eq!(recv_limited(&mut rx), expected, "{kind:?}");
    }
    producer.join().unwrap();
    assert_eq!(rx.try_recv(), None);
}

#[test]
fn single_producer_keeps_order() {
    for kind in kinds() {
        single_producer_order(kind);
    }
}

fn many_producers_order(kind: GkChannelKind) {
    const PRODUCERS: u32 = 8;
    const PER_PRODUCER: u64 = 5_000;
    let (tx, mut rx) = queue::<GkPayload>(kind, 512);
    let handles: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let tx = tx.clone();
            thread::spawn(move || {
                for seq in 0..PER_PRODUCER {
                    assert!(
                        tx.send(GkPayload::new(producer, seq)).is_ok(),
                        "receiver is alive"
                    );
                }
            })
        })
        .collect();
    drop(tx);
    let mut next = vec![0u64; PRODUCERS as usize];
    for _ in 0..(PRODUCERS as u64 * PER_PRODUCER) {
        let msg = recv_limited(&mut rx);
        assert_eq!(
            msg.seq, next[msg.producer as usize],
            "{kind:?} producer {}",
            msg.producer
        );
        assert!(msg.pad.iter().all(|byte| *byte == 0xAB));
        next[msg.producer as usize] += 1;
    }
    join_all(handles);
    assert!(next.iter().all(|n| *n == PER_PRODUCER));
    // every handle is gone and the queue is empty
    assert_eq!(
        rx.recv_timeout(LIMIT).unwrap_err(),
        GkQueueError::DISCONNECTED
    );
}

#[test]
fn many_producers_keep_per_producer_order() {
    for kind in kinds() {
        many_producers_order(kind);
    }
}

fn heap_payloads_dropped_once(kind: GkChannelKind) {
    const COUNT: usize = 1_000;
    const RECEIVED: usize = 300;
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = queue::<GkDropCounted>(kind, 2_048);
    let producer = {
        let tx = tx.clone();
        let drops = drops.clone();
        thread::spawn(move || {
            for n in 0..COUNT {
                assert!(
                    tx.send(GkDropCounted::new(n, &drops)).is_ok(),
                    "receiver is alive"
                );
            }
        })
    };
    producer.join().unwrap();
    // some are received and dropped by the consumer ...
    for n in 0..RECEIVED {
        let msg = recv_limited(&mut rx);
        assert_eq!(msg.name, format!("message-{n}"));
    }
    assert_eq!(drops.load(Ordering::SeqCst), RECEIVED, "{kind:?}");
    // ... the rest are still queued when the receiver drops
    drop(rx);
    tx.release_thread_sender();
    drop(tx);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        COUNT,
        "{kind:?} dropped exactly once"
    );
}

#[test]
fn heap_payloads_are_dropped_exactly_once() {
    for kind in kinds() {
        heap_payloads_dropped_once(kind);
    }
}

fn heap_payloads_from_the_main_thread(kind: GkChannelKind) {
    // the sender of this very thread holds queued messages when the receiver drops
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = queue::<GkDropCounted>(kind, 64);
    for n in 0..40 {
        assert!(
            tx.send(GkDropCounted::new(n, &drops)).is_ok(),
            "receiver is alive"
        );
    }
    drop(rx);
    // a send after the receiver is gone gives the value back, which the caller then drops
    let Err(back) = tx.send(GkDropCounted::new(99, &drops)) else {
        panic!("receiver is gone, the send must hand the value back");
    };
    assert_eq!(back.name, "message-99");
    drop(back);
    tx.release_thread_sender();
    drop(tx);
    assert_eq!(drops.load(Ordering::SeqCst), 41, "{kind:?}");
}

#[test]
fn queued_messages_are_dropped_when_the_receiver_drops() {
    for kind in kinds() {
        heap_payloads_from_the_main_thread(kind);
    }
}

fn backpressure(kind: GkChannelKind) {
    const CAPACITY: usize = 4;
    const TOTAL: usize = 200;
    let (tx, mut rx) = queue::<usize>(kind, CAPACITY);
    let sent = Arc::new(AtomicUsize::new(0));
    let producer = {
        let sent = sent.clone();
        thread::spawn(move || {
            for n in 0..TOTAL {
                tx.send(n).expect("receiver is alive");
                sent.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    // nothing is received: the producer must stop at the capacity
    thread::sleep(Duration::from_millis(300));
    let blocked_at = sent.load(Ordering::SeqCst);
    assert!(
        (1..=CAPACITY).contains(&blocked_at),
        "{kind:?}: producer sent {blocked_at} with capacity {CAPACITY}"
    );
    // draining releases it
    let started = Instant::now();
    for expected in 0..TOTAL {
        assert_eq!(recv_limited(&mut rx), expected);
    }
    producer.join().unwrap();
    assert!(started.elapsed() < LIMIT);
    assert_eq!(sent.load(Ordering::SeqCst), TOTAL);
}

#[test]
fn bounded_queue_blocks_the_producer_until_drained() {
    for kind in kinds() {
        backpressure(kind);
    }
}

fn timeouts_and_try_recv(kind: GkChannelKind) {
    let (tx, mut rx) = queue::<u32>(kind, 16);
    assert_eq!(rx.try_recv(), None, "{kind:?}");
    let started = Instant::now();
    let err = rx.recv_timeout(Duration::from_millis(60)).unwrap_err();
    assert_eq!(err, GkQueueError::TIMEOUT, "{kind:?}");
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(started.elapsed() < Duration::from_secs(5));
    tx.send(7).unwrap();
    assert_eq!(rx.try_recv(), Some(7));
    assert_eq!(rx.try_recv(), None);
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(5)).unwrap_err(),
        GkQueueError::TIMEOUT
    );
}

#[test]
fn recv_timeout_reports_timeout_and_try_recv_is_none_when_empty() {
    for kind in kinds() {
        timeouts_and_try_recv(kind);
    }
}

fn dropped_receiver_returns_the_value(kind: GkChannelKind) {
    let (tx, rx) = queue::<String>(kind, 8);
    tx.send("alive".to_string()).unwrap();
    drop(rx);
    assert_eq!(
        tx.send("gone".to_string()),
        Err("gone".to_string()),
        "{kind:?}"
    );
    // another thread has no sender yet: it creates one against a closed queue
    let tx2 = tx.clone();
    let result = finish_within(move || tx2.send("other".to_string()));
    assert_eq!(result, Err("other".to_string()));
}

#[test]
fn send_after_receiver_drop_gives_the_value_back() {
    for kind in kinds() {
        dropped_receiver_returns_the_value(kind);
    }
}

fn blocked_producer_is_released(kind: GkChannelKind) {
    let (tx, rx) = queue::<u64>(kind, 2);
    let (state_tx, state_rx) = mpsc::channel();
    let producer = thread::spawn(move || {
        let mut sent = 0u64;
        let returned = loop {
            // fills the queue, then blocks until the receiver drops
            match tx.send(sent) {
                Ok(()) => {
                    sent += 1;
                    let _ = state_tx.send(sent);
                }
                Err(value) => break value,
            }
        };
        (sent, returned)
    });
    // wait until the producer filled the queue and is stuck
    let mut sent = 0;
    while let Ok(n) = state_rx.recv_timeout(Duration::from_millis(300)) {
        sent = n;
    }
    assert!(sent >= 1, "{kind:?}");
    drop(rx);
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _ = done_tx.send(producer.join().unwrap());
    });
    let (total, returned) = done_rx
        .recv_timeout(LIMIT)
        .expect("the blocked producer was released");
    waiter.join().unwrap();
    assert_eq!(returned, total, "the unsent value comes back, {kind:?}");
}

#[test]
fn blocked_producer_is_released_when_the_receiver_drops() {
    for kind in kinds() {
        blocked_producer_is_released(kind);
    }
}

fn many_queues_from_one_thread(kind: GkChannelKind) {
    let started = Instant::now();
    let mut live: Vec<(GkWriterTx<GkPayload>, GkWriterRx<GkPayload>)> = Vec::new();
    for round in 0..300u64 {
        let (tx, mut rx) = queue::<GkPayload>(kind, 1_024);
        assert!(tx.send(GkPayload::new(0, round)).is_ok(), "queue is open");
        // every 10th queue stays open so the sweep has live and dead entries to tell apart
        if round % 10 == 0 {
            live.push((tx, rx));
            continue;
        }
        assert_eq!(recv_limited(&mut rx).seq, round);
        drop(rx);
        drop(tx);
    }
    // the open queues still work from this thread
    for (tx, rx) in &mut live {
        assert!(tx.send(GkPayload::new(1, 1)).is_ok(), "queue is open");
        recv_limited(rx);
        assert_eq!(recv_limited(rx).seq, 1);
    }
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{kind:?}: 300 queues took {:?}",
        started.elapsed()
    );
}

#[test]
fn creating_and_dropping_many_queues_is_fast() {
    for kind in kinds() {
        many_queues_from_one_thread(kind);
    }
}

fn producers_exit_with_queued_messages(kind: GkChannelKind) {
    const PRODUCERS: u32 = 4;
    const PER_PRODUCER: u64 = 1_000;
    let (tx, mut rx) = queue::<GkPayload>(kind, 8_192);
    let handles: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let tx = tx.clone();
            thread::spawn(move || {
                for seq in 0..PER_PRODUCER {
                    assert!(
                        tx.send(GkPayload::new(producer, seq)).is_ok(),
                        "queue is open"
                    );
                }
            })
        })
        .collect();
    join_all(handles);
    // all producers are gone, `tx` is still alive: nothing may be lost, and no disconnect yet
    let mut next = vec![0u64; PRODUCERS as usize];
    for _ in 0..(PRODUCERS as u64 * PER_PRODUCER) {
        let msg = recv_limited(&mut rx);
        assert_eq!(msg.seq, next[msg.producer as usize]);
        next[msg.producer as usize] += 1;
    }
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(80)).unwrap_err(),
        GkQueueError::TIMEOUT,
        "{kind:?}"
    );
    drop(tx);
    assert_eq!(
        rx.recv_timeout(LIMIT).unwrap_err(),
        GkQueueError::DISCONNECTED
    );
}

#[test]
fn messages_of_exited_producers_are_delivered() {
    for kind in kinds() {
        producers_exit_with_queued_messages(kind);
    }
}

fn shared_clone_across_threads(kind: GkChannelKind) {
    const THREADS: u32 = 6;
    const PER_THREAD: u64 = 2_000;
    let (tx, mut rx) = queue::<GkPayload>(kind, 128);
    let shared = Arc::new(tx);
    // the same handle by reference from several threads, plus clones moved into other threads
    let mut handles = Vec::new();
    for producer in 0..THREADS {
        let owned: GkWriterTx<GkPayload> = GkWriterTx::clone(&shared);
        let by_ref = shared.clone();
        handles.push(thread::spawn(move || {
            for seq in 0..PER_THREAD {
                // alternate between the owned clone and the shared reference
                let target = if seq % 2 == 0 { &owned } else { &*by_ref };
                assert!(
                    target.send(GkPayload::new(producer, seq)).is_ok(),
                    "queue is open"
                );
            }
        }));
    }
    drop(shared);
    let mut next = vec![0u64; THREADS as usize];
    for _ in 0..(THREADS as u64 * PER_THREAD) {
        let msg = recv_limited(&mut rx);
        assert_eq!(msg.seq, next[msg.producer as usize], "{kind:?}");
        next[msg.producer as usize] += 1;
    }
    join_all(handles);
    assert_eq!(
        rx.recv_timeout(LIMIT).unwrap_err(),
        GkQueueError::DISCONNECTED
    );
}

#[test]
fn clones_of_the_producing_end_work_across_threads() {
    for kind in kinds() {
        shared_clone_across_threads(kind);
    }
}

fn release_and_reuse(kind: GkChannelKind) {
    let (tx, mut rx) = queue::<u32>(kind, 64);
    // nothing to release yet
    tx.release_thread_sender();
    for round in 0..5u32 {
        tx.send(round * 2).unwrap();
        tx.release_thread_sender();
        tx.send(round * 2 + 1).unwrap();
    }
    let mut seen: Vec<u32> = (0..10).map(|_| recv_limited(&mut rx)).collect();
    seen.sort_unstable();
    assert_eq!(seen, (0..10).collect::<Vec<_>>(), "{kind:?}");
}

#[test]
fn released_sender_delivers_and_is_recreated_on_demand() {
    for kind in kinds() {
        release_and_reuse(kind);
    }
}

fn two_message_types_on_one_thread(kind: GkChannelKind) {
    let (tx_a, mut rx_a) = queue::<u64>(kind, 8);
    let (tx_b, mut rx_b) = queue::<String>(kind, 8);
    assert_ne!(tx_a.queue_id(), tx_b.queue_id());
    tx_a.send(5).unwrap();
    tx_b.send("five".to_string()).unwrap();
    tx_a.send(6).unwrap();
    assert_eq!(recv_limited(&mut rx_a), 5);
    assert_eq!(recv_limited(&mut rx_a), 6);
    assert_eq!(recv_limited(&mut rx_b), "five");
    assert_eq!(tx_a.kind(), kind);
    assert_eq!(rx_b.kind(), kind);
}

#[test]
fn queues_of_different_message_types_share_a_thread() {
    for kind in kinds() {
        two_message_types_on_one_thread(kind);
    }
}

thread_local! {
    static AT_EXIT: RefCell<Option<GkAtExit>> = const { RefCell::new(None) };
}

/// Sends from a thread-local destructor, when the thread's own sender map may already be destroyed.
struct GkAtExit {
    tx: GkWriterTx<u32>,
}

impl Drop for GkAtExit {
    fn drop(&mut self) {
        self.tx.send(4242).expect("receiver is alive");
    }
}

fn send_from_a_thread_local_destructor(kind: GkChannelKind) {
    let (tx, mut rx) = queue::<u32>(kind, 8);
    let worker = thread::spawn(move || {
        tx.send(1).unwrap();
        AT_EXIT.with(|slot| *slot.borrow_mut() = Some(GkAtExit { tx }));
    });
    worker.join().unwrap();
    let mut seen = vec![recv_limited(&mut rx), recv_limited(&mut rx)];
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 4242], "{kind:?}");
}

#[test]
fn sending_while_thread_locals_are_destroyed_still_delivers() {
    for kind in kinds() {
        send_from_a_thread_local_destructor(kind);
    }
}

/// Sends again from its own destructor (a re-entrant send while senders are swept or released).
struct GkEcho {
    tx: Option<GkWriterTx<GkEcho>>,
    drops: Arc<AtomicUsize>,
}

impl Drop for GkEcho {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        // only the first generation echoes
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(GkEcho {
                tx: None,
                drops: self.drops.clone(),
            });
        }
    }
}

fn reentrant_send_from_a_destructor(kind: GkChannelKind) {
    // queue A holds echoes in this thread's sender when its receiver drops; a later sweep and the release drop them
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx_a, rx_a) = queue::<GkEcho>(kind, 16);
    let (tx_b, mut rx_b) = queue::<GkEcho>(kind, 16);
    for _ in 0..4 {
        tx_a.send(GkEcho {
            tx: Some(tx_b.clone()),
            drops: drops.clone(),
        })
        .ok()
        .expect("receiver is alive");
    }
    drop(rx_a);
    // the first sender for a third queue sweeps queue A's sender on this thread
    let (tx_c, mut rx_c) = queue::<u8>(kind, 4);
    tx_c.send(1).unwrap();
    assert_eq!(recv_limited(&mut rx_c), 1);
    tx_a.release_thread_sender();
    drop(tx_a);
    // the four first-generation echoes were dropped and each sent one second-generation echo to queue B
    let mut echoed = 0;
    while let Some(echo) = rx_b.try_recv() {
        echoed += 1;
        drop(echo);
    }
    assert_eq!(echoed, 4, "{kind:?}");
    assert_eq!(drops.load(Ordering::SeqCst), 8, "{kind:?}");
}

#[test]
fn message_destructors_may_send_while_senders_are_swept() {
    for kind in kinds() {
        finish_within(move || reentrant_send_from_a_destructor(kind));
    }
}

fn disconnect_after_handles_drop(kind: GkChannelKind) {
    let (tx, mut rx) = queue::<u32>(kind, 8);
    let tx2 = tx.clone();
    tx.send(1).unwrap();
    drop(tx);
    // one handle is left: no disconnect yet
    assert_eq!(recv_limited(&mut rx), 1);
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(60)).unwrap_err(),
        GkQueueError::TIMEOUT
    );
    tx2.send(2).unwrap();
    // a plain blocking `recv` on another thread observes the disconnect once the last handle is gone
    let waiter = thread::spawn(move || {
        let first = rx.recv();
        let second = rx.recv();
        (first, second)
    });
    thread::sleep(Duration::from_millis(60));
    drop(tx2);
    let (done_tx, done_rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let _ = done_tx.send(waiter.join().unwrap());
    });
    let (first, second) = done_rx
        .recv_timeout(LIMIT)
        .expect("recv observed the disconnect");
    join.join().unwrap();
    assert_eq!(first, Ok(2), "{kind:?}");
    assert_eq!(second, Err(GkQueueError::DISCONNECTED), "{kind:?}");
}

#[test]
fn recv_reports_disconnect_after_the_last_handle_drops() {
    for kind in kinds() {
        disconnect_after_handles_drop(kind);
    }
}

#[test]
fn zero_capacity_is_rejected_and_huge_capacity_is_clamped() {
    for kind in kinds() {
        let err = writer_queue::<u8>(
            &GkWriterQueueConfig::default()
                .with_kind(kind)
                .with_capacity(0),
        )
        .expect_err("capacity 0 is invalid");
        assert_eq!(err, GkQueueError::INVALID_CONFIG);
    }
    let (tx, mut rx) = queue::<u8>(GkChannelKind::Treiber, usize::MAX);
    tx.send(9).unwrap();
    assert_eq!(recv_limited(&mut rx), 9);
}

#[test]
fn defaults_and_error_surface() {
    let config = GkWriterQueueConfig::default();
    assert_eq!(config.kind, GkChannelKind::Treiber);
    assert_eq!(GkChannelKind::default(), GkChannelKind::Treiber);
    assert_eq!(config.capacity, 1024);
    for err in [
        GkQueueError::CLOSED,
        GkQueueError::DISCONNECTED,
        GkQueueError::TIMEOUT,
        GkQueueError::OUT_OF_MEMORY,
        GkQueueError::INVALID_CONFIG,
    ] {
        assert!(!err.message().is_empty());
        assert_eq!(err.to_string(), err.message());
        assert!(err.code() != 0);
    }
    use adaptif_gatotkaca_utils::GkTreiberChannelError as Raw;
    assert_eq!(GkQueueError::from(Raw::CLOSED), GkQueueError::CLOSED);
    assert_eq!(
        GkQueueError::from(Raw::DISCONNECTED),
        GkQueueError::DISCONNECTED
    );
    assert_eq!(GkQueueError::from(Raw::TIMEOUT), GkQueueError::TIMEOUT);
    assert_eq!(
        GkQueueError::from(Raw::OUT_OF_MEMORY),
        GkQueueError::OUT_OF_MEMORY
    );
    assert_eq!(
        GkQueueError::from(Raw::INVALID_CONFIG),
        GkQueueError::INVALID_CONFIG
    );
}

// ============================================================================
// Micro-benchmark (cargo test --release --test sqlite_queue -- --ignored --nocapture)
// ============================================================================

fn run_bench(kind: GkChannelKind, producers: u32) -> f64 {
    const PER_PRODUCER: u64 = 500_000;
    let (tx, mut rx) = queue::<GkPayload>(kind, 4_096);
    let started = Instant::now();
    let handles: Vec<_> = (0..producers)
        .map(|producer| {
            let tx = tx.clone();
            thread::spawn(move || {
                for seq in 0..PER_PRODUCER {
                    assert!(
                        tx.send(GkPayload::new(producer, seq)).is_ok(),
                        "queue is open"
                    );
                }
            })
        })
        .collect();
    drop(tx);
    let total = producers as u64 * PER_PRODUCER;
    let mut received = 0u64;
    while received < total {
        let msg = rx.recv_timeout(LIMIT).expect("bench message arrives");
        std::hint::black_box(&msg);
        received += 1;
    }
    let elapsed = started.elapsed();
    join_all(handles);
    total as f64 / elapsed.as_secs_f64()
}

#[test]
#[ignore = "micro-benchmark, run with --ignored --nocapture"]
fn bench_messages_per_second() {
    println!("payload {} bytes", std::mem::size_of::<GkPayload>());
    for kind in kinds() {
        for producers in [1u32, 4] {
            // warm-up, then the measured run
            let _ = run_bench(kind, producers);
            let rate = run_bench(kind, producers);
            println!(
                "{kind:?} producers={producers}: {:.2} M msgs/s",
                rate / 1_000_000.0
            );
        }
    }
}
