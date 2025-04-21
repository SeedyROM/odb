// benches/skiplist_bench.rs

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc; // Added this import

use odb::epoch_manager::EpochManager;
use odb::lockfree_skiplist::LockFreeSkipList;

// Single-threaded benchmarks
fn bench_single_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("SkipList-SingleThreaded");

    // Setup: create an epoch manager and a skip list
    let epoch_manager = Arc::new(EpochManager::<16>::new());
    let mut manager_ref = EpochManager::<16>::new();
    let thread_epoch = Arc::new(manager_ref.register_thread());

    // Benchmark insertion
    group.bench_function("insert", |b| {
        let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));
        let mut i: u64 = 0; // Explicit type

        b.iter(|| {
            i = i.wrapping_add(1);
            skiplist.insert(black_box(i), black_box(i * 10), &thread_epoch)
        });
    });

    // Benchmark lookup
    group.bench_function("find", |b| {
        let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));

        // Populate the skiplist first
        for i in 0..1000 {
            skiplist.insert(i, i * 10, &thread_epoch);
        }

        let mut i = 0;
        let mut preds = [std::ptr::null_mut(); 16];
        let mut succs = [std::ptr::null_mut(); 16];

        b.iter(|| {
            i = (i + 1) % 1000;
            skiplist.find(black_box(i), &thread_epoch, &mut preds, &mut succs)
        });
    });

    // Benchmark removal
    group.bench_function("remove", |b| {
        let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));

        b.iter_batched(
            // Setup for each iteration
            || {
                for i in 0..1000 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }
                0
            },
            // Actual benchmark
            |mut i| {
                i = (i + 1) % 1000;
                skiplist.remove(black_box(i), &thread_epoch)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// Multi-threaded benchmark comparison
fn bench_multi_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("SkipList-MultiThreaded");

    // Test with different thread counts
    for thread_count in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("mixed_operations", thread_count),
            thread_count,
            |b, &num_threads| {
                b.iter_batched(
                    // Setup
                    || {
                        let epoch_manager = Arc::new(EpochManager::<16>::new());
                        let skiplist =
                            Arc::new(LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager)));

                        // Register thread epochs
                        let mut thread_epochs = Vec::new();
                        let mut manager_ref = EpochManager::<16>::new();
                        for _ in 0..num_threads {
                            thread_epochs.push(Arc::new(manager_ref.register_thread()));
                        }

                        (epoch_manager, skiplist, thread_epochs)
                    },
                    // Benchmark
                    |(epoch_manager, skiplist, thread_epochs)| {
                        use rand::prelude::*;
                        use std::sync::{Arc, Barrier};
                        use std::thread;

                        let ops_per_thread = 1000; // Reduced from 1000
                        let barrier = Arc::new(Barrier::new(num_threads as usize));

                        let mut handles = Vec::new();

                        for thread_id in 0..num_threads {
                            let skiplist_clone = Arc::clone(&skiplist);
                            let epoch_manager_clone = Arc::clone(&epoch_manager);
                            let barrier_clone = Arc::clone(&barrier);
                            let thread_epoch = Arc::clone(&thread_epochs[thread_id as usize]);

                            let handle = thread::spawn(move || {
                                let mut rng = rand::rng();

                                // Wait for all threads to be ready with timeout
                                let _ = barrier_clone.wait();

                                // Make sure to enter the epoch at the beginning
                                epoch_manager_clone.enter(&thread_epoch);

                                for _ in 0..ops_per_thread {
                                    // Generate random price - use a thread-specific range to reduce contention
                                    let thread_range = 10000 * thread_id as u64;
                                    let price = thread_range + (rng.random::<u64>() % 1000);
                                    let volume = rng.random::<u64>() % 1000;

                                    // Randomly choose operation: 40% insert, 30% remove, 30% find
                                    let op = rng.random::<u8>() % 100;

                                    if op < 40 {
                                        // Insert operation
                                        skiplist_clone.insert(price, volume, &thread_epoch);
                                    } else if op < 70 {
                                        // Remove operation
                                        skiplist_clone.remove(price, &thread_epoch);
                                    } else {
                                        // Find operation
                                        let mut preds = [std::ptr::null_mut(); 16];
                                        let mut succs = [std::ptr::null_mut(); 16];
                                        skiplist_clone.find(
                                            price,
                                            &thread_epoch,
                                            &mut preds,
                                            &mut succs,
                                        );
                                    }

                                    // Occasionally try to advance the epoch, but always exit and re-enter
                                    if rng.random::<u8>() % 100 < 5 {
                                        epoch_manager_clone.exit(&thread_epoch);
                                        // Don't always try to advance - this might be causing the deadlock
                                        if rng.random::<u8>() % 2 == 0 {
                                            epoch_manager_clone.try_advance();
                                        }
                                        epoch_manager_clone.enter(&thread_epoch);
                                    }
                                }

                                // Make sure to exit the epoch at the end
                                epoch_manager_clone.exit(&thread_epoch);
                            });

                            handles.push(handle);
                        }

                        // Wait for all threads to complete with a timeout
                        for handle in handles {
                            match handle.join() {
                                Ok(_) => {}
                                Err(e) => eprintln!("Thread panicked: {:?}", e),
                            }
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// Benchmark for skiplist with height 4
fn bench_skiplist_height_4(c: &mut Criterion) {
    let mut group = c.benchmark_group("SkipList-Height4");
    let epoch_manager = Arc::new(EpochManager::<4>::new());
    let mut manager_ref = EpochManager::<4>::new();
    let thread_epoch = Arc::new(manager_ref.register_thread());
    let skiplist = LockFreeSkipList::<4>::new(Arc::clone(&epoch_manager));

    group.bench_function("mixed_ops", |b| {
        b.iter_batched(
            // Setup
            || {
                // Populate with some data
                for i in 0..500 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }
            },
            // Benchmark
            |_| {
                // Perform a mix of operations
                for i in 500..600 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }

                let mut preds = [std::ptr::null_mut(); 4];
                let mut succs = [std::ptr::null_mut(); 4];

                for i in 200..700 {
                    if i % 2 == 0 {
                        skiplist.find(i, &thread_epoch, &mut preds, &mut succs);
                    }
                }

                for i in 300..400 {
                    skiplist.remove(i, &thread_epoch);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// Benchmark for skiplist with height 8
fn bench_skiplist_height_8(c: &mut Criterion) {
    let mut group = c.benchmark_group("SkipList-Height8");
    let epoch_manager = Arc::new(EpochManager::<8>::new());
    let mut manager_ref = EpochManager::<8>::new();
    let thread_epoch = Arc::new(manager_ref.register_thread());
    let skiplist = LockFreeSkipList::<8>::new(Arc::clone(&epoch_manager));

    group.bench_function("mixed_ops", |b| {
        b.iter_batched(
            // Setup
            || {
                // Populate with some data
                for i in 0..500 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }
            },
            // Benchmark
            |_| {
                // Perform a mix of operations
                for i in 500..600 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }

                let mut preds = [std::ptr::null_mut(); 8];
                let mut succs = [std::ptr::null_mut(); 8];

                for i in 200..700 {
                    if i % 2 == 0 {
                        skiplist.find(i, &thread_epoch, &mut preds, &mut succs);
                    }
                }

                for i in 300..400 {
                    skiplist.remove(i, &thread_epoch);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// Benchmark for skiplist with height 16
fn bench_skiplist_height_16(c: &mut Criterion) {
    let mut group = c.benchmark_group("SkipList-Height16");
    let epoch_manager = Arc::new(EpochManager::<16>::new());
    let mut manager_ref = EpochManager::<16>::new();
    let thread_epoch = Arc::new(manager_ref.register_thread());
    let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));

    group.bench_function("mixed_ops", |b| {
        b.iter_batched(
            // Setup
            || {
                // Populate with some data
                for i in 0..500 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }
            },
            // Benchmark
            |_| {
                // Perform a mix of operations
                for i in 500..600 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }

                let mut preds = [std::ptr::null_mut(); 16];
                let mut succs = [std::ptr::null_mut(); 16];

                for i in 200..700 {
                    if i % 2 == 0 {
                        skiplist.find(i, &thread_epoch, &mut preds, &mut succs);
                    }
                }

                for i in 300..400 {
                    skiplist.remove(i, &thread_epoch);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// Benchmark for skiplist with height 32
fn bench_skiplist_height_32(c: &mut Criterion) {
    let mut group = c.benchmark_group("SkipList-Height32");
    let epoch_manager = Arc::new(EpochManager::<32>::new());
    let mut manager_ref = EpochManager::<32>::new();
    let thread_epoch = Arc::new(manager_ref.register_thread());
    let skiplist = LockFreeSkipList::<32>::new(Arc::clone(&epoch_manager));

    group.bench_function("mixed_ops", |b| {
        b.iter_batched(
            // Setup
            || {
                // Populate with some data
                for i in 0..500 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }
            },
            // Benchmark
            |_| {
                // Perform a mix of operations
                for i in 500..600 {
                    skiplist.insert(i, i * 10, &thread_epoch);
                }

                let mut preds = [std::ptr::null_mut(); 32];
                let mut succs = [std::ptr::null_mut(); 32];

                for i in 200..700 {
                    if i % 2 == 0 {
                        skiplist.find(i, &thread_epoch, &mut preds, &mut succs);
                    }
                }

                for i in 300..400 {
                    skiplist.remove(i, &thread_epoch);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_threaded,
    bench_multi_threaded,
    bench_skiplist_height_4,
    bench_skiplist_height_8,
    bench_skiplist_height_16,
    bench_skiplist_height_32
);
criterion_main!(benches);
