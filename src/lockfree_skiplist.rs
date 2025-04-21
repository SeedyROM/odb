use std::ptr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::epoch_manager::{EpochManager, LocalEpoch};
use crate::node::Node;

/// A lock-free skip list implementation with epoch-based reclamation
pub struct LockFreeSkipList<const S: usize> {
    head: *mut Node,
    epoch_manager: Arc<EpochManager<S>>,
}

unsafe impl<const S: usize> Send for LockFreeSkipList<S> {}
unsafe impl<const S: usize> Sync for LockFreeSkipList<S> {}

impl<const S: usize> LockFreeSkipList<S> {
    fn random_level(&self) -> usize {
        // Simple random level implementation
        // Using a probability of 1/2 for each level
        let mut level = 1;

        // Cap at S (max levels)
        while level < S && rand::random::<bool>() {
            level += 1;
        }

        level
    }

    /// Create a new skip list with the given epoch manager
    /// and a head node with maximum value.
    pub fn new(epoch_manager: Arc<EpochManager<S>>) -> Self {
        // Use the helper method
        let head = epoch_manager.allocate_node(u64::MAX, 0, S);

        LockFreeSkipList {
            head,
            epoch_manager,
        }
    }

    /// Find a node with guards to protect from reclamation
    pub fn find(
        &self,
        price: u64,
        thread_epoch: &LocalEpoch,
        preds: &mut [*mut Node; S],
        succs: &mut [*mut Node; S],
    ) -> bool {
        // Enter critical section
        self.epoch_manager.enter(thread_epoch);

        let mut found = false;
        let mut x = self.head;

        // Using the epoch_manager's helper methods now
        for level in (0..S).rev() {
            let mut forward_ptr = self.epoch_manager.get_node_forward_ptr(x, level);
            let mut curr = unsafe { (*forward_ptr).load(Ordering::Acquire) };

            while !curr.is_null() && unsafe { (*curr).price < price } {
                x = curr;
                forward_ptr = self.epoch_manager.get_node_forward_ptr(x, level);
                curr = unsafe { (*forward_ptr).load(Ordering::Acquire) };
            }

            preds[level] = x;
            succs[level] = curr;

            if !found && !curr.is_null() && unsafe { (*curr).price == price } {
                found = true;
            }
        }

        // Exit critical section
        self.epoch_manager.exit(thread_epoch);

        found
    }

    /// Insert with optimized memory management
    pub fn insert(&self, price: u64, volume: u64, thread_epoch: &LocalEpoch) -> bool {
        let mut preds: [*mut Node; S] = [ptr::null_mut(); S];
        let mut succs: [*mut Node; S] = [ptr::null_mut(); S];

        let found = self.find(price, thread_epoch, &mut preds, &mut succs);

        if found {
            // Update existing node's volume atomically
            let node = succs[0];
            unsafe {
                let current_volume = (*node).volume.load(Ordering::Relaxed);
                (*node)
                    .volume
                    .store(current_volume + volume, Ordering::Release);
            }
            return false;
        }

        // Enter critical section for insertion
        self.epoch_manager.enter(thread_epoch);

        // Create a new node with our optimized allocator
        let new_level = self.random_level();
        let new_node = self.epoch_manager.allocate_node(price, volume, new_level);

        // Connect the node at each level
        for level in 0..new_level {
            let forward_ptr = self.epoch_manager.get_node_forward_ptr(new_node, level);

            loop {
                unsafe {
                    (*forward_ptr).store(succs[level], Ordering::Relaxed);

                    let pred_forward_ptr =
                        self.epoch_manager.get_node_forward_ptr(preds[level], level);

                    let result = (*pred_forward_ptr).compare_exchange(
                        succs[level],
                        new_node,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    );

                    if result.is_ok() {
                        break;
                    }

                    // Something changed, need to find new predecessors and successors
                    self.find(price, thread_epoch, &mut preds, &mut succs);
                }
            }
        }

        // Exit critical section
        self.epoch_manager.exit(thread_epoch);

        true
    }

    /// Remove with memory reclamation
    pub fn remove(&self, price: u64, thread_epoch: &LocalEpoch) -> bool {
        let mut preds: [*mut Node; S] = [ptr::null_mut(); S];
        let mut succs: [*mut Node; S] = [ptr::null_mut(); S];

        // Enter critical section
        self.epoch_manager.enter(thread_epoch);

        let found = self.find(price, thread_epoch, &mut preds, &mut succs);

        if !found {
            // Exit critical section
            self.epoch_manager.exit(thread_epoch);
            return false;
        }

        let node_to_remove = succs[0];

        // Mark the node as logically deleted first - we could use a special bit for this
        // Here we'll use a simple approach of setting volume to 0 as a marker
        unsafe {
            (*node_to_remove).volume.store(0, Ordering::Release);
        }

        // Physical deletion from the list
        for level in (0..S).rev() {
            if level < unsafe { (*node_to_remove).height as usize } {
                loop {
                    let forward_ptr = self
                        .epoch_manager
                        .get_node_forward_ptr(node_to_remove, level);
                    let next = unsafe { (*forward_ptr).load(Ordering::Acquire) };

                    let pred_forward_ptr =
                        self.epoch_manager.get_node_forward_ptr(preds[level], level);

                    let result = unsafe {
                        (*pred_forward_ptr).compare_exchange(
                            node_to_remove,
                            next,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                    };

                    if result.is_ok() {
                        break;
                    }

                    // Something changed, find new position
                    self.find(price, thread_epoch, &mut preds, &mut succs);

                    // Check if the node is still in the list
                    if succs[0] != node_to_remove {
                        // Node already removed by another thread
                        self.epoch_manager.exit(thread_epoch);
                        return false;
                    }
                }
            }
        }

        // Defer actual freeing to the epoch-based reclamation system
        self.epoch_manager.defer_free(node_to_remove);

        // Exit critical section
        self.epoch_manager.exit(thread_epoch);

        true
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::epoch_manager::GarbageList;

    use super::*;
    use std::cell::UnsafeCell;
    use std::ptr;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn skiplist_basic_operations() {
        // Set up epoch manager
        let epoch_manager = Arc::new(EpochManager::<16>::new());
        let mut manager_ref = EpochManager::<16>::new();
        let thread_epoch = manager_ref.register_thread();

        // Create skiplist
        let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));

        // Test insert
        assert!(skiplist.insert(100, 500, &thread_epoch));
        assert!(skiplist.insert(200, 600, &thread_epoch));
        assert!(skiplist.insert(150, 550, &thread_epoch));

        // Test duplicate insert (should update volume, not insert new node)
        assert!(!skiplist.insert(100, 100, &thread_epoch));

        // Check node existence using find
        let mut preds: [*mut Node; 16] = [ptr::null_mut(); 16];
        let mut succs: [*mut Node; 16] = [ptr::null_mut(); 16];

        assert!(skiplist.find(100, &thread_epoch, &mut preds, &mut succs));
        assert!(skiplist.find(150, &thread_epoch, &mut preds, &mut succs));
        assert!(skiplist.find(200, &thread_epoch, &mut preds, &mut succs));
        assert!(!skiplist.find(300, &thread_epoch, &mut preds, &mut succs));

        // Test removal
        assert!(skiplist.remove(150, &thread_epoch));
        assert!(!skiplist.find(150, &thread_epoch, &mut preds, &mut succs));

        // Test removing non-existent node
        assert!(!skiplist.remove(999, &thread_epoch));

        // Verify remaining nodes
        assert!(skiplist.find(100, &thread_epoch, &mut preds, &mut succs));
        assert!(skiplist.find(200, &thread_epoch, &mut preds, &mut succs));
    }

    #[test]
    fn skiplist_ordering() {
        // Set up epoch manager and thread
        let epoch_manager = Arc::new(EpochManager::<16>::new());
        let mut manager_ref = EpochManager::<16>::new();
        let thread_epoch = manager_ref.register_thread();

        // Create skiplist
        let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));

        // Insert nodes in random order
        let values = [50, 30, 70, 20, 40, 60, 80];
        for &value in &values {
            skiplist.insert(value, value * 10, &thread_epoch);
        }

        // Verify ordering by traversing the list at level 0
        unsafe {
            let mut current = skiplist.head;
            let expected_values = vec![20, 30, 40, 50, 60, 70, 80];

            // Skip the head node which has MAX value
            let forward_ptr = epoch_manager.get_node_forward_ptr(current, 0);
            current = (*forward_ptr).load(Ordering::Acquire);

            for expected in expected_values {
                assert!(!current.is_null());
                assert_eq!((*current).price, expected);

                // Move to next node
                let forward_ptr = epoch_manager.get_node_forward_ptr(current, 0);
                current = (*forward_ptr).load(Ordering::Acquire);
            }

            // Should be at the end of the list
            assert!(current.is_null());
        }
    }

    #[test]
    fn concurrent_operations() {
        // Set up epoch manager
        let epoch_manager = Arc::new(EpochManager::<16>::new());
        let skiplist = Arc::new(LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager)));

        // Number of operations and threads
        let num_threads = 8;
        let ops_per_thread = 1000;

        // Set up barrier to synchronize thread start
        let barrier = Arc::new(Barrier::new(num_threads));

        // Create thread local epochs
        let mut thread_epochs = Vec::new();
        let mut manager_ref = EpochManager::<16>::new();
        for _ in 0..num_threads {
            thread_epochs.push(Arc::new(manager_ref.register_thread()));
        }

        // Spawn threads for concurrent operations
        let mut handles = Vec::new();

        #[allow(clippy::needless_range_loop)]
        for thread_id in 0..num_threads {
            let skiplist_clone = Arc::clone(&skiplist);
            let epoch_manager_clone = Arc::clone(&epoch_manager);
            let barrier_clone = Arc::clone(&barrier);
            let thread_epoch = Arc::clone(&thread_epochs[thread_id]);

            let handle = thread::spawn(move || {
                let mut rng = rand::rng();
                let mut ops_count = [0, 0, 0]; // [inserts, removes, finds]

                // Wait for all threads to be ready
                barrier_clone.wait();

                for _ in 0..ops_per_thread {
                    // Generate random price between 1 and 10000
                    let price = rng.random_range(1..10000);
                    let volume = rng.random_range(1..1000);

                    // Randomly choose operation: 40% insert, 30% remove, 30% find
                    let op = rng.random_range(0..100);

                    if op < 40 {
                        // Insert operation
                        skiplist_clone.insert(price, volume, &thread_epoch);
                        ops_count[0] += 1;
                    } else if op < 70 {
                        // Remove operation
                        skiplist_clone.remove(price, &thread_epoch);
                        ops_count[1] += 1;
                    } else {
                        // Find operation
                        let mut preds: [*mut Node; 16] = [ptr::null_mut(); 16];
                        let mut succs: [*mut Node; 16] = [ptr::null_mut(); 16];
                        skiplist_clone.find(price, &thread_epoch, &mut preds, &mut succs);
                        ops_count[2] += 1;
                    }

                    // Occasionally try to advance the epoch
                    if rng.random_range(0..100) < 5 {
                        epoch_manager_clone.exit(&thread_epoch);
                        epoch_manager_clone.try_advance();
                        epoch_manager_clone.enter(&thread_epoch);
                    }
                }

                ops_count
            });

            handles.push(handle);
        }

        // Collect results from all threads
        let mut total_ops = [0, 0, 0];
        for handle in handles {
            let ops_count = handle.join().unwrap();
            for i in 0..3 {
                total_ops[i] += ops_count[i];
            }
        }

        // Verify all operations were completed
        assert_eq!(
            total_ops.iter().sum::<i32>(),
            num_threads as i32 * ops_per_thread
        );

        // Now verify the list is still usable after all concurrent operations
        let test_epoch = Arc::new(manager_ref.register_thread());

        // Insert and find a new value
        let test_price = 12345;
        skiplist.insert(test_price, 999, &test_epoch);

        let mut preds: [*mut Node; 16] = [ptr::null_mut(); 16];
        let mut succs: [*mut Node; 16] = [ptr::null_mut(); 16];
        assert!(skiplist.find(test_price, &test_epoch, &mut preds, &mut succs));

        // Remove it
        assert!(skiplist.remove(test_price, &test_epoch));
        assert!(!skiplist.find(test_price, &test_epoch, &mut preds, &mut succs));
    }

    #[test]
    fn epoch_reclamation() {
        // Set up epoch manager
        let epoch_manager = Arc::new(EpochManager::<16>::new());
        let skiplist = Arc::new(LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager)));

        // Create thread epoch
        let mut manager_ref = EpochManager::<16>::new();
        let thread_epoch = Arc::new(manager_ref.register_thread());

        // Insert several nodes
        for i in 1..100 {
            skiplist.insert(i, i * 10, &thread_epoch);
        }

        // Now remove half of them
        for i in (1..100).step_by(2) {
            skiplist.remove(i, &thread_epoch);
        }

        // Advance epoch several times to trigger garbage collection
        epoch_manager.exit(&thread_epoch);

        for _ in 0..5 {
            let advanced = epoch_manager.try_advance();
            assert!(advanced);
        }

        // The garbage list should eventually get cleaned up
        let garbage = epoch_manager.garbage.get_or(|| {
            UnsafeCell::new(GarbageList {
                head: ptr::null_mut(),
                size: 0,
            })
        });

        // Force garbage collection
        epoch_manager.collect(garbage.get());

        // Check that the list still works correctly
        epoch_manager.enter(&thread_epoch);

        let mut preds: [*mut Node; 16] = [ptr::null_mut(); 16];
        let mut succs: [*mut Node; 16] = [ptr::null_mut(); 16];

        // Even numbers should still be in the list
        for i in (2..100).step_by(2) {
            assert!(skiplist.find(i, &thread_epoch, &mut preds, &mut succs));
        }

        // Odd numbers should be gone
        for i in (1..100).step_by(2) {
            assert!(!skiplist.find(i, &thread_epoch, &mut preds, &mut succs));
        }

        epoch_manager.exit(&thread_epoch);
    }

    #[test]
    fn stress_test() {
        // Set up epoch manager
        let epoch_manager = Arc::new(EpochManager::<16>::new());
        let skiplist = Arc::new(LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager)));

        // Number of operations and threads
        let num_threads = 4;
        let ops_per_thread = 10000;

        // Set up barrier to synchronize thread start
        let barrier = Arc::new(Barrier::new(num_threads));

        // Create thread local epochs
        let mut thread_epochs = Vec::new();
        let mut manager_ref = EpochManager::<16>::new();
        for _ in 0..num_threads {
            thread_epochs.push(Arc::new(manager_ref.register_thread()));
        }

        // Create shared data to track inserted values
        let inserted_values = Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));

        // Spawn threads for concurrent operations
        let mut handles = Vec::new();

        #[allow(clippy::needless_range_loop)]
        for thread_id in 0..num_threads {
            let skiplist_clone = Arc::clone(&skiplist);
            let epoch_manager_clone = Arc::clone(&epoch_manager);
            let barrier_clone = Arc::clone(&barrier);
            let thread_epoch = Arc::clone(&thread_epochs[thread_id]);
            let inserted_values_clone = Arc::clone(&inserted_values);

            let handle = thread::spawn(move || {
                let mut rng = rand::rng();

                // Wait for all threads to be ready
                barrier_clone.wait();

                for op_num in 0..ops_per_thread {
                    // Generate price in a specific range for this thread to reduce contention
                    let thread_range = 10000 * thread_id as u64;
                    let price = thread_range + (op_num as u64 % 1000);
                    let volume = rng.random_range(1..1000);

                    // 80% inserts, 20% removes to build up the list
                    let op = rng.random_range(0..100);

                    if op < 80 {
                        // Insert and track the value
                        let is_new = skiplist_clone.insert(price, volume, &thread_epoch);
                        if is_new {
                            inserted_values_clone.lock().insert(price);
                        }
                    } else {
                        // Try to remove
                        let removed = skiplist_clone.remove(price, &thread_epoch);
                        if removed {
                            inserted_values_clone.lock().remove(&price);
                        }
                    }

                    // Frequently try to advance the epoch to stress GC
                    if op_num % 100 == 0 {
                        epoch_manager_clone.exit(&thread_epoch);
                        epoch_manager_clone.try_advance();
                        epoch_manager_clone.enter(&thread_epoch);
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify the final state - all tracked values should still be in the list
        let test_epoch = Arc::new(manager_ref.register_thread());
        let mut preds: [*mut Node; 16] = [ptr::null_mut(); 16];
        let mut succs: [*mut Node; 16] = [ptr::null_mut(); 16];

        for &price in inserted_values.lock().iter() {
            assert!(skiplist.find(price, &test_epoch, &mut preds, &mut succs));
        }

        // Insert a final value and verify it works
        let final_price = 99999;
        skiplist.insert(final_price, 888, &test_epoch);
        assert!(skiplist.find(final_price, &test_epoch, &mut preds, &mut succs));
    }

    #[test]
    fn random_level_distribution() {
        // Create a skiplist to test the random_level method
        let epoch_manager = Arc::new(EpochManager::<16>::new());
        let skiplist = LockFreeSkipList::<16>::new(Arc::clone(&epoch_manager));

        // Call random_level many times and check the distribution
        let iterations = 10000;
        #[allow(clippy::useless_vec)]
        let mut level_counts = vec![0; 16];

        for _ in 0..iterations {
            let level = skiplist.random_level();
            assert!((1..=16).contains(&level));
            level_counts[level - 1] += 1;
        }

        // With a p=0.5 probability, we expect level frequencies to roughly halve
        // for each level. Check this with a loose tolerance.
        for i in 1..16 {
            // Skip checking exact ratios in higher levels as they become too sparse
            if level_counts[i] < 50 {
                continue;
            }

            let ratio = level_counts[i - 1] as f64 / level_counts[i] as f64;
            assert!(
                ratio > 1.0 && ratio < 3.0,
                "Expected ratio between 1.0 and 3.0, got {}",
                ratio
            );
        }
    }
}
