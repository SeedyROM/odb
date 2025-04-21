use std::{
    alloc::{Layout, alloc, dealloc},
    ptr,
    sync::atomic::{AtomicPtr, AtomicU64},
};

use crate::{CACHE_LINE_SIZE, node::Node};

/// Memory manager for [`Node`] allocation
pub struct NodeAllocator<const S: usize> {
    // Pre-allocated memory pools for different node heights
    pub pools: Vec<Vec<*mut u8>>,
}

unsafe impl<const S: usize> Send for NodeAllocator<S> {}
unsafe impl<const S: usize> Sync for NodeAllocator<S> {}

impl<const S: usize> Default for NodeAllocator<S> {
    fn default() -> Self {
        NodeAllocator::new()
    }
}

impl<const S: usize> NodeAllocator<S> {
    pub fn new() -> Self {
        // Initialize with a vector of S empty vectors
        let mut pools = Vec::with_capacity(S);
        for _ in 0..S {
            pools.push(Vec::new());
        }

        NodeAllocator { pools }
    }

    /// Allocate a node with the given height
    pub(crate) fn allocate(&mut self, price: u64, volume: u64, height: usize) -> *mut Node {
        let forward_ptr_size = std::mem::size_of::<AtomicPtr<Node>>();

        // Calculate total size needed for the node
        let node_size = std::mem::size_of::<Node>() + height * forward_ptr_size;

        // Round up to cache line size
        let aligned_size = (node_size + *CACHE_LINE_SIZE - 1) & !(*CACHE_LINE_SIZE - 1);

        // Create layout with proper alignment
        let layout = Layout::from_size_align(aligned_size, *CACHE_LINE_SIZE).unwrap();

        // Allocate memory
        let ptr = unsafe { alloc(layout) as *mut Node };

        // Initialize the node
        unsafe {
            ptr.write(Node {
                price,
                volume: AtomicU64::new(volume),
                ref_count: AtomicU64::new(1),
                height: height as u32,
                _padding: [0; 32],
                forward: [],
            });

            // Initialize all forward pointers
            let forward_ptr = ptr.add(1) as *mut AtomicPtr<Node>;
            for i in 0..height {
                (forward_ptr.add(i)).write(AtomicPtr::new(ptr::null_mut()));
            }
        }

        ptr
    }

    pub(crate) fn get_forward_ptr(&self, node: *mut Node, level: usize) -> *mut AtomicPtr<Node> {
        unsafe {
            // Forward pointers start after the Node struct
            let forward_base =
                (node as *mut u8).add(std::mem::size_of::<Node>()) as *mut AtomicPtr<Node>;
            forward_base.add(level)
        }
    }

    pub(crate) fn deallocate(&mut self, node: *mut Node) {
        let height = unsafe { (*node).height as usize };
        let forward_ptr_size = std::mem::size_of::<AtomicPtr<Node>>();

        // Calculate total size
        let node_size = std::mem::size_of::<Node>() + height * forward_ptr_size;

        // Round up to cache line size
        let aligned_size = (node_size + *CACHE_LINE_SIZE - 1) & !(*CACHE_LINE_SIZE - 1);

        // Create layout with proper alignment
        let layout = Layout::from_size_align(aligned_size, *CACHE_LINE_SIZE).unwrap();

        // Deallocate
        unsafe { dealloc(node as *mut u8, layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use std::sync::atomic::Ordering;

    #[test]
    fn node_allocator_creation() {
        let allocator = NodeAllocator::<16>::new();
        assert_eq!(allocator.pools.len(), 16);
        for pool in &allocator.pools {
            assert!(pool.is_empty());
        }
    }

    #[test]
    fn node_allocation() {
        let mut allocator = NodeAllocator::<16>::new();

        // Test allocation of different heights
        let heights = [1, 4, 8, 15];

        for &height in &heights {
            let price = 100;
            let volume = 200;

            let node_ptr = allocator.allocate(price, volume, height);

            unsafe {
                // Verify node properties
                assert_eq!((*node_ptr).price, price);
                assert_eq!((*node_ptr).volume.load(Ordering::Relaxed), volume);
                assert_eq!((*node_ptr).ref_count.load(Ordering::Relaxed), 1);
                assert_eq!((*node_ptr).height, height as u32);

                // Test forward pointers - they should all be null
                for i in 0..height {
                    let forward_ptr = allocator.get_forward_ptr(node_ptr, i);
                    assert_eq!((*forward_ptr).load(Ordering::Relaxed), ptr::null_mut());
                }

                // Clean up
                allocator.deallocate(node_ptr);
            }
        }
    }

    #[test]
    fn forward_pointers() {
        let mut allocator = NodeAllocator::<16>::new();

        // Create nodes with different heights
        let node1 = allocator.allocate(100, 200, 2);
        let node2 = allocator.allocate(101, 201, 3);
        let node3 = allocator.allocate(102, 202, 1);

        unsafe {
            // Connect nodes: node1 -> node2 -> node3
            let forward_ptr1_0 = allocator.get_forward_ptr(node1, 0);
            (*forward_ptr1_0).store(node2, Ordering::SeqCst);

            let forward_ptr2_0 = allocator.get_forward_ptr(node2, 0);
            (*forward_ptr2_0).store(node3, Ordering::SeqCst);

            // Check connections
            let ptr1_next = (*forward_ptr1_0).load(Ordering::SeqCst);
            assert_eq!(ptr1_next, node2);
            assert_eq!((*ptr1_next).price, 101);

            let ptr2_next = (*forward_ptr2_0).load(Ordering::SeqCst);
            assert_eq!(ptr2_next, node3);
            assert_eq!((*ptr2_next).price, 102);

            // Test higher level pointer
            let forward_ptr1_1 = allocator.get_forward_ptr(node1, 1);
            (*forward_ptr1_1).store(node3, Ordering::SeqCst);

            let ptr1_skip = (*forward_ptr1_1).load(Ordering::SeqCst);
            assert_eq!(ptr1_skip, node3);

            // Clean up
            allocator.deallocate(node3);
            allocator.deallocate(node2);
            allocator.deallocate(node1);
        }
    }

    #[test]
    fn memory_alignment() {
        let mut allocator = NodeAllocator::<16>::new();

        // Allocate nodes with different heights
        let heights = [1, 3, 7, 12];
        let mut nodes = Vec::new();

        for &height in &heights {
            let node = allocator.allocate(100, 200, height);
            nodes.push(node);

            // Check that node is properly aligned to cache line
            assert_eq!((node as usize) % *CACHE_LINE_SIZE, 0);
        }

        // Clean up
        for node in nodes {
            allocator.deallocate(node);
        }
    }

    #[test]
    fn ref_counting() {
        let mut allocator = NodeAllocator::<16>::new();
        let node = allocator.allocate(100, 200, 1);

        unsafe {
            // Initial ref count should be 1
            assert_eq!((*node).ref_count.load(Ordering::Relaxed), 1);

            // Increment ref count
            (*node).ref_count.fetch_add(1, Ordering::SeqCst);
            assert_eq!((*node).ref_count.load(Ordering::Relaxed), 2);

            // Decrement ref count
            (*node).ref_count.fetch_sub(1, Ordering::SeqCst);
            assert_eq!((*node).ref_count.load(Ordering::Relaxed), 1);

            // Clean up
            allocator.deallocate(node);
        }
    }

    #[test]
    fn volume_updates() {
        let mut allocator = NodeAllocator::<16>::new();
        let node = allocator.allocate(100, 200, 1);

        unsafe {
            // Initial volume should be 200
            assert_eq!((*node).volume.load(Ordering::Relaxed), 200);

            // Update volume
            (*node).volume.store(300, Ordering::SeqCst);
            assert_eq!((*node).volume.load(Ordering::Relaxed), 300);

            // Increment volume
            (*node).volume.fetch_add(50, Ordering::SeqCst);
            assert_eq!((*node).volume.load(Ordering::Relaxed), 350);

            // Clean up
            allocator.deallocate(node);
        }
    }
}
