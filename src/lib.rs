pub mod epoch_manager;
pub mod lockfree_skiplist;
pub mod node;
pub mod node_allocator;

// Alignment for cache lines (typically 64 bytes on modern CPUs)
pub const CACHE_LINE_SIZE: usize = 64;
