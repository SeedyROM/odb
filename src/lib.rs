use once_cell::sync::Lazy;

pub mod epoch_manager;
pub mod lockfree_skiplist;
pub mod node;
pub mod node_allocator;

/// Alignment for cache lines (typically 64 bytes on modern CPUs)
pub(crate) static CACHE_LINE_SIZE: Lazy<usize> = Lazy::new(|| {
    // Try data cache first (most relevant for our use case)
    cache_size::cache_line_size(1, cache_size::CacheType::Data)
        // Fall back to unified cache if data cache info isn't available
        .or_else(|| cache_size::cache_line_size(1, cache_size::CacheType::Unified))
        // Try L2 cache if L1 isn't available
        .or_else(|| cache_size::cache_line_size(2, cache_size::CacheType::Data))
        .or_else(|| cache_size::cache_line_size(2, cache_size::CacheType::Unified))
        // Default to 64 bytes if all detection fails
        .unwrap_or(64)
});
