use std::sync::atomic::{AtomicPtr, AtomicU64};

// Node structure with careful memory layout optimization
#[repr(C, align(64))] // Align to cache line boundaries
pub struct Node {
    // Frequently accessed data in the first cache line
    pub(crate) price: u64,
    pub(crate) volume: AtomicU64,
    // Reference count for memory reclamation
    pub(crate) ref_count: AtomicU64,
    // Number of forward pointers this node has
    pub(crate) height: u32,
    // Padding to fill first cache line
    pub(crate) _padding: [u8; 32],
    // Forward pointers start at second cache line
    // Variable length array trick (in unsafe Rust)
    pub(crate) forward: [AtomicPtr<Node>; 0],
}
