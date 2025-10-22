pub mod queue_monitor;
pub mod queue_service;
pub mod server;

// Include the generated proto code
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/photon.rs"));

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/photon_descriptor.bin"));
}
