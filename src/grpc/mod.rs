pub mod types {
    tonic::include_proto!("types");
    pub const APP_FILE_DESCRIPTOR: &[u8] = tonic::include_file_descriptor_set!("app_descriptor");
}
pub mod node {
    tonic::include_proto!("node");
}
pub mod app {
    tonic::include_proto!("app");
}

pub mod utils;
