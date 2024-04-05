use std::path::PathBuf;

pub fn relative_project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

pub mod typedefs;
