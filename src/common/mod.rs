use std::path::PathBuf;

pub fn relative_project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

#[macro_export]
macro_rules! metric {
    {$($block:stmt;)*} => {
        use cadence_macros::is_global_default_set;
        if is_global_default_set() {
            $(
                $block
            )*
        }
    };
}

pub mod typedefs;
