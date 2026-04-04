use crate::directory::DirectoryPath;
use std::sync::atomic::{AtomicU64, Ordering};

pub trait DirectoryPathGen {
    fn generate_path(&self) -> DirectoryPath;
}

#[derive(Debug)]
pub struct SequentialDirectoryPathGen {
    base_dir: DirectoryPath,
    next_num: AtomicU64,
}

impl DirectoryPathGen for SequentialDirectoryPathGen {
    fn generate_path(&self) -> DirectoryPath {
        let next_num = self.next_num.fetch_add(1, Ordering::Relaxed);
        let filename = &next_num.to_string();
        &self.base_dir + &DirectoryPath::from(filename.as_str())
    }
}

impl SequentialDirectoryPathGen {
    pub fn new(base_dir: DirectoryPath, next_num: u64) -> Self {
        Self {
            base_dir,
            next_num: AtomicU64::new(next_num),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_directory_path_gen() {
        let generator = SequentialDirectoryPathGen::new(DirectoryPath::from("foo/bar"), 69);

        let path1 = generator.generate_path();
        let path2 = generator.generate_path();
        let path3 = generator.generate_path();

        assert_eq!(path1, DirectoryPath::from("foo/bar/69"));
        assert_eq!(path2, DirectoryPath::from("foo/bar/70"));
        assert_eq!(path3, DirectoryPath::from("foo/bar/71"));
    }
}
