use crate::encryption::EncryptedData;
use std::io::Result;
use std::path::{Path, PathBuf};

pub trait Directory {
    fn create_subdir(&self, path: &Path) -> Result<()>;
    fn delete_file(&self, path: &Path) -> Result<()>;
    fn read_file(&self, path: &Path) -> Result<EncryptedData>;
    fn write_file(&self, path: &Path, data: &EncryptedData) -> Result<()>;
}

pub struct FilesystemDirectory {
    base_dir: PathBuf,
}

impl FilesystemDirectory {
    pub fn new(base_dir: &Path) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }
}

impl Directory for FilesystemDirectory {
    fn create_subdir(&self, path: &Path) -> Result<()> {
        std::fs::create_dir(self.base_dir.join(path))
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(self.base_dir.join(path))
    }

    fn read_file(&self, path: &Path) -> Result<EncryptedData> {
        std::fs::read(self.base_dir.join(path))
            .map(|bytes| EncryptedData::new(bytes.into_boxed_slice()))
    }

    fn write_file(&self, path: &Path, data: &EncryptedData) -> Result<()> {
        std::fs::write(self.base_dir.join(path), data.data())
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::io::ErrorKind;
    use std::path::PathBuf;
    use std::sync::Mutex;

    struct FakeDirectoryState {
        subdirs: HashSet<PathBuf>,
        files: HashMap<PathBuf, EncryptedData>,
    }

    pub struct FakeDirectory {
        state: Mutex<FakeDirectoryState>,
    }

    impl FakeDirectory {
        pub fn new() -> Self {
            Self {
                state: Mutex::new(FakeDirectoryState {
                    subdirs: HashSet::from([PathBuf::new()]),
                    files: HashMap::new(),
                }),
            }
        }
    }

    impl Directory for FakeDirectory {
        fn create_subdir(&self, path: &Path) -> Result<()> {
            let mut state = self.state.lock().unwrap();

            if let Some(parent) = path.parent()
                && !state.subdirs.contains(parent)
            {
                return Err(ErrorKind::NotFound.into());
            }

            if state.subdirs.contains(path) || state.files.contains_key(path) {
                Err(ErrorKind::AlreadyExists.into())
            } else {
                state.subdirs.insert(path.to_owned());
                Ok(())
            }
        }

        fn delete_file(&self, path: &Path) -> Result<()> {
            let mut state = self.state.lock().unwrap();

            match state.files.remove(path) {
                Some(_) => Ok(()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn read_file(&self, path: &Path) -> Result<EncryptedData> {
            let state = self.state.lock().unwrap();

            match state.files.get(path) {
                Some(data) => Ok(data.clone()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn write_file(&self, path: &Path, data: &EncryptedData) -> Result<()> {
            let mut state = self.state.lock().unwrap();

            if let Some(parent) = path.parent()
            {
                if !state.subdirs.contains(parent) {
                    return Err(ErrorKind::NotFound.into())
                }
            } else {
                return Err(ErrorKind::InvalidFilename.into());
            }

            state.files.insert(path.to_owned(), data.clone());
                Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::testing::FakeDirectory;
    use tempdir::TempDir;

    #[test]
    fn test_fake_directory() {
        let dir = FakeDirectory::new();

        assert!(dir.create_subdir(Path::new("foo")).is_ok());
        assert!(dir.create_subdir(Path::new("foo/bar")).is_ok());
        assert!(dir.create_subdir(Path::new("no/exist")).is_err());
        assert!(dir.create_subdir(Path::new("")).is_err());

        assert!(
            dir.write_file(
                Path::new("foo/thing.txt"),
                &EncryptedData::new([1, 2, 3].into())
            )
            .is_ok()
        );
        assert!(
            dir.write_file(
                Path::new("foo/bar/thing2.txt"),
                &EncryptedData::new([4, 5, 6].into())
            )
            .is_ok()
        );
        assert!(
            dir.write_file(
                Path::new("thing3.txt"),
                &EncryptedData::new([7, 8, 9].into())
            )
            .is_ok()
        );
        assert!(
            dir.write_file(
                Path::new("thing3.txt"),
                &EncryptedData::new([10, 11, 12].into())
            )
            .is_ok()
        );
        assert!(
            dir.write_file(
                Path::new("no/exist.txt"),
                &EncryptedData::new([13, 14, 15].into())
            )
            .is_err()
        );
        assert!(
            dir.write_file(Path::new(""), &EncryptedData::new([16, 17, 18].into()))
                .is_err()
        );

        assert_eq!(
            dir.read_file(Path::new("foo/thing.txt")).unwrap(),
            EncryptedData::new([1, 2, 3].into())
        );
        assert_eq!(
            dir.read_file(Path::new("foo/bar/thing2.txt")).unwrap(),
            EncryptedData::new([4, 5, 6].into())
        );
        assert_eq!(
            dir.read_file(Path::new("thing3.txt")).unwrap(),
            EncryptedData::new([10, 11, 12].into())
        );
        assert!(dir.read_file(Path::new("no/exist.txt")).is_err());
        assert!(dir.read_file(Path::new("foo")).is_err());
        assert!(dir.read_file(Path::new("")).is_err());

        assert!(dir.delete_file(Path::new("foo/thing.txt")).is_ok());
        assert!(dir.read_file(Path::new("foo/thing.txt")).is_err());

        assert!(dir.delete_file(Path::new("foo/bar/thing2.txt")).is_ok());
        assert!(dir.read_file(Path::new("foo/bar/thing2.txt")).is_err());

        assert!(dir.delete_file(Path::new("thing3.txt")).is_ok());
        assert!(dir.read_file(Path::new("thing3.txt")).is_err());

        assert!(dir.read_file(Path::new("thing3.txt")).is_err());
        assert!(dir.delete_file(Path::new("no/exist.txt")).is_err());
        assert!(dir.delete_file(Path::new("foo")).is_err());
        assert!(dir.delete_file(Path::new("")).is_err());
    }

    fn temp_fs_dir() -> (TempDir, FilesystemDirectory) {
        let temp_dir = TempDir::new("filesystem_directory_test").unwrap();
        let filesystem_directory = FilesystemDirectory::new(&temp_dir.path());

        (temp_dir, filesystem_directory)
    }
    #[test]
    fn test_filesystem_create_directory() {
        let (temp_dir, fs_dir) = temp_fs_dir();

        assert!(fs_dir.create_subdir(Path::new("foo")).is_ok());
        assert!(fs_dir.create_subdir(Path::new("foo/bar")).is_ok());
        assert!(fs_dir.create_subdir(Path::new("does/not/exist")).is_err());

        assert!(temp_dir.path().join("foo").is_dir());
        assert!(temp_dir.path().join("foo").join("bar").is_dir());
    }

    #[test]
    fn test_filesystem_delete_file() {
        let (temp_dir, fs_dir) = temp_fs_dir();

        std::fs::create_dir(temp_dir.path().join("foo")).unwrap();
        std::fs::create_dir(temp_dir.path().join("foo/bar")).unwrap();
        std::fs::write(temp_dir.path().join("foo/thing.txt"), [1, 2, 3]).unwrap();
        std::fs::write(temp_dir.path().join("foo/bar/thing2.txt"), [4, 5, 6]).unwrap();
        std::fs::write(temp_dir.path().join("thing3.txt"), [7, 8, 9]).unwrap();

        assert!(fs_dir.delete_file(Path::new("foo/thing.txt")).is_ok());
        assert!(!std::fs::exists(Path::new("foo/thing.txt")).unwrap());
        assert!(fs_dir.delete_file(Path::new("foo/bar/thing2.txt")).is_ok());
        assert!(!std::fs::exists(Path::new("foo/bar/thing2.txt")).unwrap());
        assert!(fs_dir.delete_file(Path::new("thing3.txt")).is_ok());
        assert!(!std::fs::exists(Path::new("thing3.txt")).unwrap());
        assert!(fs_dir.delete_file(Path::new("thing3.txt")).is_err());
        assert!(fs_dir.delete_file(Path::new("no/exist.txt")).is_err());
    }

    #[test]
    fn test_filesystem_write_file() {
        let (temp_dir, fs_dir) = temp_fs_dir();

        std::fs::create_dir(temp_dir.path().join("foo")).unwrap();
        std::fs::create_dir(temp_dir.path().join("foo/bar")).unwrap();

        assert!(
            fs_dir
                .write_file(
                    Path::new("foo/thing.txt"),
                    &EncryptedData::new([1, 2, 3].into())
                )
                .is_ok()
        );
        assert!(
            fs_dir
                .write_file(
                    Path::new("foo/bar/thing2.txt"),
                    &EncryptedData::new([4, 5, 6].into())
                )
                .is_ok()
        );
        assert!(
            fs_dir
                .write_file(
                    Path::new("thing3.txt"),
                    &EncryptedData::new([7, 8, 9].into())
                )
                .is_ok()
        );
        assert!(
            fs_dir
                .write_file(
                    Path::new("thing3.txt"),
                    &EncryptedData::new([10, 11, 12].into())
                )
                .is_ok()
        );
        assert!(
            fs_dir
                .write_file(
                    Path::new("does/not/exist.txt"),
                    &EncryptedData::new([12, 13, 14].into())
                )
                .is_err()
        );

        assert_eq!(
            std::fs::read(temp_dir.path().join("foo/thing.txt")).unwrap(),
            [1, 2, 3]
        );
        assert_eq!(
            std::fs::read(temp_dir.path().join("foo/bar/thing2.txt")).unwrap(),
            [4, 5, 6]
        );
        assert_eq!(
            std::fs::read(temp_dir.path().join("thing3.txt")).unwrap(),
            [10, 11, 12]
        );
    }

    #[test]
    fn test_filesystem_read_file() {
        let (temp_dir, fs_dir) = temp_fs_dir();

        std::fs::create_dir(temp_dir.path().join("foo")).unwrap();
        std::fs::create_dir(temp_dir.path().join("foo/bar")).unwrap();
        std::fs::write(temp_dir.path().join("foo/thing.txt"), [1, 2, 3]).unwrap();
        std::fs::write(temp_dir.path().join("foo/bar/thing2.txt"), [4, 5, 6]).unwrap();
        std::fs::write(temp_dir.path().join("thing3.txt"), [7, 8, 9]).unwrap();

        assert_eq!(
            fs_dir.read_file(Path::new("foo/thing.txt")).unwrap(),
            EncryptedData::new([1, 2, 3].into())
        );
        assert_eq!(
            fs_dir.read_file(Path::new("foo/bar/thing2.txt")).unwrap(),
            EncryptedData::new([4, 5, 6].into())
        );
        assert_eq!(
            fs_dir.read_file(Path::new("thing3.txt")).unwrap(),
            EncryptedData::new([7, 8, 9].into())
        );
        assert!(fs_dir.read_file(Path::new("no/exist.txt")).is_err());
        assert!(fs_dir.read_file(Path::new("foo")).is_err());
    }
}
