use crate::encryption::EncryptedData;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fs;
use std::io::Result;
use std::ops::Add;
use std::path::{Path, PathBuf};

// Represents a path within the disk directory that we mount the FUSE in.
//
// VirtualPath represents a path within the exposed FUSE mount.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub struct DirectoryPath(PathBuf);

impl From<&str> for DirectoryPath {
    // Create a path within a directory
    //
    // Panics if the path is not relative, or contains ".." in it
    fn from(value: &str) -> Self {
        Self::from(Path::new(value))
    }
}

impl From<&Path> for DirectoryPath {
    // Create a path within a directory
    //
    // Panics if the path is not relative, or contains ".." in it
    fn from(value: &Path) -> Self {
        let buf: PathBuf = value.into();
        assert!(buf.is_relative());
        for component in buf.components() {
            assert_ne!(component.as_os_str(), "..");
        }
        Self(buf)
    }
}

impl Add<&DirectoryPath> for &DirectoryPath {
    type Output = DirectoryPath;

    fn add(self, rhs: &DirectoryPath) -> Self::Output {
        DirectoryPath::from(self.0.join(&rhs.0).as_path())
    }
}

impl Display for DirectoryPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

pub enum FileType {
    Unknown,
    File,
    Directory,
}

pub trait Directory: Send + Sync {
    fn create_subdir(&self, path: &DirectoryPath) -> Result<()>;
    fn delete_file(&self, path: &DirectoryPath) -> Result<()>;
    fn exists(&self, path: &DirectoryPath) -> bool {
        !self.file_type(path).is_err()
    }
    fn file_type(&self, path: &DirectoryPath) -> Result<FileType>;
    fn is_directory(&self, path: &DirectoryPath) -> bool {
        match self.file_type(path) {
            Ok(FileType::Directory) => true,
            _ => false,
        }
    }
    fn is_file(&self, path: &DirectoryPath) -> bool {
        match self.file_type(path) {
            Ok(FileType::File) => true,
            _ => false,
        }
    }
    fn read_file(&self, path: &DirectoryPath) -> Result<EncryptedData>;
    fn write_file(&self, path: &DirectoryPath, data: &EncryptedData) -> Result<()>;
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
    fn create_subdir(&self, path: &DirectoryPath) -> Result<()> {
        fs::create_dir(self.base_dir.join(&path.0))
    }

    fn delete_file(&self, path: &DirectoryPath) -> Result<()> {
        fs::remove_file(self.base_dir.join(&path.0))
    }

    fn file_type(&self, path: &DirectoryPath) -> Result<FileType> {
        let metadata = fs::metadata(self.base_dir.join(&path.0))?;
        if metadata.is_dir() {
            Ok(FileType::Directory)
        } else if metadata.is_file() {
            Ok(FileType::File)
        } else {
            Ok(FileType::Unknown)
        }
    }

    fn read_file(&self, path: &DirectoryPath) -> Result<EncryptedData> {
        fs::read(self.base_dir.join(&path.0)).map(|bytes| EncryptedData::literal(&bytes))
    }

    fn write_file(&self, path: &DirectoryPath, data: &EncryptedData) -> Result<()> {
        fs::write(self.base_dir.join(&path.0), data.data())
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use rand::rngs::ChaCha20Rng;
    use rand::{RngExt, SeedableRng};
    use std::collections::{HashMap, HashSet};
    use std::fmt::Debug;
    use std::io::ErrorKind;
    use std::sync::Mutex;

    #[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
    pub enum FsOperation {
        CreateSubdir(DirectoryPath),
        DeleteFile(DirectoryPath),
        FileType(DirectoryPath),
        ReadFile(DirectoryPath),
        WriteFile(DirectoryPath, EncryptedData),
    }

    impl FsOperation {
        pub fn is_create_subdir(&self) -> bool {
            match self {
                FsOperation::CreateSubdir(_) => true,
                _ => false,
            }
        }

        pub fn is_delete_file(&self) -> bool {
            match self {
                FsOperation::DeleteFile(_) => true,
                _ => false,
            }
        }

        pub fn is_file_type(&self) -> bool {
            match self {
                FsOperation::FileType(_) => true,
                _ => false,
            }
        }

        pub fn is_read_file(&self) -> bool {
            match self {
                FsOperation::ReadFile(_) => true,
                _ => false,
            }
        }

        pub fn is_read_for_path(&self, path: &DirectoryPath) -> bool {
            match self {
                FsOperation::ReadFile(p) => p == path,
                _ => false,
            }
        }

        pub fn is_write_file(&self) -> bool {
            match self {
                FsOperation::WriteFile(_, _) => true,
                _ => false,
            }
        }

        pub fn is_write_for_path(&self, path: &DirectoryPath) -> bool {
            match self {
                FsOperation::WriteFile(p, _) => p == path,
                _ => false,
            }
        }
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
    pub struct FsLog {
        log: Vec<FsOperation>,
    }

    impl FsLog {
        fn new<I: Iterator<Item = FsOperation>>(iterator: I) -> Self {
            Self {
                log: iterator.collect(),
            }
        }

        pub fn assert_only_matching<P: FnMut(&FsOperation) -> bool>(&self, predicate: P) {
            assert_eq!(self.len(), 1, "Expected a single element, but got {:?}", self.log);
            self.assert_single_matching(predicate);
        }

        pub fn assert_matching<P: FnMut(&FsOperation) -> bool>(
            &self,
            expected_count: usize,
            predicate: P,
        ) {
            assert_eq!(expected_count, self.count_matching(predicate));
        }

        pub fn assert_single_matching<P: FnMut(&FsOperation) -> bool>(&self, predicate: P) {
            self.assert_matching(1, predicate);
        }

        pub fn count_matching<P: FnMut(&FsOperation) -> bool>(&self, mut predicate: P) -> usize {
            self.log.iter().filter(|e| predicate(*e)).count()
        }

        pub fn len(&self) -> usize {
            self.log.len()
        }
    }

    struct FakeDirectoryState {
        subdirs: HashSet<DirectoryPath>,
        files: HashMap<DirectoryPath, EncryptedData>,
        on_operation: Box<dyn Fn(FsOperation) -> Result<()> + Send + Sync>,
        log: Vec<FsOperation>,
    }

    impl Debug for FakeDirectoryState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&format!(
                "{:?}",
                (
                    "FakeDirectoryState",
                    "subdirs",
                    &self.subdirs,
                    "files",
                    &self.files,
                )
            ))
        }
    }

    #[derive(Debug)]
    pub struct FakeDirectory {
        state: Mutex<FakeDirectoryState>,
    }

    impl FakeDirectory {
        pub fn new() -> Self {
            Self {
                state: Mutex::new(FakeDirectoryState {
                    subdirs: HashSet::new(),
                    files: HashMap::new(),
                    on_operation: Box::new(|_| Ok(())),
                    log: Vec::new(),
                }),
            }
        }

        fn file_type_unlocked(
            state: &FakeDirectoryState,
            path: &DirectoryPath,
        ) -> Result<FileType> {
            if path == &DirectoryPath::from("") || state.subdirs.contains(path) {
                Ok(FileType::Directory)
            } else if state.files.contains_key(path) {
                Ok(FileType::File)
            } else {
                Err(ErrorKind::NotFound.into())
            }
        }

        fn parent_exists_unlocked(state: &FakeDirectoryState, path: &DirectoryPath) -> bool {
            path.0
                .parent()
                .map(
                    |parent| match Self::file_type_unlocked(state, &DirectoryPath::from(parent)) {
                        Ok(FileType::Directory) => true,
                        _ => false,
                    },
                )
                .unwrap_or(false)
        }

        pub fn disconnect(&self) {
            let mut state = self.state.lock().unwrap();
            state.on_operation = Box::new(|_| Err(ErrorKind::NetworkUnreachable.into()));
        }

        pub fn flake(&self, flake_proportion: f64, rng_seed: u64) {
            let mut state = self.state.lock().unwrap();

            let rng = Mutex::new(ChaCha20Rng::seed_from_u64(rng_seed));
            state.on_operation = Box::new(move |_| {
                let mut rng_synced = rng.lock().unwrap();
                let num = rng_synced.random_range(0.0..=1.0);
                if num <= flake_proportion {
                    Err(ErrorKind::NetworkUnreachable.into())
                } else {
                    Ok(())
                }
            });
        }

        pub fn list_subdir(&self, path: &DirectoryPath) -> Result<Vec<DirectoryPath>> {
            let state = self.state.lock().unwrap();

            match Self::file_type_unlocked(&state, path) {
                Ok(FileType::File) => return Err(ErrorKind::NotADirectory.into()),
                Err(e) => return Err(e),
                _ => {}
            }

            let mut results = Vec::new();
            for (p, _) in state.files.iter() {
                if p.0.parent() == Some(&path.0) {
                    results.push(p.clone())
                }
            }
            Ok(results)
        }

        pub fn log(&self) -> FsLog {
            let state = self.state.lock().unwrap();
            FsLog::new(state.log.iter().map(|x| x.clone()))
        }

        pub fn log_during<F: FnOnce()>(&self, func: F) -> FsLog {
            let size_before = {
                let state = self.state.lock().unwrap();
                state.log.len()
            };

            func();

            let state = self.state.lock().unwrap();
            if state.log.len() == size_before {
                FsLog::new(std::iter::empty())
            } else {
                FsLog::new(state.log[size_before..].iter().map(|x| x.clone()))
            }
        }

        pub fn on_operation(&self, f: Box<dyn Fn(FsOperation) -> Result<()> + Send + Sync>) {
            let mut state = self.state.lock().unwrap();
            state.on_operation = f;
        }

        pub fn reset_on_operation(&self) {
            let mut state = self.state.lock().unwrap();
            state.on_operation = Box::new(|_| Ok(()))
        }
    }

    impl Directory for FakeDirectory {
        fn create_subdir(&self, path: &DirectoryPath) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            let operation = FsOperation::CreateSubdir(path.clone());
            state.log.push(operation.clone());
            state.on_operation.as_ref()(operation)?;

            match Self::file_type_unlocked(&state, path) {
                Ok(_) => return Err(ErrorKind::AlreadyExists.into()),
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                }
            }

            if !Self::parent_exists_unlocked(&state, path) {
                return Err(ErrorKind::NotFound.into());
            }

            state.subdirs.insert(path.to_owned());
            Ok(())
        }

        fn delete_file(&self, path: &DirectoryPath) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            let operation = FsOperation::DeleteFile(path.clone());
            state.log.push(operation.clone());
            state.on_operation.as_ref()(operation)?;

            match Self::file_type_unlocked(&state, path) {
                Ok(FileType::Directory) => return Err(ErrorKind::IsADirectory.into()),
                _ => {}
            }

            match state.files.remove(path) {
                Some(_) => Ok(()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn file_type(&self, path: &DirectoryPath) -> Result<FileType> {
            let mut state = self.state.lock().unwrap();
            let operation = FsOperation::FileType(path.clone());
            state.log.push(operation.clone());
            state.on_operation.as_ref()(operation)?;

            Self::file_type_unlocked(&state, path)
        }

        fn read_file(&self, path: &DirectoryPath) -> Result<EncryptedData> {
            let mut state = self.state.lock().unwrap();
            let operation = FsOperation::ReadFile(path.clone());
            state.log.push(operation.clone());
            state.on_operation.as_ref()(operation)?;

            match Self::file_type_unlocked(&state, path) {
                Ok(FileType::Directory) => return Err(ErrorKind::IsADirectory.into()),
                _ => {}
            }

            match state.files.get(path) {
                Some(data) => Ok(data.clone()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn write_file(&self, path: &DirectoryPath, data: &EncryptedData) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            let operation = FsOperation::WriteFile(path.clone(), data.clone());
            state.log.push(operation.clone());
            state.on_operation.as_ref()(operation)?;

            match Self::file_type_unlocked(&state, path) {
                Ok(FileType::Directory) => return Err(ErrorKind::IsADirectory.into()),
                _ => {}
            }

            if !Self::parent_exists_unlocked(&state, path) {
                return Err(ErrorKind::NotFound.into());
            }

            state.files.insert(path.to_owned(), data.clone());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::testing::assert_error_kind;
    use assertables::{assert_ok, assert_ok_eq_x};
    use rstest::rstest;
    use std::io::ErrorKind;
    use std::io::Result;
    use tempdir::TempDir;

    fn temp_fs_dir() -> (TempDir, FilesystemDirectory) {
        let temp_dir = assert_ok!(TempDir::new("filesystem_directory_test"));
        let filesystem_directory = FilesystemDirectory::new(&temp_dir.path());

        (temp_dir, filesystem_directory)
    }

    // Implement Directory for the pair above so the TempDir remains in scope while
    // the test is ongoing and deletes the TempDir when the test is done.
    impl Directory for (TempDir, FilesystemDirectory) {
        fn create_subdir(&self, path: &DirectoryPath) -> Result<()> {
            self.1.create_subdir(path)
        }

        fn delete_file(&self, path: &DirectoryPath) -> Result<()> {
            self.1.delete_file(path)
        }

        fn file_type(&self, path: &DirectoryPath) -> Result<FileType> {
            self.1.file_type(path)
        }

        fn read_file(&self, path: &DirectoryPath) -> Result<EncryptedData> {
            self.1.read_file(path)
        }

        fn write_file(&self, path: &DirectoryPath, data: &EncryptedData) -> Result<()> {
            self.1.write_file(path, data)
        }
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_create_duplicate_dir_fails(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_error_kind(
            dir.create_subdir(&DirectoryPath::from("foo")),
            ErrorKind::AlreadyExists,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_create_subdir_without_parent_fails(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.create_subdir(&DirectoryPath::from("foo/bar")),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_create_root_dir_fails(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.create_subdir(&DirectoryPath::from("")),
            ErrorKind::AlreadyExists,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_fails_for_nonexistent_file(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.delete_file(&DirectoryPath::from("foo")),
            ErrorKind::NotFound,
        );
        assert_error_kind(
            dir.delete_file(&DirectoryPath::from("foo/bar")),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_fails_for_directory(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.delete_file(&DirectoryPath::from("")),
            ErrorKind::IsADirectory,
        );
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_error_kind(
            dir.delete_file(&DirectoryPath::from("foo")),
            ErrorKind::IsADirectory,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_no_longer_exists(#[case] dir: impl Directory) {
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.delete_file(&DirectoryPath::from("foo")));
        assert_error_kind(
            dir.read_file(&DirectoryPath::from("foo")),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_in_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.delete_file(&DirectoryPath::from("foo/bar")));
        assert_error_kind(
            dir.read_file(&&DirectoryPath::from("foo/bar")),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_in_sub_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar")));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar/baz"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.delete_file(&DirectoryPath::from("foo/bar/baz")));
        assert_error_kind(
            dir.read_file(&DirectoryPath::from("foo/bar/baz")),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_exists(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("baz")));

        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/file1"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("file2"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar/file3"),
            &EncryptedData::literal(&[1, 2, 3])
        ));

        assert!(dir.exists(&DirectoryPath::from("")));
        assert!(dir.exists(&DirectoryPath::from("foo")));
        assert!(dir.exists(&DirectoryPath::from("foo/file1")));
        assert!(dir.exists(&DirectoryPath::from("foo/bar")));
        assert!(dir.exists(&DirectoryPath::from("foo/bar/file3")));
        assert!(dir.exists(&DirectoryPath::from("file2")));
        assert!(dir.exists(&DirectoryPath::from("baz")));

        assert!(!dir.exists(&DirectoryPath::from("blah")));
        assert!(!dir.exists(&DirectoryPath::from("foo/blah")));
        assert!(!dir.exists(&DirectoryPath::from("foo/bar/blah")));
    }

    #[test]
    fn test_is_create_subdir() {
        assert!(FsOperation::CreateSubdir(DirectoryPath::from("foo")).is_create_subdir());
        assert!(!FsOperation::DeleteFile(DirectoryPath::from("foo")).is_create_subdir());
    }

    #[test]
    fn test_is_delete_file() {
        assert!(FsOperation::DeleteFile(DirectoryPath::from("foo")).is_delete_file());
        assert!(!FsOperation::CreateSubdir(DirectoryPath::from("foo")).is_delete_file());
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_is_directory(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("baz")));

        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/file1"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("file2"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar/file3"),
            &EncryptedData::literal(&[1, 2, 3])
        ));

        assert!(dir.is_directory(&DirectoryPath::from("")));
        assert!(dir.is_directory(&DirectoryPath::from("foo")));
        assert!(dir.is_directory(&DirectoryPath::from("foo/bar")));
        assert!(dir.is_directory(&DirectoryPath::from("baz")));

        assert!(!dir.is_directory(&DirectoryPath::from("foo/file1")));
        assert!(!dir.is_directory(&DirectoryPath::from("foo/bar/file3")));
        assert!(!dir.is_directory(&DirectoryPath::from("file2")));
        assert!(!dir.is_directory(&DirectoryPath::from("blah")));
        assert!(!dir.is_directory(&DirectoryPath::from("foo/blah")));
        assert!(!dir.is_directory(&DirectoryPath::from("foo/bar/blah")));
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_is_file(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("baz")));

        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/file1"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("file2"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar/file3"),
            &EncryptedData::literal(&[1, 2, 3])
        ));

        assert!(dir.is_file(&DirectoryPath::from("foo/file1")));
        assert!(dir.is_file(&DirectoryPath::from("foo/bar/file3")));
        assert!(dir.is_file(&DirectoryPath::from("file2")));

        assert!(!dir.is_file(&DirectoryPath::from("")));
        assert!(!dir.is_file(&DirectoryPath::from("foo")));
        assert!(!dir.is_file(&DirectoryPath::from("foo/bar")));
        assert!(!dir.is_file(&DirectoryPath::from("baz")));
        assert!(!dir.is_file(&DirectoryPath::from("blah")));
        assert!(!dir.is_file(&DirectoryPath::from("foo/blah")));
        assert!(!dir.is_file(&DirectoryPath::from("foo/bar/blah")));
    }

    #[test]
    fn test_is_file_type() {
        assert!(FsOperation::FileType(DirectoryPath::from("foo")).is_file_type());
        assert!(!FsOperation::CreateSubdir(DirectoryPath::from("foo")).is_file_type());
    }

    #[test]
    fn test_is_read_file() {
        assert!(FsOperation::ReadFile(DirectoryPath::from("foo")).is_read_file());
        assert!(!FsOperation::CreateSubdir(DirectoryPath::from("foo")).is_read_file());
    }

    #[test]
    fn test_is_read_for_path() {
        assert!(
            FsOperation::ReadFile(DirectoryPath::from("foo"))
                .is_read_for_path(&DirectoryPath::from("foo"))
        );
        assert!(
            !FsOperation::ReadFile(DirectoryPath::from("foo"))
                .is_read_for_path(&DirectoryPath::from("bar"))
        );
        assert!(
            !FsOperation::CreateSubdir(DirectoryPath::from("foo"))
                .is_read_for_path(&DirectoryPath::from("foo"))
        );
    }

    #[test]
    fn test_is_write_file() {
        assert!(
            FsOperation::WriteFile(
                DirectoryPath::from("foo"),
                EncryptedData::literal(&[1, 2, 3])
            )
            .is_write_file()
        );
        assert!(!FsOperation::CreateSubdir(DirectoryPath::from("foo")).is_write_file());
    }

    #[test]
    fn test_is_write_for_path() {
        assert!(
            FsOperation::WriteFile(
                DirectoryPath::from("foo"),
                EncryptedData::literal(&[1, 2, 3])
            )
            .is_write_for_path(&DirectoryPath::from("foo"))
        );
        assert!(
            !FsOperation::WriteFile(
                DirectoryPath::from("foo"),
                EncryptedData::literal(&[1, 2, 3])
            )
            .is_write_for_path(&DirectoryPath::from("bar"))
        );
        assert!(
            !FsOperation::CreateSubdir(DirectoryPath::from("foo"))
                .is_write_for_path(&DirectoryPath::from("foo"))
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_read_file_fails_for_nonexistent_file(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.read_file(&DirectoryPath::from("foo")),
            ErrorKind::NotFound,
        );
        assert_error_kind(
            dir.read_file(&DirectoryPath::from("foo/bar")),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_read_file_fails_for_directory(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.read_file(&DirectoryPath::from("")),
            ErrorKind::IsADirectory,
        );
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_error_kind(
            dir.read_file(&DirectoryPath::from("foo")),
            ErrorKind::IsADirectory,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_fails_for_directory(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.write_file(
                &DirectoryPath::from(""),
                &EncryptedData::literal(&[1, 2, 3]),
            ),
            ErrorKind::IsADirectory,
        );
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_error_kind(
            dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3]),
            ),
            ErrorKind::IsADirectory,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_fails_for_missing_parent(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.write_file(
                &DirectoryPath::from("foo/bar"),
                &EncryptedData::literal(&[1, 2, 3]),
            ),
            ErrorKind::NotFound,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_multiple_files(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("dir1")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("dir1/dir2")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("dir2")));

        assert_ok!(dir.write_file(
            &DirectoryPath::from("file1"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("file2"),
            &EncryptedData::literal(&[4, 5, 6])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("dir1/file1"),
            &EncryptedData::literal(&[7, 8, 9])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("dir1/dir2/file3"),
            &EncryptedData::literal(&[10, 11, 12])
        ));

        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("file1")),
            &EncryptedData::literal(&[1, 2, 3])
        );
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("file2")),
            &EncryptedData::literal(&[4, 5, 6])
        );
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("dir1/file1")),
            &EncryptedData::literal(&[7, 8, 9])
        );
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("dir1/dir2/file3")),
            &EncryptedData::literal(&[10, 11, 12])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_overwrites(#[case] dir: impl Directory) {
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo"),
            &EncryptedData::literal(&[4, 5, 6])
        ));
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("foo")),
            &EncryptedData::literal(&[4, 5, 6])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_round_trip(#[case] dir: impl Directory) {
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("foo")),
            &EncryptedData::literal(&[1, 2, 3])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_in_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("foo/bar")),
            &EncryptedData::literal(&[1, 2, 3])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_in_sub_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar")));
        assert_ok!(dir.write_file(
            &DirectoryPath::from("foo/bar/baz"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok_eq_x!(
            &dir.read_file(&DirectoryPath::from("foo/bar/baz")),
            &EncryptedData::literal(&[1, 2, 3])
        );
    }

    mod fake_directory_tests {
        use super::super::*;
        use super::*;

        #[test]
        fn test_create_subdir_log() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            dir.log().assert_only_matching(|op| op.is_create_subdir());
        }

        #[test]
        fn test_create_subdir_log_failed_operation() {
            let dir = FakeDirectory::new();

            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo/bar")),
                ErrorKind::NotFound,
            );
            dir.log().assert_only_matching(|op| op.is_create_subdir());
        }

        #[test]
        fn test_create_subdir_with_disconnection() {
            let dir = FakeDirectory::new();
            dir.disconnect();

            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::NetworkUnreachable,
            );
        }

        #[test]
        fn test_delete_file_log() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.log_during(|| {
                assert_ok!(dir.delete_file(&DirectoryPath::from("foo")));
            })
            .assert_only_matching(|op| op.is_delete_file());
        }

        #[test]
        fn test_delete_file_log_failed_operation() {
            let dir = FakeDirectory::new();

            assert_error_kind(
                dir.delete_file(&DirectoryPath::from("foo")),
                ErrorKind::NotFound,
            );
            dir.log().assert_only_matching(|op| op.is_delete_file());
        }

        #[test]
        fn test_delete_file_with_disconnection() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            dir.disconnect();

            assert_error_kind(
                dir.delete_file(&DirectoryPath::from("foo")),
                ErrorKind::NetworkUnreachable,
            );
        }

        #[test]
        fn test_disconnect() {
            let dir = FakeDirectory::new();

            dir.disconnect();
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::NetworkUnreachable,
            );
            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("bar"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::NetworkUnreachable,
            );
            assert_error_kind(
                dir.read_file(&DirectoryPath::from("baz/qqq")),
                ErrorKind::NetworkUnreachable,
            );
        }

        #[test]
        fn test_exists_log() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("bar"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            let first_log = dir.log_during(|| {
                assert!(dir.exists(&DirectoryPath::from("foo")));
            });
            first_log.assert_only_matching(|op| op.is_file_type());

            let second_log = dir.log_during(|| {
                assert!(dir.exists(&DirectoryPath::from("bar")));
            });
            second_log.assert_only_matching(|op| op.is_file_type());
        }

        #[test]
        fn test_exists_log_not_found() {
            let dir = FakeDirectory::new();

            assert!(!dir.exists(&DirectoryPath::from("foo")));
            dir.log().assert_only_matching(|op| op.is_file_type());
        }

        #[test]
        fn test_exists_with_disconnection() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("bar"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.disconnect();

            assert!(!dir.exists(&DirectoryPath::from("foo")));
            assert!(!dir.exists(&DirectoryPath::from("bar")));
        }

        #[test]
        fn test_is_directory_log() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));

            dir.log_during(|| {
                assert!(dir.is_directory(&DirectoryPath::from("foo")));
            })
            .assert_only_matching(|op| op.is_file_type());
        }

        #[test]
        fn test_is_directory_log_not_found() {
            let dir = FakeDirectory::new();

            assert!(!dir.is_directory(&DirectoryPath::from("foo")));
            dir.log().assert_only_matching(|op| op.is_file_type());
        }

        #[test]
        fn test_is_directory_with_disconnection() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            dir.disconnect();

            assert!(!dir.is_directory(&DirectoryPath::from("foo")));
        }

        #[test]
        fn test_is_file_log() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.log_during(|| {
                assert!(dir.is_file(&DirectoryPath::from("foo")));
            })
            .assert_only_matching(|op| op.is_file_type());
        }

        #[test]
        fn test_is_file_log_not_found() {
            let dir = FakeDirectory::new();

            assert!(!dir.is_file(&DirectoryPath::from("foo")));
            dir.log().assert_only_matching(|op| op.is_file_type());
        }

        #[test]
        fn test_is_file_with_disconnection() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.disconnect();
            assert!(!dir.is_file(&DirectoryPath::from("foo")));
        }

        #[test]
        fn test_list_dir() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar")));
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo/bar/baz")));
            assert_ok!(dir.create_subdir(&DirectoryPath::from("ghlarbl")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo/bar/file1.txt"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo/bar/file2.txt"),
                &EncryptedData::literal(&[4, 5, 6])
            ));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo/file3.txt"),
                &EncryptedData::literal(&[7, 8, 9])
            ));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo/bar/baz/file3.txt"),
                &EncryptedData::literal(&[10, 11, 12])
            ));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("file4.txt"),
                &EncryptedData::literal(&[13, 14, 15])
            ));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("ghlarbl/file5.txt"),
                &EncryptedData::literal(&[16, 17, 18])
            ));

            let mut entries = assert_ok!(dir.list_subdir(&DirectoryPath::from("foo/bar")));
            entries.sort();

            assert_eq!(
                entries,
                &[
                    DirectoryPath::from("foo/bar/file1.txt"),
                    DirectoryPath::from("foo/bar/file2.txt"),
                ]
            );
        }

        #[test]
        fn test_list_dir_fails_on_file() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_error_kind(
                dir.list_subdir(&DirectoryPath::from("foo")),
                ErrorKind::NotADirectory,
            );
        }

        #[test]
        fn test_list_dir_fails_on_not_found() {
            let dir = FakeDirectory::new();

            assert_error_kind(
                dir.list_subdir(&DirectoryPath::from("foo")),
                ErrorKind::NotFound,
            );
        }

        #[test]
        fn test_log() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo/bar"),
                &EncryptedData::literal(&[1, 2, 3]),
            ));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("baz"),
                &EncryptedData::literal(&[4, 5, 6]),
            ));
            assert_ok!(dir.read_file(&DirectoryPath::from("foo/bar"),));

            let log = dir.log();
            assert_eq!(log.len(), 4);
            log.assert_single_matching(|op| op.is_create_subdir());
            log.assert_single_matching(|op| op.is_read_file());
            log.assert_matching(2, |op| op.is_write_file());
        }

        #[test]
        fn test_log_during() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            let log = dir.log_during(|| {
                assert_ok!(dir.write_file(
                    &DirectoryPath::from("foo/bar"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ));
                assert_ok!(dir.write_file(
                    &DirectoryPath::from("baz"),
                    &EncryptedData::literal(&[4, 5, 6]),
                ));
                assert_ok!(dir.read_file(&DirectoryPath::from("foo/bar")));
            });

            let log2 = dir.log_during(|| {
                assert_ok!(dir.read_file(&DirectoryPath::from("baz")));
            });

            assert_ok!(dir.write_file(
                &DirectoryPath::from("dsklfjskldj"),
                &EncryptedData::literal(&[7, 8, 9])
            ));

            assert_eq!(log.len(), 3);
            log.assert_single_matching(|op| op.is_read_file());
            log.assert_matching(2, |op| op.is_write_file());
            log2.assert_only_matching(|op| op.is_read_file());
        }

        #[test]
        fn test_log_during_no_operations() {
            let dir = FakeDirectory::new();

            let log = dir.log_during(|| {});

            assert_eq!(log.len(), 0);
        }

        #[test]
        fn test_read_file_log() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.log_during(|| {
                assert_ok!(dir.read_file(&DirectoryPath::from("foo")));
            })
            .assert_only_matching(|op| op.is_read_file());
        }

        #[test]
        fn test_read_file_log_failed_operation() {
            let dir = FakeDirectory::new();

            assert_error_kind(
                dir.read_file(&DirectoryPath::from("foo")),
                ErrorKind::NotFound,
            );
            dir.log().assert_only_matching(|op| op.is_read_file());
        }

        #[test]
        fn test_read_file_with_disconnect() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.disconnect();
            assert_error_kind(
                dir.read_file(&DirectoryPath::from("foo")),
                ErrorKind::NetworkUnreachable,
            );
        }

        #[test]
        fn test_reset_on_operation() {
            let dir = FakeDirectory::new();

            dir.disconnect();
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::NetworkUnreachable,
            );
            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("bar"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::NetworkUnreachable,
            );

            dir.reset_on_operation();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("bar"),
                &EncryptedData::literal(&[1, 2, 3])
            ),);
        }

        #[test]
        fn test_write_file_log() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3]),
            ));

            dir.log().assert_only_matching(|op| op.is_write_file());
        }

        #[test]
        fn test_write_file_log_failed_operation() {
            let dir = FakeDirectory::new();

            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("foo/bar"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::NotFound,
            );
            dir.log().assert_only_matching(|op| op.is_write_file());
        }

        #[test]
        fn test_write_file_with_injected_error() {
            let dir = FakeDirectory::new();
            dir.disconnect();

            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("foo"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::NetworkUnreachable,
            );
        }
    }
}
