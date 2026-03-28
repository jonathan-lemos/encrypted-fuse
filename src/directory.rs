use crate::encryption::EncryptedData;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::Result;
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

impl Display for DirectoryPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

pub trait Directory {
    fn create_subdir(&self, path: &DirectoryPath) -> Result<()>;
    fn delete_file(&self, path: &DirectoryPath) -> Result<()>;
    fn exists(&self, path: &DirectoryPath) -> bool {
        self.is_directory(path) || self.is_file(path)
    }
    fn is_directory(&self, path: &DirectoryPath) -> bool;
    fn is_file(&self, path: &DirectoryPath) -> bool;
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
        std::fs::create_dir(self.base_dir.join(&path.0))
    }

    fn delete_file(&self, path: &DirectoryPath) -> Result<()> {
        std::fs::remove_file(self.base_dir.join(&path.0))
    }

    fn is_directory(&self, path: &DirectoryPath) -> bool {
        self.base_dir.join(&path.0).is_dir()
    }

    fn is_file(&self, path: &DirectoryPath) -> bool {
        self.base_dir.join(&path.0).is_file()
    }

    fn read_file(&self, path: &DirectoryPath) -> Result<EncryptedData> {
        std::fs::read(self.base_dir.join(&path.0)).map(|bytes| EncryptedData::literal(&bytes))
    }

    fn write_file(&self, path: &DirectoryPath, data: &EncryptedData) -> Result<()> {
        std::fs::write(self.base_dir.join(&path.0), data.data())
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::io::ErrorKind;
    use std::path::PathBuf;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FakeDirectoryState {
        subdirs: HashSet<DirectoryPath>,
        files: HashMap<DirectoryPath, EncryptedData>,
        injected_errors: HashMap<DirectoryPath, ErrorKind>,
        total_outage: Option<ErrorKind>,
    }

    impl FakeDirectoryState {
        fn empty_directory() -> DirectoryPath {
            DirectoryPath::from("")
        }

        fn exists(&self, path: &DirectoryPath) -> bool {
            self.is_directory(path) || self.is_file(path)
        }

        fn is_directory(&self, path: &DirectoryPath) -> bool {
            if path == &Self::empty_directory() {
                return true;
            }
            self.subdirs.contains(path)
        }

        fn is_file(&self, path: &DirectoryPath) -> bool {
            self.files.contains_key(path)
        }

        fn parent_exists(&self, path: &DirectoryPath) -> bool {
            path.0
                .parent()
                .map(|parent| self.is_directory(&DirectoryPath::from(parent)))
                .unwrap_or(false)
        }

        fn return_injected_error_if_present(&self, path: &DirectoryPath) -> Result<()> {
            if let Some(total_error) = self.total_outage {
                Err(total_error.clone().into())
            } else if let Some(injected_error) = self.injected_errors.get(path) {
                Err(injected_error.clone().into())
            } else {
                Ok(())
            }
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
                    injected_errors: HashMap::new(),
                    total_outage: None,
                }),
            }
        }

        pub fn inject_error(&self, path: &DirectoryPath, error_kind: ErrorKind) {
            let mut state = self.state.lock().unwrap();
            state.injected_errors.insert(path.clone(), error_kind);
        }

        pub fn inject_total_outage(&self, error_kind: ErrorKind) {
            let mut state = self.state.lock().unwrap();
            state.total_outage = Some(error_kind);
        }

        pub fn list_subdir(&self, path: &DirectoryPath) -> Result<Vec<DirectoryPath>> {
            let state = self.state.lock().unwrap();
            state.return_injected_error_if_present(path)?;

            if state.is_file(path) {
                return Err(ErrorKind::NotADirectory.into());
            }

            if !state.is_directory(path) {
                return Err(ErrorKind::NotFound.into());
            }

            let mut results = Vec::new();
            for (p, _) in state.files.iter() {
                if p.0.parent() == Some(&path.0) {
                    results.push(p.clone())
                }
            }
            Ok(results)
        }

        pub fn uninject_error(&self, path: &DirectoryPath) {
            let mut state = self.state.lock().unwrap();
            state.injected_errors.remove(path);
        }

        pub fn uninject_total_outage(&self) {
            let mut state = self.state.lock().unwrap();
            state.total_outage = None;
        }
    }

    impl Directory for FakeDirectory {
        fn create_subdir(&self, path: &DirectoryPath) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.return_injected_error_if_present(path)?;

            if state.exists(path) {
                return Err(ErrorKind::AlreadyExists.into());
            }
            if !state.parent_exists(path) {
                return Err(ErrorKind::NotFound.into());
            }

            state.subdirs.insert(path.to_owned());
            Ok(())
        }

        fn delete_file(&self, path: &DirectoryPath) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.return_injected_error_if_present(path)?;

            if state.is_directory(path) {
                return Err(ErrorKind::IsADirectory.into());
            }

            match state.files.remove(path) {
                Some(_) => Ok(()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn exists(&self, path: &DirectoryPath) -> bool {
            let state = self.state.lock().unwrap();
            if state.injected_errors.contains_key(path) {
                return false;
            }
            state.exists(path)
        }

        fn is_directory(&self, path: &DirectoryPath) -> bool {
            let state = self.state.lock().unwrap();
            if state.injected_errors.contains_key(path) {
                return false;
            }
            state.is_directory(path)
        }

        fn is_file(&self, path: &DirectoryPath) -> bool {
            let state = self.state.lock().unwrap();
            if state.injected_errors.contains_key(path) {
                return false;
            }
            state.is_file(path)
        }

        fn read_file(&self, path: &DirectoryPath) -> Result<EncryptedData> {
            let state = self.state.lock().unwrap();
            state.return_injected_error_if_present(path)?;

            if state.is_directory(path) {
                return Err(ErrorKind::IsADirectory.into());
            }

            match state.files.get(path) {
                Some(data) => Ok(data.clone()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn write_file(&self, path: &DirectoryPath, data: &EncryptedData) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.return_injected_error_if_present(path)?;

            if state.is_directory(path) {
                return Err(ErrorKind::IsADirectory.into());
            }

            if !state.parent_exists(path) {
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

        fn exists(&self, path: &DirectoryPath) -> bool {
            self.1.exists(path)
        }

        fn is_directory(&self, path: &DirectoryPath) -> bool {
            self.1.is_directory(path)
        }

        fn is_file(&self, path: &DirectoryPath) -> bool {
            self.1.is_file(path)
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
        fn test_create_subdir_with_injected_error() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
        }

        #[test]
        fn test_delete_file_with_injected_error() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.delete_file(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
        }

        #[test]
        fn test_exists_with_injected_error() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("bar"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            dir.inject_error(&DirectoryPath::from("bar"), ErrorKind::AddrInUse);

            assert!(!dir.exists(&DirectoryPath::from("foo")));
            assert!(!dir.exists(&DirectoryPath::from("bar")));
        }

        #[test]
        fn test_inject_error_avoids_other_paths() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            assert_ok!(dir.create_subdir(&DirectoryPath::from("bar")));
        }

        #[test]
        fn test_inject_multiple_errors() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            dir.inject_error(&DirectoryPath::from("bar"), ErrorKind::AddrInUse);
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("bar")),
                ErrorKind::AddrInUse,
            );
        }

        #[test]
        fn test_inject_total_outage_blocks_everything() {
            let dir = FakeDirectory::new();

            dir.inject_total_outage(ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("bar"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::HostUnreachable,
            );
            assert_error_kind(
                dir.read_file(&DirectoryPath::from("baz/qqq")),
                ErrorKind::HostUnreachable,
            );
        }

        #[test]
        fn test_inject_total_outage_takes_precedence() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::AddrInUse);
            dir.inject_total_outage(ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
        }

        #[test]
        fn test_is_directory_with_injected_error() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);

            assert!(!dir.is_directory(&DirectoryPath::from("foo")));
        }

        #[test]
        fn test_is_file_with_injected_error() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);

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
        fn test_list_dir_with_injected_error() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo/file.txt"),
                &EncryptedData::literal(&[7, 8, 9])
            ));

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);

            assert_error_kind(
                dir.list_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
        }

        #[test]
        fn test_read_file_with_injected_error() {
            let dir = FakeDirectory::new();
            assert_ok!(dir.write_file(
                &DirectoryPath::from("foo"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.read_file(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
        }

        #[test]
        fn test_uninject_error() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
            dir.uninject_error(&DirectoryPath::from("foo"));
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
        }

        #[test]
        fn test_uninject_multiple_errors() {
            let dir = FakeDirectory::new();

            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            dir.inject_error(&DirectoryPath::from("bar"), ErrorKind::AddrInUse);

            dir.uninject_error(&DirectoryPath::from("foo"));
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("bar")),
                ErrorKind::AddrInUse,
            );

            dir.uninject_error(&DirectoryPath::from("bar"));
            assert_ok!(dir.create_subdir(&DirectoryPath::from("bar")));
        }

        #[test]
        fn test_uninject_total_outage() {
            let dir = FakeDirectory::new();

            dir.inject_total_outage(ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.create_subdir(&DirectoryPath::from("foo")),
                ErrorKind::HostUnreachable,
            );
            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("bar"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::HostUnreachable,
            );

            dir.uninject_total_outage();
            assert_ok!(dir.create_subdir(&DirectoryPath::from("foo")));
            assert_ok!(dir.write_file(
                &DirectoryPath::from("bar"),
                &EncryptedData::literal(&[1, 2, 3])
            ),);
        }

        #[test]
        fn test_write_file_with_injected_error() {
            let dir = FakeDirectory::new();
            dir.inject_error(&DirectoryPath::from("foo"), ErrorKind::HostUnreachable);
            assert_error_kind(
                dir.write_file(
                    &DirectoryPath::from("foo"),
                    &EncryptedData::literal(&[1, 2, 3]),
                ),
                ErrorKind::HostUnreachable,
            );
        }
    }
}
