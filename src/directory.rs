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
        std::fs::read(self.base_dir.join(path)).map(|bytes| EncryptedData::literal(&bytes))
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

    impl FakeDirectoryState {
        fn empty_directory() -> &'static Path {
            Path::new("")
        }

        fn parent_exists(&self, path: &Path) -> bool {
            match path.parent() {
                Some(parent) => self.subdirs.contains(parent) || parent == Self::empty_directory(),
                None => false,
            }
        }

        fn exists(&self, path: &Path) -> bool {
            self.is_directory(path) || self.is_file(path)
        }

        fn is_directory(&self, path: &Path) -> bool {
            if path == Self::empty_directory() {
                return true;
            }
            self.subdirs.contains(path)
        }

        fn is_file(&self, path: &Path) -> bool {
            self.files.contains_key(path)
        }
    }

    pub struct FakeDirectory {
        state: Mutex<FakeDirectoryState>,
    }

    impl FakeDirectory {
        pub fn new() -> Self {
            Self {
                state: Mutex::new(FakeDirectoryState {
                    subdirs: HashSet::new(),
                    files: HashMap::new(),
                }),
            }
        }

        pub fn exists(&self, path: &Path) -> bool {
            let state = self.state.lock().unwrap();
            state.exists(path)
        }

        pub fn is_directory(&self, path: &Path) -> bool {
            let state = self.state.lock().unwrap();
            state.is_directory(path)
        }

        pub fn is_file(&self, path: &Path) -> bool {
            let state = self.state.lock().unwrap();
            state.is_file(path)
        }

        pub fn list_subdir(&self, path: &Path) -> Result<Vec<PathBuf>> {
            let state = self.state.lock().unwrap();

            if state.is_file(path) {
                return Err(ErrorKind::NotADirectory.into());
            }

            if !state.is_directory(path) {
                return Err(ErrorKind::NotFound.into());
            }

            let mut results = Vec::new();
            for (p, _) in state.files.iter() {
                if p.parent() == Some(path) {
                    results.push(p.clone())
                }
            }
            Ok(results)
        }
    }

    impl Directory for FakeDirectory {
        fn create_subdir(&self, path: &Path) -> Result<()> {
            let mut state = self.state.lock().unwrap();

            if state.exists(path) {
                return Err(ErrorKind::AlreadyExists.into());
            }
            if !state.parent_exists(path) {
                return Err(ErrorKind::NotFound.into());
            }

            state.subdirs.insert(path.to_owned());
            Ok(())
        }

        fn delete_file(&self, path: &Path) -> Result<()> {
            let mut state = self.state.lock().unwrap();

            if !state.exists(path) {
                return Err(ErrorKind::NotFound.into());
            }

            if state.is_directory(path) {
                return Err(ErrorKind::IsADirectory.into());
            }

            match state.files.remove(path) {
                Some(_) => Ok(()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn read_file(&self, path: &Path) -> Result<EncryptedData> {
            let state = self.state.lock().unwrap();

            if state.is_directory(path) {
                return Err(ErrorKind::IsADirectory.into());
            }

            match state.files.get(path) {
                Some(data) => Ok(data.clone()),
                None => Err(ErrorKind::NotFound.into()),
            }
        }

        fn write_file(&self, path: &Path, data: &EncryptedData) -> Result<()> {
            let mut state = self.state.lock().unwrap();

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
    use assertables::{assert_err, assert_ok, assert_ok_eq_x};
    use rstest::rstest;
    use std::fmt::Debug;
    use std::io::ErrorKind;
    use std::io::Result;
    use tempdir::TempDir;

    fn temp_fs_dir() -> (TempDir, FilesystemDirectory) {
        let temp_dir = TempDir::new("filesystem_directory_test").unwrap();
        let filesystem_directory = FilesystemDirectory::new(&temp_dir.path());

        (temp_dir, filesystem_directory)
    }

    // Implement Directory for the pair above so the TempDir remains in scope while
    // the test is ongoing and deletes the TempDir when the test is done.
    impl Directory for (TempDir, FilesystemDirectory) {
        fn create_subdir(&self, path: &Path) -> Result<()> {
            self.1.create_subdir(path)
        }

        fn delete_file(&self, path: &Path) -> Result<()> {
            self.1.delete_file(path)
        }

        fn read_file(&self, path: &Path) -> Result<EncryptedData> {
            self.1.read_file(path)
        }

        fn write_file(&self, path: &Path, data: &EncryptedData) -> Result<()> {
            self.1.write_file(path, data)
        }
    }

    fn assert_error_kind<T: Debug>(result: Result<T>, kind: ErrorKind) {
        let err = assert_err!(&result);
        assert_eq!(err.kind(), kind);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_create_duplicate_dir_fails(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_error_kind(
            dir.create_subdir(Path::new("foo")),
            ErrorKind::AlreadyExists,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_create_subdir_without_parent_fails(#[case] dir: impl Directory) {
        assert_error_kind(dir.create_subdir(Path::new("foo/bar")), ErrorKind::NotFound);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_create_root_dir_fails(#[case] dir: impl Directory) {
        assert_error_kind(dir.create_subdir(Path::new("")), ErrorKind::AlreadyExists);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_fails_for_nonexistent_file(#[case] dir: impl Directory) {
        assert_error_kind(dir.delete_file(Path::new("foo")), ErrorKind::NotFound);
        assert_error_kind(dir.delete_file(Path::new("foo/bar")), ErrorKind::NotFound);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_fails_for_directory(#[case] dir: impl Directory) {
        assert_error_kind(dir.delete_file(Path::new("")), ErrorKind::IsADirectory);
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_error_kind(dir.delete_file(Path::new("foo")), ErrorKind::IsADirectory);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_no_longer_exists(#[case] dir: impl Directory) {
        assert_ok!(dir.write_file(Path::new("foo"), &EncryptedData::literal(&[1, 2, 3])));
        assert_ok!(dir.delete_file(Path::new("foo")));
        assert_error_kind(dir.read_file(Path::new("foo")), ErrorKind::NotFound);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_in_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_ok!(dir.write_file(Path::new("foo/bar"), &EncryptedData::literal(&[1, 2, 3])));
        assert_ok!(dir.delete_file(Path::new("foo/bar")));
        assert_error_kind(dir.read_file(Path::new("foo/bar")), ErrorKind::NotFound);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_delete_file_in_sub_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_ok!(dir.create_subdir(Path::new("foo/bar")));
        assert_ok!(dir.write_file(
            Path::new("foo/bar/baz"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok!(dir.delete_file(Path::new("foo/bar/baz")));
        assert_error_kind(dir.read_file(Path::new("foo/bar/baz")), ErrorKind::NotFound);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_read_file_fails_for_nonexistent_file(#[case] dir: impl Directory) {
        assert_error_kind(dir.read_file(Path::new("foo")), ErrorKind::NotFound);
        assert_error_kind(dir.read_file(Path::new("foo/bar")), ErrorKind::NotFound);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_read_file_fails_for_directory(#[case] dir: impl Directory) {
        assert_error_kind(dir.read_file(Path::new("")), ErrorKind::IsADirectory);
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_error_kind(dir.read_file(Path::new("foo")), ErrorKind::IsADirectory);
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_fails_for_directory(#[case] dir: impl Directory) {
        assert_error_kind(
            dir.write_file(Path::new(""), &EncryptedData::literal(&[1, 2, 3])),
            ErrorKind::IsADirectory,
        );
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_error_kind(
            dir.write_file(Path::new("foo"), &EncryptedData::literal(&[1, 2, 3])),
            ErrorKind::IsADirectory,
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_multiple_files(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(Path::new("dir1")));
        assert_ok!(dir.create_subdir(Path::new("dir1/dir2")));
        assert_ok!(dir.create_subdir(Path::new("dir2")));

        assert_ok!(dir.write_file(Path::new("file1"), &EncryptedData::literal(&[1, 2, 3])));
        assert_ok!(dir.write_file(Path::new("file2"), &EncryptedData::literal(&[4, 5, 6])));
        assert_ok!(dir.write_file(Path::new("dir1/file1"), &EncryptedData::literal(&[7, 8, 9])));
        assert_ok!(dir.write_file(
            Path::new("dir1/dir2/file3"),
            &EncryptedData::literal(&[10, 11, 12])
        ));

        assert_ok_eq_x!(
            &dir.read_file(Path::new("file1")),
            &EncryptedData::literal(&[1, 2, 3])
        );
        assert_ok_eq_x!(
            &dir.read_file(Path::new("file2")),
            &EncryptedData::literal(&[4, 5, 6])
        );
        assert_ok_eq_x!(
            &dir.read_file(Path::new("dir1/file1")),
            &EncryptedData::literal(&[7, 8, 9])
        );
        assert_ok_eq_x!(
            &dir.read_file(Path::new("dir1/dir2/file3")),
            &EncryptedData::literal(&[10, 11, 12])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_overwrites(#[case] dir: impl Directory) {
        assert_ok!(dir.write_file(Path::new("foo"), &EncryptedData::literal(&[1, 2, 3])));
        assert_ok!(dir.write_file(Path::new("foo"), &EncryptedData::literal(&[4, 5, 6])));
        assert_ok_eq_x!(
            &dir.read_file(Path::new("foo")),
            &EncryptedData::literal(&[4, 5, 6])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_round_trip(#[case] dir: impl Directory) {
        assert_ok!(dir.write_file(Path::new("foo"), &EncryptedData::literal(&[1, 2, 3])));
        assert_ok_eq_x!(
            &dir.read_file(Path::new("foo")),
            &EncryptedData::literal(&[1, 2, 3])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_in_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_ok!(dir.write_file(Path::new("foo/bar"), &EncryptedData::literal(&[1, 2, 3])));
        assert_ok_eq_x!(
            &dir.read_file(Path::new("foo/bar")),
            &EncryptedData::literal(&[1, 2, 3])
        );
    }

    #[rstest]
    #[case(FakeDirectory::new())]
    #[case(temp_fs_dir())]
    fn test_write_file_in_sub_subdir(#[case] dir: impl Directory) {
        assert_ok!(dir.create_subdir(Path::new("foo")));
        assert_ok!(dir.create_subdir(Path::new("foo/bar")));
        assert_ok!(dir.write_file(
            Path::new("foo/bar/baz"),
            &EncryptedData::literal(&[1, 2, 3])
        ));
        assert_ok_eq_x!(
            &dir.read_file(Path::new("foo/bar/baz")),
            &EncryptedData::literal(&[1, 2, 3])
        );
    }

    mod fake_directory_tests {
        use super::super::testing::*;
        use super::super::*;
        use super::*;

        #[test]
        fn test_exists() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(Path::new("foo")));
            assert_ok!(dir.create_subdir(Path::new("foo/bar")));
            assert_ok!(dir.create_subdir(Path::new("baz")));

            assert_ok!(dir.write_file(
                Path::new("foo/file1"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("file2"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("foo/bar/file3"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            assert!(dir.exists(Path::new("")));
            assert!(dir.exists(Path::new("foo")));
            assert!(dir.exists(Path::new("foo/file1")));
            assert!(dir.exists(Path::new("foo/bar")));
            assert!(dir.exists(Path::new("foo/bar/file3")));
            assert!(dir.exists(Path::new("file2")));
            assert!(dir.exists(Path::new("baz")));

            assert!(!dir.exists(Path::new("blah")));
            assert!(!dir.exists(Path::new("foo/blah")));
            assert!(!dir.exists(Path::new("foo/bar/blah")));
        }

        #[test]
        fn test_is_directory() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(Path::new("foo")));
            assert_ok!(dir.create_subdir(Path::new("foo/bar")));
            assert_ok!(dir.create_subdir(Path::new("baz")));

            assert_ok!(dir.write_file(
                Path::new("foo/file1"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("file2"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("foo/bar/file3"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            assert!(dir.is_directory(Path::new("")));
            assert!(dir.is_directory(Path::new("foo")));
            assert!(dir.is_directory(Path::new("foo/bar")));
            assert!(dir.is_directory(Path::new("baz")));

            assert!(!dir.is_directory(Path::new("foo/file1")));
            assert!(!dir.is_directory(Path::new("foo/bar/file3")));
            assert!(!dir.is_directory(Path::new("file2")));
            assert!(!dir.is_directory(Path::new("blah")));
            assert!(!dir.is_directory(Path::new("foo/blah")));
            assert!(!dir.is_directory(Path::new("foo/bar/blah")));
        }

        #[test]
        fn test_is_file() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(Path::new("foo")));
            assert_ok!(dir.create_subdir(Path::new("foo/bar")));
            assert_ok!(dir.create_subdir(Path::new("baz")));

            assert_ok!(dir.write_file(
                Path::new("foo/file1"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("file2"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("foo/bar/file3"),
                &EncryptedData::literal(&[1, 2, 3])
            ));

            assert!(dir.is_file(Path::new("foo/file1")));
            assert!(dir.is_file(Path::new("foo/bar/file3")));
            assert!(dir.is_file(Path::new("file2")));

            assert!(!dir.is_file(Path::new("")));
            assert!(!dir.is_file(Path::new("foo")));
            assert!(!dir.is_file(Path::new("foo/bar")));
            assert!(!dir.is_file(Path::new("baz")));
            assert!(!dir.is_file(Path::new("blah")));
            assert!(!dir.is_file(Path::new("foo/blah")));
            assert!(!dir.is_file(Path::new("foo/bar/blah")));
        }

        #[test]
        fn test_list_dir() {
            let dir = FakeDirectory::new();

            assert_ok!(dir.create_subdir(Path::new("foo")));
            assert_ok!(dir.create_subdir(Path::new("foo/bar")));
            assert_ok!(dir.create_subdir(Path::new("foo/bar/baz")));
            assert_ok!(dir.create_subdir(Path::new("ghlarbl")));
            assert_ok!(dir.write_file(
                Path::new("foo/bar/file1.txt"),
                &EncryptedData::literal(&[1, 2, 3])
            ));
            assert_ok!(dir.write_file(
                Path::new("foo/bar/file2.txt"),
                &EncryptedData::literal(&[4, 5, 6])
            ));
            assert_ok!(dir.write_file(
                Path::new("foo/file3.txt"),
                &EncryptedData::literal(&[7, 8, 9])
            ));
            assert_ok!(dir.write_file(
                Path::new("foo/bar/baz/file3.txt"),
                &EncryptedData::literal(&[10, 11, 12])
            ));
            assert_ok!(dir.write_file(
                Path::new("file4.txt"),
                &EncryptedData::literal(&[13, 14, 15])
            ));
            assert_ok!(dir.write_file(
                Path::new("ghlarbl/file5.txt"),
                &EncryptedData::literal(&[16, 17, 18])
            ));

            let mut entries = assert_ok!(dir.list_subdir(Path::new("foo/bar")));
            entries.sort();

            assert_eq!(
                entries,
                &[
                    Path::new("foo/bar/file1.txt"),
                    Path::new("foo/bar/file2.txt"),
                ]
            );
        }
    }
}
