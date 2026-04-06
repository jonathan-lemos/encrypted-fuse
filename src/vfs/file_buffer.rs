use crate::directory::{Directory, DirectoryPath};
use crate::encryption::EncryptedData;
use std::io::{ErrorKind, Result};

// A fixed-size file that buffers modifications in memory to reduce writes to disk.
//
// The pending data can be written to disk with flush() or when the struct is dropped.
// It's recommended to flush before dropping the struct, because if the flush fails in Drop,
// the program will panic.
#[derive(Debug)]
pub struct FileBuffer<'a, D: Directory> {
    disk_path: DirectoryPath,
    buffer: Box<[u8]>,
    directory: &'a D,
    dirty: bool,
}

impl<'a, D: Directory> FileBuffer<'a, D> {
    pub fn new(directory: &'a D, disk_path: DirectoryPath, buffer_len: usize) -> Result<Self> {
        if directory.exists(&disk_path) {
            return Err(ErrorKind::AlreadyExists.into());
        }

        let mut ret = Self {
            disk_path,
            buffer: vec![0; buffer_len].into(),
            directory,
            dirty: true,
        };
        match ret.flush() {
            Ok(_) => Ok(ret),
            Err(e) => {
                // Prevent flushing on Drop which will panic
                ret.dirty = false;
                Err(e)
            }
        }
    }

    pub fn open(directory: &'a D, disk_path: DirectoryPath) -> Result<Self> {
        directory.read_file(&disk_path).map(|data| Self {
            disk_path,
            buffer: data.data().into(),
            directory,
            dirty: false,
        })
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    pub fn disk_path(&self) -> &DirectoryPath {
        &self.disk_path
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        match self
            .directory
            .write_file(&self.disk_path, &EncryptedData::literal(&self.buffer))
        {
            Ok(()) => {
                self.dirty = false;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn write(&mut self, position: usize, data: &EncryptedData) -> Result<()> {
        if position + data.data().len() > self.buffer.len() {
            return Err(ErrorKind::InvalidInput.into());
        }
        (&mut self.buffer[position..position + data.data().len()]).copy_from_slice(data.data());
        self.dirty = true;
        Ok(())
    }
}

impl<D: Directory> Drop for FileBuffer<'_, D> {
    fn drop(&mut self) {
        self.flush()
            .expect(&format!("Failed to flush {} to disk", self.disk_path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::testing::FakeDirectory;
    use crate::testing::assert_error_kind;
    use assertables::assert_ok;

    #[test]
    fn test_disk_path() {
        let directory = FakeDirectory::new();
        let path = DirectoryPath::from("foo");
        assert_ok!(directory.write_file(&path, &EncryptedData::literal(b"bar")));

        let buffer = assert_ok!(FileBuffer::open(&directory, path.clone()));
        assert_eq!(buffer.disk_path(), &path);
    }

    #[test]
    fn test_drop() {
        let directory = FakeDirectory::new();
        let path = DirectoryPath::from("foo");
        {
            let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

            assert_ok!(buffer.write(0, &EncryptedData::literal(b"foo")));

            let pre_drop_content = assert_ok!(directory.read_file(&path));
            assert_eq!(pre_drop_content.data(), &[0; 16]);
        }

        let post_drop_content = assert_ok!(directory.read_file(&path));
        assert_eq!(
            post_drop_content.data(),
            [b"foo".as_slice(), &[0; 13]].concat()
        );
    }

    #[test]
    fn test_flush() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(0, &EncryptedData::literal(b"foo")));

        let pre_flush_content = assert_ok!(directory.read_file(&path));
        assert_eq!(pre_flush_content.data(), &[0; 16]);

        assert_ok!(buffer.flush());
        let post_flush_content = assert_ok!(directory.read_file(&path));
        assert_eq!(
            post_flush_content.data(),
            [b"foo".as_slice(), &[0; 13]].concat()
        );
    }

    #[test]
    fn test_flush_idempotent() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(0, &EncryptedData::literal(b"foo")));

        directory
            .log_during(|| {
                assert_ok!(buffer.flush());
                assert_ok!(buffer.flush());
            })
            .assert_only_matching(|op| op.is_write_file());

        let post_flush_content = assert_ok!(directory.read_file(&path));
        assert_eq!(
            post_flush_content.data(),
            [b"foo".as_slice(), &[0; 13]].concat()
        );
    }

    #[test]
    fn test_flush_does_not_skip_future_write_on_failure() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(0, &EncryptedData::literal(b"foo")));

        directory.disconnect();
        assert_error_kind(buffer.flush(), ErrorKind::NetworkUnreachable);
        directory.reset_on_operation();
        directory
            .log_during(|| {
                assert_ok!(buffer.flush());
            })
            .assert_only_matching(|op| op.is_write_file());

        let post_flush_content = assert_ok!(directory.read_file(&path));
        let expected = [b"foo".as_slice(), &[0; 13]].concat();
        assert_eq!(post_flush_content.data(), &expected);
    }

    #[test]
    fn test_len() {
        let directory = FakeDirectory::new();
        let path = DirectoryPath::from("foo");
        let buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));
        assert_eq!(buffer.len(), 16);
    }

    #[test]
    fn test_new_fails_with_existing_file() {
        let directory = FakeDirectory::new();
        let path = DirectoryPath::from("foo");

        assert_ok!(directory.write_file(&path, &EncryptedData::literal(b"bar")));

        directory
            .log_during(|| {
                assert_error_kind(
                    FileBuffer::new(&directory, path.clone(), 16),
                    ErrorKind::AlreadyExists,
                );
            })
            .assert_only_matching(|op| op.is_file_type());
    }

    #[test]
    fn test_new_fails_for_io_error() {
        let directory = FakeDirectory::new();
        let path = DirectoryPath::from("foo");

        directory.disconnect();

        let log = directory.log_during(|| {
            assert_error_kind(
                FileBuffer::new(&directory, path.clone(), 16),
                ErrorKind::NetworkUnreachable,
            );
        });
        // Should not write a second time on Drop of the intermediate struct.
        log.assert_single_matching(|op| op.is_write_file());
    }

    #[test]
    fn test_open_fails_for_nonexistent_file() {
        let directory = FakeDirectory::new();
        assert_error_kind(
            FileBuffer::open(&directory, DirectoryPath::from("foo")),
            ErrorKind::NotFound,
        );
    }

    #[test]
    fn test_open_populates_with_existing_file() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        assert_ok!(directory.write_file(&path, &EncryptedData::literal(b"bar")));

        let buffer = assert_ok!(FileBuffer::open(&directory, path.clone()));
        assert_eq!(buffer.data(), b"bar");
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_write_data_initializes_to_zero() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));
        assert_eq!(buffer.data(), &[0; 16]);
    }

    #[test]
    fn test_write_data_at_beginning() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(0, &EncryptedData::literal(b"foo")));

        let expected = [b"foo".as_slice(), &[0u8; 13]].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_data_at_end() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(13, &EncryptedData::literal(b"bar")));

        let expected = [&[0u8; 13], b"bar".as_slice()].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_data_in_middle() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(5, &EncryptedData::literal(b"bar")));

        let expected = [&[0u8; 5], b"bar".as_slice(), &[0u8; 8]].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_data_overwrites() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("file");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_ok!(buffer.write(5, &EncryptedData::literal(b"foo")));
        assert_ok!(buffer.write(7, &EncryptedData::literal(b"barbaz")));

        let expected = [&[0u8; 5], b"fobarbaz".as_slice(), &[0; 3]].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_fails_for_index_out_of_bounds() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_error_kind(
            buffer.write(16, &EncryptedData::literal(&[0])),
            ErrorKind::InvalidInput,
        );
    }

    #[test]
    fn test_write_fails_for_overrun_at_end() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_error_kind(
            buffer.write(14, &EncryptedData::literal(b"foo")),
            ErrorKind::InvalidInput,
        );
    }

    #[test]
    fn test_write_fails_for_too_big_input() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        assert_error_kind(
            buffer.write(0, &EncryptedData::literal(&[0; 17])),
            ErrorKind::InvalidInput,
        );
    }

    #[test]
    fn test_write_flushes_to_disk_for_file_not_present() {
        let directory = FakeDirectory::new();
        let path = &DirectoryPath::from("foo");
        let _buffer = assert_ok!(FileBuffer::new(&directory, path.clone(), 16));

        let disk_content = assert_ok!(directory.read_file(&path));
        assert_eq!(disk_content.data(), &[0; 16]);
    }
}
