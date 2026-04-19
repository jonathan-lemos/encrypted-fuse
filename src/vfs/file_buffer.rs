use crate::directory::{Directory, DirectoryPath};
use crate::encryption::EncryptedData;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

// A fixed-size file that buffers modifications in memory to reduce writes to disk.
//
// The pending data can be written to disk with flush() or when the struct is dropped.
// It's recommended to flush before dropping the struct, because if the flush fails in Drop,
// the program will panic.
#[derive(Debug)]
pub struct FileBuffer<D: Directory> {
    disk_path: DirectoryPath,
    buffer: Box<[u8]>,
    directory: Arc<D>,
    dirty: bool,
}

impl<D: Directory> FileBuffer<D> {
    fn disk_content_or_create_blank(
        directory: Arc<D>,
        path: &DirectoryPath,
        buffer_len: usize,
    ) -> Result<EncryptedData> {
        match directory.read_file(path) {
            Ok(content) => {
                if content.data().len() != buffer_len {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "The buffer length needs to be {} bytes, but the file on disk has {} bytes",
                            buffer_len,
                            content.data().len()
                        ),
                    ))
                } else {
                    Ok(content)
                }
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {
                    let content = EncryptedData::literal(&vec![0; buffer_len]);
                    directory.write_file(path, &content)?;
                    Ok(content)
                }
                _ => Err(err),
            },
        }
    }

    pub fn open(directory: Arc<D>, disk_path: DirectoryPath, buffer_len: usize) -> Result<Self> {
        let content =
            Self::disk_content_or_create_blank(directory.clone(), &disk_path, buffer_len)?;

        Ok(Self {
            disk_path,
            buffer: content.data().into(),
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

impl<D: Directory> Drop for FileBuffer<D> {
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
        let directory = Arc::new(FakeDirectory::new());
        let path = DirectoryPath::from("foo");
        assert_ok!(directory.write_file(&path, &EncryptedData::literal(b"bar")));

        let buffer = assert_ok!(FileBuffer::open(directory, path.clone(), b"bar".len()));
        assert_eq!(buffer.disk_path(), &path);
    }

    #[test]
    fn test_drop() {
        let directory = Arc::new(FakeDirectory::new());
        let path = DirectoryPath::from("foo");
        {
            let mut buffer = assert_ok!(FileBuffer::open(directory.clone(), path.clone(), 16));

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
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory.clone(), path.clone(), 16));

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
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory.clone(), path.clone(), 16));

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
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory.clone(), path.clone(), 16));

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
    fn test_open_creates_blank_file() {
        let directory = Arc::new(FakeDirectory::new());
        let path = DirectoryPath::from("foo");
        let buffer = assert_ok!(FileBuffer::open(directory.clone(), path.clone(), 16));

        assert_eq!(buffer.len(), 16);
        let disk_content = assert_ok!(directory.read_file(&path));
        assert_eq!(buffer.data(), &[0; 16]);
        assert_eq!(disk_content.data(), &[0; 16]);
    }

    #[test]
    fn test_open_fails_for_io_error() {
        let directory = Arc::new(FakeDirectory::new());
        let path = DirectoryPath::from("foo");

        directory.disconnect();

        let directory_clone = directory.clone();
        assert_error_kind(
            FileBuffer::open(directory_clone, path.clone(), 16),
            ErrorKind::NetworkUnreachable,
        );
    }

    #[test]
    fn test_open_populates_with_existing_file() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        assert_ok!(directory.write_file(&path, &EncryptedData::literal(b"bar")));

        let buffer = assert_ok!(FileBuffer::open(directory, path.clone(), b"bar".len()));
        assert_eq!(buffer.data(), b"bar");
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_write_data_at_beginning() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_ok!(buffer.write(0, &EncryptedData::literal(b"foo")));

        let expected = [b"foo".as_slice(), &[0u8; 13]].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_data_at_end() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_ok!(buffer.write(13, &EncryptedData::literal(b"bar")));

        let expected = [&[0u8; 13], b"bar".as_slice()].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_data_in_middle() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_ok!(buffer.write(5, &EncryptedData::literal(b"bar")));

        let expected = [&[0u8; 5], b"bar".as_slice(), &[0u8; 8]].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_data_overwrites() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("file");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_ok!(buffer.write(5, &EncryptedData::literal(b"foo")));
        assert_ok!(buffer.write(7, &EncryptedData::literal(b"barbaz")));

        let expected = [&[0u8; 5], b"fobarbaz".as_slice(), &[0; 3]].concat();
        assert_eq!(buffer.data(), expected.as_slice());
    }

    #[test]
    fn test_write_fails_for_index_out_of_bounds() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_error_kind(
            buffer.write(16, &EncryptedData::literal(&[0])),
            ErrorKind::InvalidInput,
        );
    }

    #[test]
    fn test_write_fails_for_overrun_at_end() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_error_kind(
            buffer.write(14, &EncryptedData::literal(b"foo")),
            ErrorKind::InvalidInput,
        );
    }

    #[test]
    fn test_write_fails_for_too_big_input() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let mut buffer = assert_ok!(FileBuffer::open(directory, path.clone(), 16));

        assert_error_kind(
            buffer.write(0, &EncryptedData::literal(&[0; 17])),
            ErrorKind::InvalidInput,
        );
    }

    #[test]
    fn test_write_flushes_to_disk_for_file_not_present() {
        let directory = Arc::new(FakeDirectory::new());
        let path = &DirectoryPath::from("foo");
        let _buffer = assert_ok!(FileBuffer::open(directory.clone(), path.clone(), 16));

        let disk_content = assert_ok!(directory.read_file(&path));
        assert_eq!(disk_content.data(), &[0; 16]);
    }
}
