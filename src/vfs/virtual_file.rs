use crate::directory::Directory;
use crate::encryption::EncryptedData;
use crate::vfs::directory_path_gen::DirectoryPathGen;
use crate::vfs::file_buffer_sequence::{FileBufferSequence, FileBufferSequenceDescriptor};
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::io::ErrorKind;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub struct VirtualFileDescriptor {
    file_buffer_sequence_descriptor: FileBufferSequenceDescriptor,
    total_size: usize,
}

pub struct VirtualFile<D: Directory + 'static, G: DirectoryPathGen> {
    chunk_size: usize,
    file_buffer_sequence: FileBufferSequence<D, G>,
    total_size: usize,
}

impl Default for VirtualFileDescriptor {
    fn default() -> Self {
        Self {
            file_buffer_sequence_descriptor: FileBufferSequenceDescriptor::default(),
            total_size: 0,
        }
    }
}

impl<D: Directory + 'static, G: DirectoryPathGen> VirtualFile<D, G> {
    pub fn open(directory: Arc<D>, path_gen: Arc<G>, descriptor: &VirtualFileDescriptor) -> Self {
        Self {
            chunk_size: descriptor.file_buffer_sequence_descriptor.chunk_size,
            file_buffer_sequence: FileBufferSequence::open(
                directory,
                path_gen,
                &descriptor.file_buffer_sequence_descriptor,
            ),
            total_size: descriptor.total_size,
        }
    }

    pub fn descriptor(&self) -> VirtualFileDescriptor {
        VirtualFileDescriptor {
            file_buffer_sequence_descriptor: self.file_buffer_sequence.descriptor(),
            total_size: self.total_size,
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file_buffer_sequence.flush()
    }

    pub fn len(&self) -> usize {
        self.total_size
    }

    pub fn read(&mut self, start_position: usize, len: usize) -> std::io::Result<EncryptedData> {
        if self.total_size <= start_position + len {
            return Err(ErrorKind::InvalidInput.into());
        }

        let mut chunk_no = start_position / self.chunk_size;
        let mut chunk_pos = start_position % self.chunk_size;
        let mut dest = Vec::with_capacity(len);

        while dest.len() < len {
            let remaining_len = len - dest.len();
            let chunk_len = self.chunk_size - chunk_pos;

            let read_len = min(chunk_len, remaining_len);

            let src_chunk = self.file_buffer_sequence.get_chunk(chunk_no)?;
            let src_data = &src_chunk.data()[chunk_pos..chunk_pos + read_len];

            dest.extend_from_slice(src_data);

            chunk_no += 1;
            chunk_pos = 0;
        }

        Ok(EncryptedData::literal(&dest))
    }

    pub fn write(&mut self, start_position: usize, data: &EncryptedData) -> std::io::Result<()> {
        if self.total_size < start_position {
            return Err(ErrorKind::InvalidInput.into());
        }

        let mut chunk_no = start_position / self.chunk_size;
        let mut chunk_pos = start_position % self.chunk_size;
        let mut src_pos = 0;

        while src_pos < data.data().len() {
            let remaining_len = data.data().len() - src_pos;
            let chunk_len = self.chunk_size - chunk_pos;

            let read_len = min(chunk_len, remaining_len);

            let src_data = &data.data()[src_pos..src_pos + read_len];

            let dest_chunk = self.file_buffer_sequence.get_chunk(chunk_no)?;

            dest_chunk.write(chunk_pos, &EncryptedData::literal(src_data))?;

            chunk_no += 1;
            chunk_pos = 0;
            src_pos += read_len;
        }

        self.total_size += src_pos;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::assert_ok;
    use crate::vfs::test_util::TestContext;

    #[test]
    fn test_write_partial_final_chunk() {
        let ctx = TestContext::new();

        let data = EncryptedData::literal(b"abcdefg");
        let mut file = VirtualFile::open(
            ctx.dir(),
            ctx.path_gen(),
            &VirtualFileDescriptor {
                file_buffer_sequence_descriptor: FileBufferSequenceDescriptor {
                    chunk_size: 3,
                    ..FileBufferSequenceDescriptor::default()
                },
                ..VirtualFileDescriptor::default()
            },
        );

        assert_ok!(file.write(0, &data));
        assert_ok!(file.flush());

        let chunk_paths = file
            .descriptor()
            .file_buffer_sequence_descriptor
            .chunk_paths;

        let contents = ctx.file_contents(chunk_paths);

        assert_eq!(&contents[0], b"abc");
        assert_eq!(&contents[1], b"def");
        assert_eq!(&contents[2], b"g\0\0");

        assert_eq!(file.len(), 7);
    }
    #[test]
    fn test_write_complete_final_chunk() {
        let ctx = TestContext::new();

        let data = EncryptedData::literal(b"abcdefghi");
        let mut file = VirtualFile::open(
            ctx.dir(),
            ctx.path_gen(),
            &VirtualFileDescriptor {
                file_buffer_sequence_descriptor: FileBufferSequenceDescriptor {
                    chunk_size: 3,
                    ..FileBufferSequenceDescriptor::default()
                },
                ..VirtualFileDescriptor::default()
            },
        );

        assert_ok!(file.write(0, &data));
        assert_ok!(file.flush());

        let chunk_paths = file
            .descriptor()
            .file_buffer_sequence_descriptor
            .chunk_paths;

        let contents = ctx.file_contents(chunk_paths);

        assert_eq!(&contents[0], b"abc");
        assert_eq!(&contents[1], b"def");
        assert_eq!(&contents[2], b"ghi");

        assert_eq!(file.len(), 9);
    }
}
