use crate::data_structures::cache_map::CacheMap;
use crate::directory::{Directory, DirectoryPath};
use crate::vfs::directory_path_gen::DirectoryPathGen;
use crate::vfs::file_buffer::FileBuffer;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub struct FileBufferSequenceDescriptor {
    pub chunk_paths: Vec<DirectoryPath>,
    pub max_memory_chunks: usize,
    pub chunk_size: usize,
}

impl Default for FileBufferSequenceDescriptor {
    fn default() -> Self {
        Self {
            chunk_paths: vec![],
            chunk_size: 16 * 1024 * 1024,
            max_memory_chunks: 16,
        }
    }
}

pub struct FileBufferSequence<D: Directory + 'static, G: DirectoryPathGen> {
    chunk_paths: Vec<DirectoryPath>,
    chunk_size: usize,
    chunks: CacheMap<DirectoryPath, FileBuffer<D>, std::io::Error>,
    path_generator: Arc<G>,
    max_memory_chunks: usize,
}

impl<D: Directory + 'static, G: DirectoryPathGen> Debug for FileBufferSequence<D, G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("FileBufferSequence(chunk_paths: {:?})", self.chunk_paths).as_str())
    }
}

impl<D: Directory + 'static, G: DirectoryPathGen> FileBufferSequence<D, G> {
    fn append_chunk(&mut self) -> std::io::Result<()> {
        let path = self.path_generator.generate_path();
        // Create the chunk using the CacheMap, but discard its result because we don't need it right now.
        self.chunks.try_get_mut(&path)?;
        self.chunk_paths.push(path);
        Ok(())
    }

    pub fn open(
        directory: Arc<D>,
        path_generator: Arc<G>,
        options: &FileBufferSequenceDescriptor,
    ) -> Self {
        let chunk_size = options.chunk_size;
        Self {
            path_generator,
            chunk_paths: options.chunk_paths.clone(),
            chunk_size: options.chunk_size,
            chunks: CacheMap::<DirectoryPath, FileBuffer<D>, std::io::Error>::new(
                options.max_memory_chunks,
                move |dir_path| FileBuffer::open(directory.clone(), dir_path.clone(), chunk_size),
                |_, chunk| chunk.flush(),
            ),
            max_memory_chunks: options.max_memory_chunks,
        }
    }

    pub fn descriptor(&self) -> FileBufferSequenceDescriptor {
        FileBufferSequenceDescriptor {
            chunk_paths: self.chunk_paths.clone(),
            chunk_size: self.chunk_size,
            max_memory_chunks: self.max_memory_chunks,
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.chunks.clear()
    }

    pub fn get_chunk(&mut self, index: usize) -> std::io::Result<&mut FileBuffer<D>> {
        while self.chunk_paths.len() <= index {
            self.append_chunk()?;
        }
        let path = &self.chunk_paths[index];
        Ok(self.chunks.try_get_mut(path)?)
    }

    pub fn len(&self) -> usize {
        self.chunk_paths.len()
    }
}

impl<D: Directory + 'static, G: DirectoryPathGen> Drop for FileBufferSequence<D, G> {
    fn drop(&mut self) {
        self.flush().expect("Failed to flush FileBufferSequence");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::EncryptedData;
    use crate::testing::assert_error_kind;
    use crate::vfs::test_util::TestContext;
    use assertables::assert_ok;
    use std::io::ErrorKind;

    #[test]
    fn add_chunk_no_op_for_write_failure() {
        let ctx = TestContext::new();
        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        ctx.dir().read_only();

        assert_error_kind(seq.get_chunk(0), ErrorKind::PermissionDenied);
        assert_eq!(seq.len(), 0);
        assert_eq!(seq.chunk_paths.len(), 0);
    }

    #[test]
    fn test_descriptor() {
        let ctx = TestContext::new();
        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_size: 3,
                max_memory_chunks: 69,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        assert_ok!(seq.get_chunk(2));

        let mut actual_chunk_paths = ctx.chunk_paths();
        actual_chunk_paths.sort();

        let mut seq_chunk_paths = seq.descriptor().chunk_paths;
        seq_chunk_paths.sort();

        assert_eq!(seq.chunk_size, 3);
        assert_eq!(seq.max_memory_chunks, 69);
        assert_eq!(actual_chunk_paths, seq_chunk_paths);
    }

    #[test]
    fn test_get_chunk_creates_chunks_until_chunk_number() {
        let ctx = TestContext::new();
        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        assert_ok!(seq.get_chunk(2));

        assert_eq!(seq.len(), 3);

        let chunk_contents = ctx.file_contents(ctx.chunk_paths());
        assert_eq!(chunk_contents.len(), 3);

        for content in chunk_contents {
            assert_eq!(content, &[0, 0, 0])
        }
    }

    #[test]
    fn test_get_chunk_fails_for_mismatched_chunk_size() {
        let ctx = TestContext::new();

        assert_ok!(ctx.dir().write_file(
            &DirectoryPath::from("chunks/foo"),
            &EncryptedData::literal(&[1, 2, 3])
        ));

        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_paths: vec![DirectoryPath::from("chunks/foo")],
                chunk_size: 2,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        assert_error_kind(seq.get_chunk(0), ErrorKind::InvalidInput);
    }

    #[test]
    fn test_get_chunk_gets_correct_existing_chunk_contents() {
        let ctx = TestContext::new();
        assert_ok!(ctx.dir().write_file(
            &DirectoryPath::from("chunks/foo"),
            &EncryptedData::literal(&[1, 2])
        ));
        assert_ok!(ctx.dir().write_file(
            &DirectoryPath::from("chunks/bar"),
            &EncryptedData::literal(&[3, 4])
        ));
        assert_ok!(ctx.dir().write_file(
            &DirectoryPath::from("chunks/baz"),
            &EncryptedData::literal(&[5, 6])
        ));

        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_paths: vec![
                    DirectoryPath::from("chunks/foo"),
                    DirectoryPath::from("chunks/bar"),
                    DirectoryPath::from("chunks/baz"),
                ],
                chunk_size: 2,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        assert_eq!(assert_ok!(seq.get_chunk(0)).data(), &[1, 2]);
        assert_eq!(assert_ok!(seq.get_chunk(1)).data(), &[3, 4]);
        assert_eq!(assert_ok!(seq.get_chunk(2)).data(), &[5, 6]);
    }

    #[test]
    fn test_does_not_flush_while_in_memory() {
        let ctx = TestContext::new();

        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        let buffer = assert_ok!(seq.get_chunk(0));
        assert_ok!(buffer.write(0, &EncryptedData::literal(&[69, 42, 0])));

        let disk_content = ctx.file_content(buffer.disk_path());
        assert_eq!(disk_content, &[0, 0, 0]);
    }

    #[test]
    fn test_flush() {
        let ctx = TestContext::new();
        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                max_memory_chunks: 2,
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        {
            let buffer0 = assert_ok!(seq.get_chunk(0));
            assert_ok!(buffer0.write(0, &EncryptedData::literal(&[69, 42, 0])));
        }

        {
            let buffer1 = assert_ok!(seq.get_chunk(1));
            assert_ok!(buffer1.write(0, &EncryptedData::literal(&[1, 2, 3])));
        }

        {
            let buffer2 = assert_ok!(seq.get_chunk(2));
            assert_ok!(buffer2.write(0, &EncryptedData::literal(&[4, 5, 6])));
        }

        assert_ok!(seq.flush());

        let chunk_paths = seq.descriptor().chunk_paths;
        let chunk_data = ctx.file_contents(chunk_paths);
        assert_eq!(chunk_data[0], &[69, 42, 0]);
        assert_eq!(chunk_data[1], &[1, 2, 3]);
        assert_eq!(chunk_data[2], &[4, 5, 6]);
    }

    #[test]
    fn test_flush_on_drop() {
        let ctx = TestContext::new();
        let chunk_paths = {
            let mut seq = FileBufferSequence::open(
                ctx.dir(),
                ctx.path_gen(),
                &FileBufferSequenceDescriptor {
                    max_memory_chunks: 2,
                    chunk_size: 3,
                    ..FileBufferSequenceDescriptor::default()
                },
            );

            {
                let buffer0 = assert_ok!(seq.get_chunk(0));
                assert_ok!(buffer0.write(0, &EncryptedData::literal(&[69, 42, 0])));
            }

            {
                let buffer1 = assert_ok!(seq.get_chunk(1));
                assert_ok!(buffer1.write(0, &EncryptedData::literal(&[1, 2, 3])));
            }

            {
                let buffer2 = assert_ok!(seq.get_chunk(2));
                assert_ok!(buffer2.write(0, &EncryptedData::literal(&[4, 5, 6])));
            }

            seq.descriptor().chunk_paths
        };

        assert_eq!(ctx.file_content(&chunk_paths[0]), &[69, 42, 0]);
        assert_eq!(ctx.file_content(&chunk_paths[1]), &[1, 2, 3]);
        assert_eq!(ctx.file_content(&chunk_paths[2]), &[4, 5, 6]);
    }

    #[test]
    fn test_flush_works_after_transient_disconnection() {
        let ctx = TestContext::new();
        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                max_memory_chunks: 2,
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        {
            let buffer0 = assert_ok!(seq.get_chunk(0));
            assert_ok!(buffer0.write(0, &EncryptedData::literal(&[69, 42, 0])));
        }

        {
            let buffer1 = assert_ok!(seq.get_chunk(1));
            assert_ok!(buffer1.write(0, &EncryptedData::literal(&[1, 2, 3])));
        }

        {
            let buffer2 = assert_ok!(seq.get_chunk(2));
            assert_ok!(buffer2.write(0, &EncryptedData::literal(&[4, 5, 6])));
        }

        ctx.dir().disconnect();
        assert_error_kind(seq.flush(), ErrorKind::NetworkUnreachable);
        ctx.dir().reset_on_operation();
        assert_ok!(seq.flush());

        let chunk_paths = seq.descriptor().chunk_paths;
        let contents = ctx.file_contents(chunk_paths);
        assert_eq!(&contents[0], &[69, 42, 0]);
        assert_eq!(&contents[1], &[1, 2, 3]);
        assert_eq!(&contents[2], &[4, 5, 6]);
    }

    #[test]
    fn test_flushes_to_disk_when_max_memory_chunks_exceeded() {
        let ctx = TestContext::new();
        let mut seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                max_memory_chunks: 2,
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        {
            let buffer0 = assert_ok!(seq.get_chunk(0));
            assert_ok!(buffer0.write(0, &EncryptedData::literal(&[69, 42, 0])));
        }

        {
            let buffer1 = assert_ok!(seq.get_chunk(1));
            assert_ok!(buffer1.write(0, &EncryptedData::literal(&[1, 2, 3])));
        }

        {
            let buffer2 = assert_ok!(seq.get_chunk(2));
            assert_ok!(buffer2.write(0, &EncryptedData::literal(&[4, 5, 6])));
        }

        let chunk_paths = seq.descriptor().chunk_paths;
        let contents = ctx.file_contents(chunk_paths);
        assert_eq!(&contents[0], &[69, 42, 0]);
        assert_eq!(&contents[1], &[0, 0, 0]);
        assert_eq!(&contents[2], &[0, 0, 0]);
    }

    #[test]
    fn test_new_has_zero_len() {
        let ctx = TestContext::new();
        let seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_size: 3,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        assert_eq!(seq.len(), 0);
    }

    #[test]
    fn test_open_has_length_equal_to_num_chunk_paths() {
        let ctx = TestContext::new();
        ctx.write("chunks/foo", &[1, 2]);
        ctx.write("chunks/bar", &[3, 4]);
        ctx.write("chunks/baz", &[5, 6]);

        let seq = FileBufferSequence::open(
            ctx.dir(),
            ctx.path_gen(),
            &FileBufferSequenceDescriptor {
                chunk_paths: vec![
                    DirectoryPath::from("chunks/foo"),
                    DirectoryPath::from("chunks/bar"),
                    DirectoryPath::from("chunks/baz"),
                ],
                chunk_size: 2,
                ..FileBufferSequenceDescriptor::default()
            },
        );

        assert_eq!(seq.len(), 3);
    }
}
