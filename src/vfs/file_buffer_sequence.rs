use crate::data_structures::cache_map::CacheMap;
use crate::directory::{Directory, DirectoryPath};
use crate::vfs::directory_path_gen::DirectoryPathGen;
use crate::vfs::file_buffer::FileBuffer;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct FileBufferSequenceOptions {
    pub max_memory_chunks: usize,
    pub chunk_size: usize,
}

pub struct FileBufferSequence<D: Directory + 'static, G: DirectoryPathGen> {
    directory: Arc<D>,
    path_generator: Arc<G>,
    chunk_paths: Vec<DirectoryPath>,
    max_memory_chunks: usize,
    chunks: CacheMap<DirectoryPath, FileBuffer<D>, std::io::Error>,
}

impl<D: Directory + 'static, G: DirectoryPathGen> Debug for FileBufferSequence<D, G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            format!(
                "FileBufferSequence(chunk_paths: {:?}, max_memory_chunks: {:?})",
                self.chunk_paths, self.max_memory_chunks
            )
            .as_str(),
        )
    }
}

impl<D: Directory + 'static, G: DirectoryPathGen> FileBufferSequence<D, G> {
    pub fn new(directory: Arc<D>, path_generator: Arc<G>, options: &FileBufferSequenceOptions) -> Self {
        let directory_clone = directory.clone();
        let chunk_size = options.chunk_size;
        Self {
            directory,
            path_generator,
            chunk_paths: Vec::new(),
            max_memory_chunks: options.max_memory_chunks,
            chunks: CacheMap::<DirectoryPath, FileBuffer<D>, std::io::Error>::new(
                16,
                move |dir_path| {
                    FileBuffer::new(directory_clone.clone(), dir_path.clone(), chunk_size)
                },
                |_, chunk| {
                    chunk.flush()
                }
            )
        }
    }
}
