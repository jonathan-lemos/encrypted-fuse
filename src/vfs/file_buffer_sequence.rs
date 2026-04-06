use crate::directory::{Directory, DirectoryPath};
use crate::vfs::directory_path_gen::DirectoryPathGen;
use crate::vfs::file_buffer::FileBuffer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

#[derive(Debug)]
pub struct FileBufferSequence<'a, 'b, D: Directory, G: DirectoryPathGen> {
    directory: &'a D,
    path_generator: &'b G,
    chunk_paths: Vec<DirectoryPath>,
    max_memory_chunks: usize,
    lru_cache_dict: HashMap<DirectoryPath, FileBuffer<'a, D>>,
}
