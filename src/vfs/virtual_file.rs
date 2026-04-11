use crate::directory::{Directory, DirectoryPath};
use crate::encryption::EncryptedData;
use crate::vfs::directory_path_gen::DirectoryPathGen;
use crate::vfs::file_buffer::FileBuffer;
use lru::LruCache;
use nonzero_lit::usize;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::io::ErrorKind;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub struct VirtualFileDescriptor {
    chunk_paths: Vec<DirectoryPath>,
    chunk_size: usize,
    total_size: usize,
}

#[derive(Debug)]
pub struct VirtualFile<D: Directory, G: DirectoryPathGen> {
    directory: Arc<D>,
    path_generator: Arc<G>,
    chunk_paths: Vec<DirectoryPath>,
    chunk_buffers: LruCache<DirectoryPath, FileBuffer<D>>,
    chunk_size: usize,
    total_size: usize,
}

impl<D: Directory, G: DirectoryPathGen> VirtualFile<D, G> {
    fn open_chunk(&mut self, path: DirectoryPath) -> std::io::Result<&mut FileBuffer<D>> {
        self.chunk_buffers.try_get_or_insert_mut_ref(&path, || {
            FileBuffer::open(self.directory.clone(), path.clone())
        })
    }

    fn next_chunk(&mut self) -> std::io::Result<&mut FileBuffer<D>> {
        let num_data_chunks =
            self.total_size / self.chunk_size + min(self.total_size % self.chunk_size, 1);
        if num_data_chunks < self.chunk_paths.len() {
            return self.open_chunk(self.chunk_paths[num_data_chunks].clone());
        }

        let path = self.path_generator.generate_path();
        self.chunk_buffers.try_get_or_insert_mut_ref(&path, || {
            let buffer = FileBuffer::new(self.directory.clone(), path.clone(), self.chunk_size)?;
            self.chunk_paths.push(path.clone());
            Ok(buffer)
        })
    }

    pub fn new(directory: Arc<D>, path_generator: Arc<G>, chunk_size: usize) -> Self {
        Self {
            directory,
            path_generator,
            chunk_paths: Vec::new(),
            chunk_buffers: LruCache::new(usize!(16)),
            chunk_size,
            total_size: 0,
        }
    }

    pub fn open<I: Iterator<Item = DirectoryPath>>(
        directory: Arc<D>,
        path_generator: Arc<G>,
        descriptor: VirtualFileDescriptor,
    ) -> Self {
        Self {
            directory,
            path_generator,
            chunk_paths: descriptor.chunk_paths.into_iter().collect(),
            chunk_buffers: LruCache::new(usize!(16)),
            chunk_size: descriptor.chunk_size,
            total_size: descriptor.total_size,
        }
    }

    pub fn descriptor(&self) -> VirtualFileDescriptor {
        VirtualFileDescriptor {
            chunk_paths: self.chunk_paths.clone(),
            chunk_size: self.chunk_size,
            total_size: self.total_size,
        }
    }

    pub fn len(&self) -> usize {
        self.total_size
    }

    pub fn write(&mut self, position: usize, data: &EncryptedData) -> std::io::Result<()> {
        if position > self.total_size {
            return Err(ErrorKind::InvalidInput.into());
        }

        let mut chunk_number = position / self.chunk_size;
        let mut chunk_pos = position % self.chunk_size;

        let mut data_position = 0;
        while data_position < data.data().len() {
            let chunk_remaining_len = self.chunk_size - chunk_pos;
            let data_remaining_len = data.data().len() - data_position;
            let write_len = min(chunk_remaining_len, data_remaining_len);

            let chunk = if chunk_number >= self.chunk_paths.len() {
                self.next_chunk()?
            } else {
                self.open_chunk(self.chunk_paths[chunk_number].clone())?
            };

            let write_data =
                EncryptedData::literal(&data.data()[data_position..data_position + write_len]);
            chunk.write(chunk_pos, &write_data)?;

            chunk_number += 1;
            chunk_pos = 0;
            data_position += write_len;
            self.total_size += write_len;
        }

        Ok(())
    }

    pub fn read(&mut self, position: usize, length: usize) -> std::io::Result<EncryptedData> {
        if position + length > self.total_size {
            return Err(ErrorKind::InvalidInput.into());
        }

        let mut ret = Vec::new();
        ret.reserve(length);

        let mut chunk_number = position / self.chunk_size;
        let mut chunk_pos = position % self.chunk_size;

        while ret.len() < length {
            let chunk_remaining_len = self.chunk_size - chunk_pos;
            let data_remaining_len = length - ret.len();
            let read_len = min(chunk_remaining_len, data_remaining_len);

            let chunk = self.open_chunk(self.chunk_paths[chunk_number].clone())?;

            ret.extend_from_slice(&chunk.data()[chunk_pos..chunk_pos + read_len]);

            chunk_number += 1;
            chunk_pos = 0;
        }

        assert_eq!(ret.len(), length);

        Ok(EncryptedData::literal(&ret))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::testing::FakeDirectory;
    use crate::vfs::directory_path_gen::SequentialDirectoryPathGen;
    use assertables::assert_ok;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn read_write_is_identity(data: Vec<u8>) {
        let dir_path = DirectoryPath::from("chunks");
        let dir = Arc::new(FakeDirectory::new());
        assert_ok!(dir.create_subdir(&dir_path));
        let pathgen = Arc::new(SequentialDirectoryPathGen::new(dir_path.clone(), 1));
        let mut file = VirtualFile::new(dir.clone(), pathgen, 8);
        let enc_data = EncryptedData::literal(&data);

        assert_ok!(file.write(0, &enc_data));

        let read_data = assert_ok!(file.read(0, data.len()));

        assert_eq!(read_data, EncryptedData::literal(&data));
    }
}
