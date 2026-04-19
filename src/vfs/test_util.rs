use crate::directory::testing::FakeDirectory;
use crate::directory::{Directory, DirectoryPath};
use crate::encryption::EncryptedData;
use crate::vfs::directory_path_gen::SequentialDirectoryPathGen;
use assertables::assert_ok;
use std::sync::Arc;

pub trait DirectoryPathLike {
    fn into_directory_path(self) -> DirectoryPath;
}

impl<P: Into<DirectoryPath>> DirectoryPathLike for P {
    fn into_directory_path(self) -> DirectoryPath {
        self.into()
    }
}

impl DirectoryPathLike for &DirectoryPath {
    fn into_directory_path(self) -> DirectoryPath {
        self.clone()
    }
}

pub struct TestContext {
    dir: Arc<FakeDirectory>,
    path_gen: Arc<SequentialDirectoryPathGen>,
}

impl TestContext {
    pub fn chunk_path() -> DirectoryPath {
        DirectoryPath::from("chunks")
    }

    pub fn new() -> Self {
        let dir = Arc::new(FakeDirectory::new());
        assert_ok!(dir.create_subdir(&Self::chunk_path()));
        let path_gen = Arc::new(SequentialDirectoryPathGen::new(Self::chunk_path(), 1));

        Self { dir, path_gen }
    }

    pub fn chunk_paths(&self) -> Vec<DirectoryPath> {
        assert_ok!(self.dir.list_subdir(&Self::chunk_path()))
    }

    pub fn dir(&self) -> Arc<FakeDirectory> {
        self.dir.clone()
    }

    pub fn file_content<P: DirectoryPathLike>(&self, path: P) -> Vec<u8> {
        assert_ok!(self.dir.read_file(&path.into_directory_path()))
            .data()
            .to_vec()
    }

    pub fn file_contents<P: DirectoryPathLike, I: IntoIterator<Item = P>>(
        &self,
        paths: I,
    ) -> Vec<Vec<u8>> {
        paths
            .into_iter()
            .map(|p| {
                assert_ok!(self.dir.read_file(&p.into_directory_path()))
                    .data()
                    .to_vec()
            })
            .collect()
    }

    pub fn path_gen(&self) -> Arc<SequentialDirectoryPathGen> {
        self.path_gen.clone()
    }

    pub fn write<P: DirectoryPathLike>(&self, path: P, data: &[u8]) -> () {
        assert_ok!(
            self.dir
                .write_file(&path.into_directory_path(), &EncryptedData::literal(data))
        )
    }
}
