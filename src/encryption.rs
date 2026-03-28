use aes_gcm::aead::{Aead, Nonce, OsRng};
use aes_gcm::aes::cipher::Unsigned;
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct UnencryptedData {
    data: Box<[u8]>,
}

// Represents a sequence of bytes that should not be written to disk because it's not encrypted.
impl UnencryptedData {
    pub fn literal(data: &[u8]) -> Self {
        Self { data: data.into() }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

// Represents a sequence of bytes that is safe to write to disk.
//
// This struct does not do any encryption. Its only purpose is to denote in the type system
// which byte sequences are safe to write to disk.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct EncryptedData {
    data: Box<[u8]>,
}

impl EncryptedData {
    pub fn literal(data: &[u8]) -> Self {
        Self { data: data.into() }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

pub trait EncryptorDecryptor {
    fn decrypt(&self, block: &EncryptedData) -> Option<UnencryptedData>;
    fn encrypt(&self, block: &UnencryptedData) -> Option<EncryptedData>;
}

pub struct Aes256GcmEncryptorDecryptor {
    cipher: Aes256Gcm,
}

impl Aes256GcmEncryptorDecryptor {
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            cipher: Aes256Gcm::new(&key.into()),
        }
    }
}

impl EncryptorDecryptor for Aes256GcmEncryptorDecryptor {
    fn decrypt(&self, block: &EncryptedData) -> Option<UnencryptedData> {
        let nonce_len = <Aes256Gcm as AeadCore>::NonceSize::to_usize();

        if block.data().len() < nonce_len {
            return None;
        }

        let nonce_bytes = &block.data()[..nonce_len];
        let encrypted_data = &block.data()[nonce_len..];

        let nonce = Nonce::<Aes256Gcm>::from_slice(nonce_bytes);
        match self.cipher.decrypt(nonce, encrypted_data) {
            Ok(bytes) => Some(UnencryptedData::literal(&bytes)),
            Err(_) => None,
        }
    }

    fn encrypt(&self, block: &UnencryptedData) -> Option<EncryptedData> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let nonce_length = <Aes256Gcm as AeadCore>::NonceSize::to_usize();
        let nonce_bytes = nonce.as_slice();
        assert_eq!(nonce_length, nonce_bytes.len());

        let encrypted_data = match self.cipher.encrypt(&nonce, block.data()) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let total_bytes = [nonce_bytes, &encrypted_data].concat();

        Some(EncryptedData::literal(&total_bytes))
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use std::collections::HashMap;

    pub struct FakeDataEncryptorDecryptor {
        encrypt_pairs: HashMap<UnencryptedData, EncryptedData>,
        decrypt_pairs: HashMap<EncryptedData, UnencryptedData>,
    }

    impl FakeDataEncryptorDecryptor {
        pub fn new() -> Self {
            Self {
                encrypt_pairs: HashMap::new(),
                decrypt_pairs: HashMap::new(),
            }
        }

        pub fn expect_encryption(
            &mut self,
            unencrypted_block: &UnencryptedData,
            encrypted_block: &EncryptedData,
        ) {
            self.encrypt_pairs
                .insert(unencrypted_block.clone(), encrypted_block.clone());
        }

        pub fn expect_decryption(
            &mut self,
            encrypted_block: &EncryptedData,
            unencrypted_block: &UnencryptedData,
        ) {
            self.decrypt_pairs
                .insert(encrypted_block.clone(), unencrypted_block.clone());
        }
    }

    impl EncryptorDecryptor for FakeDataEncryptorDecryptor {
        fn decrypt(&self, block: &EncryptedData) -> Option<UnencryptedData> {
            self.decrypt_pairs.get(&block).cloned()
        }

        fn encrypt(&self, block: &UnencryptedData) -> Option<EncryptedData> {
            self.encrypt_pairs.get(&block).cloned()
        }
    }
}

#[cfg(test)]
mod tests {
    use assertables::{assert_none, assert_some};
    use super::*;
    use crate::encryption::testing::FakeDataEncryptorDecryptor;

    #[test]
    fn test_fake_encryptor() {
        let mut encryptor = FakeDataEncryptorDecryptor::new();

        let unencrypted1 = UnencryptedData::literal(&[1, 2, 3, 4, 5]);
        let unencrypted2 = UnencryptedData::literal(&[69, 4, 20]);
        let unencrypted3 = UnencryptedData::literal(&[6, 9, 4, 2, 0]);

        let encrypted1 = EncryptedData::literal(&[5, 4, 3, 2, 1]);
        let encrypted2 = EncryptedData::literal(&[42, 69]);
        let encrypted3 = EncryptedData::literal(&[4, 2, 0, 6, 9]);

        encryptor.expect_encryption(&unencrypted1, &encrypted1);
        encryptor.expect_encryption(&unencrypted2, &encrypted2);
        encryptor.expect_decryption(&encrypted1, &unencrypted1);
        encryptor.expect_decryption(&encrypted2, &unencrypted3);

        assert_eq!(encryptor.encrypt(&unencrypted1), Some(encrypted1.clone()));
        assert_eq!(encryptor.encrypt(&unencrypted2), Some(encrypted2.clone()));
        assert_eq!(encryptor.encrypt(&unencrypted3), None);

        assert_eq!(encryptor.decrypt(&encrypted1), Some(unencrypted1.clone()));
        assert_eq!(encryptor.decrypt(&encrypted2), Some(unencrypted3.clone()));
        assert_eq!(encryptor.decrypt(&encrypted3), None);
    }

    #[test]
    fn test_aes256_decrypt_fails_for_smaller_data_than_nonce() {
        let encryptor = Aes256GcmEncryptorDecryptor::new([69; 32]);
        let data = EncryptedData::literal(&[1, 2, 3, 4, 5]);

        assert_none!(encryptor.decrypt(&data));
    }

    #[test]
    fn test_aes256_decrypt_fails_for_different_key() {
        let encryptor_69 = Aes256GcmEncryptorDecryptor::new([69; 32]);
        let encryptor_42 = Aes256GcmEncryptorDecryptor::new([42; 32]);

        let data = UnencryptedData::literal(&[1, 2, 3, 4, 5]);
        let encrypted = assert_some!(encryptor_69.encrypt(&data));

        assert_none!(encryptor_42.decrypt(&encrypted));
    }

    #[test]
    fn test_aes256_round_trip() {
        let encryptor = Aes256GcmEncryptorDecryptor::new([69; 32]);
        let data = UnencryptedData::literal(&[1, 2, 3, 4, 5]);

        let encrypted = assert_some!(encryptor.encrypt(&data));
        let decrypted = assert_some!(encryptor.decrypt(&encrypted));

        assert_eq!(data, decrypted);
        assert_ne!(encrypted.data(), decrypted.data());
    }

    #[test]
    fn test_aes256_round_trip_large_data() {
        let encryptor = Aes256GcmEncryptorDecryptor::new([69; 32]);
        let mut bytes = Vec::with_capacity(1_000_000);
        for i in 0..1_000_000usize {
            bytes.push(i as u8);
        }
        let data = UnencryptedData::literal(&bytes);

        let encrypted = assert_some!(encryptor.encrypt(&data));
        let decrypted = assert_some!(encryptor.decrypt(&encrypted));

        assert_eq!(data, decrypted);
        assert_ne!(encrypted.data(), decrypted.data());
    }

    #[test]
    fn test_aes256_round_trip_no_data() {
        let encryptor = Aes256GcmEncryptorDecryptor::new([69; 32]);
        let data = UnencryptedData::literal(&[]);

        let encrypted = assert_some!(encryptor.encrypt(&data));
        let decrypted = assert_some!(encryptor.decrypt(&encrypted));

        assert_eq!(data, decrypted);
        assert_ne!(encrypted.data(), decrypted.data());
    }
}
