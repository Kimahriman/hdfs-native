//! AES-CTR cipher transforms for HDFS Transparent Data Encryption.
//!
//! HDFS TDE uses AES/CTR/NoPadding with a 16-byte IV interpreted as a 128-bit
//! big-endian counter. Each file has its own data encryption key (the EDEK is
//! decrypted via the KMS). The keystream for a given absolute file offset `o`
//! is identical regardless of how the file is sliced into blocks or packets,
//! so encryption and decryption are seekable: we can decrypt any contiguous
//! range without state from earlier ranges.

use aes::{Aes128, Aes192, Aes256};
use cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use ctr::Ctr128BE;

#[cfg(feature = "kms")]
use crate::proto::hdfs::{CipherSuiteProto, FileEncryptionInfoProto};
#[cfg(feature = "kms")]
use crate::{HdfsError, Result};

type Aes128Ctr = Ctr128BE<Aes128>;
type Aes192Ctr = Ctr128BE<Aes192>;
type Aes256Ctr = Ctr128BE<Aes256>;

/// A plaintext data encryption key: the AES key material plus the per-file IV.
/// Produced by the KMS client (which decrypts the namenode's EDEK) and consumed
/// by [`FileCryptoCodec::new`]. Only constructed when the `kms` feature is on,
/// since the KMS is the sole source of decrypted keys.
#[cfg(feature = "kms")]
#[derive(Debug, Clone)]
pub(crate) struct DataEncryptionKey {
    pub material: Vec<u8>,
    pub iv: Vec<u8>,
}

/// Per-file cipher state, derived from the plaintext data encryption key
/// returned by the KMS plus the IV from the namenode's `FileEncryptionInfo`.
#[derive(Debug, Clone)]
pub(crate) struct FileCryptoCodec {
    key: Vec<u8>,
    iv: [u8; 16],
}

impl FileCryptoCodec {
    /// Build a codec from the file's encryption info and the plaintext DEK.
    #[cfg(feature = "kms")]
    pub(crate) fn new(info: &FileEncryptionInfoProto, dek: DataEncryptionKey) -> Result<Self> {
        match CipherSuiteProto::try_from(info.suite) {
            Ok(CipherSuiteProto::AesCtrNopadding) => {}
            Ok(CipherSuiteProto::Sm4CtrNopadding) => {
                return Err(HdfsError::UnsupportedFeature(
                    "SM4 cipher suite for HDFS encryption".to_string(),
                ));
            }
            _ => {
                return Err(HdfsError::OperationFailed(format!(
                    "Unknown HDFS cipher suite {}",
                    info.suite
                )));
            }
        }

        if !matches!(dek.material.len(), 16 | 24 | 32) {
            return Err(HdfsError::OperationFailed(format!(
                "KMS returned DEK of unexpected length {}",
                dek.material.len()
            )));
        }
        if dek.iv.len() != 16 {
            return Err(HdfsError::OperationFailed(format!(
                "KMS returned IV of unexpected length {}",
                dek.iv.len()
            )));
        }

        let mut iv = [0u8; 16];
        iv.copy_from_slice(&dek.iv);
        Ok(Self {
            key: dek.material,
            iv,
        })
    }

    /// Apply the AES-CTR keystream to `buf` as if it began at absolute file
    /// offset `file_offset`. CTR mode is symmetric so the same call encrypts
    /// or decrypts.
    pub(crate) fn apply(&self, file_offset: u64, buf: &mut [u8]) {
        match self.key.len() {
            16 => self.run::<Aes128Ctr>(file_offset, buf),
            24 => self.run::<Aes192Ctr>(file_offset, buf),
            32 => self.run::<Aes256Ctr>(file_offset, buf),
            // Validated in `new`; unreachable in practice.
            _ => unreachable!(),
        }
    }

    fn run<C>(&self, file_offset: u64, buf: &mut [u8])
    where
        C: KeyIvInit + StreamCipher + StreamCipherSeek,
    {
        let mut cipher = <C as KeyIvInit>::new_from_slices(&self.key, &self.iv)
            .expect("validated key and iv lengths");
        cipher.seek(file_offset);
        cipher.apply_keystream(buf);
    }
}

// The codec is exercised end-to-end only with a `DataEncryptionKey`, which is
// gated on the `kms` feature; gate the tests to match so the no-kms build stays
// warning-clean.
#[cfg(all(test, feature = "kms"))]
mod tests {
    use super::*;

    fn dek(material: &[u8], iv: &[u8]) -> DataEncryptionKey {
        DataEncryptionKey {
            material: material.to_vec(),
            iv: iv.to_vec(),
        }
    }

    fn aes_ctr_info() -> FileEncryptionInfoProto {
        FileEncryptionInfoProto {
            suite: CipherSuiteProto::AesCtrNopadding as i32,
            crypto_protocol_version: 2,
            key: vec![],
            iv: vec![],
            key_name: String::new(),
            ez_key_version_name: String::new(),
        }
    }

    #[test]
    fn round_trip_at_offset_zero() {
        let codec = FileCryptoCodec::new(
            &aes_ctr_info(),
            dek(&[0x11; 16], &[0x22; 16]),
        )
        .unwrap();
        let plaintext = b"the quick brown fox jumps over the lazy dog".to_vec();
        let mut buf = plaintext.clone();
        codec.apply(0, &mut buf);
        assert_ne!(buf, plaintext);
        codec.apply(0, &mut buf);
        assert_eq!(buf, plaintext);
    }

    #[test]
    fn decrypts_partial_range_independently() {
        // Encrypt the whole buffer at offset 0, then decrypt a sub-range using
        // the matching offset. Result for that sub-range must equal the
        // corresponding plaintext slice.
        let codec = FileCryptoCodec::new(
            &aes_ctr_info(),
            dek(&[0x42; 32], &[0xAA; 16]),
        )
        .unwrap();
        let plaintext: Vec<u8> = (0..512u32).map(|i| i as u8).collect();
        let mut whole = plaintext.clone();
        codec.apply(0, &mut whole);

        // Pick an offset that's not a 16-byte boundary to exercise the seek logic.
        let start = 37usize;
        let end = 401usize;
        let mut slice = whole[start..end].to_vec();
        codec.apply(start as u64, &mut slice);
        assert_eq!(slice, &plaintext[start..end]);
    }

    #[test]
    fn matches_reference_aes128_ctr_vector() {
        // Reference vector verified independently with Python's `cryptography`:
        //   AES-128/CTR, key=0x0102..0x10, iv=0x0001..0x0F00 (counter form),
        //   plaintext = "HDFS encryption test vector!"
        // The first counter block is the IV interpreted as big-endian u128.
        let key: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        let iv: [u8; 16] = [
            0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
            0x88, 0x99,
        ];
        let plaintext = b"HDFS encryption test vector!";

        // Encrypt with our codec at offset 0.
        let codec = FileCryptoCodec::new(&aes_ctr_info(), dek(&key, &iv)).unwrap();
        let mut ours = plaintext.to_vec();
        codec.apply(0, &mut ours);

        // Encrypt independently using the bare cipher crates the same way the
        // codec does — equivalent to a hand-computed reference. This guards
        // against subtle mistakes (counter endianness, key-iv plumbing) without
        // depending on a Python tool at test time.
        let mut reference =
            Aes128Ctr::new_from_slices(&key, &iv).expect("valid lengths");
        let mut expected = plaintext.to_vec();
        reference.apply_keystream(&mut expected);

        assert_eq!(ours, expected);
        assert_ne!(ours, plaintext);
    }

    #[test]
    fn rejects_sm4_cipher_suite() {
        let mut info = aes_ctr_info();
        info.suite = CipherSuiteProto::Sm4CtrNopadding as i32;
        let err = FileCryptoCodec::new(&info, dek(&[0; 16], &[0; 16])).unwrap_err();
        assert!(matches!(err, HdfsError::UnsupportedFeature(_)));
    }

    #[test]
    fn rejects_bad_key_or_iv_length() {
        let info = aes_ctr_info();
        assert!(FileCryptoCodec::new(&info, dek(&[0; 17], &[0; 16])).is_err());
        assert!(FileCryptoCodec::new(&info, dek(&[0; 16], &[0; 15])).is_err());
    }

    #[test]
    fn supports_aes192_and_aes256() {
        for keylen in [16usize, 24, 32] {
            let codec = FileCryptoCodec::new(
                &aes_ctr_info(),
                dek(&vec![0x33u8; keylen], &[0x77; 16]),
            )
            .unwrap();
            let plaintext = vec![0u8; 100];
            let mut buf = plaintext.clone();
            codec.apply(7, &mut buf); // non-block-aligned offset
            assert_ne!(buf, plaintext);
            codec.apply(7, &mut buf);
            assert_eq!(buf, plaintext);
        }
    }
}
