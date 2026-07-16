use bytes::{Buf, Bytes};
use prost::Message;

use crate::Result;
use crate::proto::hdfs::{AccessModeProto, BlockTokenSecretProto, StorageTypeProto};

pub use hadoop_native::security::user::User;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BlockTokenIdentifier {
    pub expiry_date: u64,
    pub key_id: u32,
    pub user_id: String,
    pub block_pool_id: String,
    pub block_id: u64,
    pub modes: Vec<i32>,
    pub storage_types: Vec<i32>,
    pub storage_ids: Vec<String>,
    pub handshake_secret: Vec<u8>,
}

#[allow(dead_code)]
impl BlockTokenIdentifier {
    fn parse_writable(reader: &mut impl Buf) -> Result<Self> {
        let expiry_date = parse_vlong(reader) as u64;
        let key_id = parse_vint(reader) as u32;
        let user_id = parse_int_string(reader)?.unwrap();
        let block_pool_id = parse_int_string(reader)?.unwrap();
        let block_id = parse_vlong(reader) as u64;
        let mut modes = Vec::new();
        let mut storage_types = Vec::new();
        let mut storage_ids = Vec::new();

        if reader.has_remaining() {
            for _ in 0..parse_vint(reader) {
                if let Some(mode) = AccessModeProto::from_str_name(&parse_vint_string(reader)?) {
                    modes.push(mode as i32);
                }
            }
        }
        if reader.has_remaining() {
            for _ in 0..parse_vint(reader) {
                if let Some(storage_type) =
                    StorageTypeProto::from_str_name(&parse_vint_string(reader)?)
                {
                    storage_types.push(storage_type as i32);
                }
            }
        }
        if reader.has_remaining() {
            for _ in 0..parse_vint(reader) {
                if let Some(storage_id) = parse_int_string(reader)? {
                    storage_ids.push(storage_id);
                }
            }
        }
        let handshake_secret = if reader.has_remaining() {
            let length = parse_vint(reader) as usize;
            reader.copy_to_bytes(length).to_vec()
        } else {
            Vec::new()
        };

        Ok(Self {
            expiry_date,
            key_id,
            user_id,
            block_pool_id,
            block_id,
            modes,
            storage_types,
            storage_ids,
            handshake_secret,
        })
    }

    fn parse_protobuf(identifier: &[u8]) -> Result<Self> {
        let proto = BlockTokenSecretProto::decode(identifier)?;
        let handshake_secret = proto.handshake_secret().to_vec();
        Ok(Self {
            expiry_date: proto.expiry_date(),
            key_id: proto.key_id(),
            user_id: proto.user_id().to_owned(),
            block_pool_id: proto.block_pool_id().to_owned(),
            block_id: proto.block_id(),
            modes: proto.modes,
            storage_types: proto.storage_types,
            storage_ids: proto.storage_ids,
            handshake_secret,
        })
    }

    pub(crate) fn from_identifier(identifier: &[u8]) -> Result<Self> {
        if identifier[0] == 0 || identifier[0] > 127 {
            Self::parse_writable(&mut Bytes::copy_from_slice(identifier))
        } else {
            Self::parse_protobuf(identifier)
        }
    }
}

fn parse_vlong(reader: &mut impl Buf) -> i64 {
    let first_byte = reader.get_i8();
    let length = if first_byte >= -112 {
        1
    } else if first_byte < -120 {
        -119 - first_byte
    } else {
        -111 - first_byte
    };
    if length == 1 {
        return first_byte as i64;
    }
    let mut value = 0i64;
    for _ in 0..length - 1 {
        value = (value << 8) | reader.get_u8() as i64;
    }
    if first_byte < -120 || (-112..0).contains(&first_byte) {
        value ^ -1
    } else {
        value
    }
}

fn parse_vint(reader: &mut impl Buf) -> i32 {
    parse_vlong(reader) as i32
}

fn parse_int_string(reader: &mut impl Buf) -> std::io::Result<Option<String>> {
    let length = reader.get_i32();
    if length == -1 {
        Ok(None)
    } else {
        parse_string(reader, length).map(Some)
    }
}

fn parse_vint_string(reader: &mut impl Buf) -> std::io::Result<String> {
    let length = parse_vint(reader);
    parse_string(reader, length)
}

fn parse_string(reader: &mut impl Buf, length: i32) -> std::io::Result<String> {
    String::from_utf8(reader.copy_to_bytes(length as usize).to_vec())
        .map_err(|_| std::io::Error::other("failed to parse string from writable"))
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::BlockTokenIdentifier;
    use crate::proto::hdfs::BlockTokenSecretProto;

    #[test]
    fn parses_protobuf_block_token() {
        let encoded = BlockTokenSecretProto {
            expiry_date: Some(42),
            key_id: Some(7),
            user_id: Some("hdfs".to_owned()),
            block_pool_id: Some("BP-1".to_owned()),
            block_id: Some(99),
            handshake_secret: Some(vec![1, 2, 3]),
            ..Default::default()
        }
        .encode_to_vec();

        let token = BlockTokenIdentifier::from_identifier(&encoded).unwrap();
        assert_eq!(token.user_id, "hdfs");
        assert_eq!(token.block_pool_id, "BP-1");
        assert_eq!(token.handshake_secret, vec![1, 2, 3]);
    }
}
