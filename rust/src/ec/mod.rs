pub mod gf256;
mod matrix;

use crate::{proto::hdfs, HdfsError, Result};
use bytes::Bytes;

use self::gf256::Coder;

const RS_CODEC_NAME: &str = "rs";
const RS_LEGACY_CODEC_NAME: &str = "rs-legacy";
const XOR_CODEC_NAME: &str = "xor";
const DEFAULT_EC_CELL_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct EcSchema {
    pub codec_name: String,
    pub data_units: usize,
    pub parity_units: usize,
    pub cell_size: usize,
}

impl EcSchema {
    pub(crate) fn row_size(&self) -> usize {
        self.cell_size * self.data_units
    }

    /// Returns the cell number (0-based) containing the offset
    pub(crate) fn cell_for_offset(&self, offset: usize) -> usize {
        offset / self.cell_size
    }

    pub(crate) fn row_for_cell(&self, cell_id: usize) -> usize {
        cell_id / self.data_units
    }

    pub(super) fn offset_for_row(&self, row_id: usize) -> usize {
        row_id * self.cell_size
    }

    pub(crate) fn max_offset(&self, mut index: usize, block_size: usize) -> usize {
        // If it's a parity cell, it's the same length as the first block
        if index >= self.data_units {
            index = 0;
        }

        // Get the number of bytes in the vertical slice for full rows
        let full_rows = block_size / self.row_size();
        let full_row_bytes = full_rows * self.row_size();

        let remaining_block_bytes = block_size - full_row_bytes;

        let bytes_in_last_row: usize = if remaining_block_bytes < index * self.cell_size {
            0
        } else if remaining_block_bytes > (index + 1) * self.cell_size {
            self.cell_size
        } else {
            remaining_block_bytes - index * self.cell_size
        };
        full_rows * self.cell_size + bytes_in_last_row
    }

    pub(crate) fn ec_decode(&self, mut vertical_stripes: Vec<Option<Bytes>>) -> Result<Vec<Bytes>> {
        let mut cells: Vec<Bytes> = Vec::new();
        if !vertical_stripes
            .iter()
            .enumerate()
            .all(|(index, stripe)| stripe.is_some() || index >= self.data_units)
        {
            match self.codec_name.as_str() {
                "rs" => {
                    let coder = Coder::new(self.data_units, self.parity_units);
                    coder.decode(&mut vertical_stripes)?;
                }
                codec => {
                    return Err(HdfsError::UnsupportedErasureCodingPolicy(format!(
                        "codec: {}",
                        codec
                    )))
                }
            }
        }

        while vertical_stripes[0].as_ref().is_some_and(|b| !b.is_empty()) {
            for stripe in vertical_stripes.iter_mut().take(self.data_units) {
                cells.push(stripe.as_mut().unwrap().split_to(self.cell_size))
            }
        }

        Ok(cells)
    }
}

/// Hadoop hard codes a default list of EC policies by ID
pub(crate) fn resolve_ec_policy(policy: &hdfs::ErasureCodingPolicyProto) -> Result<EcSchema> {
    if let Some(schema) = policy.schema.as_ref() {
        return Ok(EcSchema {
            codec_name: schema.codec_name.clone(),
            data_units: schema.data_units as usize,
            parity_units: schema.parity_units as usize,
            cell_size: policy.cell_size() as usize,
        });
    }

    match policy.id {
        // RS-6-3-1024k
        1 => Ok(EcSchema {
            codec_name: RS_CODEC_NAME.to_string(),
            data_units: 6,
            parity_units: 3,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // RS-3-2-1024k
        2 => Ok(EcSchema {
            codec_name: RS_CODEC_NAME.to_string(),
            data_units: 3,
            parity_units: 2,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // RS-6-3-1024k
        3 => Ok(EcSchema {
            codec_name: RS_LEGACY_CODEC_NAME.to_string(),
            data_units: 6,
            parity_units: 3,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // XOR-2-1-1024k
        4 => Ok(EcSchema {
            codec_name: XOR_CODEC_NAME.to_string(),
            data_units: 2,
            parity_units: 1,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        // RS-10-4-1024k
        5 => Ok(EcSchema {
            codec_name: RS_CODEC_NAME.to_string(),
            data_units: 10,
            parity_units: 4,
            cell_size: DEFAULT_EC_CELL_SIZE,
        }),
        _ => Err(HdfsError::UnsupportedErasureCodingPolicy(format!(
            "ID: {}",
            policy.id
        ))),
    }
}

#[cfg(test)]
mod test {
    use crate::ec::matrix::Matrix;

    use super::gf256::GF256;

    #[test]
    fn test_invert_matrix() {
        let mut matrix: Matrix<GF256> =
            Matrix::new(vec![vec![0, 0, 1], vec![244, 142, 1], vec![71, 167, 122]]);
        let original_matrix = matrix.clone();
        matrix.invert();

        assert_eq!(matrix * original_matrix, Matrix::identity(3));
    }
}
