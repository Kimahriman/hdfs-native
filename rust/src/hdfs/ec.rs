use bytes::{Bytes, BytesMut};
#[cfg(feature = "rs")]
use reed_solomon_erasure::{
    galois_8::{add, div, Field, ReedSolomon},
    matrix::Matrix,
};

use crate::{proto::hdfs, HdfsError, Result};

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

        let bytes_in_last_row: usize = if remaining_block_bytes < index as usize * self.cell_size {
            0
        } else if remaining_block_bytes > (index + 1) as usize * self.cell_size {
            self.cell_size
        } else {
            remaining_block_bytes - index as usize * self.cell_size
        };
        full_rows * self.cell_size + bytes_in_last_row
    }

    pub(crate) fn ec_decode(
        &self,
        mut vertical_stripes: Vec<Option<BytesMut>>,
    ) -> Result<Vec<Bytes>> {
        let mut cells: Vec<Bytes> = Vec::new();
        if !vertical_stripes
            .iter()
            .enumerate()
            .all(|(index, stripe)| stripe.is_some() || index >= self.data_units)
        {
            match self.codec_name.as_str() {
                #[cfg(feature = "rs")]
                "rs" => {
                    let matrix = gen_rs_matrix(self.data_units, self.parity_units)?;
                    let decoder =
                        ReedSolomon::new_with_matrix(self.data_units, self.parity_units, matrix)?;
                    decoder.reconstruct_data(&mut vertical_stripes)?;
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
            for index in 0..self.data_units {
                cells.push(
                    vertical_stripes[index as usize]
                        .as_mut()
                        .unwrap()
                        .split_to(self.cell_size)
                        .freeze(),
                )
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

#[cfg(feature = "rs")]
fn gen_rs_matrix(data_units: usize, parity_units: usize) -> Result<Matrix<Field>> {
    // Identity matrix for the first `data_rows` rows
    let mut data_rows: Vec<Vec<u8>> = (0..data_units)
        .map(|i| {
            let mut row = vec![0u8; data_units];
            row[i] = 1;
            row
        })
        .collect();

    // For parity rows, inverse of i ^ j, or 0 (described as 1 / (i + j) | i != j)
    let mut parity_rows: Vec<Vec<u8>> = (data_units..(data_units + parity_units))
        .map(|i| {
            let row: Vec<u8> = (0..data_units)
                .map(|j| match add(i as u8, j as u8) {
                    0 => 0,
                    mult => div(1, mult),
                })
                .collect();
            row
        })
        .collect();

    data_rows.append(&mut parity_rows);

    Ok(Matrix::new_with_data(data_rows))
}

#[cfg(feature = "rs")]
#[cfg(test)]
mod test {
    use crate::Result;

    #[test]
    fn test_build_rs_matrix() -> Result<()> {
        use super::gen_rs_matrix;
        use reed_solomon_erasure::matrix::Matrix;

        // These examples were taken directly from the matrices created by Hadoop via RSUtil.genCauchyMatrix
        assert_eq!(
            gen_rs_matrix(3, 2)?,
            Matrix::new_with_data(vec![
                vec![1, 0, 0,],
                vec![0, 1, 0,],
                vec![0, 0, 1,],
                vec![244, 142, 1,],
                vec![71, 167, 122,],
            ]),
        );

        assert_eq!(
            gen_rs_matrix(6, 3)?,
            Matrix::new_with_data(vec![
                vec![1, 0, 0, 0, 0, 0,],
                vec![0, 1, 0, 0, 0, 0,],
                vec![0, 0, 1, 0, 0, 0,],
                vec![0, 0, 0, 1, 0, 0,],
                vec![0, 0, 0, 0, 1, 0,],
                vec![0, 0, 0, 0, 0, 1,],
                vec![122, 186, 71, 167, 142, 244,],
                vec![186, 122, 167, 71, 244, 142,],
                vec![173, 157, 221, 152, 61, 170,],
            ]),
        );

        assert_eq!(
            gen_rs_matrix(10, 4)?,
            Matrix::new_with_data(vec![
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0,],
                vec![0, 1, 0, 0, 0, 0, 0, 0, 0, 0,],
                vec![0, 0, 1, 0, 0, 0, 0, 0, 0, 0,],
                vec![0, 0, 0, 1, 0, 0, 0, 0, 0, 0,],
                vec![0, 0, 0, 0, 1, 0, 0, 0, 0, 0,],
                vec![0, 0, 0, 0, 0, 1, 0, 0, 0, 0,],
                vec![0, 0, 0, 0, 0, 0, 1, 0, 0, 0,],
                vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0,],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 0,],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1,],
                vec![221, 152, 173, 157, 93, 150, 61, 170, 142, 244,],
                vec![152, 221, 157, 173, 150, 93, 170, 61, 244, 142,],
                vec![61, 170, 93, 150, 173, 157, 221, 152, 71, 167,],
                vec![170, 61, 150, 93, 157, 173, 152, 221, 167, 71,],
            ]),
        );
        Ok(())
    }
}
