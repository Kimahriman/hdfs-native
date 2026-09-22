use bytes::Bytes;
use num_traits::{One, Zero};

use super::matrix::Matrix;
use crate::Result;

g2p::g2p!(GF256, 8, modulus: 0b1_0001_1101);

impl Zero for GF256 {
    fn zero() -> Self {
        GF256(0)
    }

    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl One for GF256 {
    fn one() -> Self {
        GF256(1)
    }
}

pub struct Coder {
    data_units: usize,
    parity_units: usize,
    encode_matrix: Matrix<GF256>,
}

impl Coder {
    pub fn new(data_units: usize, parity_units: usize) -> Self {
        Self {
            data_units,
            parity_units,
            encode_matrix: Self::gen_rs_matrix(data_units, parity_units),
        }
    }

    pub fn gen_rs_matrix(data_units: usize, parity_units: usize) -> Matrix<GF256> {
        let mut matrix = Matrix::zeroes(data_units + parity_units, data_units);
        for r in 0..data_units {
            matrix[(r, r)] = GF256::one();
        }

        // For parity rows, inverse of i ^ j, or 0 (described as 1 / (i + j) | i != j)
        for r in data_units..(data_units + parity_units) {
            for c in 0..data_units {
                matrix[(r, c)] = match GF256(r as u8) + GF256(c as u8) {
                    z @ GF256(0) => z,
                    s => GF256(1) / s,
                };
            }
        }

        matrix
    }

    /// Takes a slice of data slices and returns a vector of parity slices
    #[allow(dead_code)]
    pub fn encode(&self, data: &[Bytes]) -> Vec<Bytes> {
        assert_eq!(data.len(), self.data_units);
        let shard_bytes = data[0].len();

        assert!(data.iter().skip(1).all(|s| s.len() == shard_bytes));

        let mut encode_matrix = self.encode_matrix.clone();

        // We only care about generating the parity rows
        encode_matrix.select_rows(self.data_units..self.data_units + self.parity_units);

        let parity_shards =
            encode_matrix * &data.iter().map(|r| &r[..]).collect::<Vec<&[u8]>>()[..];

        parity_shards
            .into_inner()
            .into_iter()
            .map(|shard| Bytes::from(shard.into_iter().map(Into::into).collect::<Vec<u8>>()))
            .collect()
    }

    /// Takes a slice of Option<Bytes>, and fills in any missing data shards using parity shards.
    /// Returns an error if there aren't enough parity shards to recompute missing data shards.
    pub fn decode(&self, data: &mut [Option<Bytes>]) -> Result<()> {
        let mut valid_indices: Vec<usize> = Vec::new();
        let mut invalid_indices: Vec<usize> = Vec::new();

        let mut data_matrix: Vec<&[u8]> = Vec::new();

        for (i, slice) in data.iter().enumerate() {
            if let Some(slice) = slice.as_ref() {
                if data_matrix.len() < self.data_units {
                    data_matrix.push(slice);
                }
                valid_indices.push(i);
            } else if i < self.data_units {
                // We don't care about missing parity data for decoding
                invalid_indices.push(i);
            }
        }

        if invalid_indices.is_empty() {
            // We have all the data shards so just return
            return Ok(());
        }

        if valid_indices.len() < self.data_units {
            return Err(crate::HdfsError::ErasureCodingError(
                "Not enough valid shards".to_string(),
            ));
        }

        // Build the encoding matrix
        let mut decode_matrix = self.encode_matrix.clone();

        // Select just the rows we have data for
        decode_matrix.select_rows(valid_indices.iter().cloned().take(self.data_units));

        // Invert the matrix to get the decode matrix
        decode_matrix.invert();

        // Select just the rows from the decode matrix we are missing data for.
        decode_matrix.select_rows(invalid_indices.iter().cloned());

        // Construct the missing slices
        let recovered_slices = decode_matrix * &data_matrix[..];

        for (i, slice) in recovered_slices.into_inner().into_iter().enumerate() {
            // This may require copying the slice to convert it from GF256 to u8, but hopefully the
            // compiler can optimize this to avoid that. Can do some benchmarking in the future to test
            data[invalid_indices[i]] = Some(Bytes::from(
                slice.into_iter().map(Into::into).collect::<Vec<u8>>(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::ec::{gf256::Coder, matrix::Matrix};

    #[test]
    fn test_build_rs_matrix() {
        // These examples were taken directly from the matrices created by Hadoop via RSUtil.genCauchyMatrix
        assert_eq!(
            Coder::gen_rs_matrix(3, 2),
            Matrix::new(vec![
                vec![1, 0, 0,],
                vec![0, 1, 0,],
                vec![0, 0, 1,],
                vec![244, 142, 1,],
                vec![71, 167, 122,],
            ]),
        );

        assert_eq!(
            Coder::gen_rs_matrix(6, 3),
            Matrix::new(vec![
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
            Coder::gen_rs_matrix(10, 4),
            Matrix::new(vec![
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
    }

    #[test]
    fn test_invert_matrix() {
        let mut matrix = Coder::gen_rs_matrix(3, 2);
        matrix.select_rows([2, 3, 4].into_iter());
        let original_matrix = matrix.clone();
        matrix.invert();

        assert_eq!(matrix * original_matrix, Matrix::identity(3));
    }
}
