use std::{
    collections::HashSet,
    ops::{Add, AddAssign, Div, Index, IndexMut, Mul, MulAssign, SubAssign},
};

use num_traits::{One, Zero};

/// Helper struct for matrix operations needed for erasure coded. Mostly used for inverting
/// matrices for reed-solomon decoding, and matrix multiplication for recovering data shards
/// or computing parity shards.
#[derive(PartialEq, Debug, Clone)]
pub struct Matrix<T> {
    data: Vec<Vec<T>>,
}

impl<T> Matrix<T> {
    #[cfg(test)]
    pub fn new<U, V>(data: impl AsRef<[V]>) -> Self
    where
        U: Into<T> + Copy,
        V: AsRef<[U]>,
    {
        let data = data.as_ref();
        assert!(!data.is_empty());
        let cols = data[0].as_ref().len();
        assert!(cols > 0);
        for row in data.iter() {
            assert_eq!(row.as_ref().len(), cols);
        }
        Self {
            data: data
                .iter()
                .map(|r| {
                    r.as_ref()
                        .iter()
                        .map(|d| Into::<T>::into(*d))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        }
    }

    #[inline]
    pub fn rows(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn cols(&self) -> usize {
        self.data[0].len()
    }

    pub(crate) fn zeroes(rows: usize, cols: usize) -> Self
    where
        T: Zero + Clone,
    {
        assert!(rows > 0 && cols > 0);
        let data = vec![vec![T::zero(); cols]; rows];
        Self { data }
    }

    pub(crate) fn identity(rows: usize) -> Self
    where
        T: Zero + One + Clone,
    {
        let mut matrix = Self::zeroes(rows, rows);
        for i in 0..rows {
            matrix[(i, i)] = T::one();
        }
        matrix
    }

    /// Filter this matrix down to only selected rows
    pub fn select_rows(&mut self, rows: impl Iterator<Item = usize>) {
        let rows: HashSet<usize> = rows.collect();

        let data = std::mem::take(&mut self.data);

        self.data = data
            .into_iter()
            .enumerate()
            .filter_map(|(i, row)| if rows.contains(&i) { Some(row) } else { None })
            .collect();
    }

    pub(crate) fn into_inner(self) -> Vec<Vec<T>> {
        self.data
    }

    /// Extend this matrix by inserting another matrix on the right hand side
    fn extend_cols(&mut self, other: Matrix<T>)
    where
        T: Clone,
    {
        assert_eq!(self.data.len(), other.data.len());
        for (lhr, rhr) in self.data.iter_mut().zip(other.data.iter()) {
            lhr.extend(rhr.iter().cloned());
        }
    }

    pub fn invert(&mut self)
    where
        T: Zero + One + MulAssign + SubAssign + Div<Output = T> + PartialEq + Clone + Copy,
    {
        if self.rows() != self.cols() {
            panic!("Cannot invert a non-square matrix");
        }

        // Add an identity matrix to the right hand side
        self.extend_cols(Matrix::identity(self.data.len()));

        for r in 0..self.rows() {
            if self[(r, r)].is_zero() {
                for r_swap in r + 1..self.rows() {
                    if !self[(r_swap, r)].is_zero() {
                        self.data.swap(r, r_swap);
                    }
                }
            }

            if self[(r, r)].is_zero() {
                panic!("Matrix is singular");
            }

            // Scale the row
            if !self[(r, r)].is_one() {
                let scale = T::one() / self[(r, r)];
                for c in 0..self.cols() {
                    self[(r, c)] *= scale;
                }
            }

            // Clear below the diagonal
            for r_below in r + 1..self.rows() {
                if !self[(r_below, r)].is_zero() {
                    let scale = self[(r_below, r)];
                    for c in 0..self.cols() {
                        let val = self[(r, c)];
                        self[(r_below, c)] -= val * scale;
                    }
                }
            }
        }

        // Clear above the diagonal
        for r in 1..self.rows() {
            for r_above in 0..r {
                if !self[(r_above, r)].is_zero() {
                    let scale = self[(r_above, r)];
                    for c in 0..self.cols() {
                        let val = self[(r, c)];
                        self[(r_above, c)] -= val * scale;
                    }
                }
            }
        }

        // Take just the right hand side now
        for row in self.data.iter_mut() {
            *row = row[row.len() / 2..].to_vec();
        }
    }
}

impl<T> Index<(usize, usize)> for Matrix<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: (usize, usize)) -> &Self::Output {
        &self.data[index.0][index.1]
    }
}

impl<T> IndexMut<(usize, usize)> for Matrix<T> {
    #[inline]
    fn index_mut(&mut self, index: (usize, usize)) -> &mut Self::Output {
        &mut self.data[index.0][index.1]
    }
}

impl<T: Zero + One + Add + AddAssign + Mul + Clone + Copy> Mul for Matrix<T> {
    type Output = Matrix<T>;

    fn mul(self, rhs: Self) -> Self::Output {
        assert_eq!(self.cols(), rhs.rows());

        let mut result: Matrix<T> = Matrix::zeroes(self.rows(), rhs.cols());

        for (i, row) in self.data.iter().enumerate() {
            for j in 0..rhs.cols() {
                for rhs_row in 0..rhs.rows() {
                    result[(i, j)] += row[rhs_row] * rhs[(rhs_row, j)];
                }
            }
        }

        result
    }
}

/// Maybe overly convoluted generic function for computing matrix multiplication with a 2D
/// slice of entries that can be turned into the type stored in this Matrix. In practice this
/// is only used for multiplying a Matrix of GF256 by a 2D slice of u8.
impl<T, U> Mul<&[&[U]]> for Matrix<T>
where
    T: Zero + One + Add + AddAssign + Mul + Clone + Copy + std::fmt::Debug,
    U: Into<T> + Copy + std::fmt::Debug,
{
    type Output = Matrix<T>;

    fn mul(self, rhs: &[&[U]]) -> Self::Output {
        assert_eq!(self.cols(), rhs.len());
        let rhs_cols = rhs[0].len();
        for row in rhs.iter().skip(1) {
            assert_eq!(rhs_cols, row.len());
        }

        let mut result: Matrix<T> = Matrix::zeroes(self.rows(), rhs_cols);

        for (i, rhs_row) in rhs.iter().enumerate() {
            for (lhs_row, result_row) in self.data.iter().zip(result.data.iter_mut()) {
                let lhs_value = lhs_row[i];
                for (rhs_cell, result_cell) in rhs_row.iter().zip(result_row.iter_mut()) {
                    *result_cell += lhs_value * (*rhs_cell).into();
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use super::Matrix;

    #[test]
    fn test_matrix_multiply() {
        let matrix1: Matrix<i32> = Matrix::new(vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
        let matrix2: Matrix<i32> = Matrix::new(vec![vec![1, 2, 3], vec![4, 5, 6]]);

        assert_eq!(
            matrix1 * matrix2,
            Matrix::new(vec![vec![9, 12, 15], vec![19, 26, 33], vec![29, 40, 51]])
        );
    }

    #[test]
    fn test_matrix_invert() {
        let mut matrix: Matrix<f64> = Matrix::new(vec![vec![4.0, 7.0], vec![2.0, 6.0]]);
        matrix.invert();

        // Floating point math makes it not exact
        assert!(f64::abs(matrix[(0, 0)] - 0.6) < 0.0001);
        assert!(f64::abs(matrix[(0, 1)] + 0.7) < 0.0001);
        assert!(f64::abs(matrix[(1, 0)] + 0.2) < 0.0001);
        assert!(f64::abs(matrix[(1, 1)] - 0.4) < 0.0001);
    }
}
