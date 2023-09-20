use std::collections::HashSet;

use hdfs_native::minidfs::{DfsFeatures, MiniDfs};
use pyo3::prelude::*;

#[pyclass(name = "MiniDfs")]
pub struct PyMiniDfs {
    inner: MiniDfs,
}

#[pymethods]
impl PyMiniDfs {
    #[new]
    #[pyo3(signature = (features))]
    pub fn new(features: Vec<&str>) -> Self {
        let mut feature_set: HashSet<DfsFeatures> = HashSet::new();
        for feature in features.into_iter() {
            if let Some(dfs_feature) = DfsFeatures::from(feature) {
                feature_set.insert(dfs_feature);
            }
        }
        let inner = MiniDfs::with_features(&feature_set);

        Self { inner }
    }

    pub fn get_url(&self) -> String {
        self.inner.url.clone()
    }
}
