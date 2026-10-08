use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::{HadoopError, Result};

const HADOOP_CONF_DIR: &str = "HADOOP_CONF_DIR";
const HADOOP_HOME: &str = "HADOOP_HOME";
const HADOOP_SECURITY_AUTHENTICATION: &str = "hadoop.security.authentication";

/// Hadoop configuration loaded from XML files and explicit overrides.
#[derive(Debug, Clone)]
pub struct Configuration {
    map: HashMap<String, String>,
}

impl Configuration {
    /// Load `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, and
    /// `mapred-site.xml`, then apply explicit overrides.
    pub fn new(
        conf_dir: Option<String>,
        conf_map: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let mut map = HashMap::new();

        if let Some(conf_dir) = Self::parse_conf_dir(conf_dir) {
            map = Self::parse_conf(conf_dir)?;
        }

        if let Some(conf_map) = conf_map {
            map.extend(conf_map);
        }

        Ok(Self { map })
    }

    /// Get a value, returning `None` when the key is undefined.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }

    /// Iterate over all configured key/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.map
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// Read a boolean value using Hadoop's case-insensitive `true` syntax.
    pub fn get_boolean(&self, key: &str, default: bool) -> bool {
        self.get(key)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(default)
    }

    /// Whether Hadoop security is configured with a non-simple mechanism.
    pub fn security_enabled(&self) -> bool {
        self.get(HADOOP_SECURITY_AUTHENTICATION)
            .is_some_and(|value| value != "simple")
    }

    fn read_from_file(path: &Path) -> Result<Vec<(String, String)>> {
        let content = fs::read_to_string(path)?;
        let resolver = EntityResolver::new(path)?;
        let entity_resolver = |_: Option<&str>, uri: &str| resolver.resolve(uri);
        let options = roxmltree::ParsingOptions {
            allow_dtd: true,
            entity_resolver: Some(&entity_resolver),
            ..Default::default()
        };
        let tree = roxmltree::Document::parse_with_options(&content, options)?;

        Ok(tree
            .root()
            .children()
            .find(|node| node.tag_name().name() == "configuration")
            .into_iter()
            .flat_map(|config| {
                config
                    .children()
                    .filter(|node| node.tag_name().name() == "property")
            })
            .filter_map(|property| {
                let name = property
                    .children()
                    .find(|node| node.tag_name().name() == "name")?;
                let value = property
                    .children()
                    .find(|node| node.tag_name().name() == "value")?;
                Some((name.text()?.to_owned(), value.text()?.to_owned()))
            })
            .collect())
    }

    fn parse_conf_dir(conf_dir: Option<String>) -> Option<PathBuf> {
        conf_dir.map(PathBuf::from).or_else(|| {
            env::var(HADOOP_CONF_DIR)
                .map(PathBuf::from)
                .ok()
                .or_else(|| {
                    env::var(HADOOP_HOME)
                        .map(|home| PathBuf::from(home).join("etc/hadoop"))
                        .ok()
                })
        })
    }

    fn parse_conf(conf_dir: PathBuf) -> Result<HashMap<String, String>> {
        let mut map = HashMap::new();
        for file in [
            "core-site.xml",
            "hdfs-site.xml",
            "yarn-site.xml",
            "mapred-site.xml",
        ] {
            let path = conf_dir.join(file);
            if path.exists() {
                map.extend(Self::read_from_file(&path)?);
            }
        }
        Ok(map)
    }
}

struct EntityResolver {
    basepath: PathBuf,
    bump: bumpalo::Bump,
    file_length_limit: u64,
    allocated_size_limit: u64,
}

impl EntityResolver {
    fn new(config_file_path: &Path) -> Result<Self> {
        let config_file_path = config_file_path.canonicalize()?;
        let basepath = config_file_path
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| {
                HadoopError::InvalidPath(format!(
                    "invalid base path for configuration file: {}",
                    config_file_path.display()
                ))
            })?;

        Ok(Self {
            basepath,
            bump: bumpalo::Bump::new(),
            file_length_limit: 16 * 1024 * 1024,
            allocated_size_limit: 16 * 1024 * 1024,
        })
    }

    fn resolve<'a>(&'a self, uri: &str) -> core::result::Result<Option<&'a str>, String> {
        let full_path = self.resolve_full_path(uri)?;
        if !full_path.exists() {
            return Ok(None);
        }

        let metadata = fs::metadata(&full_path).map_err(|error| {
            format!(
                "failed to get metadata of entity file {}: {error}",
                full_path.display()
            )
        })?;
        let file_size = metadata.len();
        if file_size > self.file_length_limit {
            return Err(format!(
                "entity file {} is too large ({file_size} bytes)",
                full_path.display()
            ));
        }
        if self.bump.allocated_bytes() as u64 + file_size > self.allocated_size_limit {
            return Err("entity resolver has no more memory".to_owned());
        }

        let content = fs::read_to_string(&full_path).map_err(|error| {
            format!(
                "read entity file content (path {}): {error}",
                full_path.display()
            )
        })?;
        Ok(Some(self.bump.alloc_str(&content)))
    }

    fn resolve_full_path(&self, uri: &str) -> core::result::Result<PathBuf, String> {
        use std::path::Component;

        let mut full_path = self.basepath.clone();
        for component in Path::new(uri).components() {
            match component {
                Component::CurDir => {}
                Component::Normal(part) => full_path.push(part),
                Component::ParentDir => {
                    return Err(
                        "parent directory components are not allowed in entity URIs".to_owned()
                    );
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err("absolute paths are not allowed in entity URIs".to_owned());
                }
            }
        }
        Ok(full_path)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use super::Configuration;

    #[test]
    fn explicit_values_override_defaults() {
        let config = Configuration::new(
            None,
            Some(HashMap::from([(
                "example.key".to_owned(),
                "value".to_owned(),
            )])),
        )
        .unwrap();
        assert_eq!(config.get("example.key"), Some("value"));
    }

    #[test]
    fn loads_core_and_yarn_site_files() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("core-site.xml"),
            "<configuration><property><name>core.key</name><value>core</value></property></configuration>",
        )
        .unwrap();
        fs::write(
            directory.path().join("yarn-site.xml"),
            "<configuration><property><name>yarn.key</name><value>yarn</value></property></configuration>",
        )
        .unwrap();

        let config =
            Configuration::new(Some(directory.path().to_string_lossy().into_owned()), None)
                .unwrap();
        assert_eq!(config.get("core.key"), Some("core"));
        assert_eq!(config.get("yarn.key"), Some("yarn"));
    }
}
