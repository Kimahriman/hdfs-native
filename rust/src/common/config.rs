use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const HADOOP_CONF_DIR: &str = "HADOOP_CONF_DIR";
const HADOOP_HOME: &str = "HADOOP_HOME";

const HA_NAMENODES_PREFIX: &str = "dfs.ha.namenodes";
const HA_NAMENODE_RPC_ADDRESS_PREFIX: &str = "dfs.namenode.rpc-address";

#[derive(Debug)]
pub struct Configuration {
    map: HashMap<String, String>,
}

impl Configuration {
    pub fn new() -> io::Result<Self> {
        let mut map: HashMap<String, String> = HashMap::new();

        match Self::get_conf_dir() {
            Some(conf_dir) => {
                for file in ["core-site.xml", "hdfs-site.xml"] {
                    let config_path = conf_dir.join(file);
                    if config_path.as_path().exists() {
                        Self::read_from_file(config_path.as_path())?
                            .into_iter()
                            .for_each(|(key, value)| {
                                map.insert(key, value);
                            })
                    }
                }
            }
            None => (),
        }

        Ok(Configuration { map })
    }

    /// Get a value from the config, returning None if the key wasn't defined.
    // pub fn get(&self, key: &str) -> Option<String> {
    //     self.map.get(key).map(|v| v.clone())
    // }

    pub(crate) fn get_urls_for_nameservice(&self, nameservice: &str) -> Vec<String> {
        self.map
            .get(&format!("{}.{}", HA_NAMENODES_PREFIX, nameservice))
            .into_iter()
            .flat_map(|namenodes| {
                namenodes.split(",").flat_map(|namenode_id| {
                    self.map
                        .get(&format!(
                            "{}.{}.{}",
                            HA_NAMENODE_RPC_ADDRESS_PREFIX, nameservice, namenode_id
                        ))
                        .map(|s| s.to_string())
                })
            })
            .collect()
    }

    fn read_from_file(path: &Path) -> io::Result<Vec<(String, String)>> {
        let content = fs::read_to_string(path)?;
        let tree = roxmltree::Document::parse(&content).unwrap();

        let pairs = tree
            .root()
            .children()
            .find(|d| d.tag_name().name() == "configuration")
            .into_iter()
            .flat_map(|config| {
                config
                    .children()
                    .filter(|c| c.tag_name().name() == "property")
            })
            .flat_map(|property| {
                let name = property.children().find(|n| n.tag_name().name() == "name");
                let value = property.children().find(|n| n.tag_name().name() == "value");

                match (name, value) {
                    (Some(name), Some(value)) => match (name.text(), value.text()) {
                        (Some(name), Some(text)) => Some((name.to_string(), text.to_string())),
                        _ => None,
                    },
                    _ => None,
                }
            });

        Ok(pairs.collect())
    }

    fn get_conf_dir() -> Option<PathBuf> {
        match env::var(HADOOP_CONF_DIR) {
            Ok(dir) => Some(PathBuf::from(dir)),
            Err(_) => match env::var(HADOOP_HOME) {
                Ok(dir) => Some([&dir, "etc/hadoop"].iter().collect()),
                Err(_) => None,
            },
        }
    }
}
