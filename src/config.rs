use core::fmt;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub tunnels: Vec<Tunnel>,
}

#[derive(Debug, Deserialize)]
pub struct Tunnel {
    pub host: String,
    pub services: Vec<Service>,
}

#[derive(Debug, Deserialize)]
pub struct Service {
    pub name: String,
    pub remote: u16,
    pub local: u16,
    pub proto: Protocol,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
    Both,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Protocol::Tcp => "TCP",
            Protocol::Udp => "UDP",
            Protocol::Both => "TCP & UDP",
        };
        write!(f, "{s}")
    }
}

pub fn load_config(path: &str) -> Result<Config, String> {
    let content =
        fs::read_to_string(path).map_err(|e| format!("无法读取配置文件 '{}': {}", path, e))?;

    let config: Config =
        toml::from_str(&content).map_err(|e| format!("解析配置文件失败: {}", e))?;

    Ok(config)
}
