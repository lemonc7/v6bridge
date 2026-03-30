use std::sync::Arc;

use crate::{
    config::{Protocol, load_config},
    proxy::{start_tcp_proxy, start_udp_proxy},
};

mod config;
mod proxy;

#[tokio::main]
async fn main() {
    let config = match load_config("./config.toml") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ERROR] {}", e);
            return;
        }
    };

    if config.tunnels.is_empty() {
        eprintln!("[ERROR] 未发现有效配置，程序退出");
        return;
    }

    println!(
        ">> v6bridge | 端口映射工具, 主要用于游戏 ipv6 联机 \n>> 详情参考 GitHub: https://github.com/lemonc7/v6bridge"
    );

    for tunnel in config.tunnels {
        let host = Arc::new(tunnel.host);

        for service in tunnel.services {
            let local_addr = format!("0.0.0.0:{}", service.local);
            let remote_addr = format!("{}:{}", host, service.remote);

            println!(
                "[INFO] ({}) 启动监听: {} -> {} ({})",
                service.name, service.local, remote_addr, service.proto
            );

            match service.proto {
                Protocol::Tcp => {
                    tokio::spawn(start_tcp_proxy(service.name, local_addr, remote_addr));
                }
                Protocol::Udp => {
                    tokio::spawn(start_udp_proxy(service.name, local_addr, remote_addr));
                }
                Protocol::Both => {
                    tokio::spawn(start_tcp_proxy(
                        service.name.clone(),
                        local_addr.clone(),
                        remote_addr.clone(),
                    ));
                    tokio::spawn(start_udp_proxy(service.name, local_addr, remote_addr));
                }
            }
        }
    }

    println!("[INFO] 所有隧道已建立，运行中... (按 Ctrl+C 退出)");

    tokio::signal::ctrl_c().await.unwrap();
    println!("\n收到退出信号，程序关闭");
}
