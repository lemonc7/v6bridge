# v6bridge

v6bridge 是一个端口映射工具，主要用于游戏联机(针对服务器只有公网 ipv6，但是游戏只支持 ipv4 的场景)，可以将远程服务器的端口映射到本地，服务器 ip 支持 ipv4/ipv6 和域名格式，端口支持 udp/tcp

## Getting Start

- 通过配置文件 (config.toml) 映射服务，配置文件需要放在程序同目录下
- 配置参考:
```toml
[[tunnels]]
host = "example.com"
services = [
  { name = "mc", remote = 25565, local = 25565, proto = "tcp" },
  { name = "帕鲁", remote = 8211, local = 8211, proto = "udp" },
]

[[tunnels]]
host = "192.168.100.1"
services = [{ name = "v4", remote = 7000, local = 7000, proto = "both" }]

[[tunnels]]
host = "[240e::1]"
services = [{ name = "v6", remote = 6000, local = 6000, proto = "both" }]
```