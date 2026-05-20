# v6bridge

v6bridge 是一个端口映射工具，主要用于游戏联机：远程服务器只有公网 IPv6，但游戏或本地客户端只支持 IPv4 时，可以把远程服务器端口映射到本地。服务器地址支持 IPv4、IPv6 和域名，端口支持 TCP、UDP 或同时映射。

项目当前使用 Go 实现。相比原 Rust 版本，这类网络转发小工具用 Go 更适合：标准库网络 API 直接、部署单二进制简单、跨平台构建和后续维护成本更低。

## 构建

```bash
go build -o v6bridge .
```

## 运行

把 `config.yml` 放在程序运行目录下，然后执行：

```bash
./v6bridge
```

开发验证：

```bash
go test ./...
```

## 配置示例

```yaml
network:
  session_timeout: 120s
  buffer_size: 65536

tunnels:
  - host: example.com
    services:
      - name: mc
        remote: 25565
        local: 25565
        proto: tcp
      - name: 帕鲁
        remote: 8211
        local: 8211
        proto: udp

  - host: 192.168.100.1
    services:
      - name: v4
        remote: 7000
        local: 7000
        proto: both

  - host: "[240e::1]"
    services:
      - name: v6
        remote: 6000
        local: 6000
        proto: both
```

## 优化点

- Go 版本使用标准库网络栈实现 TCP 双向转发和 UDP 会话转发，部署时只需要单个二进制。
- 配置加载使用 cleanenv 读取 YAML，并增加基础校验：空 host、空服务列表、非法协议、非法端口、非法网络参数会直接报错。
- TCP 和 UDP 监听都接入退出信号，`Ctrl+C` 后会关闭 listener、UDP socket、活跃 TCP 连接和 UDP 会话。
- `network.session_timeout` 控制 UDP 会话空闲清理时间，默认 `120s`。
- `network.buffer_size` 控制 TCP/UDP socket buffer 和转发 buffer 大小，默认 `65536` 字节。
