use bytes::Bytes;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream, UdpSocket, lookup_host};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, timeout};

// --- 常量配置 ---
const UDP_TIMEOUT: Duration = Duration::from_secs(120);
const BUF_SIZE: usize = 65535;

pub async fn start_tcp_proxy(name: String, local: String, remote: String) {
    let listener = match TcpListener::bind(&local).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[ERROR] ({}) TCP 绑定本地端口失败 ({}): {}", name, local, e);
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((mut client, peer)) => {
                println!("[INFO] ({}) 新 TCP 客户端连接: {}", name, peer);
                let target = remote.clone();
                let _ = client.set_nodelay(true);

                let name_task = name.clone();
                tokio::spawn(async move {
                    match TcpStream::connect(&target).await {
                        Ok(mut server) => {
                            let _ = server.set_nodelay(true);
                            if let Err(e) = copy_bidirectional(&mut client, &mut server).await {
                                eprintln!("[WARN] ({}) TCP 转发错误: {}", name_task, e);
                            };
                        }
                        Err(e) => eprintln!("[ERROR] ({}) TCP 远程连接失败: {}", name_task, e),
                    }
                    println!("[INFO] ({}) TCP 连接结束: {}", name_task, peer)
                });
            }
            Err(e) => {
                eprintln!("[ERROR] ({}) 接收 TCP 连接错误: {}", name, e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

pub async fn start_udp_proxy(name: String, local: String, remote: String) {
    let local_socket = Arc::new(match UdpSocket::bind(&local).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[ERROR] ({}) UDP 绑定本地端口失败: {}", name, e);
            return;
        }
    });

    // 解析 remote
    let remote_addr = match lookup_host(&remote).await {
        Ok(mut it) => match it.next() {
            Some(a) => a,
            None => {
                eprintln!("[ERROR] ({}) UDP 未解析到服务器地址: {}", name, remote);
                return;
            }
        },
        Err(e) => {
            eprintln!(
                "[ERROR] ({}) UDP 解析服务器地址失败 {}: {}",
                name, remote, e
            );
            return;
        }
    };

    // 存储客户端地址到 mpsc 发送端的映射
    let active_clients: Arc<DashMap<SocketAddr, mpsc::Sender<Bytes>>> = Arc::new(DashMap::new());
    let mut buf = vec![0u8; BUF_SIZE];

    loop {
        match local_socket.recv_from(&mut buf).await {
            Ok((len, src_addr)) => {
                let data = Bytes::copy_from_slice(&buf[..len]);
                let tx = active_clients
                    .entry(src_addr)
                    .or_insert_with(|| {
                        let (tx, rx) = mpsc::channel(1024);
                        let l_socket = Arc::clone(&local_socket);
                        let clients_map = Arc::clone(&active_clients);
                        let name_task = name.clone();

                        tokio::spawn(async move {
                            run_udp_tunnel(name_task, src_addr, rx, l_socket, remote_addr).await;
                            clients_map.remove(&src_addr);
                        });
                        tx
                    })
                    .value()
                    .clone();

                // 非阻塞发送
                if let Err(e) = tx.try_send(data) {
                    eprintln!(
                        "[WARN] ({}) UDP 丢包 (对端 {} channel 满了): {}",
                        name, src_addr, e
                    );
                }
            }
            Err(e) => eprintln!("[ERROR] ({}) 接收 UDP 连接错误: {}", name, e),
        }
    }
}

async fn run_udp_tunnel(
    name: String,
    client_addr: SocketAddr,
    mut rx: mpsc::Receiver<Bytes>,
    local_socket: Arc<UdpSocket>,
    remote_addr: SocketAddr,
) {
    // 绑定发送 socket
    let bind_addr = if remote_addr.is_ipv6() {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    };

    let send_socket = match UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[ERROR] ({}) UDP 子 socket 绑定失败: {}", name, e);
            return;
        }
    };

    // 连接远程地址
    if let Err(e) = send_socket.connect(remote_addr).await {
        eprintln!("[ERROR] ({}) UDP 连接远程失败: {}", name, e);
        return;
    }

    println!("[INFO] ({}) 新 UDP 客户端连接: {}", name, client_addr);

    let mut buf = vec![0u8; BUF_SIZE];
    let mut last_success = Instant::now();

    loop {
        if last_success.elapsed() > UDP_TIMEOUT {
            println!(
                "[INFO] ({}) UDP 连接空闲/超时，已清理: {}",
                name, client_addr
            );
            break;
        }

        let activity = timeout(Duration::from_secs(5), async {
            tokio::select! {
                // 模式A: 接收本地客户端数据，转发给远程
                msg = rx.recv() => {
                    match msg {
                        Some(data) => {
                            if send_socket.send(&data).await.is_ok() {
                                // 成功更新时间戳
                                last_success = Instant::now();
                            }
                            true
                    }
                        // 通道关闭，直接退出
                        None => {
                            eprintln!("[ERROR] ({}) UDP 通道已关闭",name);
                            false
                        },
                    }
                }
                // 模式B: 接收远程回程的数据，转发给本地客户端
                res = send_socket.recv(&mut buf) => {
                    match res {
                        Ok(len) => {
                            if local_socket.send_to(&buf[..len], client_addr).await.is_ok() {
                                // 成功更新时间戳
                                last_success = Instant::now();
                            }
                            true
                        }
                        Err(e) => {
                            eprintln!("[WARN] ({}) UDP 接收远程数据异常: {}",name, e);
                            true
                        }
                    }
                }
            }
        })
        .await;

        match activity {
            Ok(true) => continue,
            Ok(false) => break,
            Err(_) => continue,
        }
    }
}
