use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;

use crate::common::config::Configuration;
use crate::hdfs::connection::{DatanodeConnection, Op};
use crate::proto::common::{self, TokenProto};
use crate::proto::hdfs;

pub(crate) fn test_config(settings: &[(&str, &str)]) -> Configuration {
    Configuration::new(
        Some("/tmp/hdfs-native-missing-conf".to_string()),
        Some(HashMap::from_iter(
            settings
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        )),
    )
    .unwrap()
}

pub(crate) fn socket_timeout_config(millis: u64) -> Configuration {
    test_config(&[("dfs.client.socket-timeout", &millis.to_string())])
}

pub(crate) fn encode_packet(data: &[u8], offset_in_block: i64) -> Vec<u8> {
    let header = hdfs::PacketHeaderProto {
        offset_in_block,
        data_len: data.len() as i32,
        ..Default::default()
    }
    .encode_to_vec();
    let mut packet = Vec::new();
    packet.put_u32(4 + data.len() as u32);
    packet.put_u16(header.len() as u16);
    packet.extend(header);
    packet.extend(data);
    packet
}

fn datanode_id(port: u16) -> hdfs::DatanodeIdProto {
    hdfs::DatanodeIdProto {
        ip_addr: "127.0.0.1".to_string(),
        xfer_port: port as u32,
        ..Default::default()
    }
}

async fn read_delimited(socket: &mut TcpStream) -> std::io::Result<()> {
    let mut len = 0usize;
    for shift in (0..).step_by(7) {
        let byte = socket.read_u8().await?;
        len |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            break;
        }
    }
    socket.read_exact(&mut vec![0u8; len]).await?;
    Ok(())
}

async fn serve_read(mut socket: TcpStream, writes: Vec<Vec<u8>>, delay: Duration) {
    let response = hdfs::BlockOpResponseProto {
        status: hdfs::Status::Success as i32,
        ..Default::default()
    }
    .encode_length_delimited_to_vec();
    let mut op = [0u8; 3];
    while socket.read_exact(&mut op).await.is_ok() {
        if read_delimited(&mut socket).await.is_err() {
            return;
        }
        socket.write_all(&response).await.unwrap();
        for write in &writes {
            tokio::time::sleep(delay).await;
            socket.write_all(write).await.unwrap();
        }
        if read_delimited(&mut socket).await.is_err() {
            return;
        }
    }
}

pub(crate) async fn fake_datanode(writes: Vec<Vec<u8>>, delay: Duration) -> hdfs::DatanodeIdProto {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(serve_read(socket, writes.clone(), delay));
        }
    });
    datanode_id(port)
}

async fn serve_handshake(mut socket: TcpStream, writes: Vec<Vec<u8>>, delay: Duration) {
    let mut magic = [0u8; 4];
    socket.read_exact(&mut magic).await.unwrap();
    read_delimited(&mut socket).await.unwrap();
    for write in &writes {
        tokio::time::sleep(delay).await;
        socket.write_all(write).await.unwrap();
    }
    std::future::pending::<()>().await;
}

pub(crate) async fn handshake_datanode(
    writes: Vec<Vec<u8>>,
    delay: Duration,
) -> hdfs::DatanodeIdProto {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        serve_handshake(socket, writes, delay).await;
    });
    datanode_id(port)
}

pub(crate) async fn stalling_datanode(first: Vec<Vec<u8>>) -> hdfs::DatanodeIdProto {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut writes = first;
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(serve_read(
                socket,
                std::mem::take(&mut writes),
                Duration::ZERO,
            ));
        }
    });
    datanode_id(port)
}

pub(crate) type LocationRequests = Arc<Mutex<Vec<hdfs::GetBlockLocationsRequestProto>>>;

async fn serve_rpc(
    mut socket: TcpStream,
    locations: hdfs::LocatedBlocksProto,
    requests: LocationRequests,
) {
    let mut preamble = [0u8; 7];
    socket.read_exact(&mut preamble).await.unwrap();
    while let Ok(len) = socket.read_u32().await {
        let mut frame = vec![0u8; len as usize];
        socket.read_exact(&mut frame).await.unwrap();
        let mut frame = Bytes::from(frame);
        let header = common::RpcRequestHeaderProto::decode_length_delimited(&mut frame).unwrap();
        if header.call_id < 0 {
            continue;
        }
        let method = common::RequestHeaderProto::decode_length_delimited(&mut frame)
            .unwrap()
            .method_name;
        let response = match method.as_str() {
            "getServerDefaults" => {
                hdfs::GetServerDefaultsResponseProto::default().encode_length_delimited_to_vec()
            }
            "getBlockLocations" => {
                let request =
                    hdfs::GetBlockLocationsRequestProto::decode_length_delimited(&mut frame)
                        .unwrap();
                requests.lock().unwrap().push(request);
                hdfs::GetBlockLocationsResponseProto {
                    locations: Some(locations.clone()),
                }
                .encode_length_delimited_to_vec()
            }
            method => panic!("Unexpected NameNode call {method}"),
        };
        let response_header = common::RpcResponseHeaderProto {
            call_id: header.call_id as u32,
            status: common::rpc_response_header_proto::RpcStatusProto::Success as i32,
            ..Default::default()
        };
        let body = [response_header.encode_length_delimited_to_vec(), response].concat();
        socket.write_u32(body.len() as u32).await.unwrap();
        socket.write_all(&body).await.unwrap();
    }
}

pub(crate) async fn fake_namenode(
    locations: hdfs::LocatedBlocksProto,
) -> (String, LocationRequests) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("hdfs://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let requests = LocationRequests::default();
    let served = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(serve_rpc(socket, locations.clone(), Arc::clone(&served)));
        }
    });
    (url, requests)
}

pub(crate) async fn connect(
    datanode: &hdfs::DatanodeIdProto,
    config: &Configuration,
) -> DatanodeConnection {
    DatanodeConnection::connect(
        datanode,
        &TokenProto::default(),
        None,
        config,
        &Handle::current(),
    )
    .await
    .unwrap()
}

pub(crate) async fn open_read(
    datanode: &hdfs::DatanodeIdProto,
    config: &Configuration,
) -> DatanodeConnection {
    let mut conn = connect(datanode, config).await;
    conn.send(Op::ReadBlock, &hdfs::OpReadBlockProto::default())
        .await
        .unwrap();
    conn
}
