use std::io::{Read, Result, Write};
use std::net::TcpStream;

use bytes::BytesMut;
use prost::Message;

use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::common::rpc_sasl_proto::{SaslAuth, SaslState};
use crate::proto::common::{
    RpcKindProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
};

const SASL_CALL_ID: i32 = -33;

struct AuthMethod {}
impl AuthMethod {
    const SIMPLE: &str = "SIMPLE";
}

pub struct SaslClient {
    stream: TcpStream,
}

impl SaslClient {
    pub fn create(stream: TcpStream) -> Result<SaslClient> {
        let mut client = SaslClient { stream };
        client.negotiate()?;
        Ok(client)
    }

    fn create_request_header() -> RpcRequestHeaderProto {
        let mut request_header = RpcRequestHeaderProto::default();
        request_header.rpc_kind = Some(RpcKindProto::RpcProtocolBuffer as i32);
        // RPC_FINAL_PACKET
        request_header.rpc_op = Some(0);
        request_header.call_id = SASL_CALL_ID;
        request_header.client_id = Vec::new();
        request_header.retry_count = Some(-1);
        request_header
    }

    fn negotiate(&mut self) -> Result<()> {
        let header = Self::create_request_header();
        let mut rpc_sasl = RpcSaslProto::default();
        rpc_sasl.state = SaslState::Negotiate as i32;

        self.send_sasl_message(&header, &rpc_sasl)?;

        let mut done = false;
        while !done {
            let mut response: Option<RpcSaslProto> = None;
            let message = self.read_response()?;
            println!("{:?}", message);
            match SaslState::from_i32(message.state).unwrap() {
                SaslState::Negotiate => {
                    let selected_auth = self.select_method(&message.auths)?;
                    if selected_auth.method == AuthMethod::SIMPLE {
                        done = true;
                    }

                    let mut r = RpcSaslProto::default();
                    r.state = SaslState::Initiate as i32;
                    r.auths = Vec::from([selected_auth]);
                    response = Some(r);
                }
                _ => todo!(),
            }

            for r in response.iter() {
                println!("Sending response {:?}", r);
                self.send_sasl_message(&header, &r)?;
            }
        }

        Ok(())
    }

    fn send_sasl_message(
        &mut self,
        header: &RpcRequestHeaderProto,
        message: &RpcSaslProto,
    ) -> Result<()> {
        let header_buf = header.encode_length_delimited_to_vec();
        let message_buf = message.encode_length_delimited_to_vec();
        let size = (header_buf.len() + message_buf.len()) as u32;

        self.stream.write(&size.to_be_bytes())?;
        self.stream.write(&header_buf)?;
        self.stream.write(&message_buf)?;
        self.stream.flush()?;

        Ok(())
    }

    fn read_response(&mut self) -> Result<RpcSaslProto> {
        let mut buf = [0u8; 4];
        let size = self.stream.read(&mut buf).unwrap();

        let msg_length = u32::from_be_bytes(buf);

        let mut buf = BytesMut::zeroed(msg_length as usize);
        let size = self.stream.read(&mut buf).unwrap();

        let mut bytes = buf.freeze();
        let rpc_response = RpcResponseHeaderProto::decode_length_delimited(&mut bytes).unwrap();

        match RpcStatusProto::from_i32(rpc_response.status).unwrap() {
            RpcStatusProto::Error => {
                todo!("Throw a real error")
            }
            RpcStatusProto::Fatal => {
                todo!("Throw a real error")
            }
            _ => (),
        }
        println!("{:?}", rpc_response);

        let sasl_response = RpcSaslProto::decode_length_delimited(&mut bytes).unwrap();
        println!("{:?}", sasl_response);
        Ok(sasl_response)
    }

    fn select_method(&mut self, auths: &Vec<SaslAuth>) -> Result<SaslAuth> {
        Ok(auths
            .iter()
            .filter(|a| a.method == AuthMethod::SIMPLE)
            .next()
            .unwrap()
            .clone())
    }
}

impl SaslClient {
    pub fn try_clone(&self) -> Result<SaslClient> {
        let stream = self.stream.try_clone()?;
        Ok(SaslClient { stream })
    }
}

impl Read for SaslClient {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for SaslClient {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}
