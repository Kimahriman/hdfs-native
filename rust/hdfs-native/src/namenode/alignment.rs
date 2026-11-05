use std::collections::HashMap;

use bytes::Bytes;
use prost::Message;

use crate::proto::hdfs::RouterFederatedStateProto;

#[derive(Debug)]
pub(crate) struct AlignmentContext {
    state_id: i64,
    router_federated_state: Option<HashMap<String, i64>>,
}

impl AlignmentContext {
    fn update(
        &mut self,
        state_id: Option<i64>,
        router_federated_state: Option<Vec<u8>>,
    ) -> hadoop_native::Result<()> {
        if let Some(new_state_id) = state_id {
            self.state_id = i64::max(new_state_id, self.state_id);
        }

        if let Some(new_router_state) = router_federated_state {
            let new_map = RouterFederatedStateProto::decode(Bytes::from(new_router_state))?
                .namespace_state_ids;
            let current_map = self.router_federated_state.get_or_insert_with(HashMap::new);
            for (key, value) in new_map {
                current_map.insert(
                    key.clone(),
                    i64::max(value, *current_map.get(&key).unwrap_or(&i64::MIN)),
                );
            }
        }
        Ok(())
    }

    fn encode_router_state(&self) -> Option<Vec<u8>> {
        self.router_federated_state.as_ref().map(|state| {
            RouterFederatedStateProto {
                namespace_state_ids: state.clone(),
            }
            .encode_to_vec()
        })
    }
}

impl Default for AlignmentContext {
    fn default() -> Self {
        Self {
            state_id: i64::MIN,
            router_federated_state: None,
        }
    }
}

impl hadoop_native::rpc::RpcAlignmentContext for AlignmentContext {
    fn request_state(&self) -> (Option<i64>, Option<Vec<u8>>) {
        (Some(self.state_id), self.encode_router_state())
    }

    fn update_response(
        &mut self,
        state_id: Option<i64>,
        federated_state: Option<Vec<u8>>,
    ) -> hadoop_native::Result<()> {
        self.update(state_id, federated_state)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use prost::Message;

    use super::AlignmentContext;
    use crate::proto::hdfs::RouterFederatedStateProto;

    fn encode_router_state(map: &HashMap<String, i64>) -> Vec<u8> {
        RouterFederatedStateProto {
            namespace_state_ids: map.clone(),
        }
        .encode_to_vec()
    }

    #[test]
    fn merges_router_federated_state() {
        let mut context = AlignmentContext::default();
        let mut state = HashMap::from([("ns-1".to_owned(), 3)]);
        context
            .update(None, Some(encode_router_state(&state)))
            .unwrap();

        state.insert("ns-1".to_owned(), 5);
        state.insert("ns-2".to_owned(), 7);
        context
            .update(None, Some(encode_router_state(&state)))
            .unwrap();

        assert_eq!(
            context.router_federated_state,
            Some(HashMap::from([
                ("ns-1".to_owned(), 5),
                ("ns-2".to_owned(), 7),
            ]))
        );
    }
}
