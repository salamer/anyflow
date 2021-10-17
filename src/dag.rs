use async_trait::async_trait;
use futures::future::join_all;
use futures::future::FutureExt;
use futures::future::{Either, Future};
use futures::select;
use futures::stream::FuturesUnordered;
use futures_timer::Delay;
use macros::*;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum NodeResult {
    Ok(HashMap<String, Arc<dyn Any + std::marker::Send>>),
    Err(&'static str),
}

unsafe impl Send for NodeResult {}
unsafe impl Sync for NodeResult {}

impl NodeResult {
    pub fn new() -> NodeResult {
        NodeResult::Ok(HashMap::new())
    }
    pub fn get<T: Any + Debug + Clone>(&self, key: &str) -> Option<T> {
        match self {
            NodeResult::Ok(kv) => match kv.get(&key.to_string()) {
                Some(val) => val.downcast_ref::<T>().cloned(),
                None => None,
            },
            NodeResult::Err(_) => None,
        }
    }
    pub fn set<T: Any + Debug + Clone + std::marker::Send>(
        &self,
        key: &str,
        val: &T,
    ) -> NodeResult {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                new_kv.insert(key.to_string(), Arc::new(val.clone()));
                NodeResult::Ok(new_kv)
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }
    fn merge(&self, other: &NodeResult) -> NodeResult {
        match self {
            NodeResult::Ok(kv) => {
                let mut new_kv = kv.clone();
                match other {
                    NodeResult::Ok(other_kv) => new_kv.extend(other_kv.clone()),
                    NodeResult::Err(_e) => {}
                }
                NodeResult::Ok(new_kv)
            }
            NodeResult::Err(e) => NodeResult::Err(e),
        }
    }

    fn get_map<T: Any + Debug + Clone + std::marker::Send>(&self) -> Option<HashMap<String, T>> {
        match self {
            NodeResult::Ok(kv) => {
                let mut ret = HashMap::new();
                for (k, v) in kv {
                    match v.downcast_ref::<T>().cloned() {
                        Some(val) => ret.insert(k.clone(), val.clone()),
                        None => None,
                    };
                }
                Some(ret)
            }
            NodeResult::Err(_) => None,
        }
    }
}

#[derive(Deserialize, Default, Debug, Clone)]
struct NodeConfig {
    name: String,
    node: String,
    deps: Vec<String>,
    params: Box<RawValue>,
    #[serde(default)]
    necessary: bool,
}

#[derive(Deserialize, Default, Debug, Clone)]
struct DAGConfig {
    nodes: Vec<NodeConfig>,
}

pub struct DAGNode {
    node_config: NodeConfig,
    prevs: HashSet<String>,
    nexts: HashSet<String>,
}


fn handle_wrapper<'a, E: Send + Sync>(
    graph_args: &'a Arc<E>,
    input: Arc<NodeResult>,
    // params: Arc<AnyParams>,
) -> NodeResult {
    return NodeResult::new();
}

pub struct Flow<T: Default + Sync + Send, E: Send + Sync> {
    nodes: HashMap<String, Box<DAGNode>>,

    // global configures
    timeout: Duration,
    pre: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a NodeResult) -> T + Send + Sync>,
    post: Arc<dyn for<'a> Fn(&'a Arc<E>, &'a NodeResult, &T) + Send + Sync>,
    timeout_cb: Arc<dyn for<'a> Fn() + Send + Sync>,
    failure_cb: Arc<dyn for<'a> Fn(&'a NodeResult)>,

    // register
    node_mapping: HashMap<
        String,
        Arc<dyn for<'a> Fn(&'a Arc<E>, Arc<NodeResult>) -> NodeResult + Sync + Send>,
    >,
}

impl<T: Default + Send + Sync, E: Send + Sync> Flow<T, E> {
    fn new() -> Flow<T, E> {
        Flow {
            nodes: HashMap::new(),
            timeout: Duration::from_secs(5),
            pre: Arc::new(|a, b| T::default()),
            post: Arc::new(|a, b, c| {}),
            timeout_cb: Arc::new(|| {}),
            failure_cb: Arc::new(|a| {}),
            node_mapping: HashMap::new(),
        }
    }

    fn register(&mut self, node_name: &str, handle: &(dyn for<'a> Fn(&'a Arc<E>, Arc<NodeResult>) -> NodeResult + Sync + Send)) {

    }

    fn registers(&mut self, nodes: &[(&str, &(dyn for<'a> Fn(&'a Arc<E>, Arc<NodeResult>) -> NodeResult + Sync + Send))]) {

    }

    fn init(&mut self, conf_content: &str) -> Result<(), String> {
        let dag_config: DAGConfig = serde_json::from_str(conf_content).unwrap();
        for node_config in dag_config.nodes.iter() {
            self.nodes.insert(
                node_config.name.clone(),
                Box::new(DAGNode {
                    node_config: node_config.clone(),
                    nexts: HashSet::new(),
                    prevs: HashSet::new(),
                }),
            );
        }
        for node_config in dag_config.nodes.iter() {
            for dep in node_config.deps.iter() {
                if dep == &node_config.name {
                    return Err(format!("{:?} depend itself", node_config.name));
                }
                if !self.nodes.contains_key(&dep.clone()) {
                    return Err(format!(
                        "{:?}'s dependency {:?} do not exist",
                        node_config.name, dep
                    ));
                }
                self.nodes
                    .get_mut(&node_config.name.clone())
                    .unwrap()
                    .prevs
                    .insert(dep.clone());
                self.nodes
                    .get_mut(&dep.clone())
                    .unwrap()
                    .nexts
                    .insert(dep.clone());
            }
        }

        Ok(())
    }

    async fn make_flow(&self, args: Arc<E>) -> Vec<NodeResult> {
        let leaf_nodes: HashSet<String> = self
            .nodes
            .values()
            .filter(|node| node.nexts.is_empty())
            .map(|node| node.node_config.name.clone())
            .collect();

        let mut dag_futures = HashMap::new();

        self.nodes.iter().for_each(|(node_name, _node)| {
            let entry = async { NodeResult::new() };
            dag_futures.insert(node_name.clone(), entry.boxed().shared());
        });

        for (node_name, node) in self.nodes.iter() {
            let deps: Vec<_> = self
                .nodes
                .get(node_name)
                .unwrap()
                .prevs
                .iter()
                .map(|dep| dag_futures.get(dep).unwrap().clone())
                .collect();

            let depss: FuturesUnordered<_> = self
                .nodes
                .get(node_name)
                .unwrap()
                .prevs
                .iter()
                .map(|dep| dag_futures.get(dep).unwrap().clone())
                .collect();

            let arg_ptr = Arc::clone(&args);
            let params_ptr = node.node_config.params.clone();
            let pre_fn = Arc::clone(&self.pre);
            let post_fn = Arc::clone(&self.post);
            let handle_fn = Arc::clone(self.node_mapping.get(&node.node_config.node).unwrap());
            *dag_futures.get_mut(node_name).unwrap() = join_all(deps)
                .then(|x| async move {
                    // let params = node_instance.deserialize(&params_ptr);
                    let prev_res = Arc::new(x.iter().fold(NodeResult::new(), |a, b| a.merge(b)));
                    // TODO: timeout
                    let pre_result: T = pre_fn(&arg_ptr, &prev_res);
                    let res = async {
                        handle_fn(&arg_ptr, prev_res.clone())
                    }.await;
                    post_fn(&arg_ptr, &prev_res, &pre_result);
                    res
                })
                .boxed()
                .shared();
        }

        async {
            let leaf_nodes: Vec<_> = leaf_nodes
                .iter()
                .map(|x| dag_futures.get(x).unwrap().clone())
                .collect();
            join_all(leaf_nodes).await
        }
        .await
    }
}


fn demo() {
    let dag = Flow::<i32, i32>::new().register("handle_wrapper", &handle_wrapper);
}