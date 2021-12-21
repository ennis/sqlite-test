use crate::atom::Atom;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, Debug)]
enum PathNodeKind {
    Root,
    Part { parent: Arc<PathNode>, name: Atom },
}

// TODO they should probably be allocated in an object pool
#[derive(Debug)]
struct PathNode {
    kind: PathNodeKind,
}

impl PathNode {
    fn new_root() -> Arc<PathNode> {
        Arc::new(PathNode {
            kind: PathNodeKind::Root,
        })
    }

    /// Converts the path represented by this node into its string representation.
    fn to_string(&self) -> String {
        match &self.kind {
            PathNodeKind::Root => "".to_string(),
            PathNodeKind::Part { parent, name } => {
                let mut p = parent.to_string();
                p.push_str("/");
                p.push_str(name.as_ref());
                p
            }
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct PathKey {
    parent: usize, // pointer
    part: Atom,
}

lazy_static! {
    static ref ROOT_PATH_NODE: Arc<PathNode> = PathNode::new_root();
    static ref PATH_NODE_TABLE: DashMap<PathKey, Arc<PathNode>> = DashMap::new();
}

//fn root_path_node() -> &'static Arc<PathNode> {
//    &*ROOT_PATH_NODE
//}

//fn path_node_table() -> &'static DashMap<PathKey, Arc<PathNode>> {
//    &*PATH_NODE_TABLE
//}

//--------------------------------------------------------------------------------------------------

/// Paths of the form:
///
/// # Examples of paths
///
/// - `/network/node/param`: absolute path
///
#[derive(Clone, Debug)]
pub struct ModelPath {
    node: Arc<PathNode>,
}

impl From<Arc<PathNode>> for ModelPath {
    fn from(node: Arc<PathNode>) -> Self {
        ModelPath { node }
    }
}

impl PartialEq for ModelPath {
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.node) == Arc::as_ptr(&other.node)
    }
}

impl Eq for ModelPath {}

impl Hash for ModelPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(Arc::as_ptr(&self.node) as usize)
    }
}

impl ModelPath {
    /// Returns the path to the root object.
    pub fn root() -> ModelPath {
        ModelPath {
            node: ROOT_PATH_NODE.clone(),
        }
    }

    /// Concatenates.
    pub fn join(&self, part: impl Into<Atom>) -> ModelPath {
        let name = part.into();
        let node = PATH_NODE_TABLE
            .entry(PathKey {
                parent: Arc::as_ptr(&self.node) as usize,
                part: name.clone(),
            })
            .or_insert_with(|| {
                Arc::new(PathNode {
                    kind: PathNodeKind::Part {
                        parent: self.node.clone(),
                        name: name.clone(),
                    },
                })
            })
            .clone();

        ModelPath { node }
    }

    /// Relative path.
    pub fn is_relative(&self) -> bool {
        false
    }

    /// Whether this is an absolute path.
    pub fn is_absolute(&self) -> bool {
        true
    }

    /// Whether this is the root path.
    pub fn is_root(&self) -> bool {
        Arc::as_ptr(&self.node) == Arc::as_ptr(&*ROOT_PATH_NODE)
    }

    /// Returns the parent path.
    pub fn parent(&self) -> Option<ModelPath> {
        match self.node.kind {
            PathNodeKind::Root => None,
            PathNodeKind::Part { ref parent, .. } => Some(ModelPath {
                node: parent.clone(),
            }),
        }
    }

    /// Split last
    pub fn split_last(&self) -> Option<(ModelPath, Atom)> {
        match self.node.kind {
            PathNodeKind::Root => None,
            PathNodeKind::Part {
                ref parent,
                ref name,
            } => Some((
                ModelPath {
                    node: parent.clone(),
                },
                name.clone(),
            )),
        }
    }

    pub fn is_prefix(&self, other: &ModelPath) -> bool {
        let mut p = Some(other);
        while let Some(p) = p {
            if p == self {
                return true
            }
        }
        false
    }

    pub fn name(&self) -> Atom {
        match self.node.kind {
            PathNodeKind::Root => Atom::default(),
            PathNodeKind::Part { ref name, .. } => name.clone(),
        }
    }

    pub fn to_string(&self) -> String {
        match &self.node.kind {
            PathNodeKind::Root => "/".to_string(),
            _ => self.node.to_string(),
        }
    }

    pub fn parse(path: &str) -> ModelPath {
        if let Some((prefix, name)) = path.rsplit_once('/') {
            Self::parse(prefix).join(name)
        } else {
            Self::root()
        }
    }
}
