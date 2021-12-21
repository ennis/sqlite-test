mod atom;
mod path;

use crate::atom::Atom;
use crate::path::ModelPath;
use anyhow::{anyhow, Result};
use rpds::HashTrieMap;
use rpds::Vector;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OpenFlags};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

struct SerializationContext {
    path: ModelPath,
}

#[derive(Clone, Debug)]
pub struct NamedObject {
    id: i64,
    path: ModelPath,
}

impl NamedObject {
    /// Writes this node into an open database.
    pub fn write(&self, conn: &rusqlite::Connection) -> Result<()> {
        let name = self.path.name().to_string();
        let path = self.path.to_string();
        conn.execute(
            "update named_objects set name=?1, path=?2 where rowid=?3",
            params![name, path, self.id],
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    base: NamedObject,
    children: HashTrieMap<Atom, Node>,
}

impl Node {
    /// Writes this node into an open database.
    pub fn write(&self, conn: &rusqlite::Connection) -> Result<()> {
        // recursively write this node and children
        self.base.write(conn)?;
        for (_, n) in self.children.iter() {
            n.write(conn);
        }
        Ok(())
    }

    pub fn find_child(&self, name: &Atom) -> Option<&Node> {
        self.children.get(name)
    }

    pub fn find_child_mut(&mut self, name: &Atom) -> Option<&mut Node> {
        self.children.get_mut(name)
    }

    fn add_child(&mut self, node: Node) {
        self.children.insert_mut(node.base.path.name(), node);
    }

    pub fn dump(&self, indent: usize) {
        let name = self.base.path.name();

        println!(
            "{:indent$}{}",
            "",
            if name.is_empty() { "<root>" } else { &name },
            indent = indent
        );

        {
            let indent = indent + 2;
            for n in self.children.values() {
                n.dump(indent);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShareGroup {}

#[derive(Clone, Debug)]
pub struct Document {
    root: Node,
    share_groups: Vector<ShareGroup>,
}

fn setup_schema(conn: &rusqlite::Connection) -> Result<()> {
    // named_objects: {obj_id} -> name, parent_obj_id      (name, parent must be unique)
    // share_groups: {share_id, obj_id}
    conn.execute(
        "create table if not exists named_objects \
             (id      integer primary key, \
              name    text not null, \
              path    text unique not null, \
              parent  text)",
        [],
    )?;

    conn.execute(
        "create table if not exists share_groups \
                            (share_id     integer,\
                             obj_id       integer,\
                             primary key (share_id, obj_id))",
        [],
    )?;

    // root node
    conn.execute(
        "insert or ignore into named_objects (name, path, parent) values ('','',null)",
        [],
    );

    Ok(())
}

impl Document {
    /// Opens a document.
    pub fn open(conn: &Connection) -> Result<Document> {
        setup_schema(conn)?;
        //let root_id : i64 = conn.query_row("select rowid from named_objects where path is null",[], |row| row.get(0))?;

        // load nodes
        let mut stmt = conn.prepare("select rowid, path from named_objects")?;
        let mut node_rows = stmt.query([])?;
        let mut nodes = Vec::new();

        while let Some(row) = node_rows.next()? {
            let id: i64 = row.get(0)?;
            let path = match row.get_ref(1)? {
                ValueRef::Null => ModelPath::root(),
                e @ ValueRef::Text(text) => ModelPath::parse(e.as_str()?),
                _ => {
                    anyhow::bail!("invalid column type")
                }
            };

            nodes.push((
                path.to_string(),
                Node {
                    base: NamedObject { id, path },
                    children: Default::default(),
                },
            ));
        }

        nodes.sort_by(|(a, _), (b, _)| a.cmp(b));
        //eprintln!("sorted nodes: {:#?}", nodes);

        let mut document = Document {
            root: Node {
                base: NamedObject {
                    id: 0,
                    path: ModelPath::root(),
                },
                children: Default::default(),
            },
            share_groups: Default::default(),
        };

        // FIXME this is not very efficient
        for (_, n) in nodes.iter() {
            if n.base.path.is_root() {
                document.root = n.clone();
            } else {
                let mut parent = document
                    .find_node_mut(&n.base.path.parent().unwrap())
                    .unwrap();
                parent.add_child(n.clone());
            }
        }

        /*// establish connections
        for (path, node) in nodes.iter() {
            if let Some(parent) = node.borrow().base.path.parent() {
                // TODO report error on non existent parent
                let parent_node = nodes.get(&parent).unwrap();
                parent_node.borrow_mut().add_child(node.borrow().clone());
            }
        }

        let root = nodes
            .get(&ModelPath::root())
            .ok_or(anyhow!("root node not found"))?
            .borrow()
            .clone();*/

        Ok(document)
    }

    pub fn write(&self, conn: &rusqlite::Connection) -> Result<()> {
        // recursively write nodes
        self.root.write(conn)?;
        Ok(())
    }

    pub fn find_node(&self, path: &ModelPath) -> Option<&Node> {
        match path.split_last() {
            None => Some(&self.root),
            Some((prefix, last)) => {
                let parent = self.find_node(&prefix)?;
                parent.find_child(&last)
            }
        }
    }

    pub fn find_node_mut(&mut self, path: &ModelPath) -> Option<&mut Node> {
        match path.split_last() {
            None => Some(&mut self.root),
            Some((prefix, last)) => {
                let parent = self.find_node_mut(&prefix)?;
                parent.find_child_mut(&last)
            }
        }
    }

    fn insert_node(&mut self, node: Node) {
        // to reconstruct: sort nodes by path, lexicographically?

        // /a
        // /a/b
        // /a/b/c

        let mut parent = self
            .find_node_mut(&node.base.path.parent().unwrap())
            .unwrap();
    }

    pub fn create_node(&mut self, conn: &rusqlite::Connection, path: ModelPath) -> Result<Node> {
        // insert the node in the DB first, this will take care of ensuring that the path is unique
        let path_str = path.to_string();
        let name = path.name().to_string();
        conn.execute(
            "insert into named_objects (name, path, parent) values (?1,?2,null)",
            params![name, path_str],
        )?;
        let id = conn.last_insert_rowid();

        let n = Node {
            base: NamedObject { id, path },
            children: Default::default(),
        };

        // TODO: should insert the node in the document structure
        Ok(n)
    }

    pub fn dump(&self) {
        println!("Document");
        self.root.dump(0);
    }
}

fn main() -> Result<()> {
    let connection = rusqlite::Connection::open("test.artifice")?;

    let mut doc = Document::open(&connection)?;
    //eprintln!("{:#?}", doc);
    //let mut doc = Document::init(&connection)?;
    doc.create_node(&connection, ModelPath::parse("/node_a"));
    doc.create_node(&connection, ModelPath::parse("/node_a/node_b"));
    doc.create_node(&connection, ModelPath::parse("/node_a/node_b/node_c"));
    doc.create_node(&connection, ModelPath::parse("/node_a/node_b/node_d"));
    doc.create_node(&connection, ModelPath::parse("/node_a/node_b/node_e"));
    doc.create_node(&connection, ModelPath::parse("/node_a/node_f"));
    doc.create_node(&connection, ModelPath::parse("/node_g"));

    doc.dump();

    Ok(())
}
