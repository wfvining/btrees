use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::io::{Seek, SeekFrom, Write, Read, BufReader};

use bincode::{serialize, deserialize,
              deserialize_from, serialized_size,
              SizeLimit, Infinite};

type BTreeData = Option<(i64,u64)>;

pub enum BTreeError {
    Exists,
    NotFound,
    IO(IOError),
}

/// Representation of a node in the BTree. This derives Encodable and
/// Decodable so we can just read and write it from the file.
#[derive(Serialize, Deserialize)]
struct BTreeNode {
    children: Vec<Option<u64>>, // Offsets of this node's children
    parent:   u64,
    data:     Vec<BTreeData>,
}

impl BTreeNode {
    fn load_node(tree: &mut BTree, offset: u64)
                 -> Result<BTreeNode, BTreeError> {
        // This is actually somewhat tricky... Since I don't tnink
        // there is a simple way to determine the serialized size of a
        // BTreeNode. I think I should probaby use a BufferedReader
        // and the buncode::deserialize_from() function to read. A
        // BufferedReader will have bad performance; however, since it
        // invalidates it's cache on every seek. If I create the
        // reader with a very small buffer size, that might make the
        // performance less bad...
        //
        // An alternative solution may be to simply prepend the size
        // of the root block to the begining of the file. Then when
        // the BTree is opened we can read a few bytes, get the size,
        // and smoothly read nodes in as they are required. This comes
        // at no significant overhead, as the size needs only be
        // written once and may never change.
        //
        // There is also a `serialized_size` function that could be
        // used here. The drawback to that is requiring the creation
        // of an unnecessary BTreeNode. Also leads to a "bootstrap"
        // problem: what do we do for reading the root node? still
        // don't know how big the read should be since we don't
        // neceessarily have details about the tree until it is read.
        //
        // For now it looks like the best answer is to store the size
        // in the beginning of the file read it on open and just use
        // it here.
        //
        // Although judging by what is happening below this isn't
        // quite so simple...

        // try to seek
        match tree.file.seek(SeekFrom::Start(offset)) {
            Ok(_) => {  
              let mut encoded_node = vec![0; tree.node_length as usize];
                match tree.file.read_exact(&mut encoded_node[..]) {
                    Ok(_) => Ok(deserialize(&encoded_node[..])
                                .unwrap()),
                    Err(ioerror)     => Err(BTreeError::IO(ioerror)),
                }
            },
            Err(ioerror) => Err(BTreeError::IO(ioerror))
        }
        
    }

    fn store_node(&self, tree: &mut BTree, offset: u64)
                  -> Result<(), IOError> {
        let encoded_node: Vec<u8> = serialize(self, Infinite)
            .unwrap(); // ?? Should we expct serialize to always succeed
        match tree.file.seek(SeekFrom::Start(offset)) {
            Ok(_)    => tree.file.write_all(&encoded_node[..]),
            Err(why) => Err(why),
        }
    }

    fn new(k: usize, parent: u64) -> BTreeNode {
        BTreeNode {
            parent:   parent,
            children: vec![None; 2*k],
            data:     vec![None; 2*k + 1],
        }
    }

    /// Test whether a node is a leaf.
    ///
    /// Returns true if the node has no children, otherwise returns
    /// false.
    fn is_leaf(&self) -> bool {
        match self.children[0] {
            Some(_) => false,
            None    => true,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct BTreeHeader(u64, usize);

/// Simple first cut, map uuid keys to globs of text, no generics.
pub struct BTree {
    root: BTreeNode,
    file: File,       // The file that the btree is stored in
    node_length: u64, // The size of a serialized BTreeNode
    k: usize,
}

impl BTree {
    /// Create a new BTree. If a BTree already exists on disk with the
    /// same name the this function will fail. The BTree must not
    /// already exist.
    ///
    /// # Arguments
    ///
    /// * `name` - A string slice that holds the name of the BTree.
    ///            The name is the name of the file that holds the
    ///            data.
    /// * `k`    - The BTree will have 2*k children per node and
    ///            2*k + 1 data elements per node.
    pub fn new(name: &str, k: usize) -> Result<BTree, IOError> {
        let btree_path = Path::new(name);
        let mut file = match OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(btree_path) {
                Ok(file) => file,
                Err(why) => return Err(why),
            };
        let root_node = BTreeNode::new(k, 0);
        let header = BTreeHeader(serialized_size(&root_node), k);
        let serialized_header = serialize(&header, Infinite)
            .unwrap();
        match (&mut file).write_all(&serialized_header) {
            Ok(_)        =>{
                let mut tree = BTree {
                    file: file,
                    node_length: serialized_size(&BTreeNode::new(k, 0)),
                    root: BTreeNode::new(k, 0),
                    k: k };
                // panic if it couldn't be stored.
                root_node.store_node( &mut tree,
                                       serialized_size(&header)).unwrap();
                Ok(tree)
            },
            Err(ioerror) => Err(ioerror),
        }
    }

    /// Open an existing BTree.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the BTree, as described in
    ///            `BTree::new()`
    pub fn open(name: &str) -> Result<BTree, IOError> {
        let btree_path = Path::new(name);
        let mut file = match OpenOptions::new()
            .write(true)
            .read(true)
            .open(btree_path) {
                Ok(file) => file,
                Err(why) => return Err(why),
            };
        // This may not be the most efficient way to do this, but it
        // is probably the least amount of code. 32 bytes is probably
        // big enough to read the whole header, but it is just a
        // guess.
        let header: BTreeHeader;
        {
            let mut reader = BufReader::new(&mut file);
            header =
                deserialize_from(&mut reader, Infinite).unwrap();
        }
        let BTreeHeader(node_length, k) = header;
        let mut tree = BTree { file: file,
                               node_length: node_length, k: k,
                               root: BTreeNode::new(k, 0)};
        match BTreeNode::load_node(&mut tree, serialized_size(&header)) {
            Ok(root)                     =>
                Ok(BTree { root: root, .. tree }),
            Err(BTreeError::IO(ioerror)) =>
                Err(ioerror),
            Err(_)                       =>
                Err(IOError::new(IOErrorKind::InvalidData,
                                 "Failed to decode node")),
            
        }
    }

    /// insert a node into the BTree 
    pub fn insert(self, key: i64, data: BTreeData) -> Result<(), BTreeError> {
        // For the first cut, I will assume the data is all of a fixed
        // size, thus We can just do insert/delete/update without much
        // difficulty.
        //
        // A note on pointer to children: pointers to children are
        // simply offsets into the file. This can spare us the
        // difficulty of needing to re-write large chunks of the tree
        // whenever we do a delete (such as if the tree is mapped to a
        // flat array). Instead we just change the "pointers."
        Err(BTreeError::NotFound)
    }

    fn search(&mut self, node: BTreeNode, key: i64)
              -> Result<BTreeData, BTreeError> {
        for (i, data) in node.data.enumerate() {
            match data {
                Some((k, v)) => {
                    if key == k {
                        return Ok(data);
                    }
                    else if key < k {
                        break; // ???
                    }
                },
                None => break,
            }
        }
        if node.is_leaf() { Ok(None) }
        else {
            // load the child node at i and serch() in it for the key
            // Can I do recursion with a mutable reference? Probably not.
        }
    }
    
    /// Find a key in the B-Tree.  I believe self must be mutable
    /// because reading from a file mutates the handle (ie. the read
    /// pointer moves).
    pub fn lookup(&mut self, key: i64) -> Result<BTreeData, BTreeError> {
        self.search(&mut self.root, key)
    }

    pub fn delete(self, key: i64) -> Result<(), BTreeError> {
        Err(BTreeError::NotFound)
    }
}
