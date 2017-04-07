/// Second attempt at file-backed B-Trees. I am finding it difficult
/// to implement functions like `search()` because the B-Tree has to
/// be borrowed mutably (since even "read only" operations like
/// `search` might require reading from the file which mutates the
/// file handle).
///
/// I need to re-think the "normal" algorithms for these operations.
/// I By making the operations only work on a single node that has
/// already been loaded into memory. The return value needs to be more
/// complex--indicating success, failure, and "load child". Insert
/// might be more complicated than that... but hopefully will fit in
/// nicely.

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::io::{Seek, SeekFrom, Read, Write};
use std::fs::{File, OpenOptions};
use std::path::Path;

use serde;

use bincode::{
    serialize_into,
    deserialize_from,
    Infinite,
};

/// The Storage trait provides functions needed to put and get btrees
/// from some (possibly persistent) storage medium.
pub trait Storage {
    /// Put data into storage at the given offset.
    fn put(&mut self, data: &[u8], offset: u64) -> Result<(), IOError>;

    /// Append data to the storage location returning the offset
    /// where the write began.
    fn append(&mut self, data: &[u8]) -> Result<u64, IOError> {
        Err(IOError::new(IOErrorKind::Other, "not implemented"))
    }

    /// Get data from storage at the given offset. Attempts to fill
    /// the entire slice refered to by buffer, if it cannot (ie. not
    /// enough data) then the result will be an error.
    ///
    /// # Arguments
    ///
    /// * `offset` - the "address" to begin reading at
    ///
    /// * `buffer` - the buffer to fill with the data. This should
    ///              work like the buffer parameter to
    ///              `Read::read_exact()`
    fn get(&mut self, offset: u64, buffer: &mut [u8]) -> Result<(), IOError>;
}

impl Storage for File {
    fn put(&mut self, data: &[u8], offset: u64) -> Result<(), IOError> {
        self.seek(SeekFrom::Start(offset)).map_err(|err| err)
            .and_then(|at| {
                if at != offset {
                    Err(IOError::new(IOErrorKind::Other, "failed to seek"))
                }
                else {
                    self.write_all(data)
                }
            })
    }

    fn append(&mut self, data: &[u8]) -> Result<u64, IOError> {
        self.seek(SeekFrom::End(0)).map_err(|err| err)
            .and_then(|at| {
                match self.write_all(data) {
                    Ok(_)    => Ok(at),
                    Err(err) => Err(err),
                }
            })
    }

    fn get(&mut self, offset: u64, buffer: &mut [u8]) -> Result<(), IOError> {
        self.seek(SeekFrom::Start(offset)).map_err(|err| err)
            .and_then(|at| {
                if at != offset {
                    Err(IOError::new(IOErrorKind::Other, "failed to seek"))
                }
                else {
                    self.read_exact(buffer)
                }
            })
    }
}

/// A node needs to have m data elements and m+1 children pointers.
///
/// NOTE: To make this work D needs to have a fixed size when it is
/// serialized. At the very least it needs a fixed upper bound so we
/// can add padding. Alternatively we can use an append-only scheme to
/// allow for arbitrarily large nodes (the `Storage` trait may or may
/// not complicate this, let's say it doesn't...). This requires some
/// kind of garbage collection facility such as that employed by
/// CouchDB (not space efficient) or log-structured file systems. LFS
/// uses segments as the structure that is reclaimed by garbage
/// collection. 
///
/// Compatction is probably the easiest to implement, but I don't like
/// the space issues. Log structured is frustrating because it
/// requires rewriting every node in the path from the root to the
/// node we are updating (I guess that is just O(log N) writes, but
/// still). How large are all the writes? $2m+1 * S + c$ where m is
/// the degree of the B-Tree and S the size of the data elements
/// (O(m), but with a potentially large constant factor).
///
/// ## How does append only play with the Storage trait?
///
/// We can no longer know the offset that a particular block will be
/// written at. Instead I guess we write append-only (Could use a
/// BufWriter if safety is not absolutely necessary) and return the
/// offset where the node was written.
#[derive(Serialize, Deserialize)]
struct Node<K, V> {
    num_children: usize,
    parent:       u64,
    children:     Vec<u64>,
    data:         Vec<(K, V)>,
}

impl<K, V> Node<K, V>
    where K: serde::Deserialize + serde::Serialize,
          V: serde::Deserialize + serde::Serialize {

    /// Using Read + Seek here makes the implementation of load/store
    /// significantly easier because we can use
    /// deserialize_/serialize_from. Rather than out own Storage trait
    /// we just require Read and Seek
    fn load<R: Read + Seek>(from: &mut R, at: u64)
                            -> Result<Node<K, V>, IOError> {
        // can't just do a read... need to read at the specified offset.
        try!(from.seek(SeekFrom::Start(at)));
        deserialize_from(from, Infinite)
            .map_err(|_| IOError::new(IOErrorKind::Other,
                                      "failed to deserialize node"))
    }
    
    fn store<W: Write + Seek>(&self, to: &mut W)
                                   -> Result<u64, IOError> {
        let offset = try!(to.seek(SeekFrom::End(0)));
        serialize_into(to, self, Infinite)
            .map_err(|_|
                     IOError::new(IOErrorKind::Other,
                                  "failed to serialize node"))
            .map(|_| offset)
    }

    fn new() -> Node<K,V> {
        Node { num_children: 0, children: vec![], data: vec![], parent: 0 }
    }
}

// Question: How do we know what offset the root node starts at?  It
// will always be the last thing in the file, but its size will
// change, so we don't know how far to rewind from the end of the file
// (or storage or whatever).
//
// We can write the offset to the end of the file immediately
// following the root node. 
struct BTree<K,V> {
    storage:     File,
    root_offset: u64,
    root:        Node<K, V>,
    degree:      usize,
}

enum SearchResult<D> { 
    Found(D),
    SearchChild(u64),
    NotFound
}

impl<K, V> BTree<K, V>
    where K: serde::Serialize + serde::Deserialize,
          V: serde::Serialize + serde::Deserialize {

    /// Create a new BTree
    pub fn new(name: &str, degree: usize) -> Result<BTree<K, V>, IOError> {
        let btree_path = Path::new(name);
        let mut file = try!(OpenOptions::new()
                            .write(true)
                            .read(true)
                            .open(btree_path));
        let root: Node<K, V> = Node::new();
        let root_offset = try!(root.store(&mut file));
        // Store the location of the root node at the end of the file.
        // This isn't really necessary for an empty tree, but once the
        // root moves, and changes size we will need the "footer"
        // locate the root node.
        serialize_into(&mut file, &root_offset, Infinite)
            .map_err(|err| IOError::new(IOErrorKind::Other,
                                        "failed to serialize tree footer"))
            .map(|_|
                 BTree { storage: file,
                         root_offset: root_offset,
                         root: root,
                         degree: degree }
            )
    }

    pub fn get(&mut self, key: K) -> Result<Option<V>, IOError> {
        let mut search_done = false;
        while !search_done {
            let mut ref node = self.root;
            match
        }
        Ok(None)
    }
}
