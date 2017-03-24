#[macro_use]
extern crate serde_derive;
extern crate bincode;

pub mod btree;

#[cfg(test)]
mod tests {
    use btree::{BTree};
    use std::error::Error;
    use std::fs;
    
    #[test]
    /// Verify that we can create an empty tree, close it, and reopen
    /// it.
    fn open_empty_tree() {
        let tree_name = "open_empty_tree";
        { // create a btree and let it go out of scope
            match BTree::new(tree_name, 4) {
                Ok(_btree)   => assert!(true),
                Err(ioerror) => {
                    println!("failed to create btree: {}",
                             ioerror.description());
                    assert!(false);
                },
            }
        }
        { // open it again and verify that it is empty
            match BTree::open(tree_name) {
                Ok(_btree)   => assert!(true),
                Err(ioerror) => {
                    println!("failed to open btree: {}",
                             ioerror.description());
                    assert!(false);
                }
            }
        }
        fs::remove_file(tree_name);
    }
}
