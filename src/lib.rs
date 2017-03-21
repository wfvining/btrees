#[macro_use]
extern crate serde_derive;
extern crate bincode;

pub mod btree;

#[cfg(test)]
mod tests {
    use btree::{BTree};
    use std::error::Error;
    
    #[test]
    /// Verify that we can create an empty tree, close it, and reopen
    /// it.
    fn open_empty_tree() {
        { // create a btree and let it go out of scope
            match BTree::new("it_works", 4) {
                Ok(_btree)   => assert!(true),
                Err(ioerror) => {
                    println!("failed to create btree: {}",
                             ioerror.description());
                    assert!(false);
                },
            }
        }
        { // open it again and verify that it is empty
            match BTree::open("it_works") {
                Ok(_btree)   => assert!(true),
                Err(ioerror) => {
                    println!("failed to open btree: {}",
                             ioerror.description());
                    assert!(false);
                }
            }
        }
    }
}
