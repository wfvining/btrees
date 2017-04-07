#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate serde;

pub mod btree;

#[cfg(test)]
mod tests {
    use btree::{BTree};
    use std::error::Error;
    use std::fs;
    
    #[test]
    fn it_works() {
    }
}
