use std::collections::HashMap;
use cursive_tree_view::{Placement, TreeView};
use std::fmt::Display;

#[derive(Debug)]
struct Index {
    name: String,
    is_unique: bool,
    id: usize
}

impl Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

pub struct CollectionListView {
    collections: HashMap<usize, Collection>,
    pub tree_view: TreeView<String>,
}



struct Collection {
    name: String,
    indices: HashMap<usize, Index>,
    id: usize
}


impl CollectionListView {

    pub fn new() -> Self {
        return CollectionListView {
            collections: HashMap::new(),
            tree_view: TreeView::new(),
        };
    }

    pub fn add_collection(&mut self, collection_name: &str, indices: &Vec<(String, bool)>) {
        let mut collection = Collection {
            name: collection_name.to_string(),
            indices: HashMap::new(),
            id: 0
        };

        let col_row = self.tree_view.insert_item(collection.name.clone(), Placement::LastChild, 0).unwrap();

        for (index_name, is_unique) in indices {
            let mut index = Index {
                name: index_name.clone(),
                is_unique: *is_unique,
                id: 0
            };
            
            let index_id = self.tree_view.insert_item(index_name.to_string(), Placement::LastChild, col_row).unwrap();
            index.id = index_id;
            collection.indices.insert(index_id, index);
        }

        collection.id = col_row;
        self.collections.insert(col_row, collection);
    }
}