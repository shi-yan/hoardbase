use hoardbase::database::{Database, DatabaseConfig};
// External Dependencies ------------------------------------------------------
use cursive::align::HAlign;
use cursive::direction::Orientation;
use cursive::traits::*;
use cursive::view::SizeConstraint;
use cursive::views::Button;
use cursive::views::DebugView;
use cursive::views::EditView;
use cursive::views::NamedView;
use cursive::views::TextArea;
use cursive::views::{Dialog, DummyView, LinearLayout, Panel, ResizedView, TextView};
use cursive::Cursive;
use cursive_tabs::TabPanel;
// Modules --------------------------------------------------------------------
use cursive_table_view::{TableView, TableViewItem};
use cursive_tree_view::{Placement, TreeView};

use std::collections::HashMap;
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

struct Collection {
    name: String,
    indices: HashMap<usize, Index>,
    id: usize
}

pub struct DatabaseWidget {
    path: String,
    db: hoardbase::database::Database,
    collections: Vec<Collection>,
}

impl DatabaseWidget {
    pub fn new(path : &str) -> Self {
        let mut config = DatabaseConfig::new(path);
        config.trace(false);
        config.profile(false);
    
        let mut db = Database::open(&config).unwrap();

        let mut db_ui = DatabaseWidget {
            path: path.to_string(),
            db: db,
            collections: Vec::new(),
        };

        let collections = db_ui.db.list_collections();

        for collection in collections {
            let mut col = Collection {
                name: collection.0,
                indices: HashMap::new(),
                id: 0
            };

            /*for (index_name, is_unique) in indices {
                let mut index = Index {
                    name: index_name.clone(),
                    is_unique: *is_unique,
                    id: 0
                };
                
                col.indices.insert(index_id, index);
            }*/
            db_ui.collections.push(col);
            
        }



        return db_ui;






    
    }

    pub fn run(&mut self) {
        let mut siv = cursive::default();

        let mut h_split = LinearLayout::new(Orientation::Horizontal);
        let mut tree_view = TreeView::new();
        for i in 0..self.collections.len() {
            let id = tree_view.insert_item( self.collections[i].name.clone(), Placement::LastChild, 0).unwrap();
            self.collections[i].id = id;
        }

        let mut left_panel = ResizedView::with_full_height(Panel::new(tree_view.with_name("collections_view").scrollable()).title("Collection"));
        left_panel.set_width(SizeConstraint::AtLeast(28));
        h_split.add_child(left_panel);
        let mut main_panel = LinearLayout::new(Orientation::Vertical);
        main_panel.add_child(h_split);
        main_panel.add_child(ResizedView::with_fixed_height(8, Panel::new(DebugView::new().scrollable()).title("Log")));
    
        siv.add_fullscreen_layer(main_panel);

        let collections = &self.collections;

        tree_view.set_on_submit(move |siv: &mut Cursive, row| {
            //let value = siv.call_on_name("collections_view", move |tree: &mut TreeView<String>| tree.borrow_item(row).unwrap().to_string());
    
            /*siv.add_layer(Dialog::around(TextView::new(value.unwrap())).title("Item submitted").button("Close", |s| {
                s.pop_layer();
            }));
    
            set_status(siv, row, "Submitted");*/

            for i in 0..collections.len() {
                if collections[i].id == row {
                    println!("select collection {}", collections[i].name);
                }
            }
        });


        siv.run();
    }
}