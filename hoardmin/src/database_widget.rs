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

#[derive(Debug, Clone)]
struct Collection {
    id: usize,
    name: String
}

#[derive(Debug,Clone)]
struct Index {
    id: usize,
    name: String,
    is_unique: bool,
    collection:String
}

#[derive(Debug, Clone)]
enum TreeItemPayload {
    Collection(Collection),
    Index(Index)
}

#[derive(Debug, Clone)]
struct TreeItem {
    payload: TreeItemPayload
}

// todo: maybe just remove TreeItem and use TreeItemPayload directly
impl Display for TreeItem {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.payload {
            TreeItemPayload::Collection(collection) => {
                write!(f, "{}", collection.name)
            }
            TreeItemPayload::Index(index) => {
                write!(f, "{}", index.name)
            }
        }
    }
}

pub struct DatabaseWidget {
    path: String,
    db: hoardbase::database::Database,
}

impl DatabaseWidget {
    pub fn new(path : &str) -> Self {
        let mut config = DatabaseConfig::new(path);
        config.trace(false);
        config.profile(false);
    
        let mut db = Database::open(&config).unwrap();

        let mut db_ui = DatabaseWidget {
            path: path.to_string(),
            db: db
        };

        return db_ui;    
    }

    pub fn run(&mut self) {
        let mut siv = cursive::default();

        let mut h_split = LinearLayout::new(Orientation::Horizontal);
        let mut tree_view = TreeView::new();

        let collections = self.db.list_collections();

        for collection in collections {
            let id = tree_view.insert_item( TreeItem{payload: TreeItemPayload::Collection(Collection{id:0, name: collection.0})} , Placement::LastChild, 0).unwrap();
        }



        tree_view.set_on_submit(|siv: &mut Cursive, row| {
            let value = siv.call_on_name("collections_view", move |tree: &mut TreeView<TreeItem>| (*tree.borrow_item(row).unwrap()).clone()).unwrap();
            

            println!("selected collection: {}", value);

            /*siv.add_layer(Dialog::around(TextView::new(value.unwrap())).title("Item submitted").button("Close", |s| {
                s.pop_layer();
            }));
    
            set_status(siv, row, "Submitted");*/

            
        });

        let mut left_panel = ResizedView::with_full_height(Panel::new(tree_view.with_name("collections_view").scrollable()).title("Collection"));
        left_panel.set_width(SizeConstraint::AtLeast(28));
        h_split.add_child(left_panel);
        let mut main_panel = LinearLayout::new(Orientation::Vertical);
        main_panel.add_child(h_split);
        main_panel.add_child(ResizedView::with_fixed_height(8, Panel::new(DebugView::new().scrollable()).title("Log")));
    
        siv.add_fullscreen_layer(main_panel);
        siv.run();
    }
}