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
use crate::collection_list::CollectionListView;

pub struct DatabaseWidget {
    path: String,
    db: hoardbase::database::Database,
    siv: cursive::CursiveRunnable,
    collection_tree_view: CollectionListView,
}

impl DatabaseWidget {
    pub fn new(path : &str) -> Self {
        let mut config = DatabaseConfig::new(path);
        config.trace(false);
        config.profile(false);
    
        let mut db = Database::open(&config).unwrap();
        let mut siv = cursive::default();
        let mut collection_tree_view = CollectionListView::new();

        let collections = db.list_collections();

        for collection in collections {
            collection_tree_view.add_collection(&collection.0, &Vec::new());
        }
        let mut h_split = LinearLayout::new(Orientation::Horizontal);

        let mut left_panel = ResizedView::with_full_height(Panel::new(collection_tree_view.tree_view .with_name("tree").scrollable()).title("Collection"));
        left_panel.set_width(SizeConstraint::AtLeast(28));
        h_split.add_child(left_panel);
        let mut main_panel = LinearLayout::new(Orientation::Vertical);
        main_panel.add_child(h_split);
        main_panel.add_child(ResizedView::with_fixed_height(8, Panel::new(DebugView::new().scrollable()).title("Log")));
    
        siv.add_fullscreen_layer(main_panel);

        return DatabaseWidget {
            path: path.to_string(),
            db: db,
            siv: siv,
            collection_tree_view: collection_tree_view,
        };
     
    }

    pub fn run(&mut self) {

        self.siv.run();
    }
}