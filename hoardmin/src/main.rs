extern crate cursive_table_view;
extern crate cursive_tree_view;

// Crate Dependencies ---------------------------------------------------------
use cursive;


use std::cmp::Ordering;

use clap::{App, Arg, SubCommand};

use hoardbase::base::CollectionTrait;

mod database_widget;
use crate::database_widget::DatabaseWidget;
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum BasicColumn {
    Name,
    Count,
    Rate,
}

impl BasicColumn {
    fn as_str(&self) -> &str {
        match *self {
            BasicColumn::Name => "Name",
            BasicColumn::Count => "Count",
            BasicColumn::Rate => "Rate",
        }
    }
}
/*
#[derive(Clone, Debug)]
struct Foo {
    name: String,
    count: usize,
    rate: usize,
}

impl TableViewItem<BasicColumn> for Foo {
    fn to_column(&self, column: BasicColumn) -> String {
        match column {
            BasicColumn::Name => self.name.to_string(),
            BasicColumn::Count => format!("{}", self.count),
            BasicColumn::Rate => format!("{}", self.rate),
        }
    }

    fn cmp(&self, other: &Self, column: BasicColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            BasicColumn::Name => self.name.cmp(&other.name),
            BasicColumn::Count => self.count.cmp(&other.count),
            BasicColumn::Rate => self.rate.cmp(&other.rate),
        }
    }
}
*/
fn main() {
    let matches = App::new("Hoardmin").version("1.0").author("Shi Yan.").about("a database").arg(Arg::with_name("file").required(true).takes_value(true)).get_matches();

    println!("{:?}", matches.args.get("file").unwrap().vals[0]);
    println!("{:?}", env!("CARGO_PKG_VERSION"));
    //println!("{:?}", env!("GIT_HASH"));

    let mut db_ui  = database_widget::DatabaseWidget::new("test.db");

    db_ui.run();

    /*let mut siv = cursive::default();

    // Tree -------------------------------------------------------------------
    let mut tree = TreeView::new();
    tree.insert_item("tree_view".to_string(), Placement::LastChild, 0);

    tree.insert_item("src".to_string(), Placement::LastChild, 0);
    tree.insert_item("tree_list".to_string(), Placement::LastChild, 1);
    tree.insert_item("mod.rs".to_string(), Placement::LastChild, 2);

    tree.insert_item("2b".to_string(), Placement::LastChild, 0);
    tree.insert_item("3b".to_string(), Placement::LastChild, 4);
    tree.insert_item("4b".to_string(), Placement::LastChild, 5);

    tree.insert_item("yet".to_string(), Placement::After, 0);
    tree.insert_item("another".to_string(), Placement::After, 0);
    tree.insert_item("tree".to_string(), Placement::After, 0);
    tree.insert_item("view".to_string(), Placement::After, 0);
    tree.insert_item("item".to_string(), Placement::After, 0);
    tree.insert_item("last".to_string(), Placement::After, 0);

    // Callbacks --------------------------------------------------------------
    tree.set_on_submit(|siv: &mut Cursive, row| {
        let value = siv.call_on_name("tree", move |tree: &mut TreeView<String>| tree.borrow_item(row).unwrap().to_string());

        siv.add_layer(Dialog::around(TextView::new(value.unwrap())).title("Item submitted").button("Close", |s| {
            s.pop_layer();
        }));

        set_status(siv, row, "Submitted");
    });

    tree.set_on_select(|siv: &mut Cursive, row| {
        set_status(siv, row, "Selected");
    });

    tree.set_on_collapse(|siv: &mut Cursive, row, collpased, _| {
        if collpased {
            set_status(siv, row, "Collpased");
        } else {
            set_status(siv, row, "Unfolded");
        }
    });

    // Controls ---------------------------------------------------------------
    fn insert_row(s: &mut Cursive, text: &str, placement: Placement) {
        let row = s.call_on_name("tree", move |tree: &mut TreeView<String>| {
            let row = tree.row().unwrap_or(0);
            tree.insert_item(text.to_string(), placement, row).unwrap_or(0)
        });
        set_status(s, row.unwrap(), "Row inserted");
    }

    siv.add_global_callback('b', |s| insert_row(s, "Before", Placement::Before));
    siv.add_global_callback('a', |s| insert_row(s, "After", Placement::After));
    siv.add_global_callback('p', |s| insert_row(s, "Parent", Placement::Parent));
    siv.add_global_callback('f', |s| insert_row(s, "FirstChild", Placement::FirstChild));
    siv.add_global_callback('l', |s| insert_row(s, "LastChild", Placement::LastChild));

    siv.add_global_callback('r', |s| {
        s.call_on_name("tree", move |tree: &mut TreeView<String>| {
            if let Some(row) = tree.row() {
                tree.remove_item(row);
            }
        });
    });

    siv.add_global_callback('h', |s| {
        s.call_on_name("tree", move |tree: &mut TreeView<String>| {
            if let Some(row) = tree.row() {
                tree.remove_children(row);
            }
        });
    });

    siv.add_global_callback('e', |s| {
        s.call_on_name("tree", move |tree: &mut TreeView<String>| {
            if let Some(row) = tree.row() {
                tree.extract_item(row);
            }
        });
    });

    siv.add_global_callback('c', |s| {
        s.call_on_name("tree", move |tree: &mut TreeView<String>| {
            tree.clear();
        });
    });

    let mut h_split = LinearLayout::new(Orientation::Horizontal);
    let mut left_panel = ResizedView::with_full_height(Panel::new(tree.with_name("tree").scrollable()).title("Collection"));
    left_panel.set_width(SizeConstraint::AtLeast(28));
    h_split.add_child(left_panel);

    let mut table = TableView::<Foo, BasicColumn>::new()
        .column(BasicColumn::Name, "Name", |c| c.width_percent(20))
        .column(BasicColumn::Count, "Count", |c| c.align(HAlign::Center))
        .column(BasicColumn::Rate, "Rate", |c| c.ordering(Ordering::Greater).align(HAlign::Right).width_percent(20));
    let mut items = Vec::new();
    for i in 0..50 {
        items.push(Foo { name: format!("Name {}", i), count: 23, rate: 2 });
    }

    table.set_items(items);

    table.set_on_sort(|siv: &mut Cursive, column: BasicColumn, order: Ordering| {
        siv.add_layer(Dialog::around(TextView::new(format!("{} / {:?}", column.as_str(), order))).title("Sorted by").button("Close", |s| {
            s.pop_layer();
        }));
    });

    table.set_on_submit(|siv: &mut Cursive, row: usize, index: usize| {
        let value = siv.call_on_name("table", move |table: &mut TableView<Foo, BasicColumn>| format!("{:?}", table.borrow_item(index).unwrap())).unwrap();

        siv.add_layer(Dialog::around(TextView::new(value)).title(format!("Removing row # {}", row)).button("Close", move |s| {
            s.call_on_name("table", |table: &mut TableView<Foo, BasicColumn>| {
                table.remove_item(index);
            });
            s.pop_layer();
        }));
    });

    let mut toolbar = LinearLayout::new(Orientation::Horizontal);
    toolbar.add_child(Button::new("Prev Page", |s| s.quit()));
    toolbar.add_child(Button::new("Next Page", |s| s.quit()));
    toolbar.add_child(ResizedView::with_full_width(DummyView {}));
    toolbar.add_child(TextView::new("[Page 10]"));
    toolbar.add_child(TextView::new("[Count 342]"));

    //  siv.add_layer(Dialog::around(table.with_name("table").min_size((50, 20))).title("Table View"));
    let mut dh_split = LinearLayout::new(Orientation::Vertical);

    dh_split.add_child(ResizedView::with_fixed_height(8, Panel::new(ResizedView::with_full_width(TextArea::new())).with_name("Command")));
    dh_split.add_child(ResizedView::with_full_width(toolbar));
    dh_split.add_child(ResizedView::with_full_height(Panel::new(table.with_name("table").min_size((50, 20)))));

    let mut panel = TabPanel::new().with_tab(TextView::new("This is the first view!").with_name("First")).with_tab(NamedView::new("second", dh_split));

    let mut right_panel = ResizedView::with_full_height(panel);
    right_panel.set_width(SizeConstraint::Full);
    h_split.add_child(right_panel);

    let mut main_panel = LinearLayout::new(Orientation::Vertical);
    main_panel.add_child(h_split);
    main_panel.add_child(ResizedView::with_fixed_height(8, Panel::new(DebugView::new().scrollable()).title("Log")));

    siv.add_fullscreen_layer(main_panel);
    fn set_status(siv: &mut Cursive, row: usize, text: &str) {
        let value = siv.call_on_name("tree", move |tree: &mut TreeView<String>| tree.borrow_item(row).map(|s| s.to_string()).unwrap_or_else(|| "".to_string()));

        siv.call_on_name("status", move |view: &mut TextView| {
            view.set_content(format!("Last action: {} row #{} \"{}\"", text, row, value.unwrap()));
        });
    }

    fn edit_submitted(s: &mut Cursive, name: &str) {}

    siv.run();*/
}
