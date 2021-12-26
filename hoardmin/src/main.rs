extern crate cursive_tree_view;
extern crate cursive_table_view;


   
// Crate Dependencies ---------------------------------------------------------
use cursive;


// External Dependencies ------------------------------------------------------
use cursive::direction::Orientation;
use cursive::traits::*;
use cursive::views::{Dialog, DummyView, LinearLayout, Panel, ResizedView, TextView};
use cursive::Cursive;
use cursive::align::HAlign;

// Modules --------------------------------------------------------------------
use cursive_tree_view::{Placement, TreeView};
use cursive_table_view::{TableView, TableViewItem};
use std::cmp::Ordering;


use clap::{App, Arg, SubCommand};

use hoardbase::base::CollectionTrait;


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


fn main() {
    let matches = App::new("Hoardmin").version("1.0").author("Shi Yan.").about("a database").arg(Arg::with_name("file").required(true).takes_value(true)).get_matches();

    println!("{:?}", matches.args.get("file").unwrap().vals[0]);

    let mut siv = cursive::default();

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
        let value = siv.call_on_name("tree", move |tree: &mut TreeView<String>| {
            tree.borrow_item(row).unwrap().to_string()
        });

        siv.add_layer(
            Dialog::around(TextView::new(value.unwrap()))
                .title("Item submitted")
                .button("Close", |s| {
                    s.pop_layer();
                }),
        );

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
            tree.insert_item(text.to_string(), placement, row)
                .unwrap_or(0)
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

    // UI ---------------------------------------------------------------------
    let mut v_split = LinearLayout::new(Orientation::Vertical);
    v_split.add_child(
        TextView::new(
            r#"
-- Controls --
Enter - Collapse children or submit row.
b - Insert before row.
a - Insert after row.
p - Insert parent above row.
f - Insert as first child of row.
l - Insert as last child of row.
e - Extract row without children.
r - Remove row and children.
h - Remove only children.
c - Clear all items.
"#,
        )
        .min_height(13),
    );

    v_split.add_child(ResizedView::with_full_height(DummyView));
    v_split.add_child(TextView::new("Last action: None").with_name("status"));

    let mut h_split = LinearLayout::new(Orientation::Horizontal);
    h_split.add_child(v_split);
    h_split.add_child(ResizedView::with_fixed_size((4, 0), DummyView));
    h_split.add_child(Panel::new(tree.with_name("tree").scrollable()));

    siv.add_fullscreen_layer(h_split);
    let mut table = TableView::<Foo, BasicColumn>::new()
        .column(BasicColumn::Name, "Name", |c| c.width_percent(20))
        .column(BasicColumn::Count, "Count", |c| c.align(HAlign::Center))
        .column(BasicColumn::Rate, "Rate", |c| {
            c.ordering(Ordering::Greater)
                .align(HAlign::Right)
                .width_percent(20)
        });
    
    let mut items = Vec::new();
    for i in 0..50 {
        items.push(Foo {
            name: format!("Name {}", i),
            count: 23,
            rate: 2,
        });
    }

    table.set_items(items);

    table.set_on_sort(|siv: &mut Cursive, column: BasicColumn, order: Ordering| {
        siv.add_layer(
            Dialog::around(TextView::new(format!("{} / {:?}", column.as_str(), order)))
                .title("Sorted by")
                .button("Close", |s| {
                    s.pop_layer();
                }),
        );
    });

    table.set_on_submit(|siv: &mut Cursive, row: usize, index: usize| {
        let value = siv
            .call_on_name("table", move |table: &mut TableView<Foo, BasicColumn>| {
                format!("{:?}", table.borrow_item(index).unwrap())
            })
            .unwrap();

        siv.add_layer(
            Dialog::around(TextView::new(value))
                .title(format!("Removing row # {}", row))
                .button("Close", move |s| {
                    s.call_on_name("table", |table: &mut TableView<Foo, BasicColumn>| {
                        table.remove_item(index);
                    });
                    s.pop_layer();
                }),
        );
    });
    siv.add_layer(Dialog::around(table.with_name("table").min_size((50, 20))).title("Table View"));


    fn set_status(siv: &mut Cursive, row: usize, text: &str) {
        let value = siv.call_on_name("tree", move |tree: &mut TreeView<String>| {
            tree.borrow_item(row)
                .map(|s| s.to_string())
                .unwrap_or_else(|| "".to_string())
        });

        siv.call_on_name("status", move |view: &mut TextView| {
            view.set_content(format!(
                "Last action: {} row #{} \"{}\"",
                text,
                row,
                value.unwrap()
            ));
        });
    }

    siv.run();
}
