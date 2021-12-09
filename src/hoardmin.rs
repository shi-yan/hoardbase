use clap::{App, Arg, SubCommand};
use cursive::views::{Dialog, TextView};

mod collection;
mod database;
mod query_translator;

fn main() {
    let matches = App::new("Hoardmin").version("1.0").author("Shi Yan.").about("a database").arg(Arg::with_name("file").required(true).takes_value(true)).get_matches();

    println!("{:?}", matches.args.get("file").unwrap().vals[0]);

    // Creates the cursive root - required for every application.
    //let mut siv = cursive::default();

    // Creates a dialog with a single "Quit" button
    // siv.add_layer(Dialog::around(TextView::new("Hello Dialog!"))
    //                     .title("Cursive")
    //                   .button("Quit", |s| s.quit()));

    // Starts the event loop.
    //siv.run();
}
