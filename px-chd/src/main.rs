use px_chd::{run, Config};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("px_chd=info")
        .init();

    let config = match Config::parse() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("px-chd config error: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = run(config) {
        eprintln!("px-chd error: {err}");
        std::process::exit(1);
    }
}
