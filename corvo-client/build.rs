fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only regenerate if PROTOC is available. Otherwise, use pre-generated stubs
    // checked into src/gen/. Run `cargo build` with protoc installed to regenerate.
    if std::env::var("PROTOC").is_ok() || which_protoc() {
        tonic_build::configure()
            .build_server(false)
            .out_dir("src/gen")
            .compile_protos(&["../proto/corvo/v1/worker.proto"], &["../proto"])?;
    }
    Ok(())
}

fn which_protoc() -> bool {
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}
