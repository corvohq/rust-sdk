fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = "../proto/corvo/v1/worker.proto";
    println!("cargo:rerun-if-changed={proto_path}");
    println!("cargo:rerun-if-changed=../proto");

    // Only regenerate if protoc is available AND the proto file exists.
    // When publishing, cargo extracts the crate to a temp dir where the
    // proto file won't be at ../proto. Pre-generated stubs in src/gen/ are
    // used instead.
    if std::path::Path::new(proto_path).exists() && (std::env::var("PROTOC").is_ok() || which_protoc()) {
        tonic_build::configure()
            .build_server(false)
            .out_dir("src/gen")
            .compile_protos(&[proto_path], &["../proto"])?;
    }
    Ok(())
}

fn which_protoc() -> bool {
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}
