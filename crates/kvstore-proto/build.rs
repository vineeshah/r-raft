use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from("proto");
    let protos = ["kv.proto", "raft.proto"];
    let paths: Vec<PathBuf> = protos.iter().map(|p| proto_dir.join(p)).collect();

    for path in &paths {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&paths, &[proto_dir])?;

    Ok(())
}
