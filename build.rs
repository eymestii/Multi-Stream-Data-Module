use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Usamos un protoc embebido para no exigir que esté instalado en el sistema.
    env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    let well_known = protoc_bin_vendored::include_path()?;
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    tonic_build::configure()
        // Descriptor para gRPC reflection (grpcurl, Postman, etc. descubren el servicio solos).
        .file_descriptor_set_path(out_dir.join("streamchat_descriptor.bin"))
        .compile_protos(
            &[
                PathBuf::from("proto/streamchat.proto"),
                PathBuf::from("proto/oauth.proto"),
            ],
            &[PathBuf::from("proto"), well_known],
        )?;

    println!("cargo:rerun-if-changed=proto/streamchat.proto");
    println!("cargo:rerun-if-changed=proto/oauth.proto");
    Ok(())
}
