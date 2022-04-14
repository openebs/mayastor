fn main() {
    let profile = std::env::var("PROFILE").unwrap();
    let spdk_rpath =
        format!("{}/target/{}", std::env::var("SRCDIR").unwrap(), profile);
    println!("cargo:rustc-link-search=native={}", spdk_rpath);
    println!("cargo:rustc-link-arg=-Wl,-rpath={}", spdk_rpath);
}
