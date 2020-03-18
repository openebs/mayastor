extern crate bindgen;
extern crate cc;

use bindgen::callbacks::{MacroParsingBehavior, ParseCallbacks};
use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
struct MacroCallback {
    macros: Arc<RwLock<HashSet<String>>>,
}

impl ParseCallbacks for MacroCallback {
    fn will_parse_macro(&self, name: &str) -> MacroParsingBehavior {
        self.macros.write().unwrap().insert(name.into());

        if name == "IPPORT_RESERVED" {
            return MacroParsingBehavior::Ignore;
        }

        MacroParsingBehavior::Default
    }
}

fn build_wrapper() {
    cc::Build::new()
        .file("logwrapper.c")
        .flag("-Ispdk/include")
        .flag("-Ispdk/lib")
        .flag("-Ispdk/module")
        .compile("logwrapper");
}

fn main() {
    #![allow(unreachable_code)]
    #[cfg(not(target_arch = "x86_64"))]
    panic!("spdk-sys crate is only for x86_64 cpu architecture");
    #[cfg(not(target_os = "linux"))]
    panic!("spdk-sys crate works only on linux");

    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);

    build_wrapper();

    let macros = Arc::new(RwLock::new(HashSet::new()));
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        // If we did not use private interfaces those would not be needed.
        // All needed headers should be in /usr/local/include.
        .clang_arg("-Ispdk/include")
        .clang_arg("-Ispdk/lib")
        .clang_arg("-Ispdk/module")
        .rustfmt_bindings(true)
        .whitelist_function("^spdk.*")
        .whitelist_function("*.aio.*")
        .whitelist_function("*.iscsi.*")
        .whitelist_function("*.crypto_disk.*")
        .whitelist_function("*.lvs.*")
        .whitelist_function("*.lvol.*")
        .whitelist_function("*.uring.*")
        .blacklist_type("^longfunc")
        .whitelist_var("^NVMF.*")
        .whitelist_var("^SPDK.*")
        .whitelist_var("^spdk.*")
        .trust_clang_mangling(false)
        .layout_tests(false)
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .generate_inline_functions(true)
        .parse_callbacks(Box::new(MacroCallback {
            macros,
        }))
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("libspdk.rs"))
        .expect("Couldn't write bindings!");

    println!("cargo:rustc-link-lib=spdk_fat");
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=rdmacm");
    println!("cargo:rustc-link-lib=aio");
    println!("cargo:rustc-link-lib=iscsi");
    println!("cargo:rustc-link-lib=numa");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=rt");
    println!("cargo:rustc-link-lib=uuid");
    println!("cargo:rustc-link-lib=crypto");
    println!("cargo:rustc-link-lib=uring");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=logwrapper.c");
}
