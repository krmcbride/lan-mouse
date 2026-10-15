fn main() {
    let unix = cfg!(unix);
    let layer_shell = cfg!(feature = "layer_shell");
    let libei = cfg!(feature = "libei");
    let x11 = cfg!(feature = "x11");
    let evdev = cfg!(feature = "evdev");
    let linux = std::env::var("CARGO_CFG_TARGET_OS").is_ok_and(|os| os == "linux");
    let macos = cfg!(target_os = "macos");

    let libei = unix && !macos && libei;
    let layer_shell = unix && !macos && layer_shell;
    let x11 = unix && !macos && x11;
    let evdev = linux && evdev;

    println!("cargo::rustc-check-cfg=cfg(layer_shell)");
    println!("cargo::rustc-check-cfg=cfg(libei)");
    println!("cargo::rustc-check-cfg=cfg(x11)");
    println!("cargo::rustc-check-cfg=cfg(evdev)");

    if layer_shell {
        println!("cargo::rustc-cfg=layer_shell");
    }
    if libei {
        println!("cargo::rustc-cfg=libei");
    }
    if x11 {
        println!("cargo::rustc-cfg=x11");
    }
    if evdev {
        println!("cargo::rustc-cfg=evdev");
    }
}
