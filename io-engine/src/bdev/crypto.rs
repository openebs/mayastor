//!
//! This modules deals with managing crypto vbdev. The crypto vbdev is
//! a virtual bdev that needs a base bdev for it to be created. The crypto vbdev
//! concept can be applied to encrypt the complete lvolstore or a particular lvol.
//! For encrypting lvolstore(Mayastor diskpool), the crypto vbdev is created on top
//! of base bdev of the disk pool e.g. aio bdev. For lvol level encryption, the
//! crypto vbdev is to be created on top of the lvol vbdev.
//!
//! A general workflow example for creating a spdk crypto vbdev for encrypted lvolstore is as below:
//!
//! sudo $SPDK_SCRIPT_PATH/rpc.py accel_crypto_key_create -c AES_CBC -k 01234567891234560123456789deadc0 -n dskey_aes128_cbc_1
//! sudo $SPDK_SCRIPT_PATH/rpc.py bdev_aio_create /dev/loop0 MyBaseBdev 512
//! sudo $SPDK_SCRIPT_PATH/rpc.py bdev_crypto_create MyBaseBdev MyCryptoVbdev -n key_aes128_cbc
//!
//! The vbdev MyCryptoVbdev can now be used to create lvolstore on it.
//!

use futures::channel::oneshot;
use io_engine_api::v1 as v1rpc;
use nix::errno::Errno;
use spdk_rs::{
    ffihelper::IntoCString,
    libspdk::{
        accel_dpdk_cryptodev_enable, accel_dpdk_cryptodev_set_driver, create_crypto_disk,
        create_crypto_opts_by_name, delete_crypto_disk, spdk_accel_assign_opc,
        spdk_accel_crypto_key, spdk_accel_crypto_key_create, spdk_accel_crypto_key_create_param,
        spdk_accel_crypto_key_destroy, spdk_accel_crypto_key_get,
    },
};
use std::{
    fmt::{Debug, Display, Formatter},
    os::raw::c_char,
};

use crate::{
    bdev_api::BdevError,
    core::CoreError,
    ffihelper::{cb_arg, pair, FfiResult},
};

use once_cell::sync::Lazy;
use std::{
    collections::{HashMap, HashSet},
    sync::RwLock,
};

/// Global synchronized HashMap (Crypto Key -> Set of crypto vbdevs).
/// Since this doesn't lie in IO path, not using per-bucket locks in hashmap.
static KEY_CRYPTO_VBDEV_MAP: Lazy<RwLock<HashMap<String, HashSet<String>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Adds a dependency ('crypto_vbdev_name' uses 'key_name')
fn add_key_user(
    key_params: &EncryptionKey,
    crypto_vbdev_name: String,
) -> Result<*mut spdk_accel_crypto_key, BdevError> {
    let mut map = KEY_CRYPTO_VBDEV_MAP.write().unwrap();

    // Create the key first.
    let key = create_crypto_key(key_params)?;

    map.entry(key_params.key_name.clone())
        .or_default()
        .insert(crypto_vbdev_name);
    Ok(key)
}

/// Removes a dependency ('crypto_vbdev_name' stops using 'key_name')
fn remove_key_user(key_name: &str, crypto_vbdev_name: String) {
    let mut map = KEY_CRYPTO_VBDEV_MAP.write().unwrap();
    if let Some(users) = map.get_mut(key_name) {
        users.remove(&crypto_vbdev_name);
        // Clean up empty sets immediately
        if users.is_empty() {
            // delete the key in spdk.
            unsafe {
                let key = spdk_accel_crypto_key_get(
                    key_name.to_string().into_cstring().as_ptr() as *mut c_char
                );
                if key.is_null() {
                    warn!("Non-existent key in SPDK.");
                } else {
                    let err = spdk_accel_crypto_key_destroy(key);
                    if err != 0 {
                        error!("Failed to destroy spdk accel crypto key: {err}");
                        // Return and don't remove from the map yet.
                        return;
                    }
                }
            }
            map.remove(key_name);
        }
    }
}

/// Gets all crypto vbdev names using a given key_name.
fn get_users_of(key_name: &str) -> Option<HashSet<String>> {
    let map = KEY_CRYPTO_VBDEV_MAP.read().unwrap();
    // Clone or return ref perhaps?
    map.get(key_name).cloned()
}

/// Reverse lookup: O(n * m)
/// Finds which hash key contains a particular crypto vbdev name (scans the map). Should be
/// ok to bear this cost as there won't be exhorbitant number of pool or keys per io-engine.
fn find_key_for_crypto_vbdev(crypto_vbdev_name: &str) -> Option<String> {
    let map = KEY_CRYPTO_VBDEV_MAP.read().unwrap();
    map.iter().find_map(|(key, users)| {
        if users.contains(crypto_vbdev_name) {
            Some(key.to_owned())
        } else {
            None
        }
    })
}

/// Purges all key entries that have no crypto vbdev users left. This will not be usually
/// needed because remove call is taking care of deleting the key entry upon removal of
/// last crypto vbdev user.
#[allow(dead_code)]
fn purge_empty_keys() {
    let mut map = KEY_CRYPTO_VBDEV_MAP.write().unwrap();
    map.retain(|_, users| !users.is_empty());
}

/// Representation of a crypto vbdev to be created in SPDK.
/// TODO: Currently we don't fit crypto vbdev into the uri based bdev management
/// as the information required to completely setup the crypto vbdev isn't part of the uri,
/// and also the crypto uri isn't provided by user rather we'll have to do some masking
/// using crypto:// scheme.
#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct Crypto {
    // Name of the crypto vbdev.
    pub name: String,
    // Name of the base bdev.
    pub base_bdev_name: String,
    // Encryption key params.
    pub key: EncryptionKey,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Cipher mode for the encryption enablement. The default is kept as AES_XTS here
/// because operations are default assigned to software module, which only supports
/// AES_XTS. We don't expect to use default though.
pub enum Cipher {
    AesCbc,
    #[default]
    AesXts,
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
/// A struct that represents the parameters required to create a key.
pub struct EncryptionKey {
    pub cipher: Cipher,
    #[serde(default)]
    pub key_name: String,
    pub key: String,
    pub key_len: u32,
    pub key2: Option<String>,
    pub key2_len: Option<u32>,
}

impl Debug for EncryptionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("cipher", &self.cipher)
            .field("key_name", &self.key_name)
            .finish()
    }
}

impl Debug for Crypto {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "crypto vbdev '{}', 'base_bdev: {}', keyname '{}', cipher: '{:?}'",
            self.name, self.base_bdev_name, self.key.key_name, self.key.cipher
        )
    }
}

impl Display for Cipher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let c = match self {
            Self::AesCbc => "AES_CBC",
            Self::AesXts => "AES_XTS",
        };
        write!(f, "{c}")
    }
}

impl From<v1rpc::common::Cipher> for Cipher {
    fn from(value: v1rpc::common::Cipher) -> Self {
        match value {
            v1rpc::common::Cipher::AesCbc => Self::AesCbc,
            v1rpc::common::Cipher::AesXts => Self::AesXts,
        }
    }
}

/// Driver types for dpdk_cryptodev module. We won't be using any other
/// than aesni-mb.
#[derive(Debug, Default)]
enum AccelDpdkCryptodevDriverType {
    #[default]
    AccelDpdkCryptodevAesniMb,
    _AccelDpdkCryptodevQat,
    _AccelDpdkCryptodevMlx5Pci,
    _AccelDpdkCryptodevUadk,
    _AccelDpdkCryptodevLast,
}

impl Display for AccelDpdkCryptodevDriverType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let drv = match self {
            Self::AccelDpdkCryptodevAesniMb => "crypto_aesni_mb",
            Self::_AccelDpdkCryptodevQat => "crypto_qat",
            Self::_AccelDpdkCryptodevMlx5Pci => "mlx5_pci",
            Self::_AccelDpdkCryptodevUadk => "crypto_uadk",
            Self::_AccelDpdkCryptodevLast => "crypto_driver_invalid",
        };
        write!(f, "{drv}")
    }
}

// This function enables the dpdk_cryptodev module with aesni_mb driver in spdk,
// and assigns the encrypt and decrypt operation to it, which by default are assigned to
// software module initially.
#[allow(dead_code)]
pub fn enable_dpdk_cryptodev_accel_module() -> Result<(), CoreError> {
    info!("Enabling accel dpdk_cryptodev module");
    unsafe {
        accel_dpdk_cryptodev_enable();
        let setdrv_ret = accel_dpdk_cryptodev_set_driver(
            AccelDpdkCryptodevDriverType::AccelDpdkCryptodevAesniMb
                .to_string()
                .into_cstring()
                .as_ptr(),
        );
        if setdrv_ret != 0 {
            return Err(CoreError::InitCryptoModule {
                reason: format!("Unsupported driver: {setdrv_ret:?}"),
            });
        }
        let err_enc = spdk_accel_assign_opc(8, "dpdk_cryptodev".into_cstring().as_ptr());
        let err_dec = spdk_accel_assign_opc(9, "dpdk_cryptodev".into_cstring().as_ptr());
        trace!("assign opc err_enc: {err_enc}, err_dec: {err_dec}");
        if err_enc != 0 || err_dec != 0 {
            return Err(CoreError::InitCryptoModule {
                reason: format!(
                    "opcode assignment failed. err_enc {err_enc:?}, err_dec {err_dec:?}"
                ),
            });
        }
        Ok(())
    }
}

/// A simple wrapper around `spdk_accel_crypto_key_create`. The keys' strings here are
/// in hexlified format.
fn create_crypto_key(key_params: &EncryptionKey) -> Result<*mut spdk_accel_crypto_key, BdevError> {
    let cipher = key_params.cipher.to_string().into_cstring();
    let key = key_params.key.as_str().into_cstring();
    let key_name = key_params.key_name.as_str().into_cstring();
    let hex_key2_str = key_params.key2.as_deref().map(|k2| k2.into_cstring());
    let key2 = if let Some(ref k2) = hex_key2_str {
        k2.as_ptr()
    } else {
        std::ptr::null_mut()
    };

    debug!(
        "Creating {:?} key of size {}",
        key_params.cipher.to_string(),
        key_params.key_len,
    );

    let create_key_params = spdk_accel_crypto_key_create_param {
        cipher: cipher.as_ptr() as *mut c_char,
        hex_key: key.as_ptr() as *mut c_char,
        hex_key2: key2 as *mut c_char,
        key_name: key_name.as_ptr() as *mut c_char,
        tweak_mode: std::ptr::null_mut(),
    };

    unsafe {
        // Check if the key with this name exists. If it does, then we reuse that.
        let existing_key = spdk_accel_crypto_key_get(create_key_params.key_name);
        if !existing_key.is_null() {
            info!("Key {:?} exists already, reusing it.", key_params.key_name);
            return Ok(existing_key);
        };
        let ret = spdk_accel_crypto_key_create(&create_key_params);
        if ret != 0 {
            Err(BdevError::CreateBdevFailed {
                source: Errno::from_raw(ret.abs()),
                name: format!("Key create failed: {:?}", create_key_params.key_name),
            })
        } else {
            let key_ptr = spdk_accel_crypto_key_get(create_key_params.key_name);
            trace!("Key '{:?}' created", key_params.key_name);
            Ok(key_ptr)
        }
    }
}

/// callback when operation has been performed on crypto vbdev
extern "C" fn crypto_vbdev_op_cb(arg: *mut std::os::raw::c_void, errno: i32) {
    let sender = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
    trace!("crypto_vbdev_op_cb: errno {errno}");
    sender.send(errno).unwrap();
}

/// Destroy a crypto vbdev.
pub async fn destroy_crypto_vbdev(
    vbdev_name: String,
    key_name: Option<String>,
) -> Result<(), BdevError> {
    let (s, r) = pair::<i32>();
    unsafe {
        delete_crypto_disk(
            vbdev_name.clone().into_cstring().as_ptr() as *mut std::os::raw::c_char,
            Some(crypto_vbdev_op_cb),
            cb_arg(s),
        );
    }

    let ret = r
        .await
        .expect("callback gone while deleting crypto disk")
        .to_result(|e| BdevError::DestroyBdevFailed {
            source: Errno::from_raw(e),
            name: vbdev_name.to_string(),
        });
    if let Err(e) = ret {
        match e {
            BdevError::BdevNotFound { .. } => {}
            _else => return Err(_else),
        }
    }

    info!("crypto vbdev {vbdev_name} destroyed successfully");

    let key_name = match key_name.or_else(|| find_key_for_crypto_vbdev(&vbdev_name)) {
        Some(k) => k,
        None => {
            warn!("Mapped key not found for {vbdev_name}");
            return Ok(());
        }
    };

    remove_key_user(key_name.as_str(), vbdev_name);
    if let Some(users) = get_users_of(key_name.as_str()) {
        trace!("[remove] Remaining key users {key_name}: {users:?}");
    }
    Ok(())
}
/// The primary function to create a crypto vbdev in spdk.
pub fn create_crypto_vbdev_on_base_bdev(
    crypto_vbdev_name: &str,
    base_bdev_name: &str,
    key_params: &EncryptionKey,
) -> Result<(), BdevError> {
    // Create the key and add to the map.
    let key = add_key_user(key_params, crypto_vbdev_name.to_string())?;
    if let Some(users) = get_users_of(key_params.key_name.as_str()) {
        trace!(
            "[add] updated key users {:?}: {users:?}",
            key_params.key_name
        );
    }

    // Setup the crypto opts using the key now.
    let crypto_opts_ptr = unsafe {
        create_crypto_opts_by_name(
            crypto_vbdev_name.into_cstring().as_ptr() as *mut c_char,
            base_bdev_name.into_cstring().as_ptr() as *mut c_char,
            key,
            false,
        )
    };

    // Now create the crypto vbdev.
    let ret = unsafe { create_crypto_disk(crypto_opts_ptr) };

    if ret != 0 {
        return Err(BdevError::CreateBdevFailed {
            source: Errno::from_raw(ret),
            name: crypto_vbdev_name.to_string(),
        });
    }
    info!("crypto vbdev {crypto_vbdev_name} created on base bdev {base_bdev_name}");

    Ok(())
}
