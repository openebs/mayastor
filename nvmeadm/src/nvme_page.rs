use crate::nvme_page;
use libc::{c_char, c_uchar};
use std::fmt;

#[repr(C)]
#[derive(Default)]
pub struct ZeroSizeArray<T>(::core::marker::PhantomData<T>, [T; 0]);

impl<T> ZeroSizeArray<T> {
    pub unsafe fn as_ptr(&self) -> *const T {
        self as *const nvme_page::ZeroSizeArray<T> as *const T
    }
    pub unsafe fn as_slice(&self, len: usize) -> &[T] {
        ::core::slice::from_raw_parts(self.as_ptr(), len)
    }
}

impl<T> ::core::fmt::Debug for ZeroSizeArray<T> {
    fn fmt(&self, fmt: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        fmt.write_str("ZeroSizeArray")
    }
}

#[repr(C)]
pub struct NvmfDiscRspPageHdr {
    pub genctr: u64,
    pub numrec: u64,
    pub recfmt: u16,
    pub resv14: [c_uchar; 1006usize],
    pub entries: ZeroSizeArray<NvmfDiscRspPageEntry>,
}

impl fmt::Debug for NvmfDiscRspPageHdr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("NvmfDiscRspPageHdr")
            .field("genctr", &self.genctr)
            .field("numrec", &self.numrec)
            .field("recfmt", &self.recfmt)
            .finish()
    }
}

impl Default for NvmfDiscRspPageHdr {
    fn default() -> Self {
        Self::new()
    }
}

impl NvmfDiscRspPageHdr {
    fn new() -> Self {
        Self {
            ..unsafe { std::mem::zeroed() }
        }
    }
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct NvmeAdminCmd {
    pub opcode: c_uchar,
    pub flags: c_uchar, // fuse, rsvd1,psdt,cid
    pub cid: u16,
    pub nsid: u32,
    pub cdw2: u32,
    pub cdw3: u32,
    pub mptr: u64,
    pub dptr: u64,
    pub mptr_len: u32,
    pub dptr_len: u32,
    pub cdw10: u32,
    pub cdw11: u32,
    pub cdw12: u32,
    pub cdw13: u32,
    pub cdw14: u32,
    pub cdw15: u32,
    pub timeout_ms: u32,
    pub result: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct NvmfDiscRspPageEntry {
    pub trtype: c_uchar,
    pub adrfam: c_uchar,
    pub subtype: c_uchar,
    pub treq: c_uchar, // 0:2 secure channel, reserved
    pub portid: u16,
    pub cntlid: u16,
    pub asqsz: u16, // admin queue size
    pub resv8: [c_uchar; 22usize],
    pub trsvcid: [c_char; 32usize],
    pub resv64: [c_uchar; 192usize],
    pub subnqn: [c_char; 256usize],
    pub traddr: [c_char; 256usize],
    pub tsas: NvmfDiscRspPageEntryTsas,
}

impl Default for NvmfDiscRspPageEntry {
    fn default() -> Self {
        Self::new()
    }
}

impl NvmfDiscRspPageEntry {
    fn new() -> Self {
        Self {
            ..unsafe { std::mem::zeroed() }
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union NvmfDiscRspPageEntryTsas {
    pub common: [::std::os::raw::c_char; 256usize],
    pub rdma: nvmf_disc_rsp_page_entry_tsas_rdma,
    pub tcp: NvmfDiscRspPageEntryTsasTcp,
    __union_align: [u16; 128usize],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct NvmfDiscRspPageEntryTsasTcp {
    pub sectype: c_uchar,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct nvmf_disc_rsp_page_entry_tsas_rdma {
    pub qptype: c_uchar,
    pub prtype: c_uchar,
    pub cms: c_uchar,
    pub resv3: [c_uchar; 5usize],
    pub pkey: u16,
    pub resv10: [c_uchar; 246usize],
}
