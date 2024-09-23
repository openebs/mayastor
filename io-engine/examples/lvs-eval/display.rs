use io_engine::{
    core::{LogicalVolume, UntypedBdev},
    lvs::{Lvol, Lvs, LvsLvol},
};
use prettytable::{row, Table};
use spdk_rs::libspdk::{
    spdk_bit_array,
    spdk_bit_array_capacity,
    spdk_bit_array_get,
    spdk_bit_pool,
    spdk_bit_pool_capacity,
    spdk_bit_pool_is_allocated,
    spdk_blob_calc_used_clusters,
    spdk_blob_is_thin_provisioned,
    spdk_blob_mut_data,
    spdk_blob_store,
};

/// TODO
pub async fn print_lvs(lvs: &Lvs) {
    print_separator("LVS", 0);

    print_bdev(lvs.base_bdev());
    print_lvs_data(lvs);
    print_replicas(lvs);
}

/// TODO
pub fn print_bdev(bdev: UntypedBdev) {
    print_separator("Bdev", 0);

    let mut tab = Table::new();

    tab.add_row(row!["name", bdev.name()]);
    tab.add_row(row!["size", bdev.size_in_bytes(), "bytes"]);
    tab.add_row(row!["  |-", bdev.size_in_bytes() / (1024 * 1024), "Mb"]);
    tab.add_row(row!["block_len", bdev.block_len(), "bytes"]);
    tab.add_row(row!["num_blocks", bdev.num_blocks(), "blocks"]);

    print_table(tab);

    println!();
}

/// TODO
pub fn print_lvs_data(lvs: &Lvs) {
    print_separator("Blob store", 0);

    let bs = unsafe { &*lvs.blob_store() };

    let mut tab = Table::new();

    tab.add_row(row!["md_start", bs.md_start, "pages"]);
    tab.add_row(row!["md_len", bs.md_len, "pages"]);
    tab.add_row(row!["cluster_sz", bs.cluster_sz, "bytes"]);
    tab.add_row(row!["  |-------", bs.cluster_sz / (1024 * 1024), "Mb"]);
    tab.add_row(row!["total_clusters", bs.total_clusters]);
    tab.add_row(row!["total_data_clusters", bs.total_data_clusters]);
    tab.add_row(row![
        "md_clusters",
        bs.total_clusters - bs.total_data_clusters
    ]);
    tab.add_row(row!["num_free_clusters", bs.num_free_clusters]);
    tab.add_row(row!["pages_per_cluster", bs.pages_per_cluster]);
    tab.add_row(row!["io_unit_size", bs.io_unit_size, "bytes"]);
    tab.add_row(row![]);
    tab.add_row(row!["page_size", lvs.page_size(), "bytes"]);
    tab.add_row(row!["md_pages", lvs.md_pages()]);
    tab.add_row(row!["md_used_pages", lvs.md_used_pages()]);

    print_table(tab);
    println!();

    // Used MD pages.
    println!("Used MD pages:");
    print!("  ");
    print_used_array_bits(bs.used_md_pages, Some(bs.md_len));
    println!();

    // Used clusters.
    println!("Used clusters:");
    print!("  ");
    print_used_pool_bits(bs.used_clusters, Some(bs.total_clusters as u32));
    println!();

    // Used blob IDs.
    println!("Used blob IDs:");
    print!("  ");
    print_used_array_bits(bs.used_blobids, None);
    println!();

    // Open blobs.
    println!("Open blob IDs:");
    print!("  ");
    print_used_array_bits(bs.open_blobids, None);
    println!();
}

/// TODO
pub fn print_replicas(lvs: &Lvs) {
    print_separator("Replicas", 0);

    for (idx, lvol) in lvs.lvols().unwrap().enumerate() {
        print_separator(&format!("Replica #{idx}:"), 1);
        print_replica(&lvol);
    }

    print_separator("End of replicas", 0);
}

/// TODO
pub fn print_replica(lvol: &Lvol) {
    let blob = unsafe { &*lvol.blob_checked() };
    let bs = unsafe { &*blob.bs };

    let mut tab = Table::new();

    let num_allocated_clusters =
        unsafe { spdk_blob_calc_used_clusters(blob as *const _ as *mut _) };

    tab.add_row(row!["id", format!("0x{:x}", blob.id)]);
    tab.add_row(row!["parent_id", format!("0x{:x}", blob.parent_id)]);
    tab.add_row(row!["name", lvol.name()]);
    tab.add_row(row!["uuid", lvol.uuid()]);
    tab.add_row(row!["is thin", unsafe {
        spdk_blob_is_thin_provisioned(blob as *const _ as *mut _)
    }]);
    tab.add_row(row!["num_clusters", blob.active.num_clusters]);
    tab.add_row(row!["alloc clusters", num_allocated_clusters]);
    tab.add_row(row![
        "size",
        blob.active.num_clusters * bs.cluster_sz as u64,
        "bytes"
    ]);

    print_table(tab);

    println!();
    print_blob_data("Active data", bs, &blob.active);

    println!();
}

/// TODO
pub fn print_blob_data(
    name: &str,
    bs: &spdk_blob_store,
    blob: &spdk_blob_mut_data,
) {
    println!("{name}:");

    // Clusters.
    println!(
        "  Clusters: {} / {}",
        blob.num_clusters, blob.num_allocated_clusters
    );
    print!("    ");
    for i in 0 .. blob.num_allocated_clusters as isize {
        let lba = unsafe { *blob.clusters.offset(i) };
        let num = lba_to_cluster(bs, lba);
        print!("0x{num:x} ");
    }
    println!("\n");

    // LBAs.
    println!(
        "  LBAs: {} / {}",
        blob.num_clusters, blob.num_allocated_clusters
    );
    print!("    ");
    for i in 0 .. blob.num_allocated_clusters as isize {
        let c = unsafe { *blob.clusters.offset(i) };
        print!("0x{c:x} ");
    }
    println!("\n");

    // EPs.
    println!(
        "  Extent_pages: {} / {}",
        blob.num_extent_pages, blob.extent_pages_array_size
    );
    print!("    ");
    for i in 0 .. blob.extent_pages_array_size as isize {
        let c = unsafe { *blob.extent_pages.offset(i) };
        print!("0x{c:x} ");
    }
    println!();
}

/// TODO
fn print_table(mut tab: Table) {
    tab.set_titles(row!["Name", "Value", "Units"]);
    tab.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    tab.printstd();
}

/// TODO
fn print_used_array_bits(ba: *const spdk_bit_array, cnt: Option<u32>) {
    let cnt = cnt.unwrap_or_else(|| unsafe { spdk_bit_array_capacity(ba) });
    let mut total = 0;

    for i in 0 .. cnt {
        let v = unsafe { spdk_bit_array_get(ba, i) };
        if v {
            print!("0x{i:x} ");
            total += 1;
        }
    }

    println!();
    println!("  Total: {total}");
}

/// TODO
fn print_used_pool_bits(bp: *const spdk_bit_pool, cnt: Option<u32>) {
    let cnt = cnt.unwrap_or_else(|| unsafe { spdk_bit_pool_capacity(bp) });
    let mut total = 0;

    for i in 0 .. cnt {
        let v = unsafe { spdk_bit_pool_is_allocated(bp, i) };
        if v {
            print!("0x{i:x} ");
            total += 1;
        }
    }

    println!();
    println!("  Total: {total}");
}

/// TODO
fn print_separator(title: &str, level: u8) {
    let title = format!(" {title} ");
    if level == 0 {
        println!("{:=^1$}\n", title, 70);
    } else {
        println!("{:-^1$}\n", title, 50);
    }
}

/// TODO
fn lba_to_cluster(bs: &spdk_blob_store, lba: u64) -> u64 {
    unsafe { lba / (bs.cluster_sz / (*bs.dev).blocklen) as u64 }
}
