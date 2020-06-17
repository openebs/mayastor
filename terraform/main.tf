module "k8s" {
  source = "./mod/k8s"

  num_nodes = var.num_nodes
  ssh_user  = var.ssh_user
  private_key_path = var.private_key_path
  node_list =  module.provider.node_list
  overlay_cidr = var.overlay_cidr
  nr_hugepages = var.nr_hugepages
  modprobe_nvme = var.modprobe_nvme
}

module "provider" {
  #source = "./mod/lxd"
  source = "./mod/libvirt"

  # lxd and libvirt
  ssh_user  = var.ssh_user
  ssh_key   = var.ssh_key
  num_nodes = var.num_nodes

  # libvirt
  image_path         = var.image_path
  hostname_formatter = var.hostname_formatter
  private_key_path   = var.private_key_path
  disk_size          = var.disk_size
  qcow2_image        = var.qcow2_image
}

output "kluster" {
  value =  module.provider.ks-cluster-nodes
}
