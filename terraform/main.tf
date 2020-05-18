module "libvirt" {
  source = "./mod/libvirt"

  image_path         = var.image_path
  num_nodes          = var.num_nodes
  hostname_formatter = var.hostname_formatter
  ssh_user           = var.ssh_user
  ssh_key            = var.ssh_key
  private_key_path   = var.private_key_path
  disk_size          = var.disk_size
  qcow2_image        = var.qcow2_image
}

#module "lxd" {
#  source   = "./mod/lxd"
#  ssh_user = var.ssh_user
#  ssh_key  = var.ssh_key
#  num_nodes = var.num_nodes
#}

module "k8s" {
  source = "./mod/k8s"

  num_nodes = var.num_nodes
  ssh_user  = var.ssh_user
  private_key_path = var.private_key_path
  #node_list = module.lxd.node_list
  node_list = module.libvirt.node_list
}

output "kluster" {
 	value =  module.libvirt.ks-cluster-nodes
}
