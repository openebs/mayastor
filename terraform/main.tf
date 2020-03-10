module "libvirt" {
  source = "./mod/libvirt"

  image_path         = var.image_path
  num_nodes          = var.num_nodes
  hostname_formatter = var.hostname_formatter
  ssh_user           = var.ssh_user
  ssh_key            = var.ssh_key
  disk_size          = var.disk_size

}

module "k8s" {
  source = "./mod/k8s"

  num_nodes          = var.num_nodes
  ssh_user           = var.ssh_user
  ssh_key            = var.ssh_key
  node_list          = module.libvirt.node_list
}

output "kluster" {
	value =  module.libvirt.ks-cluster-nodes
}

