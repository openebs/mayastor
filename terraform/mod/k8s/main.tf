variable "k8s_cluster_token" {
  default = "abcdef.1234567890abcdef"
}

variable "overlay_cidr" {
  default = "10.244.0.0/16"
}

variable "num_nodes" {

}

variable "ssh_user" {

}

variable "private_key_path" {

}

variable "node_list" {

}

resource "null_resource" "k8s" {
  count = var.num_nodes

  connection {
    host        = element(var.node_list, count.index)
    user        = var.ssh_user
    private_key = file(var.private_key_path)
  }

  provisioner "file" {
    content     = data.template_file.master-configuration.rendered
    destination = "/tmp/kubeadm_config.yaml"
  }


  provisioner "remote-exec" {
    inline = [element(data.template_file.install.*.rendered, count.index)]
  }

  provisioner "remote-exec" {
    inline = [
      count.index == 0 ? data.template_file.master.rendered : data.template_file.node.rendered
    ]
  }
}

data "template_file" "master-configuration" {
  template = file("${path.module}/kubeadm_config.yaml")

  vars = {
    master_ip = element(var.node_list, 0)
    token     = var.k8s_cluster_token
    cert_sans = element(var.node_list, 0)
    pod_cidr  = var.overlay_cidr
  }
}

data "template_file" "install" {
  count    = var.num_nodes
  template = file("${path.module}/repo.sh")
}

data "template_file" "master" {
  template = file("${path.module}/master.sh")
}

data "template_file" "node" {
  template = file("${path.module}/node.sh")
  vars = {
    master_ip = element(var.node_list, 0)
    token     = var.k8s_cluster_token
  }
}
