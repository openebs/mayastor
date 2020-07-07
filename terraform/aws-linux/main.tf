variable "num_nodes" {
	default = 1
}

variable "hostname_formatter" {
	default = "amz-%d"
}

variable "ssh_user" {
	default = "gila"
}

variable "ssh_key" {
	default = "/home/gila/.ssh/id_rsa.pub"

}

variable "private_key_path" {
	default = "/home/gila/.ssh/id_rsa"

}

variable "disk_size" {
	default     = 53687091200
}

# I'd recommond to copy this locally and change the path
# see https://cdn.amazonlinux.com/os-images/2.0.20200602.0/kvm for the lastest
# images
variable image_source {
  default = "https://cdn.amazonlinux.com/os-images/2.0.20200602.0/kvm/amzn2-kvm-2.0.20200602.0-x86_64.xfs.gpt.qcow2"
}

provider "libvirt" {
  uri = "qemu:///system"
}

# a pool is where we store all images and cloud-init iso's
resource "libvirt_pool" "aws-pool" {
  name = "aws-pool"
  type = "dir"
  path = "/images"
}


# our base image from aws, that includes by default cloud-init
resource "libvirt_volume" "aws-qcow2" {
  name   = "aws-base"
  pool   = libvirt_pool.aws-pool.name
  source = var.image_source
  format = "qcow2"
}

# we want to, based of the first image, create 3 separate images each with their own
# cloud-init settings
resource "libvirt_volume" "aws-qcow2-resized" {
  name           = format(var.hostname_formatter, count.index + 1)
  count          = var.num_nodes
  base_volume_id = libvirt_volume.aws-qcow2.id
  pool           = libvirt_pool.aws-pool.name
  size           = var.disk_size
}

# user data that we pass to cloud init that reads variables from variables.tf and
# passes them to a template file to be filled in.

data "template_file" "user_data" {
  count = var.num_nodes
  template = templatefile("${path.module}/user-data.yaml",
    { ssh_user = var.ssh_user, ssh_key = file(var.ssh_key),
  hostname = format(var.hostname_formatter, count.index + 1) })
}

# likewise for networking
data "template_file" "meta-data" {
  template = file("${path.module}/meta-data.yaml")
}

# our cloud-init disk resource
resource "libvirt_cloudinit_disk" "commoninit" {
  name           = format("commoninit-%d.iso", count.index + 1)
  count          = var.num_nodes
  user_data      = data.template_file.user_data[count.index].rendered
  meta_data      = data.template_file.meta-data.rendered
  pool           = libvirt_pool.aws-pool.name
}

# create the actual VMs for the cluster
resource "libvirt_domain" "aws-domain" {
  count     = var.num_nodes
  name      = format(var.hostname_formatter, count.index + 1)
  memory    = 4096
  vcpu      = 2
  autostart = true

  cloudinit = libvirt_cloudinit_disk.commoninit[count.index].id

  cpu = {
    mode = "host-passthrough"
  }

  disk {
    volume_id = libvirt_volume.aws-qcow2-resized[count.index].id
    scsi      = "true"
  }

  console {
    type        = "pty"
    target_type = "serial"
    target_port = "0"
  }

  network_interface {
    network_name   = "default"
    hostname       = format(var.hostname_formatter, count.index + 1)
    wait_for_lease = true
  }

  console {
    type        = "pty"
    target_type = "virtio"
    target_port = "1"
  }

  provisioner "remote-exec" {
    inline = ["sudo cloud-init status --wait"]
    connection {
      type        = "ssh"
      user        = var.ssh_user
      host        = self.network_interface.0.addresses.0
      private_key = file(var.private_key_path)
    }
  }
}

terraform {
  required_version = ">= 0.12"
}
