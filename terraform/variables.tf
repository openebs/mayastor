variable "private_key_path" {
  type        = string
  description = "SSH private key path"
  #default     = "/home/user/.ssh/id_rsa"
}

variable "ssh_key" {
  type        = string
  description = "SSH pub key to use"
  #default     = "ssh-rsa ..."
}

variable "ssh_user" {
  type        = string
  description = "The user that should be created and who has sudo power"
  #default     = "user"
}

variable "image_path" {
  type        = string
  description = "Where the images will be stored"
  default     = "/images"
}

variable "disk_size" {
  type        = number
  description = "The size of the root disk in bytes"
  default     = 5361393664
}

variable "hostname_formatter" {
  type    = string
  default = "ksnode-%d"
}

variable "num_nodes" {
  type        = number
  default     = 3
  description = "The number of nodes to create (should be > 1)"
}

variable "qcow2_image" {
  type        = string
  description = "Ubuntu image for VMs - only needed for libvirt provider"
  default     = "/bionic-server-cloudimg-amd64.img"
}
