variable "image_path" {
  type        = string
  description = "Where the images will be stored"
  default     = "/code/ubuntu-pool"
}

variable "ssh_key" {
  type        = string
  description = "SSH pub key to use"
  default     = "<contents of ~/.ssh/id_rsa.pub>"
}

# this variable is used in two different ways. One is to create the user
# but we also use the ~/$user/.ssh path to grab the private_key to connect
# to the VMs.

variable "ssh_user" {
  type        = string
  description = "The user that should be created and who has sudo power"
  default     = "gila"
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
  description = "The number of nodes to create"
}
