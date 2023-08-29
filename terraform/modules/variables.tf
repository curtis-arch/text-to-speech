variable "namespace" {
  description = "A namespace to use for all AWS resources."
  type = string
}

variable "project" {
  description = "A meaningful project name."
  type = string
}

variable "tags" {
  description = "Standard tags for the AWS resources."
  type = map(string)
}
