locals {
  namespace = "quotient"
  project = "text2speech"
}

module "infrastructure" {
  source = "../modules"

  namespace = local.namespace
  project = local.project

  tags = {
    Terraform   = "true"
    Namespace   = local.namespace
    Project     = local.project
  }
}
