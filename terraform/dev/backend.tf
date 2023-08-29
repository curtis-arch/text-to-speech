terraform {
  backend "remote" {
    hostname = "app.terraform.io"
    organization = "rschatz-training"

    workspaces {
      prefix = "quotient-text2speech-"
    }
  }
}
