variable "prefix" { type = string }
variable "lake_bucket" { type = string }
variable "aws_role_arn" {
  description = "IAM role the storage integration assumes (created by the aws module)."
  type        = string
}
variable "feast_rsa_public_key" {
  description = "PEM body (no header/footer) of the Feast service user's public key."
  type        = string
}
variable "credit_quota" {
  description = "Monthly credit cap; the warehouse suspends at 100%."
  type        = number
  default     = 10
}
