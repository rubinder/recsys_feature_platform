variable "region" {
  type    = string
  default = "us-east-1"
}
variable "env" {
  type    = string
  default = "dev"
}
variable "prefix" {
  type    = string
  default = "recsys"
}
variable "alert_email" {
  description = "AWS Budget alert recipient."
  type        = string
}
variable "budget_limit_usd" {
  type    = number
  default = 25
}
variable "snowflake_credit_quota" {
  type    = number
  default = 10
}
variable "feast_rsa_public_key" {
  description = "Public key body for the Feast service user (make snowflake-keys)."
  type        = string
}
variable "enable_msk" {
  description = "MSK Kafka, ~$70/month while it exists."
  type        = bool
  default     = false
}
variable "enable_elasticache" {
  description = "ElastiCache Redis, ~$12/month while it exists."
  type        = bool
  default     = false
}
variable "enable_eks" {
  description = "EKS + ECR, ~$110/month while it exists (Phase 5 optional)."
  type        = bool
  default     = false
}
variable "eks_public_access_cidrs" {
  type    = list(string)
  default = []
}
