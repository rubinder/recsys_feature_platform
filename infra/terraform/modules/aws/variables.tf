variable "prefix" { type = string }
variable "account_id" { type = string }
variable "region" { type = string }

variable "snowflake_role_name" {
  description = "IAM role Snowflake's storage integration assumes (name fixed so the ARN is known before the integration exists)."
  type        = string
}
variable "snowflake_iam_user_arn" {
  description = "From the Snowflake storage integration (STORAGE_AWS_IAM_USER_ARN)."
  type        = string
}
variable "snowflake_external_id" {
  description = "From the Snowflake storage integration (STORAGE_AWS_EXTERNAL_ID)."
  type        = string
}

variable "budget_limit_usd" {
  type    = number
  default = 25
}
variable "alert_email" {
  description = "Where AWS Budget alerts go."
  type        = string
}

variable "enable_elasticache" {
  description = "ElastiCache Redis (~$12/month). Off by default."
  type        = bool
  default     = false
}
variable "enable_msk" {
  description = "MSK provisioned Kafka, 2 x kafka.t3.small (~$70/month). Off by default."
  type        = bool
  default     = false
}
