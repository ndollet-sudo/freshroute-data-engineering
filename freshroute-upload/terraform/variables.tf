variable "organization_name" { type = string }
variable "account_name" { type = string }
variable "admin_user" { type = string }

variable "private_key_path" {
  type    = string
  default = "../secrets/admin_rsa_key.p8"
}

variable "service_user" {
  type        = string
  default     = "FRESHROUTE_SVC"
  description = "Utilisateur technique (key pair) utilisé par Docker et dbt"
}

variable "monthly_credit_quota" {
  type    = number
  default = 10
}
