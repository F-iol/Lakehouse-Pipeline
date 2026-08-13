terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~>5.0"
    }
  }
}

provider "aws" {
  region = var.location
}
###################### Buckets

resource "aws_s3_bucket" "bronze" {
    bucket = "${var.project_name}-bronze-${var.suffix}"
    force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-silver-${var.suffix}"
force_destroy = true
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-gold-${var.suffix}"
force_destroy = true
}


resource "aws_s3_bucket" "glue_config" {
  bucket = "${var.project_name}-glue-config-${var.suffix}"
  force_destroy = true  
}


#################### CATALOG

resource "aws_glue_catalog_database" "lakehouse_catalog" {
    name = "${var.project_name}_catalog_${var.suffix}"
  
}


################# Permissions

resource "aws_iam_role" "glue_service_role" {
    name = "${var.project_name}_glue_service_role_${var.suffix}"

    assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
  
}

resource "aws_iam_policy" "glue_s3_access" {
    name = "${var.project_name}-glue-s3-access-${var.suffix}"
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.glue_config.arn,
          "${aws_s3_bucket.glue_config.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*",
        ]
      }
    ]
  })
  
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
