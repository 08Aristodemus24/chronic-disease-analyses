provider "aws" {
  region = "ap-southeast-2"
}

resource "aws_s3_bucket" "cdi_analyses_bucket" {
  bucket = "${var.project_name}-bucket"

  tags = {
    Name        = "${var.project_name}-bucket"
    Environment = "Dev"
  }

  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "example" {
  bucket = aws_s3_bucket.cdi_analyses_bucket.id

  # Block public access to buckets and objects granted through new access control lists (ACLs)
  # When set to true causes the following behavior:
  # PUT Bucket ACL and PUT Object ACL calls will fail if the specified ACL allows public access.
  # PUT Object calls will fail if the request includes an object ACL.
  block_public_acls = false

  # Block public access to buckets and objects granted through new public bucket or access point policies
  # When set to true causes Amazon S3 to:
  # Reject calls to PUT Bucket policy if the specified bucket policy allows public access.
  block_public_policy = false

  # Block public access to buckets and objects granted through any access control lists (ACLs)
  ignore_public_acls = true

  # Block public and cross-account access to buckets and objects through any public bucket or access point policies
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "cdi_analyses_bucket_access_policy" {
  bucket = aws_s3_bucket.cdi_analyses_bucket.id

  # we need to have this access policy be jsonified otherwise
  # it will throw a ` Inappropriate value for attribute "policy": string required.`
  policy = data.aws_iam_policy_document.cdi_analyses_bucket_access_policy.json
}

data "aws_iam_policy_document" "cdi_analyses_bucket_access_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.cdi_analyses_bucket.arn,
      "${aws_s3_bucket.cdi_analyses_bucket.arn}/*",
    ]
  }
}