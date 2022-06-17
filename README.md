# f2cloud

## execution
S3 keys can be read from the env as follows:
 * `s3-access-key` from `AWS_ACCESS_KEY_ID`
 * `s3-secret-key` from `AWS_SECRET_ACCESS_KEY`

 in that case, both must come from the env!

## dependencies
The following package are required to build:
 * pkg-config
 * nettle-dev
 * libssl-dev
 * libfuse-dev