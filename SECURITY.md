# Security Policy

## Reporting a Vulnerability

If you discover a potential security vulnerability in this project, please report it responsibly by opening a [GitHub Security Advisory](https://github.com/sparkdq-community/sparkdq/security/advisories/new) rather than a public issue.

Include as much detail as possible:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a minimal proof of concept
- The affected version(s)

We will acknowledge the report and work toward a fix. Public disclosure should be coordinated with the maintainers.

## Supported Versions

We recommend always using the latest stable release. Only the latest release receives security patches.

| Version | Supported |
| ------- | --------- |
| Latest  | Yes       |
| Older   | No        |

## Scope

SparkDQ is a data validation framework. It does not handle authentication, authorization, or persistent data storage. Users are responsible for securing:

- Data sources and sinks (e.g., S3, HDFS, JDBC connections)
- Access to Spark clusters and execution environments
- Configuration files and secrets

The framework does not process or store sensitive data outside of Spark memory unless explicitly written by the user.
