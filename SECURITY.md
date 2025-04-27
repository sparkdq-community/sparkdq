# 🛡️ Security Policy

## Reporting a Vulnerability

If you discover a potential security issue in this project, please report it responsibly.

Provide as much information as possible to help us reproduce and verify the issue.

We will work on a timely fix.

## Supported Versions

We recommend always using the latest stable release to benefit from security patches and improvements.

## Scope of the Project

This project is a data validation framework and does not handle authentication, authorization, or data storage. Users are responsible for securing:

* Data sources and sinks (e.g., S3, HDFS, JDBC).

* Access to Spark clusters and execution environments.

* Configuration files and secrets.

The framework does not process or store sensitive data outside of Spark memory unless explicitly written by the user.

