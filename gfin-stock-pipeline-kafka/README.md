# GFIN Stock Pipeline - Kafka VM Deployment

This module manages the automated deployment and synchronization of the Kafka-based stock data pipeline to a Google Cloud Compute Engine Virtual Machine using **Google Cloud Build**.

## Overview

The continuous integration and deployment (CI/CD) pipeline is defined in `cloudbuild.yaml`. It automates the process of moving local code to a remote VM where the Kafka scraper runs.

### Deployment Workflow

1. **Stage Code**: Copies the pipeline source code to a Google Cloud Storage (GCS) bucket.
2. **Provision Infrastructure**: Wakes up/starts the designated Google Compute Engine (GCE) VM instance.
3. **Synchronize**: Connects to the VM remotely via SSH and uses `gsutil rsync` to securely pull the latest code from the GCS bucket directly into the VM's working directory.

## Prerequisites

To run this deployment, ensure you have the following configured in your Google Cloud environment:
- **Google Cloud Build API** enabled.
- **Compute Engine API** enabled.
- A target **Compute Engine VM** created and configured to run Kafka.
- A **Google Cloud Storage (GCS) Bucket** to act as a staging area.
- Proper IAM permissions for the Cloud Build Service Account to manage Compute Engine instances and access Cloud Storage.

## Usage

You can trigger this pipeline manually using the Google Cloud CLI from the root of the project:

```bash
gcloud builds submit --config=gfin-stock-pipeline-kafka/cloudbuild.yaml .
```

Alternatively, it can be connected to a Cloud Build Trigger to automatically deploy on pushes to the `main` branch.

## Security & Configuration

**Note:** For security purposes, project-specific identifiers (such as Project IDs, VM instance names, and bucket names) are meant to be configured via Cloud Build substitution variables (`$_PROJECT_ID`, `$_VM_NAME`, etc.) rather than being hardcoded in the public repository.

## Architecture highlights

- **Idempotent Code Sync**: By using `rsync`, we ensure the VM only downloads changed files, saving bandwidth and time.
- **Cost Control**: The pipeline explicitly starts the VM when needed. A complementary shutdown script/job can be paired with this to ensure the VM doesn't run idle.