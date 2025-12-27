# Hermes Broker - GCP/GKE Operations Guide

This documentation provides the operational procedures for managing the Hermes Broker distributed cluster on **Google Kubernetes Engine (GKE)**. It utilizes **StatefulSets** and **Persistent Disks** to ensure the resilience of the Raft consensus algorithm.

## Prerequisites

* **Google Cloud SDK (gcloud CLI)**: Installed, authenticated, and updated.
* **kubectl**: Installed and configured to communicate with GKE.
* **Project ID**: `oval-bricolage-482523-t7`
* **Region/Zone**: `southamerica-east1` / `southamerica-east1-a`

---

## Initial Deployment (Day Zero)

Follow these steps only the first time you set up the environment or if you have deleted the entire project.

### Project & API Setup

```bash
# Set the active project
gcloud config set project oval-bricolage-482523-t7

# Enable required Google APIs
gcloud services enable container.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com

# Link billing account (if not already linked)
# gcloud billing projects link oval-bricolage-482523-t7 --billing-account=[ACCOUNT_ID]

```

### Infrastructure Provisioning

```bash
# Create the Docker Artifact Registry
gcloud artifacts repositories create hermes-repo \
    --repository-format=docker \
    --location=southamerica-east1 \
    --description="Hermes Broker Image Repository"

# Create the GKE Cluster (Standard mode for Raft Quorum)
gcloud container clusters create hermes-cluster \
    --zone=southamerica-east1-a \
    --num-nodes=3 \
    --machine-type=e2-medium \
    --disk-size=20GB \
    --scopes=cloud-platform

```

### First-Time Deployment

```bash
# 1. Build and push the initial image to the Registry
gcloud builds submit --config cloud-build/cloudbuild.yaml .

# 2. Apply Network Services (Internal Headless and Client LoadBalancer)
kubectl apply -f cloud-build/internal-svc.yaml
kubectl apply -f cloud-build/client-svc.yaml

# 3. Apply the StatefulSet (Provisioning Pods and SSD Disks)
kubectl apply -f cloud-build/statefulset.yaml

```

---

## Deploying a New Version

Whenever you modify the Go source code or update the configuration files and want to push the changes to production:

```bash
# Execute the automated Build & Deploy pipeline
gcloud builds submit --config cloud-build/cloudbuild.yaml .

```

> **Note**: The `cloudbuild.yaml` triggers a **Rolling Update**. Kubernetes will restart one node at a time (starting from the highest index) to ensure the Raft cluster never loses quorum during the update.

---

## Cost Management (Scaling to Zero)

Since GKE charges per hour for active Compute Engine instances, use these commands to stop costs when you are not actively testing.

### To Shut Down (Scale to Zero)

This removes the virtual machines but **keeps your Persistent Volumes (SSDs) intact**. Your data and Raft state are safe.

```bash
# 1. Resize the GKE node pool to zero
gcloud container clusters resize hermes-cluster --num-nodes=0 --zone=southamerica-east1-a

```

### To Power Up (Resume Testing)

The Google Cloud will provision new VMs, reattach the existing SSDs, and the Raft cluster will automatically recover the state from the last snapshot.

```bash
# 1. Scale the cluster back to 3 nodes
gcloud container clusters resize hermes-cluster --num-nodes=3 --zone=southamerica-east1-a

# 2. Verify pods are initializing
kubectl get pods -w

```

---

## Essential Troubleshooting Commands

| Command | Description |
| --- | --- |
| `kubectl get pods -w` | Monitor pod lifecycle and status in real-time. |
| `kubectl logs -f hermes-0` | Follow the Leader's logs (Heartbeats, Elections). |
| `kubectl get pvc` | Ensure Persistent Volume Claims are "Bound" to disks. |
| `kubectl port-forward svc/hermes-broker 50051:50051` | Tunnel gRPC traffic to your local machine. |
| `kubectl exec -it hermes-0 -- ls -lh /root/data` | Check Raft and Broker database files on disk. |

---