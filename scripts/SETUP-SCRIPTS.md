# Setup Scripts Guide

## Overview

Two setup scripts are available for deploying the messaging system:

1. **setup.sh** - Automated, non-interactive setup
2. **setup-interactive.sh** - Interactive setup with path configuration

## setup.sh - Quick Automated Setup

### When to Use
- Standard directory structure (provider/, consumer-app/, monitoring/ in same directory)
- Quick deployments and testing
- CI/CD pipelines
- Default configuration is acceptable

### Usage
```bash
cd /Users/anuragmishra/Desktop/workspace/messaging
./setup.sh
```

### What It Does
1. Cleans up existing containers
2. Builds broker and consumer applications
3. Builds Docker images
4. Starts services in correct order
5. Imports Grafana dashboards
6. Verifies system health

### Assumptions
- All code is in standard locations relative to script
- JDK is configured and trusts Maven repositories
- Docker Desktop is running with 4GB+ RAM

## setup-interactive.sh - Customizable Setup

> **⚠️ IMPORTANT FOR PRODUCTION**: This script imports the CA certificate into your JDK. This is **MANDATORY** because:
> - Docker builds use the host's JDK to compile code inside containers
> - The Dockerfile runs `RUN ./gradlew :broker:build` which needs Maven Central access
> - Without the certificate, production builds will fail with SSL/certificate errors
> - You will need sudo access to import the certificate

### When to Use
- Custom directory structure
- First-time setup on new machine
- **REQUIRED for production deployments** (CA certificate import)
- Want to verify paths before deployment
- Non-standard project layout

### Usage
```bash
cd /Users/anuragmishra/Desktop/workspace/messaging
./setup-interactive.sh
```

### Interactive Prompts

The script will ask for:

1. **Provider path** (broker Java code)
   - Default: `${CURRENT_DIR}/provider`
   - Contains: Gradle broker project

2. **Consumer path** (consumer app Java code)
   - Default: `${CURRENT_DIR}/consumer-app`
   - Contains: Gradle consumer application

3. **Monitoring path** (Grafana/Prometheus config)
   - Default: `${CURRENT_DIR}/monitoring`
   - Contains: prometheus/, grafana/ subdirectories

4. **Cloud server script path**
   - Default: `${CURRENT_DIR}/cloud-test-server.py`
   - The Python script for data injection

5. **CA Certificate path** (for JDK keychain)
   - Default: `${PROVIDER_PATH}/mvnrepository.com.pem`
   - Certificate to import into JDK trust store

### What It Does (Beyond setup.sh)

1. **Path Configuration**
   - Validates all paths exist
   - Shows configuration summary
   - Asks for confirmation

2. **CA Certificate Import** (**MANDATORY for Production**)
   - Locates JDK installation
   - Finds cacerts file
   - Checks if certificate already imported
   - **Automatically imports certificate** with alias "messaging_maven_ca"
   - Uses sudo for keystore write access
   - **Required**: Docker builds use host JDK to compile code
   - **Failure to import**: Production builds will fail when accessing Maven repositories

3. **docker-compose.yml Updates**
   - Creates timestamped backup
   - Updates build context paths
   - Updates volume mount paths
   - Uses Python regex for precise replacements

4. **Full Deployment**
   - Same as setup.sh after path configuration

### Example Session

```
========================================
  Messaging System Interactive Setup
========================================

========================================
  Step 1: Path Configuration
========================================

Provider path (broker Java code)
Enter path [default: /Users/anuragmishra/Desktop/workspace/messaging/provider]:

Consumer path (consumer app Java code)
Enter path [default: /Users/anuragmishra/Desktop/workspace/messaging/consumer-app]:

Monitoring path (Grafana/Prometheus config)
Enter path [default: /Users/anuragmishra/Desktop/workspace/messaging/monitoring]:

Cloud server script path
Enter path [default: /Users/anuragmishra/Desktop/workspace/messaging/cloud-test-server.py]:

CA Certificate path (for JDK)
Enter path [default: /Users/anuragmishra/Desktop/workspace/messaging/provider/mvnrepository.com.pem]:

Configuration Summary:
  Provider:    /Users/anuragmishra/Desktop/workspace/messaging/provider
  Consumer:    /Users/anuragmishra/Desktop/workspace/messaging/consumer-app
  Monitoring:  /Users/anuragmishra/Desktop/workspace/messaging/monitoring
  Cloud Script: /Users/anuragmishra/Desktop/workspace/messaging/cloud-test-server.py
  CA Cert:     /Users/anuragmishra/Desktop/workspace/messaging/provider/mvnrepository.com.pem

Continue with these paths? (y/n): y

========================================
  Step 2: Validating Paths
========================================
[✓] Provider directory found
[✓] Consumer directory found
[✓] Monitoring directory found
[✓] Cloud server script found
[✓] CA certificate found

========================================
  Step 3: Import CA Certificate to JDK
========================================
[ℹ] Java Home: /Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home
[✓] Found cacerts: /Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home/lib/security/cacerts
[ℹ] Importing CA certificate...
[✓] CA certificate imported successfully

... (continues with build and deployment)
```

## Comparison Table

| Feature | setup.sh | setup-interactive.sh |
|---------|----------|---------------------|
| **Interactive** | No | Yes |
| **Custom Paths** | No (uses defaults) | Yes (prompts user) |
| **CA Import** | No | Yes (automatic) |
| **docker-compose.yml Update** | No | Yes (dynamic) |
| **Path Validation** | No | Yes |
| **Best For** | Quick testing, CI/CD | First setup, custom layouts |
| **Time to Run** | ~3-5 minutes | ~5-8 minutes (includes prompts) |

## Common Use Cases

### Use Case 1: Standard Quick Start
```bash
# You have standard directory structure
# Just want to test the system quickly
./setup.sh
```

### Use Case 2: First Time Setup
```bash
# Need to configure paths
# Need to import Maven CA certificate
# Want to verify everything before building
./setup-interactive.sh
```

### Use Case 3: CI/CD Pipeline
```bash
# Automated deployment
# No interactive prompts
./setup.sh
```

### Use Case 4: Custom Directory Structure
```bash
# Code is in different locations
# Need to update docker-compose.yml automatically
./setup-interactive.sh

# Example prompts:
# Provider: /home/user/projects/messaging-broker
# Consumer: /home/user/projects/messaging-consumers
# Monitoring: /etc/monitoring
# etc.
```

## Troubleshooting

### setup.sh Issues

**Build Fails with SSL/Certificate Error**
```
Solution: Use setup-interactive.sh to import CA certificate
```

**Wrong paths in docker-compose.yml**
```
Solution: Use setup-interactive.sh to configure paths
```

### setup-interactive.sh Issues

**CA Certificate Import Fails (Permission Denied)**
```bash
# The script uses sudo for keytool
# Enter your system password when prompted
```

**Path Validation Fails**
```
# Ensure paths exist before running
# Or enter correct paths at prompts
```

**docker-compose.yml Not Updated**
```bash
# Check Python 3 is installed
python3 --version

# Restore from backup if needed
cp docker-compose.yml.backup.YYYYMMDD_HHMMSS docker-compose.yml
```

## After Setup

Both scripts will display:

```
========================================
  Setup Complete!
========================================

Services:
  • Broker:       http://localhost:8081/health
  • Prometheus:   http://localhost:9090
  • Grafana:      http://localhost:3000 (admin/admin)
  • Cloud Server: http://localhost:8080/health

Grafana Dashboards:
  • Multi-Consumer: http://localhost:3000/d/messaging-broker-multi-consumer
  • Per-Consumer:   http://localhost:3000/d/per-consumer-metrics

System is ready!
```

## Next Steps

1. Open Grafana: http://localhost:3000
2. Login: admin / admin
3. View dashboards to monitor system
4. To use actual data:
   - Edit cloud-test-server.py
   - Replace `inject_actual_data()` function
   - Set `DATA_MODE=PRODUCTION` in docker-compose.yml
   - Run: `docker compose restart cloud-server`

## Getting Help

- **Quick Reference**: See QUICK-START.md
- **Full Documentation**: See DEPLOYMENT-GUIDE.md
- **View Logs**: `docker compose logs -f [service-name]`
- **Check Status**: `docker compose ps`
