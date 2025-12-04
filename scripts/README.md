# Messaging System - Complete Deployment Package

## 📦 What's Included

This package provides a complete messaging broker system with:

- **1 Broker** (300MB memory limit)
- **13 Consumer Services** (200MB each)
- **24 Topic Subscriptions** across all consumers
- **Full Monitoring Stack** (Prometheus + Grafana)
- **Per-Consumer Metrics** with dedicated dashboards
- **Actual Data Injection Support** via cloud-test-server.py

**Total Docker Memory**: ~3.3GB / 4GB limit

## 🚀 Quick Start

Choose your setup method:

### Method 1: Quick Setup (Fastest)
```bash
cd /Users/anuragmishra/Desktop/workspace/messaging
./setup.sh
```
**Best for**: Testing, CI/CD, standard directory structure

### Method 2: Interactive Setup (Recommended for First Time)
```bash
cd /Users/anuragmishra/Desktop/workspace/messaging
./setup-interactive.sh
```
**Best for**: Custom paths, CA certificate import, first-time setup

## 📊 Access After Setup

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Broker Health | http://localhost:8081/health | - |
| Cloud Server | http://localhost:8080/health | - |

### Grafana Dashboards
- **Multi-Consumer**: http://localhost:3000/d/messaging-broker-multi-consumer
- **Per-Consumer**: http://localhost:3000/d/per-consumer-metrics

## 📁 Directory Structure

```
messaging/
├── provider/                    # Broker code (Gradle project)
│   ├── broker/                  # Main broker module
│   ├── mvnrepository.com.pem    # Maven CA certificate
│   └── Dockerfile
├── consumer-app/                # Consumer applications (Gradle project)
│   └── Dockerfile
├── monitoring/                  # Monitoring configuration
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── dashboards/
│       │   ├── messaging-broker-dashboard.json
│       │   └── per-consumer-dashboard.json
│       └── datasources/
├── cloud-test-server.py         # Cloud API simulator / data injection
├── docker-compose.yml           # Service definitions with memory limits
├── setup.sh                     # Quick automated setup
├── setup-interactive.sh         # Interactive setup with path configuration
├── README.md                    # This file
├── QUICK-START.md               # Quick reference guide
├── DEPLOYMENT-GUIDE.md          # Comprehensive documentation
└── SETUP-SCRIPTS.md             # Detailed comparison of setup scripts

```

## 📖 Documentation

| Document | Purpose |
|----------|---------|
| **README.md** (this file) | Overview and quick links |
| **QUICK-START.md** | One-page quick reference |
| **SETUP-SCRIPTS.md** | Detailed comparison of setup scripts |
| **DEPLOYMENT-GUIDE.md** | Full deployment and troubleshooting guide |

## 🔧 Configuration Highlights

### Memory Limits (Total: 3.3GB)
- Broker: 300MB (`-Xms128m -Xmx300m`)
- Each Consumer: 200MB (13 × 200MB = 2.6GB)
- Prometheus: 200MB
- Grafana: 200MB

### 24 Topics Available
```
prices-v1, reference-data-v5, non-promotable-products, prices-v4,
minimum-price, deposit, product-base-document, search-product,
location, location-clusters, selling-restrictions,
colleague-facts-jobs, colleague-facts-legacy,
loss-prevention-configuration, loss-prevention-store-configuration,
loss-prevention-product, loss-prevention-rule-config,
stored-value-services-banned-promotion,
stored-value-services-active-promotion, colleague-card-pin,
colleague-card-pin-v2, dcxp-content, restriction-rules, dcxp-ugc
```

### 13 Consumer Services
1. **price-quote** - 6 topics (prices, reference data, promotable products)
2. **product-svc-lite** - 1 topic (product base document)
3. **search-enterprise** - 1 topic (search product)
4. **tesco-location-service** - 2 topics (location, clusters)
5. **customer-order-on-till** - 1 topic (selling restrictions)
6. **colleague-facts** - 2 topics (jobs, legacy)
7. **loss-prevention-api** - 4 topics (configuration, store config, product, rules)
8. **stored-value-services** - 2 topics (banned/active promotions)
9. **colleague-identity** - 1 topic (card pin)
10. **distributed-identity** - 1 topic (card pin v2)
11. **dcxp-content** - 1 topic (content)
12. **restriction-service-on-tills** - 1 topic (restriction rules)
13. **dcxp-ugc** - 1 topic (user-generated content)

## 📝 Using Actual Data

The system is pre-configured with test data generation. To inject your actual data:

### Step 1: Edit cloud-test-server.py
```python
def inject_actual_data():
    # Replace with your data source
    # Examples provided for: Database, Kafka, CSV, REST API
    
    message = {
        "msgKey": "unique_id",
        "eventType": "MESSAGE",
        "topic": "prices-v1",  # One of 24 topics
        "data": json.dumps(your_data),
        "createdAt": datetime.utcnow().isoformat() + "Z"
    }
    
    with messages_lock:
        messages.append(message)
```

### Step 2: Enable PRODUCTION Mode
Edit `docker-compose.yml`:
```yaml
cloud-server:
  environment:
    - DATA_MODE=PRODUCTION  # Changed from TEST
```

### Step 3: Restart Cloud Server
```bash
docker compose restart cloud-server
```

## 🔍 Common Commands

```bash
# View logs
docker compose logs -f broker
docker compose logs -f consumer-price-quote

# Check status
docker compose ps
curl http://localhost:8081/health

# Restart services
docker compose restart broker
docker compose restart consumer-price-quote

# Stop all
docker compose down

# Clean restart
docker compose down -v && ./setup.sh
```

## 📈 Monitoring Metrics

### Per-Consumer Metrics Available
- `broker_consumer_offset{consumer_id, topic, group}` - Current offset
- `broker_consumer_lag{consumer_id, topic, group}` - Messages behind
- `broker_consumer_messages_sent_total` - Total messages delivered
- `broker_consumer_bytes_sent_bytes_total` - Total bytes delivered
- `broker_consumer_delivery_latency_seconds` - Delivery latency (p50/p95/p99)
- `broker_consumer_active` - Active consumer count

### Query Examples
```bash
# Check active consumers
curl 'http://localhost:9090/api/v1/query?query=broker_consumer_active'

# Check consumer lag
curl 'http://localhost:9090/api/v1/query?query=broker_consumer_lag'
```

## 🆘 Troubleshooting

| Issue | Solution |
|-------|----------|
| Broker won't start | `docker logs messaging-broker`<br>`lsof -i:9092` (kill if occupied) |
| Build fails with SSL error | Use `./setup-interactive.sh` to import CA certificate |
| No consumers connecting | Check broker health: `curl http://localhost:8081/health` |
| High memory usage | Check: `docker stats` |
| No metrics in Grafana | Restart Prometheus & Grafana |
| Wrong paths | Use `./setup-interactive.sh` to configure paths |

## ✅ Verification Checklist

After running setup:

- [ ] Grafana accessible at http://localhost:3000
- [ ] Can login with admin/admin
- [ ] See 2 dashboards in menu
- [ ] Multi-Consumer dashboard shows 24 active subscriptions
- [ ] Per-Consumer dashboard shows individual consumer metrics
- [ ] Prometheus shows broker as UP at http://localhost:9090
- [ ] Cloud-server returns "healthy" at http://localhost:8080/health
- [ ] Broker returns "UP" at http://localhost:8081/health

## 🎯 What Makes This System Special

1. **Per-Consumer Metrics**: Track each consumer's offset, lag, throughput, and latency individually
2. **Memory Optimized**: Runs entire system with 13 consumers in under 4GB
3. **Production Ready**: Easy data injection from any source (DB, Kafka, API, CSV)
4. **Full Observability**: Comprehensive Grafana dashboards with filtering
5. **Easy Setup**: One command deployment with interactive or automated options
6. **CA Certificate Handling**: Automatic import for secure Maven/Gradle builds

## 🏗️ Architecture

```
┌─────────────────┐
│  Cloud Server   │ (Data Injection Point - cloud-test-server.py)
│    (50MB)       │ TEST mode: Random data | PRODUCTION: Your data
└────────┬────────┘
         │ HTTP Poll (/pipe/poll)
         ↓
┌─────────────────┐
│     Broker      │ ← Prometheus scrapes metrics (port 8081)
│    (300MB)      │   Per-consumer tracking enabled
│   Port: 9092    │
└────────┬────────┘
         │ TCP Protocol
         ├──────────┬──────────┬─────────┐
         ↓          ↓          ↓         ↓
    [Consumer1] [Consumer2] ... [Consumer13]
      200MB       200MB           200MB
    (24 total topic subscriptions)
         │          │          │         │
         └──────────┴──────────┴─────────┘
                    │
              ┌─────┴─────┐
              ↓           ↓
         Prometheus   Grafana
          (200MB)    (200MB)
```

## 📦 System Requirements

- **Docker Desktop**: 4.x+ with 4GB RAM allocated
- **Java**: 17 or higher (for builds)
- **Gradle**: 8.x (wrapper included in projects)
- **Python**: 3.x (for cloud-test-server)
- **Ports Required**: 3000, 8080-8102, 9090, 9092

## 🚀 Ready to Deploy!

1. Choose your setup method (quick or interactive)
2. Run the setup script
3. Open Grafana at http://localhost:3000
4. Monitor your system in real-time
5. (Optional) Replace `inject_actual_data()` for production use

**Questions?** See DEPLOYMENT-GUIDE.md for comprehensive documentation.

---

**System Status**: Ready for deployment 🎉

All features implemented:
✅ Automated setup scripts (2 options)
✅ Memory limits configured (3.3GB total)
✅ Per-consumer metrics tracking
✅ Grafana dashboards with filtering
✅ Actual data injection support
✅ CA certificate import for builds
✅ Complete documentation
