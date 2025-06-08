# MLDetection

> **Flow‑based anomaly detection for Zeek, powered by Isolation Forest**

This repo lets you stream Zeek `conn.log` lines over SSH, score them with an unsupervised model, and ship live alerts directly into Elasticsearch/Kibana.  Everything runs off‑sensor on a small Ubuntu VM so your Zeek box stays clean.

---

## 0  Prerequisites

| Component     | Tested version                 | Notes                                              |
| ------------- | ------------------------------ | -------------------------------------------------- |
| Ubuntu Server | 22.04 LTS                      | Any modern Debian works – PEP 668 requires a venv  |
| Python        |  3.12                          | Comes with Ubuntu 22.04                            |
| Zeek sensor   | 5.x                            | Remote host with `/opt/zeek/logs/current/conn.log` |
| Elasticsearch | 8 +                            | or OpenSearch; adjust URL                          |
| API key       | Kibana → Stack Mgmt → API Keys | grants `write` to index `mlids-*`                  |

---

## 1  Provision the Analytics VM

```bash
# basic OS packages
sudo apt update
sudo apt install python3-venv build-essential libffi-dev libssl-dev \
                 libjpeg-dev zlib1g-dev git

# project checkout & virtual‑env
cd /opt
sudo git clone https://github.com/YOURORG/MLDetection.git
sudo chown -R $(whoami): $(basename $_)
cd MLDetection
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### .env (SSH + Elasticsearch)

```ini
# Zeek sensor
ZEEK_HOST=10.31.0.224
ZEEK_USER=zeek
ZEEK_PASS=zeek
ZEEK_REMOTE_ROOT=/opt/zeek/logs

# live scorer
IDS_THRESHOLD=-0.15           # optional

# Elasticsearch
ES_URL=https://elk.example.net:9200
ES_API_KEY=<Base64Key>
```

> **Self‑signed ES cert?**  Add `REQUESTS_CA_BUNDLE=/path/to/ca.pem` here or set `verify=False` in `serve_iso.py` (lab only).

---

## 2  First‑time ETL + model

```bash
# pull yesterday’s Zeek folder
python -m mldetection.io    # defaults to yesterday
# parse → parquet
python prepare_logs.py      # defaults to yesterday
# train Isolation Forest on all parquet files
python train_iso.py
```

`ids_iso.joblib` appears in repo root.

---

## 3  systemd services & timers

### 3.1  Live scorer

`/etc/systemd/system/serve-iso.service`

```ini
[Unit]
Description=MLDetection live Isolation-Forest scorer
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/MLDetection
EnvironmentFile=/opt/MLDetection/.env
ExecStart=/opt/MLDetection/.venv/bin/python /opt/MLDetection/serve_iso.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now serve-iso.service
journalctl -u serve-iso.service -f   # follow alerts
```

### 3.2  Nightly download & prepare, weekly retrain

Create **mldownload**, **mlprepare**, **mltrain** service/timer pairs in `/etc/systemd/system/`.

Example `mldownload.timer`:

```ini
[Unit]
Description=Download yesterday’s Zeek folder
[Timer]
OnCalendar=*-*-* 02:05:00
Persistent=true
Unit=mldownload.service
[Install]
WantedBy=timers.target
```

(Companion *.service* just runs `python -m mldetection.io` in WorkingDirectory.)

Enable all three timers:

```bash
sudo systemctl enable --now mldownload.timer mlprepare.timer mltrain.timer
systemctl list-timers --all | grep ml
```

---

## 4  Elasticsearch dashboard

1. Discover → create pattern `mlids-*`, set timestamp = `ts`.
2. Lens → histogram on `score`, terms on `src`, etc.
3. Add ILM policy to roll indices after 30 days.

---

## 5  Troubleshooting cheat‑sheet

| Symptom                                       | Fix                                                                                             |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `Failed to load environment files` in journal | wrong path in `EnvironmentFile=`                                                                |
| `AuthenticationException` from Paramiko       | bad SSH creds / key;                                                                            |
| verify Zeek user has sftp access              |                                                                                                 |
| `KeyError: 'orig_bytes'`                      | make sure you’re tailing **conn.log** only; patch uses guard but you can raise threshold        |
| Alerts not in Kibana                          | `curl -k -XGET "$ES_URL/mlids-*/_count" -H"Authorization: ApiKey $ES_API_KEY"` to verify ingest |

---

Enjoy realtime anomaly detection on your Zeek traffic!  Pull requests and issues welcome.
