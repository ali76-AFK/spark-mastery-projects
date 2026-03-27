# Project 6 ✓ Logs/Telemetry Anomaly Detection

**1.44M Apache/Nginx logs → outage prediction**

## Live Results (Your Run)
🖥️ 1,440,000 server logs processed (528MB)
🚨 92K+ system alerts (6.4% error rate)
🔥 CRITICAL: web06 | 76,923ms latency (Z=63.8σ)
⚠️ DDoS detected: 503 errors across web01-10
⚡ Runtime: ~2:30min (16GB laptop)
## Anomaly Types Detected
| Incident | Z-Score | Impact |
|----------|---------|---------|
| **Server Outage** | **63.8σ** | 76s latency |
| **DDoS Attack** | **61.7σ** | /admin endpoint |
| **404 Spikes** | **5.2σ** | High error rate |

## SRE Pipeline
```python
rt_zscore = abs((response_time - 296ms) / 1201ms)
filter(rt_zscore > 3 | status_code >= 400)
