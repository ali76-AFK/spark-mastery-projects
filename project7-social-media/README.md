# Project 7 ✓ Social Media Trend Detection

**1.44M posts → viral trends + bot networks**

## Live Results (Your Run)
📱 1,440,000 social posts processed (426MB)
🚨 VIRAL TRENDS: 23K+ posts detected
🤖 BOT NETWORKS: 92K accounts (6.4%)
🔥 Top trend: 49,535 likes (Z=50σ engagement!)
Runtime: ~2:30min (16GB laptop)
## Threat Patterns Detected
| Pattern | Count | Business Impact |
|---------|-------|-----------------|
| **Viral Bursts** | **23K** | Trend amplification |
| **Bot Networks** | **92K** | Fake engagement |
| **#CryptoPump** | **High** | Market manipulation |

## ML Pipeline
```python
engagement = (likes × 0.7 + retweets × 0.3) / followers
likes_zscore = abs((likes - 149) / 985)
filter(likes_zscore > 3 | is_bot == True)
