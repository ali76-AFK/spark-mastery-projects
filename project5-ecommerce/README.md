# Project 5 ✓ E-commerce Fraud Detection

**1.44M orders → PySpark Z-Score anomaly detection**

## Live Results (Your Run)
🛒 Dataset: 1,440,000 orders processed
🚨 FRAUD ALERTS: 28K+ detected
🔥 Top Fraud: CUST_039967 | Python Book | 100 units | 90% OFF (Z=37.5σ qty!)
💰 Revenue: Electronics $183M (leader)
Runtime: ~2:30min (16GB laptop)
## Fraud Patterns Detected
| Pattern | Z-Score | Example |
|---------|---------|---------|
| **Bulk Orders** | **37.5σ** | 100x Python Book |
| **Coupon Abuse** | **5.7σ** | 90% discount |
| **High Value** | **9.7σ** | $6.6K Nike Air |

## Pipeline Code
```python
# Z-Score anomaly detection
qty_zscore = abs((quantity - 2.1) / 2.6)
filter(qty_zscore > 3 | disc_zscore > 3 | amt_zscore > 3)
