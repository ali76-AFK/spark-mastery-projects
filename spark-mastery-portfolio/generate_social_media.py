#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
print("📱 Generating 1.44M social media posts...")

# Realistic social media data
hashtags = ['#AI', '#MachineLearning', '#DataScience', '#Python', '#Spark', 
            '#DevOps', '#Cloud', '#Kubernetes', '#Docker', '#BigData']
users = [f"user_{i:06d}" for i in range(10000)]
topics = ['tech', 'ai', 'crypto', 'gaming', 'sports', 'music']

n_posts = 1_440_000
posts = []

base_time = datetime(2026, 3, 20)
for i in range(n_posts):
    timestamp = base_time + timedelta(seconds=np.random.exponential(0.3))
    
    # Viral trends (bursts) + bots
    if np.random.random() < 0.02:  # Viral hashtag burst
        htags = random.choices(['#AI', '#CryptoBoom'], k=3)
        likes = np.random.exponential(5000)
        is_bot = np.random.random() < 0.3
    elif np.random.random() < 0.01:  # Bot network
        htags = ['#CryptoPump', '#BuyNow']
        likes = np.random.exponential(100)
        is_bot = True
    else:  # Normal posts
        htags = random.choices(hashtags, k=random.randint(1,4))
        likes = np.random.exponential(50)
        is_bot = np.random.random() < 0.05
    
    posts.append({
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'user_id': random.choice(users),
        'text': f"Post about {random.choice(topics)} {''.join(htags)}",
        'hashtags': '|'.join(htags),
        'likes': max(0, int(likes)),
        'retweets': int(np.random.exponential(10)),
        'is_bot': is_bot,
        'follower_count': np.random.exponential(1000)
    })

df = pd.DataFrame(posts)
df.to_csv('social_media_posts.csv', index=False)
print(f"💾 Saved: social_media_posts.csv ({df.memory_usage(deep=True).sum()//1024//1024}MB)")
print(f"📊 {len(df):,} posts | Viral: {len(df[df['likes']>1000]):,} | Bots: {df['is_bot'].sum()}")
