#!/usr/bin/env python3
"""
Initialize collections on isolated PocketBase instance: http://127.0.0.1:8092
"""
import requests
import sys

PB_URL = "http://127.0.0.1:8092"
EMAIL = "admin@local.test"
PASSWORD = "Pb@Scr4p3r#2026!xK"

s = requests.Session()

print(f"Connecting to PocketBase at {PB_URL}...")
r = s.post(f"{PB_URL}/api/collections/_superusers/auth-with-password",
           json={"identity": EMAIL, "password": PASSWORD}, timeout=15)
if r.status_code != 200:
    print(f"Authentication failed: {r.status_code} {r.text}")
    sys.exit(1)

token = r.json()["token"]
s.headers["Authorization"] = token
print("Authenticated successfully as superuser!")

collections_to_create = [
    {
        "name": "scraping_runs",
        "type": "base",
        "listRule": "",
        "viewRule": "",
        "createRule": "",
        "updateRule": "",
        "deleteRule": "",
        "fields": [
            {"name": "actor_name", "type": "text", "required": False},
            {"name": "group_url", "type": "text", "required": True},
            {"name": "group_name", "type": "text", "required": False},
            {"name": "group_id", "type": "text", "required": False},
            {"name": "post_limit", "type": "number", "required": True},
            {"name": "scraped_count", "type": "number", "required": False},
            {"name": "status", "type": "text", "required": False},  # pending, running, completed, failed, cancelled
            {"name": "start_time", "type": "date", "required": False},
            {"name": "end_time", "type": "date", "required": False},
            {"name": "duration", "type": "text", "required": False},
            {"name": "error", "type": "text", "required": False},
            {"name": "pid", "type": "number", "required": False},
        ],
        "indexes": [
            "CREATE INDEX `idx_runs_status` ON `scraping_runs` (`status`)"
        ]
    },
    {
        "name": "posts",
        "type": "base",
        "listRule": "",
        "viewRule": "",
        "createRule": "",
        "updateRule": "",
        "deleteRule": "",
        "fields": [
            {"name": "run_id", "type": "text", "required": False},
            {"name": "group_id", "type": "text", "required": False},
            {"name": "group_name", "type": "text", "required": False},
            {"name": "group_url", "type": "text", "required": False},
            {"name": "city", "type": "text", "required": False},
            {"name": "body", "type": "text", "required": False},
            {"name": "author", "type": "text", "required": False},
            {"name": "author_profile_url", "type": "text", "required": False},
            {"name": "post_type", "type": "text", "required": False},
            {"name": "permalink", "type": "text", "required": False},
            {"name": "post_url", "type": "text", "required": False},
            {"name": "facebook_post_id", "type": "text", "required": False},
            {"name": "temporary_id", "type": "text", "required": False},
            {"name": "source", "type": "text", "required": False},
            {"name": "image_urls", "type": "json", "required": False},
            {"name": "images", "type": "json", "required": False},
            {"name": "image_count", "type": "number", "required": False},
            {"name": "scraped_at", "type": "date", "required": False},
            {"name": "posted_at", "type": "date", "required": False},
        ],
        "indexes": [
            "CREATE UNIQUE INDEX `idx_posts_temporary_id` ON `posts` (`temporary_id`)",
            "CREATE INDEX `idx_posts_run_id` ON `posts` (`run_id`)",
            "CREATE INDEX `idx_posts_facebook_post_id` ON `posts` (`facebook_post_id`)"
        ]
    },
    {
        "name": "scraping_run_logs",
        "type": "base",
        "listRule": "",
        "viewRule": "",
        "createRule": "",
        "updateRule": "",
        "deleteRule": "",
        "fields": [
            {"name": "run_id", "type": "text", "required": True},
            {"name": "level", "type": "text", "required": False},
            {"name": "message", "type": "text", "required": False},
            {"name": "timestamp", "type": "date", "required": False},
        ],
        "indexes": [
            "CREATE INDEX `idx_run_logs_run_id` ON `scraping_run_logs` (`run_id`)"
        ]
    }
]

for col in collections_to_create:
    col_name = col["name"]
    res = s.get(f"{PB_URL}/api/collections/{col_name}")
    if res.status_code == 200:
        print(f"Collection '{col_name}' already exists. Updating...")
        cid = res.json()["id"]
        up_res = s.patch(f"{PB_URL}/api/collections/{cid}", json=col)
        print(f"Collection '{col_name}' update status: {up_res.status_code}")
    else:
        print(f"Creating collection '{col_name}'...")
        c_res = s.post(f"{PB_URL}/api/collections", json=col)
        if c_res.status_code in (200, 201):
            print(f"Collection '{col_name}' created successfully!")
        else:
            print(f"Failed to create '{col_name}': {c_res.status_code} {c_res.text}")

print("All schemas initialized successfully on Port 8092!")

