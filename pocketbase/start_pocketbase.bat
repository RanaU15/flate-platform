@echo off
title PocketBase 8092 - Facebook Scraper Platform
echo Starting PocketBase on 127.0.0.1:8092...
"%~dp0pocketbase.exe" serve --http=127.0.0.1:8092 --dir="%~dp0pb_data"

