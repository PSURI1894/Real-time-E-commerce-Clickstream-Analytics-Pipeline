#!/bin/sh
mc alias set myminio http://localhost:9000 admin password123
mc mb myminio/clickstream-lakehouse
