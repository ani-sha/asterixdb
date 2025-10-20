#!/bin/bash
echo "🧹 Cleaning ._ AppleDouble files..."
find . -name "._*" -type f -print -delete
