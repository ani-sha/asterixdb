#!/bin/bash
echo "🔥 Deep cleaning AppleDouble + target/ directories..."
find . -name "._*" -type f -print -delete
find . -name ".DS_Store" -type f -print -delete
rm -rf $(find . -type d -name "target")
