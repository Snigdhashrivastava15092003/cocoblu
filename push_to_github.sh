#!/bin/bash

echo "🚀 Pushing to GitHub..."
echo ""

# Check if git is initialized
if [ ! -d .git ]; then
    echo "→ Initializing git repository..."
    git init
fi

# Check if remote exists
if ! git remote | grep -q origin; then
    echo "→ Adding remote origin..."
    git remote add origin https://github.com/SeesecProjectsInfo/Cocoblu-RetailAgent.git
else
    echo "→ Remote origin already exists"
fi

# Add all files
echo "→ Adding files..."
git add .

# Commit
echo "→ Committing..."
git commit -m "Product comparison system with AI-powered similarity analysis

Features:
- Amazon scraping via Lambda API
- Flipkart scraping with Playwright
- 7-step AI comparison (Bedrock GenAI)
- DynamoDB storage
- Local JSON output
- Automatic environment loading"

# Push
echo "→ Pushing to GitHub..."
git push -u origin main 2>&1 || git push -u origin master 2>&1

echo ""
echo "✅ Done! Check: https://github.com/SeesecProjectsInfo/Cocoblu-RetailAgent"
