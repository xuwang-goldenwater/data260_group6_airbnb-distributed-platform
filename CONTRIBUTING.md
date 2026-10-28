# How we work in this repo

## One-time setup
```bash
git clone https://github.com/xuwang-goldenwater/data260_group6_airbnb-distributed-platform.git
cd data260_group6_airbnb-distributed-platform
```

## Every task
1. Start from the latest main
   ```bash
   git switch main
   git pull
   ```
2. Create a branch: `<role>/<task>`, e.g. `a/user-property-api`, `b/booking-worker`
   ```bash
   git switch -c a/user-property-api
   ```
3. Work, then commit with your own GitHub account
   ```bash
   git add .
   git commit -m "API doc: user and property endpoints"
   git push -u origin a/user-property-api
   ```
4. On GitHub, click **Compare & pull request** → write one line on what changed → **Create pull request**.
5. Ask one teammate to review. After 1 approval, click **Merge pull request**, then **Delete branch**.

## Rules
- Never push to main directly (it is blocked).
- Only edit files you own. For the API doc: only your file in `docs/api/`.
- Pull main before starting each new task.
- Commit with your own account — the contributions page (§12) is built from commit history.
- Never commit `.env`, passwords, or API keys.

## If main changed while you were working
```bash
git switch main
git pull
git switch <your-branch>
git merge main
```
Fix any conflicts, commit, push again.
