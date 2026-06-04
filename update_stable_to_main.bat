git add .
git stash
git switch stable
git rebase main
git push
git switch main
git stash pop