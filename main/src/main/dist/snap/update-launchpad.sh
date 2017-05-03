#!/bin/bash

set -e

if [ ! -f snapcraft.yaml ]
then
    echo "snapcraft.yaml not found" >&2
    exit 1
fi
if [ -d .git ]
then
    rm -Rf .git
fi
git init
git checkout -b $SNAP_CHANNEL
ssh -v -o BatchMode=yes -o StrictHostKeyChecking=no $LAUNCHPAD_USER@$LAUNCHPAD_HOST true || true
git remote add origin git+ssh://$LAUNCHPAD_USER@$LAUNCHPAD_HOST/~$LAUNCHPAD_USER/+git/$PACKAGE_NAME-snap
if ! git ls-remote|grep -q refs/heads/$SNAP_CHANNEL
then
    git commit --allow-empty -m "init"
    git push --set-upstream origin $SNAP_CHANNEL
fi
git fetch
git checkout `git log --pretty=oneline origin/$SNAP_CHANNEL | tail -1 | sed 's/ .*$//'`
git stash save -a
git pull origin $SNAP_CHANNEL
rm * -Rf
git stash pop
git add *
if git commit -a -m "build"
then
    git push origin HEAD:$SNAP_CHANNEL
fi