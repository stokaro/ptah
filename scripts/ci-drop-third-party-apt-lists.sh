#!/usr/bin/env bash
# Remove the third-party apt sources the hosted runner image adds.
#
# The GitHub Ubuntu images ship apt sources for Google Chrome and Microsoft.
# Nothing in this repository installs from either: graphviz, busybox-static and
# the Chromium dependencies `playwright install --with-deps` pulls all come from
# Ubuntu's own archive.
#
# When one of those repositories republishes its index, apt fetches a
# Packages.gz that does not match the hash its Release file announces and
# `apt-get update` exits 100 -- after having refreshed every list that mattered.
# The job then fails on a repository it was never going to install from. On
# 2026-09-09 dl.google.com served such a snapshot for over half an hour and
# reddened master three times over: Docs, Install Smoke and Integration Tests,
# each on the same "Hash Sum mismatch", each with the Ubuntu archive fetched and
# intact. Two re-runs failed identically, which is what says this is not a blip
# to wait out.
#
# The repositories are named here rather than classified. A rule that decided
# which sources to KEEP would have to recognize Ubuntu's own, and on noble that
# file names no host to recognize -- `URIs: mirror+file:/etc/apt/apt-mirrors.txt`
# -- so the failure mode of a clever rule is deleting the archive itself.
# Removing only what is named cannot do that. The worst it can do is miss a
# repository a future image adds, which fails the same visible way this failed,
# and is fixed by one more name.
#
# Each name is matched with a trailing glob because the image writes the two in
# different formats -- Chrome as deb822 `google-chrome.sources`, Microsoft as
# one-line `microsoft-prod.list` -- and either may change to the other.
#
# This has to run before apt does, and before `playwright install --with-deps`,
# whose own apt call takes no argument of ours. A no-op where there is no apt,
# so the macOS leg of the install-smoke matrix can call it unconditionally.
set -euo pipefail

THIRD_PARTY_SOURCES="google-chrome microsoft-prod"

if [ ! -d /etc/apt/sources.list.d ]; then
	echo "ci-drop-third-party-apt-lists: no apt on this runner, nothing to do"
	exit 0
fi

removed=0
for name in $THIRD_PARTY_SOURCES; do
	for source in "/etc/apt/sources.list.d/$name."*; do
		[ -f "$source" ] || continue
		echo "ci-drop-third-party-apt-lists: removing $source"
		sudo rm -f "$source"
		removed=$((removed + 1))
	done
done

# What is left is printed rather than judged. A repository this does not know
# about is the next version of the same outage, and the log is where a reader
# finds its name.
echo "ci-drop-third-party-apt-lists: ${removed} removed; sources remaining:"
for source in /etc/apt/sources.list /etc/apt/sources.list.d/*; do
	[ -f "$source" ] || continue
	echo "    $source"
done
