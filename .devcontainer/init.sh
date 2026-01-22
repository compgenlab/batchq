#!/bin/sh
# .devcontainer/init.sh
git_common_dir="$(git rev-parse --git-common-dir)"
case $git_common_dir in
  /*) ;;
  *) git_common_dir=$PWD/$git_common_dir
esac
git_repo_root="$(dirname "$git_common_dir")"
env_file=".devcontainer/.env"
# Use a portable realpath fallback for macOS.
git_repo_root="$(cd "$git_repo_root" && pwd -P)"
printf "GIT_REPO=%s\n" "$git_repo_root" > "$env_file"
