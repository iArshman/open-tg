from sys import version_info
from .db import db
from .reaction_handler import MessageReactionsUpdated, on_message_reactions_updated
import git

__all__ = [
    "modules_help",
    "requirements_list",
    "python_version",
    "prefix",
    "gitrepo",
    "userbot_version",
    "on_message_reactions_updated",
    "MessageReactionsUpdated",
]


modules_help = {}
requirements_list = []

python_version = f"{version_info[0]}.{version_info[1]}.{version_info[2]}"

prefix = db.get("core.main", "prefix", ".")

try:
    gitrepo = git.Repo(".")
except git.exc.InvalidGitRepositoryError:
    repo = git.Repo.init()
    origin = repo.create_remote(
        "origin", "https://github.com/iArshman/open-tg"
    )
    origin.fetch()
    repo.create_head("main", origin.refs.main)
    repo.heads.main.set_tracking_branch(origin.refs.main)
    repo.heads.main.checkout(True)
    gitrepo = git.Repo(".")

if len(gitrepo.tags) > 0:
    # Fix: sort tags by committed date, not alphabetically (avoids v1.10 < v1.9 bug)
    sorted_tags = sorted(gitrepo.tags, key=lambda t: t.commit.committed_date)
    commits_since_tag = list(gitrepo.iter_commits(f"{sorted_tags[-1].name}..HEAD"))
else:
    commits_since_tag = []
userbot_version = f"2.5.{len(commits_since_tag)}"
