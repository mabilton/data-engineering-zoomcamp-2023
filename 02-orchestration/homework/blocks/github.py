import argparse

from prefect.filesystems import GitHub


def create_github_block(repo: str, block_name: str) -> None:
    gh_block = GitHub(repository=repo, include_git_objects=False)
    gh_block.save(block_name)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates a Github block in Prefect.")
    parser.add_argument(
        "--repo", required=True, help="URL to Github repository.", type=str
    )
    parser.add_argument(
        "--block_name",
        required=True,
        help="Name to give to created Github block.",
        type=str,
    )
    args_dict = vars(parser.parse_args())
    create_github_block(**args_dict)
