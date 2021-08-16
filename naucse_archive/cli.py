from pathlib import Path
import sys
import json

import click
import yaml

from naucse_archive.definitions import find_definitions
from naucse_archive.archival import archive


@click.option(
    "--data", "data_path",
    default=Path.cwd(),
    help="Directory with data files (should contain `runs` and/or `courses` "
         "directories. Default: current directory.",
)
@click.option(
    "-o", "--output", "output_path",
    default=Path.cwd() / 'archived',
    help="Directory to which data will be written. "
        + "Default: 'archived' in the current directory.",
)
@click.option(
    "-c", "--cache", "cache_path",
    default=Path.cwd() / '.cache/naucse/archive',
    help="Directory for a cache. "
        + "Default: '.cache/naucse' in the current directory.",
)
@click.option(
    "--container-tool", "container_tool",
    default='podman',
    help="Container tool to use (`podman` or `docker`). "
        + "(Note that docker is untested, please report any issues with it.)",
)
@click.argument(
    "course_slugs", metavar="COURSE_SLUGS",
    nargs=-1,
)
@click.command()
def main(data_path, output_path, course_slugs, container_tool, cache_path):
    """Archive course(s) that use naucse_render 0.x

    Courses can be selected by passing their slugs as positional arguments.
    Globs are accepted; default is '*' (all found courses).
    """
    output_path = Path(output_path).resolve()
    data_path = Path(data_path).resolve()
    cache_path = Path(cache_path).resolve()

    if not course_slugs:
        course_slugs = ['*']

    courses = list(find_definitions(data_path, course_slugs))
    if not courses:
        print("No courses match", file=sys.stderr)
        exit(1)

    result = {}

    for course in courses:
        try:
            slug, info = archive(course, data_path, output_path, cache_path, container_tool)
        except:
            print(f'Error archiving {course["slug"]}', file=sys.stderr)
            raise
        result[slug] = info

    print(yaml.safe_dump(result))
