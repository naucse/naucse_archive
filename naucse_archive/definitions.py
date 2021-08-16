import dataclasses
from pathlib import Path
from fnmatch import fnmatch
import difflib
import io

import yaml


def find_definitions(path, patterns):
    """Find definitions of courses in the current directory.
    """
    for basename, glob in (
        ('courses', '*/link.yml'),
        ('runs', '*/*/link.yml'),
    ):
        base = path / basename
        for p in base.glob(glob):
            slug = str(p.relative_to(base).parent)
            if globs_match(slug, patterns):
                yield {
                    'source': yaml.safe_load(p.read_text()),
                    'slug': slug,
                }


def globs_match(name, patterns):
    return any(fnmatch(name, pattern) for pattern in patterns)
