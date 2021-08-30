from functools import partial
from pathlib import Path
import contextlib
import subprocess
import textwrap
import tempfile
import datetime
import hashlib
import shutil
import json
import time
import sys
import re
import os

from naucse_archive import fixes

HOUR = 3600
REFETCH_TIME = HOUR
FETCH_DEPTH = 10

CONTAINER_PYTHON_COMMAND = '/naucse/env/bin/python'
PIP_CACHE_DIR = '.naucse-archive/pip-cache'

def printerr(*args, **kwargs):
    """print to stderr"""
    print(*args, **kwargs, file=sys.stderr)

def _quote_cmd_word(word):
    """Quote a word of a shell command"""
    word = str(word)
    if re.match('^[-_.=/:a-zA-Z0-9]+$', word):
        return word
    word_repl = word.replace("'", r"'\''")
    return f"'{word_repl}'"

def run(*cmd, check=True, encoding='utf-8', **kwargs):
    """Run the given command.

    Like subprocess.run(), with different defaults and logging to stderr"""
    printerr('$', ' '.join(_quote_cmd_word(c) for c in cmd))
    start_time = time.time()
    env = {
        **kwargs.pop('env', os.environ),
        'GIT_CONFIG_GLOBAL': '/dev/null',
        'GIT_CONFIG_SYSTEM': '/dev/null',
    }
    try:
        proc = subprocess.run(cmd, check=check, encoding=encoding, env=env, **kwargs)
    except subprocess.CalledProcessError as e:
        returncode = e.returncode
        raise
    else:
        returncode = proc.returncode
    finally:
        elapsed = time.time() - start_time
        printerr(_quote_cmd_word(cmd[0]), '->', returncode, f'({elapsed:.2f}s)')
    return proc


def archive(course_def, data_path, output_path, cache_path, container_command):
    """Archive a single course."""
    slug = course_def['slug']
    repo = course_def['source']['repo']
    branch = course_def['source']['branch']
    archive_branch = f'archive/{slug}'

    remote_name = git_config_key(repo)
    branch_ref = f'refs/remotes/{remote_name}/{branch}'

    (data_path / PIP_CACHE_DIR).mkdir(exist_ok=True, parents=True)

    with contextlib.ExitStack() as context:
        fetch(data_path, repo, remote_name)
        commit_id = get_commit_id(data_path, branch_ref)
        worktree = make_worktree(data_path, branch_ref, context)
        get_image = choose_get_image(worktree)
        image = get_image(data_path, worktree, cache_path, container_command)

        result_path = context.enter_context(tempdir_path())
        save_env_info(container_command, worktree, commit_id, image, slug, result_path)
        save_course(container_command, worktree, course_def, commit_id, image, slug, result_path)

        dest_path = output_path / slug
        if dest_path.exists():
            shutil.rmtree(dest_path)
        dest_path.parent.mkdir(exist_ok=True, parents=True)
        shutil.copytree(result_path, dest_path)

    return slug, {
        'path': slug,
        'url': '<url here>',
        'branch': 'main',
    }


def fetch(data_path, repo, remote_name):
    """Fetch the given Git remote

    A remote is created with the given `repo` URL, if it doesn't already exist.
    To avoid overburdening servers, fetches within REFETCH_TIME are ignored.
    (The last fetch time is stored in Git config; remove the entry to force
    refetch.)
    """
    config_key = f'naucse.last_fetch.{remote_name}'
    proc = run(
        'git', 'config', config_key,
        cwd=data_path,
        check=False,
        stdout=subprocess.PIPE
    )
    last_fetch = proc.stdout.strip()
    now = datetime.datetime.now()
    if last_fetch:
        last_fetch_date = datetime.datetime.fromisoformat(last_fetch)
        refetch_delta = datetime.timedelta(seconds=REFETCH_TIME)
        if last_fetch_date + refetch_delta > now:
            return

    proc = run(
        'git', 'remote', 'add', remote_name, repo,
        cwd=data_path,
        check=False,
    )
    if proc.returncode == 3:
        run(
            'git', 'fetch', remote_name,
            cwd=data_path,
        )
    else:
        run(
            'git', 'fetch', remote_name,
            '--depth', str(FETCH_DEPTH),
            cwd=data_path,
        )
    run(
        'git', 'config', config_key, now.isoformat(),
        cwd=data_path,
        check=False,
        stdout=subprocess.PIPE
    )


def get_commit_id(data_path, branch_ref):
    """Get the Git ID (hash) for a given reference (e.g. branch name)"""
    proc = run(
        'git', 'rev-parse', branch_ref,
        cwd=data_path,
        stdout=subprocess.PIPE,
    )
    return proc.stdout.strip()


def make_worktree(data_path, branch_ref, context):
    """Make a Git worktree with the given reference.

    The directory is removed when the `context` ExitStack exits.
    """
    worktree_path = context.enter_context(tempdir_path())
    run(
        'git', 'worktree', 'add', worktree_path, branch_ref,
        cwd=data_path,
        check=False,
    )
    context.callback(
        run, 'git', 'worktree', 'remove', '-f', worktree_path,
        cwd=data_path,
    )
    return Path(worktree_path)


def choose_get_image(worktree):
    """Choose which to use for archiving. Return the function to create it."""
    if (worktree / 'Pipfile.lock').exists():
        return get_image_micropipenv
    elif (worktree / 'requirements.txt').exists():
        return get_image_piptools


def get_image_micropipenv(data_path, worktree, cache_path, container_command):
    """Get a container image for building, using micropipenv for requirements."""
    lockfile = worktree / 'Pipfile.lock'
    with lockfile.open(encoding='utf-8') as f:
        data = json.load(f)
    try:
        python_version = str(
            data['_meta']['requires']['python_version']
        )
    except (TypeError, KeyError):
        # must be an old pipfile, use an old Python version
        python_version = '3.6'
    base_name = get_python_image(container_command, python_version)
    name = f'localhost/naucse-py{python_version}-micropipenv'
    with ImageMaker(container_command, name) as imgm:
        imgm.write(f'FROM {base_name}')
        imgm.write(f'RUN {CONTAINER_PYTHON_COMMAND} -m pip install micropipenv')
        imgm.add_build_args(
            '-v', f'{data_path / PIP_CACHE_DIR}:/naucse/pip-cache:rw,Z',
        )
    proc = run(
        container_command, 'run',
        '--rm',
        '-v', f'{worktree}:/naucse/wd:O',
        '-v', f'{data_path / PIP_CACHE_DIR}:/naucse/pip-cache:rw,Z',
        name,
        CONTAINER_PYTHON_COMMAND, '-m', 'micropipenv',
        'requirements', '--no-dev',
        stdout=subprocess.PIPE,
    )
    return get_image_from_requirements(container_command, data_path, python_version, proc.stdout)

def get_image_piptools(data_path, worktree, cache_path, container_command):
    """Get a container image for building, using `piptools compile` for requirements."""
    python_version = '3.6'
    base_name = get_python_image(container_command, python_version)
    name = f'localhost/naucse-py{python_version}-piptools'
    with ImageMaker(container_command, name) as imgm:
        imgm.write(f'FROM {base_name}')
        imgm.write(f'RUN {CONTAINER_PYTHON_COMMAND} -m pip install pip-tools')
        imgm.add_build_args(
            '-v', f'{data_path / PIP_CACHE_DIR}:/naucse/pip-cache:rw,U,Z',
        )
    reqs = (worktree / 'requirements.txt').read_text()
    reqs = fixes.fix_old_requirements_txt(reqs)
    (worktree / 'requirements-fixed.txt').write_text(reqs)
    print('requirements-fixed.txt:')
    print(textwrap.indent(reqs, '    '))
    # piptools takes a lot of time; cache the result
    reqs_hash = hashlib.sha256(reqs.encode()).hexdigest()
    cache_dir = cache_path / f'piptools-{python_version}-{reqs_hash}'
    output_path = cache_dir / 'output.txt'
    if output_path.exists():
        result = output_path.read_text()
    else:
        proc = run(
            container_command, 'run',
            '--rm',
            '-v', f'{worktree}:/naucse/wd:O',
            '-v', f'{data_path / PIP_CACHE_DIR}:/naucse/pip-cache:rw,U,Z',
            name,
            CONTAINER_PYTHON_COMMAND, '-m', 'piptools', 'compile',
            '--generate-hashes',
            '--output-file=-',
            'requirements-fixed.txt',
            stdout=subprocess.PIPE,
            input=reqs,
        )
        (worktree / 'requirements-fixed.txt').unlink()
        result = proc.stdout
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / 'input.txt').write_text(reqs)
        output_path.write_text(result)
    return get_image_from_requirements(container_command, data_path, python_version, result)

def get_image_from_requirements(container_command, data_path, python_version, reqs):
    """Get a container image given a set of requirements."""
    reqs_hash = hashlib.sha256(reqs.encode()).hexdigest()
    name = f'localhost/naucse-py{python_version}-{reqs_hash}'
    base_name = get_python_image(container_command, python_version)
    with ImageMaker(container_command, name) as imgm:
        if imgm.tempdir:
            (imgm.tempdir / 'requirements.txt').write_text(reqs)
            (imgm.tempdir / 'requirements-pre.txt').write_text(fixes.find_prerequisites(reqs))
        imgm.write(f'FROM {base_name}')
        imgm.write(f'ADD requirements-pre.txt /naucse/requirements-pre.txt')
        imgm.write(f'RUN {CONTAINER_PYTHON_COMMAND} -m pip install -r /naucse/requirements-pre.txt')
        imgm.write(f'ADD requirements.txt /naucse/requirements.txt')
        imgm.write(f'RUN {CONTAINER_PYTHON_COMMAND} -m pip install -r /naucse/requirements.txt')
        imgm.add_build_args(
            '-v', f'{data_path / PIP_CACHE_DIR}:/naucse/pip-cache:rw,U,Z',
        )
    return name


def get_python_image(container_command, python_version):
    """Get a base container image for the given Python version."""
    name = f'localhost/naucse-py{python_version}'
    with ImageMaker(container_command, name) as imgm:
        imgm.write('FROM fedora')
        imgm.write(f'RUN dnf install -y --setopt=install_weak_deps=False python{python_version} python-pip-wheel && dnf clean all')
        imgm.write(f'RUN mkdir /naucse /naucse/wd /naucse/aux')
        imgm.write(f'RUN python{python_version} -m venv /naucse/env')
        imgm.write(f'RUN {CONTAINER_PYTHON_COMMAND} -m pip install -U pip wheel')
        imgm.write(f'ENV PIP_CACHE_DIR /naucse/pip-cache')
        imgm.write(f'WORKDIR /naucse/wd')
    return name


def save_env_info(container_command, worktree, commit_id, image_name, slug, result_path):
    """Save reference info about a given container image."""
    envinfo_path = result_path / 'env-info'
    envinfo_path.mkdir()
    with envinfo_path.joinpath('os-release').open('w') as f:
        run(
            container_command, 'run', '--rm', image_name,
            'cat', '/etc/os-release',
            stdout=f,
        )
    with envinfo_path.joinpath('dnf.txt').open('w') as f:
        run(
            container_command, 'run', '--rm', image_name,
            'dnf', 'list', 'installed',
            stdout=f,
        )
    with envinfo_path.joinpath('pip.txt').open('w') as f:
        run(
            container_command, 'run', '--rm', image_name,
            CONTAINER_PYTHON_COMMAND, '-m', 'pip', 'freeze', '--all',
            stdout=f,
        )
    with envinfo_path.joinpath('source-commit.txt').open('w') as f:
        print(commit_id, file=f)
    with envinfo_path.joinpath('course.txt').open('w') as f:
        print(slug, file=f)


def save_course(container_command, worktree, course_def, commit_id, image_name, slug, result_path):
    """Save the given course."""
    info = get_course(container_command, worktree, image_name, slug)
    course = info['course']
    course.setdefault('etag', commit_id)

    # Update the course data to API 0.4
    version = tuple(info['api_version'])
    if version >= (0, 4):
        raise ValueError(
            f'API version {version} is too new. For this course, '
            + 'use `python -m naucse_render compile` directly.'
        )
    if version < (0, 1):
        fixes.add_serials(course)
    course.setdefault('timezone', 'Europe/Prague')  # mandatory since 0.3
    info['api_version'] = (0, 4)

    #print(json.dumps(info, indent=2))
    course_vars = course.get('vars', {})
    lesson_slugs = set()
    for session in course.get('sessions', ()):
        for material in session.get('materials', ()):
            lesson_slug = material.get('lesson_slug', None)
            if lesson_slug:
                lesson_slugs.add(lesson_slug)
    course['lessons'] = save_lessons(
        container_command, worktree, image_name, result_path,
        lesson_slugs, course_vars,
    )

    course.setdefault('edit_info', {
        'url': course_def['source']['repo'],
        'branch': course_def['source']['branch'],
    })

    with open(result_path / 'course.json', 'w', encoding='utf-8') as f:
        json.dump(info, f, sort_keys=True, ensure_ascii=True, indent=1)

    return commit_id

def save_lessons(container_command, worktree, image_name, result_path, lesson_slugs, course_vars, _done_slugs=()):
    """Save all lessons from a course."""
    result = {}
    for try_number in range(50):
        info = get_lessons(
            container_command, worktree, image_name, sorted(lesson_slugs),
            course_vars,
        )
        data = info.pop('data')
        _done_slugs = set(_done_slugs)
        for slug, lesson in data.items():
            _done_slugs.add(slug)
            if '.' in slug:
                raise ValueError(slug)
            outpath = joinpath(result_path / 'lessons', slug.lower())
            outpath.mkdir(parents=True, exist_ok=False)
            for name, page in lesson['pages'].items():
                content = page.pop('content')
                content_path = joinpath(outpath, f'{name}.html')
                content_path.write_text(content)
                lesson_slugs.update(fixes.find_lesson_slugs(content))
                page['content'] = {
                    'path': str(content_path.relative_to(result_path)),
                }
                for index, solution in enumerate(page.get('solutions', ())):
                    content = solution.pop('content')
                    content_path = joinpath(outpath, f'solution-{index}.html')
                    content_path.write_text(content)
                    lesson_slugs.update(fixes.find_lesson_slugs(content))
                    solution['content'] = {
                        'path': str(content_path.relative_to(result_path)),
                    }
            for name, info in lesson['static_files'].items():
                static_dir = joinpath(outpath, 'static')
                static_dir.mkdir(parents=True, exist_ok=True)
                srcpath = joinpath(worktree, info['path'])
                name = re.sub('[^a-z0-9./_-]+', '-', name.lower())
                destpath = joinpath(static_dir, name)
                info['path'] = str(destpath.relative_to(result_path))
                destpath.parent.mkdir(parents=True, exist_ok=True)
                if destpath.exists():
                    raise ValueError(f'{destpath} already exists')
                shutil.copy(srcpath, destpath)

            result[slug] = lesson

        lesson_slugs -= _done_slugs
        if not lesson_slugs:
            break
    else:
        raise ValueError(f'Lessons are linked too deeply')

    return result

def get_course(container_command, worktree, image_name, slug):
    return get_data(
        container_command, worktree, image_name,
        'naucse_render', 'get_course', [slug], {'version': 1, 'path': '.'},
    )

def get_lessons(container_command, worktree, image_name, slugs, course_vars):
    return get_data(
        container_command, worktree, image_name,
        'naucse_render', 'get_lessons', [slugs],
        {'vars': course_vars, 'path': '.'},
    )

def get_data(container_command, worktree, image_name, mod, obj, args, kwargs):
    """Run a Python function in a container and get data out."""
    with tempdir_path() as tempdir:
        inpath = tempdir / 'input.json'
        outpath = tempdir / 'output.json'
        runnerpath = tempdir / 'runner.py'
        runnerpath.write_text(RUNNER)
        with open(inpath, 'w', encoding='utf-8') as f:
            json.dump([mod, obj, args, kwargs], f)
        printerr(f'>> {mod}:{obj}{repr_args_kwargs(args, kwargs)}')
        proc = run(
            container_command, 'run',
            '--rm',
            '-v', f'{worktree}:/naucse/wd:O',
            '-v', f'{tempdir}:/naucse/aux:Z',
            image_name,
            CONTAINER_PYTHON_COMMAND, '/naucse/aux/runner.py',
            '/naucse/aux/input.json', '/naucse/aux/output.json',
        )
        with open(outpath, encoding='utf-8') as f:
            return json.load(f)


@contextlib.contextmanager
def tempdir_path():
    with tempfile.TemporaryDirectory(prefix='naucse-tmp-') as dirname:
        yield Path(dirname)


def joinpath(base, end):
    """Like Path.joinpath(), but ensures the result is inside `base`.

    Should be used for user-supplied `end`.
    """
    result = (base / end).resolve()
    if base not in result.parents:
        print(base, end, result)
        raise ValueError(end)
    return result

class ImageMaker:
    """Context manager for making a container image.

    Does common setup work, lets the user write a Containerfile,
    then executes it when the context is exited.
    """
    def __init__(self, container_command, name=None):
        self.container_command = container_command
        self.name = name
        self.context = contextlib.ExitStack()
        self.extra_build_args = []
        self.containerfile = None
        self.tempdir = None

    def write(self, *args, **kwargs):
        print(*args, **kwargs, file=self.containerfile)


    def add_build_args(self, *args):
        self.extra_build_args.extend(args)

    def __enter__(self):
        if self.name:
            proc = run(
                self.container_command, 'image', 'exists', self.name,
                check=False,
            )
            if proc.returncode == 0:
                self.write = lambda *a, **ka: None
                return self
        self.tempdir = self.context.enter_context(tempdir_path())
        self.containerfile = self.context.enter_context(
            (self.tempdir / 'Containerfile').open('w', encoding='utf-8')
        )
        return self

    def __exit__(self, tp, val, tb):
        try:
            if tp is None and self.containerfile:
                self.containerfile.close()
                if self.name:
                    self.add_build_args('-t', self.name)
                proc = run(
                    self.container_command, 'build', self.tempdir, '--layers',
                    *self.extra_build_args,
                )
                if not self.name:
                    self.name = proc.stdout.strip()
        finally:
            self.context.close()

def repr_args_kwargs(args, kwargs):
    """Pretty-print arguments to a function"""
    repr_args = ', '.join(repr(a) for a in args)
    if not kwargs:
        return f'({repr_args})'
    repr_kwargs = ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
    return f'({repr_args}, {repr_kwargs})'


def git_config_key(string):
    """Convert arbitrary string to a Git config key.

    According to Git docs:
    The variable names are case-insensitive, allow only alphanumeric
    characters and -, and must start with an alphabetic character. 
    """
    def _replacement(match):
        char = match[0]
        result = dict(['.p', '/s', ':k', '?q', '=i', '#g']).get(char)
        if result:
            return '-' + result
        value = ord(char)
        if value <= 0xff:
            return f'-{value:-2x}'
        elif value <= 0xffff:
            return f'-u{value:-4x}'
        elif value <= 0xffffffff:
            return f'-m{value:-8x}'
        else:
            raise ValueError(match)
    string = re.sub('[^a-z0-9]', _replacement, string)
    if not string or string[0] in '0123456789x':
        return 'x' + string
    else:
        return string

RUNNER = """
''' This code launches a task, serialized as JSON.
'''

from importlib import import_module
import json
import sys

with open(sys.argv[1], encoding='utf-8') as infile:
    module_name, obj_name, args, kwargs = json.load(infile)

obj = import_module(module_name)
obj = getattr(obj, obj_name)

result = obj(*args, **kwargs)

with open(sys.argv[2], 'w', encoding='utf-8') as outfile:
    json.dump(result, outfile)
"""
