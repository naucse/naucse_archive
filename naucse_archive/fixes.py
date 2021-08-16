"""Fixes for older versions of naucse_render"""

import urllib.parse

import lxml.html

def find_lesson_slugs(text):
    """Find lessons that might not be mentioned in the lesson list"""
    for fragment in lxml.html.fragments_fromstring(text):
        yield from _find_lesson_slugs(fragment)

def _find_lesson_slugs(element):
    for attr_name in {'href', 'src'}:
        link_text = element.attrib.get(attr_name)
        if link_text:
            link = urllib.parse.urlparse(link_text)
            if link.scheme == 'naucse' and link.path == 'page':
                qs = urllib.parse.parse_qs(link.query, separator='&')
                yield from qs['lesson']
    for child in element:
        yield from _find_lesson_slugs(child)


def fix_old_requirements_txt(reqs_in):
    """Add common missing requirements"""
    result = [reqs_in]
    reqs_set = set(reqs_in.splitlines())
    if 'naucse_render<1.0' in reqs_set:
        result.append('')
        result.append('# compatibility requirements')
        result.append('naucse_render < 1.4')
        result.append('nbconvert < 6')

    return '\n'.join(result)

def find_prerequisites(reqs_in):
    """Add build-time pre-requisites for older libraries"""
    result = []
    reqs_set = set(reqs_in.splitlines())
    if any(line.startswith('markupsafe==1.0') for line in reqs_set):
        result.append('setuptools < 46')
    return '\n'.join(result)
