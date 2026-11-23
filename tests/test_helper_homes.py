"""A helper two tests share lives in a support module (``tests/support.py``, ``tests/fakes.py``,
``tests/*_support.py``), so no test module imports another."""

import ast

from tests.support import ROOT


def _imports(path):
    """The dotted names of the modules ``path`` imports."""

    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), str(path))):
        if isinstance(node, ast.ImportFrom):
            yield node.module or ""
        elif isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)


def test_no_test_module_imports_another():
    importers = {
        path.name: sorted(name for name in _imports(path) if name.startswith("tests.test_"))
        for path in (ROOT / "tests").glob("test_*.py")
    }
    assert {name: imports for name, imports in importers.items() if imports} == {}
