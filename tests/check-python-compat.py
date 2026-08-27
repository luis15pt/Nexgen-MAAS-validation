#!/usr/bin/env python3
"""Assert the report generator still runs on the oldest Python we deploy to.

The generator is developed on a host with a newer Python than the host it is
actually run from, and `from __future__ import annotations` hides most of the
difference: the `X | None` annotations never get evaluated, so the module
imports cleanly on 3.8 and the first genuine incompatibility only surfaces
mid-run, after it has already talked to MAAS.

That is exactly how `str.removesuffix` (3.9+) got through -- it raised an
AttributeError partway through fetching commissioning output.

Two checks, because they catch different things:
  * syntax, via ast.parse(feature_version=...)
  * runtime API use, which no syntax check can see
"""
import ast
import sys
from pathlib import Path

TARGET = (3, 8)
FILES = ["reporting/device_certificate.py"]

# name -> (version, note). Methods here are looked up as attributes, so a
# syntax check cannot see them; they fail only when the line executes.
NEW_ATTRS = {
    "removesuffix": ((3, 9), "use tests/../_strip_sh or a slice"),
    "removeprefix": ((3, 9), "use a slice"),
    "pairwise":     ((3, 10), "itertools.pairwise"),
    "cache":        ((3, 9), "functools.cache -> functools.lru_cache(maxsize=None)"),
    "lcm":          ((3, 9), "math.lcm"),
    "nextafter":    ((3, 9), "math.nextafter"),
    "ulp":          ((3, 9), "math.ulp"),
}
NEW_BUILTINS = {"aiter": (3, 10), "anext": (3, 10)}
GENERIC_BUILTINS = {"list", "dict", "tuple", "set", "frozenset", "type"}


class RuntimeScan(ast.NodeVisitor):
    """Collect >TARGET API use, ignoring annotations (deferred by __future__)."""

    def __init__(self):
        self.problems = []

    # Skip signature annotations: they are strings at runtime.
    def visit_FunctionDef(self, node):
        for stmt in node.body:
            self.visit(stmt)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_AnnAssign(self, node):
        if node.value:
            self.visit(node.value)

    def visit_Attribute(self, node):
        info = NEW_ATTRS.get(node.attr)
        if info and info[0] > TARGET:
            self.problems.append((node.lineno, f".{node.attr}()", info[0], info[1]))
        self.generic_visit(node)

    def visit_Name(self, node):
        ver = NEW_BUILTINS.get(node.id)
        if ver and ver > TARGET:
            self.problems.append((node.lineno, node.id, ver, "builtin"))
        self.generic_visit(node)

    def visit_BinOp(self, node):
        # dict | dict is 3.9+; the annotation form is deferred so only literals
        # and clearly-runtime operands matter here.
        if isinstance(node.op, ast.BitOr) and (
            isinstance(node.left, ast.Dict) or isinstance(node.right, ast.Dict)
        ):
            self.problems.append((node.lineno, "dict | dict", (3, 9), "use {**a, **b}"))
        self.generic_visit(node)

    def visit_Subscript(self, node):
        # list[str] as a runtime expression (not an annotation) is 3.9+.
        if isinstance(node.value, ast.Name) and node.value.id in GENERIC_BUILTINS:
            self.problems.append(
                (node.lineno, f"{node.value.id}[...]", (3, 9), "use typing.List etc.")
            )
        self.generic_visit(node)


def main():
    root = Path(__file__).resolve().parent.parent
    want = ".".join(map(str, TARGET))
    rc = 0
    print(f"Python {want} compatibility (generator runs on an older host than this one):")

    for rel in FILES:
        path = root / rel
        src = path.read_text()

        try:
            ast.parse(src, filename=str(path), feature_version=TARGET)
            print(f"  ok   {rel}: parses under {want}")
        except SyntaxError as exc:
            print(f"  FAIL {rel}: line {exc.lineno}: {exc.msg} (needs > {want})")
            rc = 1
            continue

        scan = RuntimeScan()
        scan.visit(ast.parse(src))
        if scan.problems:
            rc = 1
            for line, what, ver, hint in sorted(set(scan.problems)):
                v = ".".join(map(str, ver))
                print(f"  FAIL {rel}:{line}: {what} needs Python {v} -- {hint}")
        else:
            print(f"  ok   {rel}: no runtime API newer than {want}")

    if rc:
        print(f"\nThis will import fine on {want} and then fail mid-run.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
