r"""Limit underscore-separated word count in bound identifiers (AST scan)."""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def word_count(name: str) -> int:
    """Count non-empty segments split on underscore (snake_case words)."""
    return len([p for p in name.split("_") if p])


def _report_if_over(
    limit: int,
    name: str,
    file_rel: str,
    lineno: int,
    violations: list[str],
) -> None:
    n = word_count(name)
    if n > limit:
        violations.append(
            f"{file_rel}:{lineno}: name {name!r} has {n} words (limit {limit})"
        )


def _iter_lvalue_names(node: ast.expr) -> list[tuple[str, int]]:
    """Collect ``(id, lineno)`` for simple names in assignment/for targets."""
    out: list[tuple[str, int]] = []
    if isinstance(node, ast.Name):
        out.append((node.id, node.lineno))
    elif isinstance(node, (ast.Tuple, ast.List)):
        for elt in node.elts:
            out.extend(_iter_lvalue_names(elt))
    elif isinstance(node, ast.Starred):
        out.extend(_iter_lvalue_names(node.value))
    return out


def _visit_arguments(
    limit: int,
    args: ast.arguments,
    file_rel: str,
    violations: list[str],
) -> None:
    for a in (*args.posonlyargs, *args.args, *args.kwonlyargs):
        _report_if_over(limit, a.arg, file_rel, a.lineno, violations)
    if args.vararg:
        _report_if_over(
            limit, args.vararg.arg, file_rel, args.vararg.lineno, violations
        )
    if args.kwarg:
        _report_if_over(limit, args.kwarg.arg, file_rel, args.kwarg.lineno, violations)


def _visit_pattern(
    limit: int,
    pattern: ast.pattern,
    file_rel: str,
    violations: list[str],
) -> None:
    if isinstance(pattern, ast.MatchAs):
        if pattern.name:
            _report_if_over(limit, pattern.name, file_rel, pattern.lineno, violations)
        if pattern.pattern:
            _visit_pattern(limit, pattern.pattern, file_rel, violations)
    elif isinstance(pattern, ast.MatchStar):
        if pattern.name:
            _report_if_over(limit, pattern.name, file_rel, pattern.lineno, violations)
    elif isinstance(pattern, ast.MatchMapping):
        for sub in pattern.patterns:
            _visit_pattern(limit, sub, file_rel, violations)
        if pattern.rest:
            _report_if_over(limit, pattern.rest, file_rel, pattern.lineno, violations)
    elif isinstance(pattern, (ast.MatchSequence, ast.MatchOr, ast.MatchClass)):
        if isinstance(pattern, ast.MatchClass):
            subs = (*pattern.patterns, *pattern.kwd_patterns)
        else:
            subs = tuple(pattern.patterns)
        for sub in subs:
            _visit_pattern(limit, sub, file_rel, violations)


def _visit_comp_generators(
    limit: int,
    generators: list[ast.comprehension],
    file_rel: str,
    violations: list[str],
) -> None:
    for gen in generators:
        for ident, ln in _iter_lvalue_names(gen.target):
            _report_if_over(limit, ident, file_rel, ln, violations)
        _visit_expr(limit, gen.iter, file_rel, violations)
        for if_ in gen.ifs:
            _visit_expr(limit, if_, file_rel, violations)


def _visit_expr(
    limit: int,
    expr: ast.expr,
    file_rel: str,
    violations: list[str],
) -> None:
    if isinstance(expr, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
        _visit_comp_generators(limit, expr.generators, file_rel, violations)
        _visit_expr(limit, expr.elt, file_rel, violations)
    elif isinstance(expr, ast.DictComp):
        _visit_comp_generators(limit, expr.generators, file_rel, violations)
        _visit_expr(limit, expr.key, file_rel, violations)
        _visit_expr(limit, expr.value, file_rel, violations)
    elif isinstance(expr, ast.NamedExpr):
        _report_if_over(limit, expr.target.id, file_rel, expr.target.lineno, violations)
        _visit_expr(limit, expr.value, file_rel, violations)
    elif isinstance(expr, ast.Lambda):
        _visit_arguments(limit, expr.args, file_rel, violations)
        _visit_expr(limit, expr.body, file_rel, violations)
    elif isinstance(expr, ast.IfExp):
        _visit_expr(limit, expr.test, file_rel, violations)
        _visit_expr(limit, expr.body, file_rel, violations)
        _visit_expr(limit, expr.orelse, file_rel, violations)
    elif isinstance(expr, ast.Await):
        _visit_expr(limit, expr.value, file_rel, violations)
    elif isinstance(expr, ast.Yield):
        if expr.value:
            _visit_expr(limit, expr.value, file_rel, violations)
    elif isinstance(expr, ast.YieldFrom):
        _visit_expr(limit, expr.value, file_rel, violations)
    elif isinstance(expr, ast.Call):
        _visit_expr(limit, expr.func, file_rel, violations)
        for a in expr.args:
            _visit_expr(limit, a, file_rel, violations)
        for kw in expr.keywords:
            _visit_expr(limit, kw.value, file_rel, violations)
    elif isinstance(expr, ast.UnaryOp):
        _visit_expr(limit, expr.operand, file_rel, violations)
    elif isinstance(expr, ast.BinOp):
        _visit_expr(limit, expr.left, file_rel, violations)
        _visit_expr(limit, expr.right, file_rel, violations)
    elif isinstance(expr, ast.BoolOp):
        for v in expr.values:
            _visit_expr(limit, v, file_rel, violations)
    elif isinstance(expr, ast.Compare):
        _visit_expr(limit, expr.left, file_rel, violations)
        for c in expr.comparators:
            _visit_expr(limit, c, file_rel, violations)
    elif isinstance(expr, ast.Attribute):
        _visit_expr(limit, expr.value, file_rel, violations)
    elif isinstance(expr, ast.Subscript):
        _visit_expr(limit, expr.value, file_rel, violations)
        if isinstance(expr.slice, ast.Slice):
            if expr.slice.lower:
                _visit_expr(limit, expr.slice.lower, file_rel, violations)
            if expr.slice.upper:
                _visit_expr(limit, expr.slice.upper, file_rel, violations)
            if expr.slice.step:
                _visit_expr(limit, expr.slice.step, file_rel, violations)
        else:
            _visit_expr(limit, expr.slice, file_rel, violations)
    elif isinstance(expr, (ast.List, ast.Tuple, ast.Set)):
        for e in expr.elts:
            _visit_expr(limit, e, file_rel, violations)
    elif isinstance(expr, ast.Dict):
        for k, v in zip(expr.keys, expr.values, strict=False):
            if k:
                _visit_expr(limit, k, file_rel, violations)
            _visit_expr(limit, v, file_rel, violations)
    elif isinstance(expr, ast.Starred):
        _visit_expr(limit, expr.value, file_rel, violations)


def _visit_stmt_list(
    limit: int,
    stmts: list[ast.stmt],
    file_rel: str,
    violations: list[str],
) -> None:
    for child in stmts:
        _visit_stmt(limit, child, file_rel, violations)


def _visit_stmt(
    limit: int,
    stmt: ast.stmt,
    file_rel: str,
    violations: list[str],
) -> None:
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
        _report_if_over(limit, stmt.name, file_rel, stmt.lineno, violations)
        _visit_arguments(limit, stmt.args, file_rel, violations)
        for d in stmt.decorator_list:
            _visit_expr(limit, d, file_rel, violations)
        for default in stmt.args.defaults:
            _visit_expr(limit, default, file_rel, violations)
        for default in stmt.args.kw_defaults:
            if default is not None:
                _visit_expr(limit, default, file_rel, violations)
        _visit_stmt_list(limit, stmt.body, file_rel, violations)
    elif isinstance(stmt, ast.ClassDef):
        _report_if_over(limit, stmt.name, file_rel, stmt.lineno, violations)
        for d in stmt.bases:
            _visit_expr(limit, d, file_rel, violations)
        for d in stmt.keywords:
            _visit_expr(limit, d.value, file_rel, violations)
        for d in stmt.decorator_list:
            _visit_expr(limit, d, file_rel, violations)
        _visit_stmt_list(limit, stmt.body, file_rel, violations)
    elif isinstance(stmt, ast.Assign):
        for t in stmt.targets:
            if isinstance(t, ast.Name):
                _report_if_over(limit, t.id, file_rel, t.lineno, violations)
            else:
                for ident, ln in _iter_lvalue_names(t):
                    _report_if_over(limit, ident, file_rel, ln, violations)
        _visit_expr(limit, stmt.value, file_rel, violations)
    elif isinstance(stmt, ast.AnnAssign):
        if isinstance(stmt.target, ast.Name):
            _report_if_over(
                limit, stmt.target.id, file_rel, stmt.target.lineno, violations
            )
        elif isinstance(stmt.target, (ast.Tuple, ast.List)):
            for ident, ln in _iter_lvalue_names(stmt.target):
                _report_if_over(limit, ident, file_rel, ln, violations)
        if stmt.value:
            _visit_expr(limit, stmt.value, file_rel, violations)
        if stmt.annotation:
            _visit_expr(limit, stmt.annotation, file_rel, violations)
    elif isinstance(stmt, ast.AugAssign) and isinstance(stmt.target, ast.Name):
        _report_if_over(limit, stmt.target.id, file_rel, stmt.target.lineno, violations)
        _visit_expr(limit, stmt.value, file_rel, violations)
    elif isinstance(stmt, (ast.For, ast.AsyncFor)):
        for ident, ln in _iter_lvalue_names(stmt.target):
            _report_if_over(limit, ident, file_rel, ln, violations)
        _visit_expr(limit, stmt.iter, file_rel, violations)
        _visit_stmt_list(limit, stmt.body, file_rel, violations)
        _visit_stmt_list(limit, stmt.orelse, file_rel, violations)
    elif isinstance(stmt, (ast.While, ast.If)):
        _visit_expr(limit, stmt.test, file_rel, violations)
        _visit_stmt_list(limit, stmt.body, file_rel, violations)
        _visit_stmt_list(limit, stmt.orelse, file_rel, violations)
    elif isinstance(stmt, ast.With | ast.AsyncWith):
        for item in stmt.items:
            if item.optional_vars:
                if isinstance(item.optional_vars, ast.Name):
                    _report_if_over(
                        limit,
                        item.optional_vars.id,
                        file_rel,
                        item.optional_vars.lineno,
                        violations,
                    )
                else:
                    for ident, ln in _iter_lvalue_names(item.optional_vars):
                        _report_if_over(limit, ident, file_rel, ln, violations)
            _visit_expr(limit, item.context_expr, file_rel, violations)
        _visit_stmt_list(limit, stmt.body, file_rel, violations)
    elif isinstance(stmt, ast.Try | ast.TryStar):
        _visit_stmt_list(limit, stmt.body, file_rel, violations)
        for h in stmt.handlers:
            if h.name:
                _report_if_over(limit, h.name, file_rel, h.lineno, violations)
            if h.type:
                _visit_expr(limit, h.type, file_rel, violations)
            _visit_stmt_list(limit, h.body, file_rel, violations)
        _visit_stmt_list(limit, stmt.orelse, file_rel, violations)
        _visit_stmt_list(limit, stmt.finalbody, file_rel, violations)
    elif isinstance(stmt, ast.Match):
        _visit_expr(limit, stmt.subject, file_rel, violations)
        for case in stmt.cases:
            _visit_pattern(limit, case.pattern, file_rel, violations)
            if case.guard:
                _visit_expr(limit, case.guard, file_rel, violations)
            _visit_stmt_list(limit, case.body, file_rel, violations)
    elif isinstance(stmt, ast.Import):
        for alias in stmt.names:
            bound = alias.asname or alias.name.split(".")[0]
            _report_if_over(limit, bound, file_rel, stmt.lineno, violations)
    elif isinstance(stmt, ast.ImportFrom):
        for alias in stmt.names:
            if alias.name == "*":
                continue
            bound = alias.asname or alias.name
            _report_if_over(limit, bound, file_rel, stmt.lineno, violations)
    elif isinstance(stmt, ast.Expr):
        _visit_expr(limit, stmt.value, file_rel, violations)
    elif isinstance(stmt, ast.Assert):
        _visit_expr(limit, stmt.test, file_rel, violations)
        if stmt.msg:
            _visit_expr(limit, stmt.msg, file_rel, violations)
    elif isinstance(stmt, ast.Raise):
        if stmt.exc:
            _visit_expr(limit, stmt.exc, file_rel, violations)
        if stmt.cause:
            _visit_expr(limit, stmt.cause, file_rel, violations)


def _scan_module(
    tree: ast.Module, file_rel: str, limit: int, violations: list[str]
) -> None:
    for stmt in tree.body:
        _visit_stmt(limit, stmt, file_rel, violations)


def collect_violations(repo_root: Path, roots: list[str], limit: int) -> list[str]:
    """List bound identifiers whose snake_case word count exceeds ``limit``."""
    violations: list[str] = []
    for root_name in roots:
        root = repo_root / root_name
        if not root.is_dir():
            violations.append(
                f"max_snake_binding_words: root is not a directory: {root_name}"
            )
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(repo_root).as_posix()
            text = path.read_text(encoding="utf-8")
            try:
                tree = ast.parse(text, filename=rel, type_comments=False)
            except SyntaxError as exc:
                violations.append(f"{rel}: cannot parse ({exc.msg})")
                continue
            _scan_module(tree, rel, limit, violations)
    return violations
