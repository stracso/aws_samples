#!/usr/bin/env python3
"""Compare AWS policy actions between two text files.

Each file contains policies in the format:
    # PolicyName
    { ... JSON policy ... }
"""

import json
import sys
import re


def _to_str(value):
    """Convert any value to a stable string representation."""
    if isinstance(value, list):
        return ", ".join(str(v) for v in sorted(value, key=str))
    return str(value)


def parse_policies(filepath):
    """Parse a policy file into a dict of {policy_name: set_of_action_strings}."""
    policies = {}
    with open(filepath, "r") as f:
        content = f.read()

    blocks = re.split(r"^(# .+)$", content, flags=re.MULTILINE)

    for i in range(1, len(blocks), 2):
        name = blocks[i].lstrip("# ").strip()
        body = blocks[i + 1].strip()
        if not body:
            continue
        policy = json.loads(body)
        actions = set()
        for stmt in policy.get("Statement", []):
            stmt_actions = stmt.get("Action", [])
            if isinstance(stmt_actions, str):
                stmt_actions = [stmt_actions]
            effect = stmt.get("Effect", "")
            resource = _to_str(stmt.get("Resource", ""))
            for a in stmt_actions:
                actions.add(f"{a} | Effect={effect} | Resource={resource}")
        policies[name] = actions
    return policies


def compare(file1, file2):
    policies1 = parse_policies(file1)
    policies2 = parse_policies(file2)

    names1 = set(policies1.keys())
    names2 = set(policies2.keys())

    only_in_1 = names1 - names2
    only_in_2 = names2 - names1
    common = names1 & names2

    print(f"=== Policies only in {file1} ===")
    for name in sorted(only_in_1):
        for entry in sorted(policies1[name]):
            print(f"  [{name}] {entry}")

    print(f"\n=== Policies only in {file2} ===")
    for name in sorted(only_in_2):
        for entry in sorted(policies2[name]):
            print(f"  [{name}] {entry}")

    print("\n=== Common policies with differences ===")
    for name in sorted(common):
        diff1 = policies1[name] - policies2[name]
        diff2 = policies2[name] - policies1[name]
        if diff1 or diff2:
            print(f"  Policy: {name}")
            for entry in sorted(diff1):
                print(f"    - (file1) {entry}")
            for entry in sorted(diff2):
                print(f"    + (file2) {entry}")
        else:
            print(f"  Policy: {name} — identical")

    if not only_in_1 and not only_in_2 and not common:
        print("No policies found to compare.")


if __name__ == "__main__":
    f1 = sys.argv[1] if len(sys.argv) > 1 else "sample1.txt"
    f2 = sys.argv[2] if len(sys.argv) > 2 else "sample2.txt"
    compare(f1, f2)
