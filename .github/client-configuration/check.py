#!/usr/bin/env python3
"""Finds the settings a client update brings, and opens an issue for each new one.

The adapter wraps a client or an engine and passes on only the settings somebody chose to pass
on. A Renovate pull request raises the client, the build stays green, and a setting the new
version added stays invisible. This script makes it visible.

It reads the settings a user can set out of the published jars of two client versions, compares
them, and holds the result against client-configuration-inventory.yml. That file sits at the
root of the repository and says what this script reads (the client, its jars, its classes) and
how the adapter answers each setting: supported, set-by-adapter, not-supported or open.

It finds three kinds of change:

  - a setting which is new in the client: an issue in this repository, once per setting,
  - a setting which is gone: one line in the job summary. If the adapter used it, the build
    has already failed on it,
  - a setting which is newly deprecated while the inventory says the adapter supports it: an
    issue as well, because no build fails on that.

A find never makes this script fail. It fails only when it cannot do its work, for example when
a jar or a class is not where the inventory says. A check which finds nothing because it read
nothing must not look like a quiet week.

Commands:

  check      compares two versions and reports. The versions come from two POMs (--base-pom,
             --head-pom) or are given (--old, --new). Without --create-issues it only prints
             the issues it would open.
  self-test  runs the comparison of the 'self-test' block of the inventory and fails unless it
             finds the settings that block names.
  list       prints the settings of one version, to write or extend the inventory.

It needs Python 3.10 or newer, PyYAML, Maven Central, and for issues the GitHub CLI 'gh'.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - the message is the point
    sys.exit("This script needs PyYAML: pip install pyyaml")

LABEL = "client-configuration"
LABEL_COLOR = "1D76DB"
LABEL_DESCRIPTION = "A client update brought a setting the adapter does not pass on yet"
STATUSES = ("supported", "set-by-adapter", "not-supported", "open")
# what each status has to say besides the names, so that the answer can be checked later
STATUS_FIELD = {
    "supported": "key",
    "set-by-adapter": "reason",
    "not-supported": "reason",
    "open": "issue",
}
MARKER = "<!-- client-configuration: {} -->"


class ToolError(Exception):
    """The script cannot do its work. This and only this makes the job red."""


# ---------------------------------------------------------------------------------------------
# Maven Central


def central() -> str:
    return os.environ.get("MAVEN_CENTRAL", "https://repo1.maven.org/maven2").rstrip("/")


def cache_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(Path.home(), ".cache")
    path = Path(base) / "vanillabp-client-configuration"
    path.mkdir(parents=True, exist_ok=True)
    return path


def fetch(coordinates: str, version: str, classifier: str | None = None) -> Path | None:
    """The jar of an artifact, from the cache or from Maven Central. None if Central has none."""
    group, artifact = coordinates.split(":")
    name = f"{artifact}-{version}{'-' + classifier if classifier else ''}.jar"
    target = cache_dir() / group / name
    if target.exists():
        return target
    url = f"{central()}/{group.replace('.', '/')}/{artifact}/{version}/{name}"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()
            target.parent.mkdir(parents=True, exist_ok=True)
            partial = target.with_suffix(".part")
            partial.write_bytes(data)
            partial.replace(target)
            return target
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            if attempt == 2:
                raise ToolError(f"Maven Central answered {error.code} for {url}") from error
        except urllib.error.URLError as error:
            if attempt == 2:
                raise ToolError(f"Maven Central could not be reached for {url}: {error}") from error
        time.sleep(2 * (attempt + 1))
    return None


def required_jar(coordinates: str, version: str) -> Path:
    """The jar of an artifact. 'a|b' names an artifact which was renamed: the first one found wins."""
    for alternative in coordinates.split("|"):
        jar = fetch(alternative.strip(), version)
        if jar is not None:
            return jar
    raise ToolError(
        f"Maven Central has no {coordinates.replace('|', ' or ')} in version {version}. Either the version is"
        f" wrong or the artifact moved, and then client-configuration-inventory.yml has to name the new one.")


def sources_jar(coordinates: str, version: str) -> Path | None:
    for alternative in coordinates.split("|"):
        if fetch(alternative.strip(), version) is not None:
            return fetch(alternative.strip(), version, "sources")
    return None


# ---------------------------------------------------------------------------------------------
# class files
#
# The classes are read as bytes and never loaded. Loading them would need every library they
# refer to, and the engine refers to dozens.

ACC_PUBLIC = 0x0001
ACC_STATIC = 0x0008
ACC_FINAL = 0x0010
ACC_BRIDGE = 0x0040
ACC_INTERFACE = 0x0200
ACC_SYNTHETIC = 0x1000
DEPRECATED_ANNOTATIONS = ("Ljava/lang/Deprecated;", "Lkotlin/Deprecated;")


@dataclasses.dataclass
class Member:
    access: int
    name: str
    descriptor: str
    deprecated: bool
    constant: object = None


@dataclasses.dataclass
class ClassFile:
    name: str
    access: int
    superclass: str | None
    interfaces: list[str]
    fields: list[Member]
    methods: list[Member]


class _Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.position = 0

    def take(self, size: int) -> bytes:
        chunk = self.data[self.position:self.position + size]
        self.position += size
        return chunk

    def u1(self) -> int:
        return self.take(1)[0]

    def u2(self) -> int:
        return struct.unpack(">H", self.take(2))[0]

    def u4(self) -> int:
        return struct.unpack(">I", self.take(4))[0]


def parse_class(data: bytes) -> ClassFile:
    reader = _Reader(data)
    if reader.u4() != 0xCAFEBABE:
        raise ToolError("not a class file")
    reader.take(4)  # minor and major version
    pool: list[object] = [None] * reader.u2()
    index = 1
    while index < len(pool):
        tag = reader.u1()
        if tag == 1:
            pool[index] = reader.take(reader.u2()).decode("utf-8", errors="replace")
        elif tag in (3, 4):
            pool[index] = reader.take(4)
        elif tag in (5, 6):
            reader.take(8)
            index += 1  # a long or a double takes two slots
        elif tag in (7, 8, 16, 19, 20):
            pool[index] = ("ref", reader.u2())
        elif tag in (9, 10, 11, 12, 17, 18):
            reader.take(4)
        elif tag == 15:
            reader.take(3)
        else:
            raise ToolError(f"unknown constant pool tag {tag}")
        index += 1

    def utf8(i: int) -> str:
        return pool[i]  # type: ignore[return-value]

    def class_name(i: int) -> str | None:
        if i == 0:
            return None
        return utf8(pool[i][1])  # type: ignore[index]

    def skip_element_value() -> None:
        tag = chr(reader.u1())
        if tag in "BCDFIJSZsc":
            reader.u2()
        elif tag == "e":
            reader.take(4)
        elif tag == "@":
            annotation_type()
        elif tag == "[":
            for _ in range(reader.u2()):
                skip_element_value()

    def annotation_type() -> str:
        type_name = utf8(reader.u2())
        for _ in range(reader.u2()):
            reader.u2()
            skip_element_value()
        return type_name

    def members() -> list[Member]:
        result = []
        for _ in range(reader.u2()):
            access, name, descriptor = reader.u2(), utf8(reader.u2()), utf8(reader.u2())
            deprecated = False
            constant = None
            for _ in range(reader.u2()):
                attribute, length = utf8(reader.u2()), reader.u4()
                end = reader.position + length
                if attribute == "Deprecated":
                    deprecated = True
                elif attribute == "RuntimeVisibleAnnotations":
                    for _ in range(reader.u2()):
                        if annotation_type() in DEPRECATED_ANNOTATIONS:
                            deprecated = True
                elif attribute == "ConstantValue":
                    value = pool[reader.u2()]
                    if isinstance(value, tuple):
                        constant = utf8(value[1])
                reader.position = end
            result.append(Member(access, name, descriptor, deprecated, constant))
        return result

    access = reader.u2()
    this_class = class_name(reader.u2())
    superclass = class_name(reader.u2())
    interfaces = [class_name(reader.u2()) for _ in range(reader.u2())]
    fields = members()
    methods = members()
    return ClassFile(this_class or "", access, superclass, interfaces, fields, methods)  # type: ignore[arg-type]


class ClassPath:
    """The classes of a few jars, parsed when they are first asked for."""

    def __init__(self, jars: list[Path]):
        self.archives = [zipfile.ZipFile(jar) for jar in jars]
        self.parsed: dict[str, ClassFile | None] = {}

    def find(self, internal_name: str) -> ClassFile | None:
        if internal_name not in self.parsed:
            self.parsed[internal_name] = None
            for archive in self.archives:
                try:
                    data = archive.read(internal_name + ".class")
                except KeyError:
                    continue
                self.parsed[internal_name] = parse_class(data)
                break
        return self.parsed[internal_name]

    def hierarchy(self, internal_name: str) -> list[ClassFile]:
        """The class, its superclasses and its interfaces, as far as these jars hold them."""
        seen: dict[str, ClassFile] = {}
        todo = [internal_name]
        while todo:
            name = todo.pop(0)
            if name in seen:
                continue
            found = self.find(name)
            if found is None:
                continue
            seen[name] = found
            if found.superclass:
                todo.append(found.superclass)
            todo.extend(found.interfaces)
        return list(seen.values())


def parameter_count(descriptor: str) -> int:
    parameters = descriptor[1:descriptor.index(")")]
    return len(re.findall(r"\[*(?:[BCDFIJSZ]|L[^;]*;)", parameters))


def decapitalize(name: str) -> str:
    # the rule of java.beans.Introspector, so that a name reads like the property Spring binds
    if len(name) > 1 and name[0].isupper() and name[1].isupper():
        return name
    return name[:1].lower() + name[1:]


# ---------------------------------------------------------------------------------------------
# settings


@dataclasses.dataclass
class Setting:
    name: str
    deprecated: bool
    # the class which declares it, or the type of a starter property, to find a description
    source: str | None = None
    description: str | None = None
    # the value of a constant, which for a key constant is the key
    constant: str | None = None


def read_surface(surface: dict, version: str) -> dict[str, Setting]:
    """The settings one surface of the client has in one version, by name."""
    kind = surface["read"]
    artifacts = surface["artifacts"]
    if kind == "spring-metadata":
        return read_spring_metadata(surface, version)
    jars = [required_jar(artifact, version) for artifact in artifacts]
    classpath = ClassPath(jars)
    settings: dict[str, Setting] = {}
    for class_name in surface["classes"]:
        internal = class_name.replace(".", "/")
        if classpath.find(internal) is None:
            raise ToolError(
                f"{class_name} is not in {', '.join(artifacts)} {version}. The client moved it,"
                f" so client-configuration-inventory.yml has to name where it went.")
        for declaring in reversed(classpath.hierarchy(internal)):
            for setting in settings_of(kind, declaring):
                if surface.get("qualify"):
                    # several unrelated classes, so a name alone would mix them up
                    setting.name = f"{class_name.rsplit('.', 1)[-1]}.{setting.name}"
                # a subclass declares the newer word on deprecation, so it wins
                settings[setting.name] = setting
    if not settings:
        raise ToolError(f"Surface '{surface['id']}' has no settings in {version}, which cannot be right.")
    return settings


def settings_of(kind: str, declaring: ClassFile) -> list[Setting]:
    source = declaring.name.replace("/", ".")
    found: dict[str, list[Member]] = {}
    if kind == "setters":
        for method in declaring.methods:
            if (method.access & ACC_PUBLIC and not method.access & (ACC_STATIC | ACC_BRIDGE | ACC_SYNTHETIC)
                    and re.match(r"set[A-Z0-9_]", method.name) and parameter_count(method.descriptor) == 1):
                found.setdefault(decapitalize(method.name[3:]), []).append(method)
    elif kind == "methods":
        for method in declaring.methods:
            if (method.access & ACC_PUBLIC and not method.access & (ACC_STATIC | ACC_BRIDGE | ACC_SYNTHETIC)
                    and not method.name.startswith("<")):
                found.setdefault(method.name, []).append(method)
    elif kind == "getters":
        for method in declaring.methods:
            match = re.match(r"(?:get|is)([A-Z].*)", method.name)
            if (match and method.access & ACC_PUBLIC and not method.access & (ACC_STATIC | ACC_BRIDGE | ACC_SYNTHETIC)
                    and parameter_count(method.descriptor) == 0 and method.name != "getClass"):
                found.setdefault(decapitalize(match.group(1)), []).append(method)
    elif kind == "constants":
        for field in declaring.fields:
            if field.access & ACC_PUBLIC and field.access & ACC_STATIC and field.access & ACC_FINAL:
                found.setdefault(field.name, []).append(field)
    else:
        raise ToolError(f"Unknown way to read a surface: '{kind}'")
    result = []
    for name, overloads in found.items():
        # deprecated only when no way to set it is left which is not
        deprecated = all(member.deprecated for member in overloads)
        constant = overloads[0].constant if kind == "constants" else None
        description = f"A constant with the value `{constant}`." if isinstance(constant, str) else None
        result.append(Setting(name, deprecated, source, description, constant if isinstance(constant, str) else None))
    return result


def read_spring_metadata(surface: dict, version: str) -> dict[str, Setting]:
    settings: dict[str, Setting] = {}
    prefix = surface.get("prefix", "")
    for artifact in surface["artifacts"]:
        with zipfile.ZipFile(required_jar(artifact, version)) as archive:
            try:
                metadata = json.loads(archive.read("META-INF/spring-configuration-metadata.json"))
            except KeyError as error:
                raise ToolError(
                    f"{artifact} {version} has no META-INF/spring-configuration-metadata.json any more.") from error
        for entry in metadata.get("properties", []):
            name = entry["name"]
            if not name.startswith(prefix):
                continue
            deprecated = bool(entry.get("deprecated")) or "deprecation" in entry
            settings[name] = Setting(name, deprecated, entry.get("sourceType"), entry.get("description"))
    if not settings:
        raise ToolError(f"Surface '{surface['id']}' has no properties starting with '{prefix}' in {version}.")
    return settings


def describe(surface: dict, setting: Setting, version: str) -> str | None:
    """What the client says about a setting: the starter's description or the Javadoc."""
    if surface["read"] == "spring-metadata" or not setting.source:
        return setting.description
    return javadoc_of(surface, setting, version) or setting.description


def javadoc_of(surface: dict, setting: Setting, version: str) -> str | None:
    name = setting.name.rsplit(".", 1)[-1] if surface.get("qualify") else setting.name
    path = setting.source.split("$")[0].replace(".", "/")
    for artifact in surface["artifacts"]:
        sources = sources_jar(artifact, version)
        if sources is None:
            continue
        with zipfile.ZipFile(sources) as archive:
            for extension in (".java", ".kt"):
                try:
                    text = archive.read(path + extension).decode("utf-8", errors="replace")
                except KeyError:
                    continue
                return javadoc_for(text, name, surface["read"])
    return None


def javadoc_for(text: str, name: str, kind: str) -> str | None:
    if kind in ("setters", "getters", "constants"):
        capitalized = name[:1].upper() + name[1:]
        # the field usually carries the explanation, the setter and the getter rarely do
        # the Java field, the Kotlin property, the setter and the getter, in this order
        candidates = [rf"\b{re.escape(name)}\s*(?:=[^;]*)?$", rf"^(?:\w+\s+)*(?:val|var)\s+{re.escape(name)}\b",
                      rf"\bset{capitalized}\s*\(", rf"\b(?:get|is){capitalized}\s*\("]
    else:
        candidates = [rf"\b{re.escape(name)}\s*\("]
    blocks = [(match.group(1), match.group(2)) for match in
              re.finditer(r"/\*\*(.*?)\*/\s*(?:@\w+(?:\([^)]*\))?\s*)*((?:(?!/\*)[^;{])*)", text, re.S)]
    for candidate in candidates:
        for comment, declaration in blocks:
            if re.search(candidate, declaration.strip()):
                cleaned = clean_javadoc(comment)
                if cleaned:
                    return cleaned
    return None


def clean_javadoc(comment: str) -> str:
    lines = [re.sub(r"^\s*\*?\s?", "", line) for line in comment.strip().splitlines()]
    body = []
    for line in lines:
        if line.lstrip().startswith("@"):
            break
        body.append(line)
    text = "\n".join(body)
    text = re.sub(r"\{@(?:link|linkplain)\s+[^\s}]+\s+([^}]+)\}", r"\1", text)
    text = re.sub(r"\{@(?:code|link|linkplain|literal)\s+([^}]*)\}", r"`\1`", text)
    text = re.sub(r"<p>|<br\s*/?>", "\n\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:1500]


# ---------------------------------------------------------------------------------------------
# the inventory


@dataclasses.dataclass
class Answer:
    status: str
    detail: str


class Inventory:
    def __init__(self, path: Path):
        self.path = path
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as error:
            raise ToolError(f"{path} cannot be read: {error}") from error
        self.client = data.get("client") or {}
        self.surfaces = data.get("surfaces") or []
        self.self_test = data.get("self-test")
        self.answers: dict[str, dict[str, Answer]] = {}
        problems = []
        for key in ("name", "version-properties", "suggested-key-prefix"):
            if key not in self.client:
                problems.append(f"'client' has no '{key}'")
        surface_ids = [surface.get("id") for surface in self.surfaces]
        for surface in self.surfaces:
            for key in ("id", "title", "read", "artifacts"):
                if key not in surface:
                    problems.append(f"a surface has no '{key}': {surface}")
            if surface.get("read") != "spring-metadata" and not surface.get("classes"):
                problems.append(f"surface '{surface.get('id')}' names no classes")
        settings = data.get("settings") or {}
        for surface_id, by_status in settings.items():
            if surface_id not in surface_ids:
                problems.append(f"'settings' has a section '{surface_id}', but there is no such surface")
                continue
            answers = self.answers.setdefault(surface_id, {})
            for status, groups in (by_status or {}).items():
                if status not in STATUSES:
                    problems.append(f"'{surface_id}' has the status '{status}'; it can be {', '.join(STATUSES)}")
                    continue
                field = STATUS_FIELD[status]
                for group in groups or []:
                    names = group.get("names") or []
                    detail = group.get(field)
                    if not names:
                        problems.append(f"a '{status}' group of '{surface_id}' names nothing")
                    if not detail:
                        problems.append(f"the '{status}' group of '{surface_id}' with {names[:3]} has no '{field}'")
                    for name in names:
                        if name in answers:
                            problems.append(f"'{surface_id}' answers '{name}' twice")
                        answers[name] = Answer(status, str(detail))
        if problems:
            raise ToolError(f"{path} is not valid:\n  - " + "\n  - ".join(problems))

    def answer(self, surface_id: str, name: str) -> Answer | None:
        return self.answers.get(surface_id, {}).get(name)


def versions_in_pom(text: str, properties: list[str]) -> dict[str, str]:
    result = {}
    for prop in properties:
        match = re.search(rf"<{re.escape(prop)}>\s*([^<\s]+)\s*</{re.escape(prop)}>", text)
        if match:
            result[prop] = match.group(1)
    return result


# ---------------------------------------------------------------------------------------------
# comparing


@dataclasses.dataclass
class Change:
    surface: dict
    setting: Setting
    kind: str  # new, gone, deprecated
    answer: Answer | None


def compare(inventory: Inventory, old: str, new: str) -> list[Change]:
    changes = []
    for surface in inventory.surfaces:
        before = read_surface(surface, old)
        after = read_surface(surface, new)
        for name in sorted(after.keys() - before.keys()):
            changes.append(Change(surface, after[name], "new", inventory.answer(surface["id"], name)))
        for name in sorted(before.keys() - after.keys()):
            changes.append(Change(surface, before[name], "gone", inventory.answer(surface["id"], name)))
        for name in sorted(after.keys() & before.keys()):
            if after[name].deprecated and not before[name].deprecated:
                changes.append(Change(surface, after[name], "deprecated", inventory.answer(surface["id"], name)))
    return changes


def normalized(surface: dict, name: str) -> str:
    """One spelling for a setting, so that a setter and a starter property of it meet."""
    leaf = name[len(surface.get("prefix", "")):] if name.startswith(surface.get("prefix", "")) else name
    return re.sub(r"[^a-z0-9]", "", leaf.lower())


def suggested_key(inventory: Inventory, change: Change) -> str:
    """The key the adapter would get, in the kebab case of VanillaBP's other keys."""
    name = change.setting.name
    prefix = change.surface.get("prefix")
    if prefix and name.startswith(prefix):
        leaf = name[len(prefix):]
    else:
        if change.surface.get("qualify"):
            name = name.rsplit(".", 1)[-1]
        if change.surface["read"] == "constants":
            # a constant names its key in its value, and its own name is in upper case
            name = change.setting.constant or name.lower().replace("_", "-")
        leaf = re.sub(r"(?<=[a-z0-9])([A-Z])", r"-\1", name).lower()
    return inventory.client["suggested-key-prefix"] + leaf


@dataclasses.dataclass
class Finding:
    """What becomes one issue: one setting, maybe seen on several surfaces."""
    kind: str  # new or deprecated
    key: str
    changes: list[Change]


def findings_for_issues(changes: list[Change]) -> list[Finding]:
    grouped: dict[tuple[str, str], Finding] = {}
    for change in changes:
        if change.kind == "new" and change.answer is None:
            kind = "new"
        elif change.kind == "deprecated" and change.answer is not None and change.answer.status == "supported":
            kind = "deprecated"
        else:
            continue
        key = normalized(change.surface, change.setting.name)
        grouped.setdefault((kind, key), Finding(kind, key, [])).changes.append(change)
    return list(grouped.values())


# ---------------------------------------------------------------------------------------------
# GitHub


def gh(*args: str, token: str | None = None, data: dict | None = None) -> object:
    environment = dict(os.environ)
    if token:
        environment["GH_TOKEN"] = token
    command = ["gh", "api", *args]
    if data is not None:
        command += ["--input", "-"]
    completed = subprocess.run(command, input=json.dumps(data) if data is not None else None,
                               capture_output=True, text=True, env=environment)
    if completed.returncode != 0:
        raise ToolError(f"gh {' '.join(args[:2])} failed: {completed.stderr.strip() or completed.stdout.strip()}")
    output = completed.stdout.strip()
    return json.loads(output) if output else None


def gh_lines(*args: str) -> list[dict]:
    """A paginated listing, one compact JSON object per line, so the pages need no joining."""
    completed = subprocess.run(["gh", "api", "--paginate", *args, "--jq", ".[]"], capture_output=True, text=True)
    if completed.returncode != 0:
        raise ToolError(f"gh {args[0]} failed: {completed.stderr.strip()}")
    return [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]


def existing_issue(repository: str, finding: Finding) -> dict | None:
    """An issue about the same setting, open or closed. A closed one means somebody decided."""
    names = {change.setting.name for change in finding.changes}
    marker = MARKER.format(f"{finding.kind}:{finding.key}")

    def matches(issue: dict) -> bool:
        if "pull_request" in issue:
            return False
        if marker in (issue.get("body") or ""):
            return True
        title = issue.get("title") or ""
        return any(re.search(rf"(?<![\w.-]){re.escape(name)}(?![\w-])", title) for name in names) and (
            (finding.kind == "deprecated") == ("deprecated" in title.lower()))

    # the label finds what this script opened, without the delay of the search index
    for issue in gh_lines(f"repos/{repository}/issues?state=all&labels={LABEL}&per_page=100"):
        if matches(issue):
            return issue
    # the search finds an issue somebody opened by hand
    for name in sorted(names):
        query = f'repo:{repository} is:issue "{name}"'
        found = gh("-X", "GET", "search/issues", "-f", f"q={query}", "-f", "per_page=50") or {}
        for issue in found.get("items", []):  # type: ignore[union-attr]
            if matches(issue) or name in (issue.get("body") or ""):
                return issue
    return None


def ensure_label(repository: str) -> None:
    try:
        gh(f"repos/{repository}/labels/{LABEL}")
    except ToolError:
        gh(f"repos/{repository}/labels", "-X", "POST",
           data={"name": LABEL, "color": LABEL_COLOR, "description": LABEL_DESCRIPTION})


def add_to_project(issue: dict, project: int, owner: str, token: str) -> str:
    query = "query($owner:String!,$number:Int!){organization(login:$owner){projectV2(number:$number){id}}}"
    answer = gh("graphql", "-f", f"query={query}", "-f", f"owner={owner}", "-F", f"number={project}", token=token)
    project_id = answer["data"]["organization"]["projectV2"]["id"]  # type: ignore[index]
    mutation = "mutation($project:ID!,$content:ID!){addProjectV2ItemById(input:{projectId:$project,contentId:$content}){item{id}}}"
    gh("graphql", "-f", f"query={mutation}", "-f", f"project={project_id}", "-f", f"content={issue['node_id']}",
       token=token)
    return f"added to project {project}"


def issue_text(inventory: Inventory, finding: Finding, old: str, new: str, pull_request: str | None) -> tuple[str, str]:
    client = inventory.client["name"]
    first = finding.changes[0]
    if finding.kind == "new":
        title = f"{client} {new}: new {first.surface['title']} {first.setting.name}"
    else:
        title = f"{client} {new}: supported {first.surface['title']} {first.setting.name} is deprecated"
    lines = [MARKER.format(f"{finding.kind}:{finding.key}"), ""]
    if finding.kind == "new":
        lines += [
            f"{client} {new} has a setting which {client} {old} did not have. The adapter only passes on the",
            "settings it has a key for, so users of the adapter cannot set this one yet.",
            "",
        ]
    else:
        lines += [
            f"{client} {new} deprecates a setting which the adapter supports. No build fails on that,",
            "but the adapter uses no deprecated API, so the key has to move to what replaces it.",
            "",
        ]
    lines += ["| Where | Name |", "|---|---|"]
    for change in finding.changes:
        where = change.surface["title"]
        if change.setting.source and change.surface["read"] != "spring-metadata":
            where += f" (`{change.setting.source.rsplit('.', 1)[-1]}`)"
        lines.append(f"| {where} | `{change.setting.name}` |")
    lines.append("")
    description = None
    for change in finding.changes:
        description = describe(change.surface, change.setting, new)
        if description:
            break
    lines += ["**What it does**", ""]
    if description:
        lines += ["> " + line if line else ">" for line in description.splitlines()]
    else:
        lines.append(f"{client} {new} has no description of it in the jars this check reads. Its source code is"
                     " the only description.")
    lines.append("")
    if finding.kind == "new":
        # a starter names its key already, so its spelling is the better suggestion
        key = next((suggested_key(inventory, c) for c in finding.changes if c.surface.get("prefix")),
                   suggested_key(inventory, first))
        lines += [f"**Suggested key:** `{key}`", ""]
    else:
        lines += [f"**The key which sets it today:** `{first.answer.detail}`", ""]  # type: ignore[union-attr]
    if pull_request:
        lines += [f"**Found in:** {pull_request}, which raises {client} from {old} to {new}.", ""]
    else:
        lines += [f"**Found by:** a run of the check by hand, comparing {client} {old} with {new}.", ""]
    lines += [
        "**What to do**",
        "",
        "Decide whether a user of the adapter needs this, and write the answer into",
        "`client-configuration-inventory.yml` at the root of the repository:",
        "",
        "- `supported`, with the key which sets it, once the adapter has that key.",
        "- `not-supported`, with the reason why a user does not need it. That is a good answer too.",
        "- `open`, with a link to this issue, while nobody has decided yet.",
        "",
        "A check opened this issue. It opens one issue per setting, and it does not open a second one, even"
        " after this one is closed.",
    ]
    return title, "\n".join(lines)


# ---------------------------------------------------------------------------------------------
# commands


class Report:
    def __init__(self, summary: str | None):
        self.summary = summary
        self.lines: list[str] = []

    def add(self, line: str = "") -> None:
        self.lines.append(line)
        print(line)

    def write(self) -> None:
        if self.summary:
            with open(self.summary, "a", encoding="utf-8") as file:
                file.write("\n".join(self.lines) + "\n")


def run_check(args: argparse.Namespace) -> int:
    inventory = Inventory(Path(args.inventory))
    report = Report(args.summary)
    client = inventory.client["name"]
    report.add(f"## {client} settings")
    report.add()
    if args.old and args.new:
        bumps = [("given", args.old, args.new)]
        heads = [args.new]
    else:
        properties = inventory.client["version-properties"]
        head_pom = Path(args.head_pom).read_text(encoding="utf-8")
        head = versions_in_pom(head_pom, properties)
        missing = [prop for prop in properties if prop not in head]
        if missing:
            raise ToolError(f"{args.head_pom} has no {', '.join(missing)}, so this cannot know the client version.")
        base_text = Path(args.base_pom).read_text(encoding="utf-8") if args.base_pom else ""
        base = versions_in_pom(base_text, properties)
        bumps = [(prop, base[prop], head[prop]) for prop in properties if base.get(prop, head[prop]) != head[prop]]
        heads = list(dict.fromkeys(head[prop] for prop in properties))
    repository = args.repository or os.environ.get("GITHUB_REPOSITORY")
    if not bumps:
        report.add(f"This pull request does not change the {client} version, so there is nothing new to report.")
    for prop, old, new in bumps:
        report.add(f"### {client} {old} → {new}" + (f" (`{prop}`)" if prop != "given" else ""))
        report.add()
        changes = compare(inventory, old, new)
        report_changes(report, changes)
        for finding in findings_for_issues(changes):
            handle_finding(report, inventory, finding, old, new, args, repository)
        report.add()
    report_coverage(report, inventory, heads)
    report.write()
    return 0


def report_changes(report: Report, changes: list[Change]) -> None:
    if not changes:
        report.add("No setting was added, removed or deprecated.")
        report.add()
        return
    words = {"new": "new", "gone": "gone", "deprecated": "newly deprecated"}
    report.add("| Change | Surface | Setting | Inventory |")
    report.add("|---|---|---|---|")
    for change in changes:
        answer = "not listed"
        if change.answer:
            # a reason is too long for a table, a key and an issue are what somebody looks for
            answer = change.answer.status
            if change.answer.status in ("supported", "open"):
                answer += f": {change.answer.detail}"
        report.add(f"| {words[change.kind]} | {change.surface['id']} | `{change.setting.name}` | {answer} |")
    report.add()


def handle_finding(report: Report, inventory: Inventory, finding: Finding, old: str, new: str,
                   args: argparse.Namespace, repository: str | None) -> None:
    title, body = issue_text(inventory, finding, old, new, args.pull_request)
    if not repository:
        report.add(f"- Would open the issue '{title}', but no repository is known to look for a duplicate in.")
        print(body)
        return
    duplicate = existing_issue(repository, finding)
    if duplicate:
        report.add(f"- '{title}': already there as {duplicate['html_url']} ({duplicate['state']}), nothing opened.")
        return
    if not args.create_issues:
        report.add(f"- Dry run: would open the issue '{title}' in {repository}.")
        print("----- issue body -----")
        print(body)
        print("----- end of issue body -----")
        return
    ensure_label(repository)
    issue = gh(f"repos/{repository}/issues", "-X", "POST", data={"title": title, "body": body, "labels": [LABEL]})
    project = inventory.client.get("project")
    where = f"project {project} picks it up by its label '{LABEL}'"
    token = os.environ.get("PROJECT_TOKEN")
    if project and token:
        where = add_to_project(issue, int(project), repository.split("/")[0], token)  # type: ignore[arg-type]
    report.add(f"- Opened {issue['html_url']} ({where}).")  # type: ignore[index]


def report_coverage(report: Report, inventory: Inventory, versions: list[str]) -> None:
    """What the inventory does not answer, and what it answers for nothing."""
    unanswered = []
    present: dict[str, set[str]] = {}
    for surface in inventory.surfaces:
        names: set[str] = set()
        for version in versions:
            names |= read_surface(surface, version).keys()
        present[surface["id"]] = names
        unanswered += [(surface["id"], name) for name in sorted(names)
                       if inventory.answer(surface["id"], name) is None]
    stale = [(surface_id, name) for surface_id, answers in inventory.answers.items()
             for name in sorted(answers) if name not in present.get(surface_id, set())]
    report.add(f"### The inventory against {inventory.client['name']} {', '.join(versions)}")
    report.add()
    total = sum(len(names) for names in present.values())
    report.add(f"{total} settings read, {total - len(unanswered)} of them answered by the inventory.")
    if unanswered:
        report.add()
        report.add("Not answered yet, each should have an issue:")
        for surface_id, name in unanswered:
            report.add(f"- {surface_id}: `{name}`")
    if stale:
        report.add()
        report.add("Answered, but the client no longer has them:")
        for surface_id, name in stale:
            report.add(f"- {surface_id}: `{name}`")


def run_self_test(args: argparse.Namespace) -> int:
    inventory = Inventory(Path(args.inventory))
    test = inventory.self_test
    if not test:
        raise ToolError(f"{args.inventory} has no 'self-test' block.")
    old, new = str(test["old"]), str(test["new"])
    changes = compare(inventory, old, new)
    found = {(change.surface["id"], change.setting.name) for change in changes if change.kind == "new"}
    missing = [(surface_id, name) for surface_id, names in test["new-settings"].items() for name in names
               if (surface_id, name) not in found]
    for change in changes:
        if change.kind == "new":
            print(f"new in {new}: {change.surface['id']} {change.setting.name}")
    if missing:
        raise ToolError(f"Comparing {old} with {new} must find these new settings, and did not: {missing}")
    print(f"Self test passed: {old} → {new} finds every setting the inventory expects.")
    return 0


def run_list(args: argparse.Namespace) -> int:
    inventory = Inventory(Path(args.inventory))
    for surface in inventory.surfaces:
        for name, setting in sorted(read_surface(surface, args.version).items()):
            answer = inventory.answer(surface["id"], name)
            if args.unanswered and answer:
                continue
            flags = " (deprecated)" if setting.deprecated else ""
            print(f"{surface['id']}\t{name}{flags}\t{answer.status if answer else '-'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--inventory", default="client-configuration-inventory.yml")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check")
    check.add_argument("--base-pom")
    check.add_argument("--head-pom", default="pom.xml")
    check.add_argument("--old")
    check.add_argument("--new")
    check.add_argument("--repository", help="owner/name, default $GITHUB_REPOSITORY")
    check.add_argument("--pull-request", help="the URL of the pull request which raises the client")
    check.add_argument("--create-issues", action="store_true")
    check.add_argument("--summary", help="a Markdown file to append the report to")
    commands.add_parser("self-test")
    listing = commands.add_parser("list")
    listing.add_argument("--version", required=True)
    listing.add_argument("--unanswered", action="store_true")
    args = parser.parse_args()
    try:
        return {"check": run_check, "self-test": run_self_test, "list": run_list}[args.command](args)
    except ToolError as error:
        print(f"::error::{error}" if os.environ.get("GITHUB_ACTIONS") else f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
