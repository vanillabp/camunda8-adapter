#!/usr/bin/env bash
#
# Is a release line the preview line? The answer is the camunda8.line.preview property of
# the line-<line> profile, and this script reads it out of the POM text.
#
# Asking Maven for it needs the credentials of whoever runs it. The POM of a repository
# which uses the client-api-changes workflow imports the VanillaBP BOM as a snapshot from
# GitHub Packages, and a job which only reports on a pull request has no token for that.
# The first run of that workflow in businesscockpit-camunda8-adapter died on exactly this.
# Maven could not resolve the import. The message went into a command substitution instead
# of into the log, so the preview line was read as a GA line, and the check went red over
# something which had nothing to do with the client. Text needs no credentials, and it is
# how the same workflow reads the client pins.
#
# The release lines are defined in this repository, so the POM read by default is the one
# next to this script. A workflow called from another repository checks this repository out
# to get the script, and gets the line definitions with it.
#
# Prints 'true' or 'false' and says on stderr what it read. Exit code 2 and a sentence on
# stderr mean it cannot tell. Nobody may then pick an answer anyway.
#
# Usage:  bin/line-preview.sh <line> [pom]
#
# The 'client-api-changes-selftest' job of .github/workflows/checks.yaml holds it against a
# preview profile, a GA profile, a line which is not in the POM and a value which is
# neither true nor false.
#
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $(basename "$0") <line> [pom]" >&2
  exit 2
fi

line="$1"
pom="${2:-$(dirname "$0")/../pom.xml}"

if [ ! -f "$pom" ]; then
  echo "There is no ${pom}, so nothing says whether line ${line} is the preview line." >&2
  exit 2
fi

# The profile of this line, from its id to the end of that profile. Profiles do not nest,
# so the first closing tag after the id closes this one, whatever it holds in between.
anchor="${line//./\\.}"
block="$(sed -n "/<id>line-${anchor}<\/id>/,/<\/profile>/p" "$pom")"

if [ -z "$block" ]; then
  echo "${pom} has no profile 'line-${line}'. The lines it defines are:" >&2
  sed -n 's|.*<id>line-\([0-9][^<]*\)</id>.*|  \1|p' "$pom" >&2
  echo "A line without a profile cannot be told apart from the preview line. The lines" >&2
  echo "are defined in vanillabp/camunda8-adapter, so add it there first." >&2
  exit 2
fi

value="$(printf '%s\n' "$block" \
  | sed -n 's|.*<camunda8\.line\.preview>\([^<]*\)</camunda8\.line\.preview>.*|\1|p')"

case "$value" in
  # A GA line carries no such property. The profile was read, so this is an answer and not
  # a guess made after something failed.
  "")
    echo "Line ${line} sets no camunda8.line.preview in ${pom}, so it is a GA line." >&2
    echo "false"
    ;;
  true | false)
    echo "Line ${line} sets camunda8.line.preview to ${value} in ${pom}." >&2
    echo "$value"
    ;;
  *)
    echo "Line ${line} sets camunda8.line.preview to '${value}' in ${pom}." >&2
    echo "That is neither true nor false. Two profiles for one line look like this too." >&2
    echo "Either way nobody here may pick an answer." >&2
    exit 2
    ;;
esac
