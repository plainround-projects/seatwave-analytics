#!/usr/bin/env bash
# Mutation check of every test that guards a rule: each mutant removes one rule from a copy of the
# project under build/mutants, runs the unit tests, and lists the tests that failed. A mutant that
# no test kills fails this script. The first three are the module seed's mutants.
# Usage: scripts/mutants.sh
set -euo pipefail
cd "$(dirname "$0")/.."
readonly MAIN="src/main/java/io/plainround/seatwave/analytics"
readonly KAFKA="kafka/src/main/java/io/plainround/seatwave/analytics/kafka"

# name | file | text to replace | replacement | rule
mutants=(
  "nodedupe|$MAIN/MinuteWindows.java|if (settings.isDeduplicating() && !window.seenMessageIds.add(event.messageId())) {|if (settings.isDeduplicating() && !window.seenMessageIds.add(event.messageId()) && accepted < 0) {|a repeated message_id is counted again"
  "nocorrection|$MAIN/MinuteWindows.java|closed.merge(key, one, Totals::plus);|closed.merge(key, Totals.ZERO, Totals::plus);|the side-output replay adds nothing"
  "sameversion|$MAIN/WarehouseTable.java|if (event.bookingVersion() == existing.bookingVersion()) {|if (event.bookingVersion() == existing.bookingVersion() && rowsByBookingId.isEmpty()) {|a repeated version rewrites the row"
  "processing-time|$KAFKA/SalesPerMinuteTopology.java|.withTimestampExtractor(new OccurredAtExtractor()))|)|windows are keyed by the send time instead of occurred_at"
  "late-dropped|$KAFKA/SalesPerMinuteTopology.java|boolean isLate = closesAtMillis <= context.currentStreamTimeMs();|boolean isLate = closesAtMillis < 0;|a record after the grace is dropped by the count with no trace"
  "unparseable-stops|$KAFKA/BookingEventJson.java|return new ReadResult.Unreadable(\"not JSON: \" + notJson.getOriginalMessage());|throw new IllegalStateException(\"not JSON\", notJson);|an unparseable record stops the stream instead of going to the dead-letter topic"
  "load-twice|warehouse/fact_bookings.sql|WHERE EXCLUDED.booking_version > fact_bookings.booking_version;|;|loading a partition again rewrites every row"
)

failed_tests() {
  python3 - "$1" <<'PY'
import glob, sys, xml.etree.ElementTree as tree
names = []
for report in sorted(glob.glob(sys.argv[1] + "/*.xml")):
    for case in tree.parse(report).getroot().iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            names.append(case.get("classname").rsplit(".", 1)[-1] + "." + case.get("name").split("(")[0])
print(", ".join(names) if names else "none")
PY
}

survivors=0
for mutant in "${mutants[@]}"; do
  IFS='|' read -r name file original replacement rule <<< "$mutant"
  copy="build/mutants/$name"
  rm -rf "$copy"
  mkdir -p "$copy"
  tar --exclude ./build --exclude ./.gradle --exclude ./lake -cf - . | tar -xf - -C "$copy"
  python3 - "$copy/$file" "$original" "$replacement" <<'PY'
import pathlib, sys
path, original, replacement = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
text = path.read_text(encoding="utf-8")
if text.count(original) != 1:
    sys.exit("mutant text not found exactly once in " + str(path) + ": " + original)
path.write_text(text.replace(original, replacement), encoding="utf-8")
PY
  ( cd "$copy" && ./gradlew --quiet test -x spotlessCheck > test.log 2>&1 ) || true
  unit="$(failed_tests "$copy/build/test-results/test")"
  echo "$name ($rule): tests failed: $unit"
  if [[ "$unit" == "none" ]]; then
    survivors=$((survivors + 1))
  fi
done
echo "mutants: ${#mutants[@]}, survivors: $survivors"
[[ "$survivors" -eq 0 ]]
