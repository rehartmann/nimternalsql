import osproc

var failedTests: seq[string]

proc execTest(testFile: string) =
  if execCmd("nim c -r " & testFile) != 0:
    failedTests.add(testFile)

execTest("binary.nim")
execTest("boolean.nim")
execTest("char.nim")
execTest("commit.nim")
execTest("default.nim")
execTest("distinct.nim")
execTest("expjoin.nim")
execTest("group.nim")
execTest("impjoin.nim")
execTest("leftjoin.nim")
execTest("key.nim")
execTest("like.nim")
execTest("null.nim")
execTest("numeric.nim")
execTest("order.nim")
execTest("prepared.nim")
execTest("rollback.nim")
execTest("snapshots.nim")
execTest("subqueries.nim")
execTest("union.nim")
execTest("update.nim")
execTest("where.nim")
execTest("txlog.nim")
execTest("txlog_snapshot.nim")
execTest("datetime.nim")
execTest("cast.nim")
execTest("autoinc.nim")
execTest("autoinc_txlog.nim")
execTest("except.nim")
execTest("intersect.nim")
execTest("with.nim")

if failedTests.len > 0:
  echo "FAILURE: tests failed: " & $failedTests
