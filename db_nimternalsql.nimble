# Package

version     = "1.0.0"
author      = "Rene Hartmann"
description = "An in-memory SQL database library"
license     = "MIT"
srcDir      = "src"

# Deps

requires "nim >= 1.4.0"

task test, "Runs all the tests":
  cd "tests"
  exec "nim c -r alltests.nim"
