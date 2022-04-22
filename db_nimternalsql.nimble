# Package

version     = "1.3.2"
author      = "Rene Hartmann"
description = "An in-memory SQL database library"
license     = "MIT"
srcDir      = "src"

# Deps

requires "nim >= 1.6.0"

task test, "Runs all the tests":
  cd "tests"
  exec "nim c -r alltests.nim"
