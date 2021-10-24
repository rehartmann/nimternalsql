## Unicode LIKE matching
import unicode

const matchMany = runeAt("%", 0)
const matchSingle = runeAt("_", 0)

func matchesLike*(s: seq[Rune], pattern: seq[Rune], startAt: Natural = 0): bool =
  var patIdx = 0
  var idx = startAt

  while idx < s.len:
    if patIdx >= pattern.len:
      return false
    case pattern[patIdx]:
      of matchSingle:
        idx += 1
        patIdx += 1
      of matchMany:
        for i in idx..s.len:
          if matchesLike(s, pattern[patIdx + 1..pattern.high], i):
            return true
        return false
      else:
        if s[idx] != pattern[patIdx]:
          return false
        idx = idx + 1
        patIdx = patIdx + 1
  for c in pattern[patIdx..pattern.high]:
    if c != matchMany:
      return false
  result = true

func matchesLike*(s: string, pattern: string): bool =
  result = matchesLike(toRunes(s), toRunes(pattern))
