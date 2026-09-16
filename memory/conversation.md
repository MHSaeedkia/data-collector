---
name: conversation
description: HARD RULE from the user 2026-09-16 — in Persian replies, never start a sentence, list item, heading or table cell with a non-Persian word (English word, code identifier, unit, file link, backticked code), because the line's direction flips and the sentence becomes unreadable.
metadata:
    type: feedback
---

# Talking to the people who work on this

**In a Persian conversation, no sentence starts with a non-Persian word.**
Not an English word, a code identifier, a unit like `bps`, a file link or
backticked code. The same holds for the first word of every list item, heading
and table cell.

## Why

A line's direction is set by its first strong character. Open it with an English
word and the whole line is laid out left to right, so the Persian words after it
come out in the wrong order and the sentence cannot be read.

## How to apply

When the English term has to come early, rephrase so a Persian word leads:

| Not | But |
|---|---|
| `market` یک ستون است | ستون `market` … |
| `STALENESS_MS` جلوی اجرا را می‌گیرد | مقدار `STALENESS_MS ` … |
| bps مخفف basis points است | واحد bps مخفف … |

English inside a sentence is fine; only the opening word matters. Code blocks
are exempt, since they are a block of their own.
