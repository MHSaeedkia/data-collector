# Memory & Codebase Intelligence

This project has **two distinct memory systems**. They do not overlap — use both, for different things.

| System                      | What it holds                                                                                     | Who writes it                    | How to access             |
| --------------------------- | ------------------------------------------------------------------------------------------------- | -------------------------------- | ------------------------- |
| `./memory/` (files)         | Human-authored durable knowledge: decisions, conventions, gotchas, todos, "why we did X"          | You and Claude, in prose         | Read/write files directly |
| `codebase-memory-mcp` (MCP) | Auto-generated structural map of the code: call chains, architecture, routes, impact/blast-radius | The indexer (never hand-written) | Query via the MCP tools   |

Rule of thumb: **`./memory/` = intent and history. MCP = structure of the code as it is right now.**

---

## Memory Rules — `./memory/` (CRITICAL — Non-Negotiable)

- **ALWAYS read `./memory/` before doing ANYTHING in this project.**
- **NEVER store project memory in global memory (`~/.claude/` or anywhere outside this project root).** All authored memory lives in `./memory/` only.
- After finishing any implementation, **update `./memory/` immediately** — record decisions made, conventions discovered, and anything a future session would waste time re-deriving.
- Always update `todo.md` when you are done.
- **If you ignore any bit of memory or these rules, the user will be mad at you.**

Do **not** duplicate structural facts (call graphs, who-calls-what, file lists) into `./memory/` — that's the MCP's job and it will go stale. `./memory/` is for reasoning and decisions, not for a map of the code.

---

# Token Management

- **Read files chunk by chunk.** This is the main token-saving lever.
- Always update `todo.md` when you are done.

---

# Guidelines

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.
- **Before editing shared/widely-called code, run an impact/call-path query via `codebase-memory-mcp`** so you know the blast radius before you touch it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. Signal Uncertainty

**Don't state guesses as facts. When confidence is low, say so.**

When your knowledge is incomplete, a hallucination inferred, or unverified:

- Preface responses with "possibly", "likely", "I'm not certain", or "you should verify this" — don't omit them.
- Distinguish between what you know and what you're inferring.
- If a claim requires external verification before acting on it, flag that explicitly.
- Never let confident tone substitute for confident knowledge.

When you notice you're filling a gap with an assumption:

- Name the gap: "I don't have visibility into X, so I'm assuming Y."
- Offer to stop rather than guess: "I can proceed on that assumption, or you can verify first."
- Don't bury uncertainty at the end of a long confident response.

The test: Could a developer act on this response and only discover it was wrong after the damage is done? If yes, the uncertainty wasn't signalled clearly enough.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, clarifying questions come before implementation rather than after mistakes, structural questions are answered from the MCP instead of costly file exploration, and wrong information is flagged before it causes damage.
