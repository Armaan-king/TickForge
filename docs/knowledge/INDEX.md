# Knowledge Base

Curated engineering memory for TickForge. Everything here is something the
**source code cannot tell you**: why an abstraction exists, why a boundary is
where it is, what must not be merged, what bit us once.

**The source code is always the ultimate source of truth.** These documents are
hints and reasons, not facts about the current state of the code. If a document
and the code disagree, the code wins — then fix the document.

## What belongs here

✅ Design reasons — why this abstraction, why this boundary
✅ Architectural rules — what must stay separate, and what breaks if it doesn't
✅ Domain knowledge — terms, invariants, rules of the problem space
✅ Non-obvious interactions — behaviour you'd never guess from reading one file
✅ Rejected alternatives — so they don't get re-proposed every three months

## What does NOT belong here

❌ Which file imports which — `rg` knows, and it's never stale
❌ Function signatures, class members, parameter lists — read the code
❌ Anything a grep answers in one call
❌ Step-by-step implementation detail — it dates the moment you refactor

## Map

| File | Holds |
| --- | --- |
| `architecture.md` | The shape of the system and where its boundaries are |
| `decisions.md` | Dated decisions, with reasons and rejected alternatives |
| `pitfalls.md` | Cross-cutting traps — ones that bite regardless of component |
| `concepts/` | One file per real concept. Component-specific gotchas live here |

Concept files are created when a concept becomes real, not in advance.
Copy `concepts/_TEMPLATE.md` to start one.

## When to update

The trigger is **surprise**, not "significant change" — surprise fires on its
own, "significant" requires a judgment call you'll skip when you're tired.

Write something down when:

- You were surprised by how something works
- Someone had to explain a *why* to you that wasn't in the code
- You made a design decision that a future reader could reasonably undo
- You hit a trap and worked out how to avoid it
- You considered an approach and rejected it for a reason worth preserving

Do not update for renames, refactors, or new functions. Those are code events.
