# Decisions

Newest first. One entry per decision that would otherwise get re-argued in six
months. Record the rejected options too — that's what stops the re-argument.

---

## 2026-09-02 — Knowledge base is curated markdown, not a structured graph

**Decision:** `docs/knowledge/` stores only what the source code cannot
explain — design reasons, architectural boundaries, domain knowledge, non-obvious
interactions. Plain markdown, modular, no machine-readable index.

**Why:** The dividing line is *derivable vs. not*. Anything derivable from the
source (imports, call graphs, signatures, "what does this change affect") is
answered correctly and instantly by `rg` and the compiler. Recording it by hand
produces a lower-fidelity second copy that drifts. Reasons and boundaries don't
drift when files move, so they're safe to write down.

**Rejected — YAML dependency graph (`relationships.yaml`):** duplicates the
import graph. Its failure mode is the dangerous one: an agent reads a stale
relationship as ground truth and *skips* the grep that would have caught it.
Wrong architectural memory is worse than none.

**Rejected — YAML index file:** a list of concept files that `ls` already
produces, with the added property of being able to go stale.

**Rejected — single growing `Project_Knowledge.md`:** forces a full read for
one fact, and grows until nobody maintains it.

**Consequence:** the knowledge base is only as good as its update discipline.
The trigger is surprise (see `INDEX.md`), chosen because it self-detects where
"update on significant change" does not.

---

## YYYY-MM-DD — _Template, copy this_

**Decision:** _what was decided_

**Why:** _the reasoning that would otherwise be lost_

**Rejected:** _the alternatives, and what was wrong with each_

**Consequence:** _what this costs us, or what it now constrains_
