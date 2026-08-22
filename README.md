# hive-dsl

<!-- hive-badges -->

[![Clojars Project](https://img.shields.io/clojars/v/io.github.hive-agi/hive-dsl.svg)](https://clojars.org/io.github.hive-agi/hive-dsl)
[![cljdoc](https://cljdoc.org/badge/io.github.hive-agi/hive-dsl)](https://cljdoc.org/d/io.github.hive-agi/hive-dsl/CURRENT)
[![release](https://github.com/hive-agi/hive-dsl/actions/workflows/release.yml/badge.svg)](https://github.com/hive-agi/hive-dsl/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

<!-- /hive-badges -->

**The error-handling and value vocabulary the hive libraries are written in.** A
`Result` type, algebraic data types, total coercions, bounded state, and
lifecycle scopes — portable `.cljc` with no dependency beyond Clojure,
`data.json` and `core.async`.

Failures here are *values*, not control flow. A function that can fail returns
`(r/err :kind {:ctx …})`; callers thread with `let-ok` / `ok->` and never
have to guess which exception a library will throw.

## Coordinates

```clojure
;; deps.edn
io.github.hive-agi/hive-dsl {:mvn/version "0.5.17"}
```

## Result

```clojure
(require '[hive-dsl.result :as r])

(r/ok 42)                               ;; => {:ok 42}
(r/err :db/timeout {:url url})          ;; => {:error :db/timeout, :url url}

;; Monadic let — binds :ok values, short-circuits on the first error
(r/let-ok [conn (connect! url)
           rows (query conn sql)]
  (r/ok (count rows)))

;; Thread-first with smart-wrap: a step returning a Result binds,
;; a step returning a plain value is auto-wrapped in ok
(r/ok-> (validate-order order catalog)
        price-order
        (acknowledge create-letter)
        log-order)

;; Supervision boundary — catch ANY throwable, return the fallback.
;; Error context rides along as metadata, not as a log line.
(r/rescue [] (traverse ids))            ;; => [] on failure
(::r/error (meta result))               ;; => {:message "…" :form "(traverse ids)"}

;; Selective catch, when you care WHAT failed
(r/guard java.io.IOException nil (slurp path))
```

`let-ok` is **strict**: a Result-bound right-hand side that evaluates to a
non-Result throws with category `:result/non-result-binding`, naming the
offending symbol — so a function that quietly stopped returning a Result is a
loud failure rather than a silent one. Bind plain values with `:let [v expr]`.

`map-ok`, `map-err`, `bind`, `on-error`, `with-error-handler`, `ensure-result`
and `rescue-log` cover the rest of the surface. `hive-dsl.result.taxonomy`
registers the error-category vocabulary so failures stay a closed, greppable set.

## ADTs

```clojure
(require '[hive-dsl.adt :as adt])

(adt/defadt EventType
  "Event types for hivemind communication."
  [:event/started  {:task string?}]
  [:event/progress {:message string?}]
  :event/completed)

(event-type :event/started {:task "X"})
;; => {:adt/type :EventType, :adt/variant :event/started, :task "X"}

(adt/adt-case EventType evt
  :event/started   (str "task: " (:task evt))
  :event/progress  (str "msg: "  (:message evt))
  :event/completed "done")
```

`defadt` generates the type var, a kebab-case constructor, a `…?` predicate, a
`->…` keyword coercion, and `EventTypeMalli` — one malli `:multi` schema for the
whole ADT. `adt-case` checks exhaustiveness **at macro-expansion time**: a
missing variant or a typo is a compile error, not a runtime `nil`.

Closed sets get an ADT. The variant table is the single definition; validation,
`serialize` / `deserialize` and `hive-dsl.adt.schema/adt->malli` are all
projections of it.

## The rest

| Namespace | Provides |
|---|---|
| `hive-dsl.result` | `Result` type, `let-ok`, `ok->`, `rescue`, `guard` |
| `hive-dsl.result.taxonomy` | Registry of error kinds |
| `hive-dsl.result.agentop` | Agent-flavoured combinators — `retry-on`, `with-budget`, `with-persona`, `fan-in` |
| `hive-dsl.adt` | `defadt`, `adt-case`, variant construction and validation |
| `hive-dsl.coerce` | Total coercions — `->int`, `->double`, `->boolean`, `->keyword`, `->vec`, `->enum`, `coerce-map` |
| `hive-dsl.bounded-atom` | Size-capped atom with sweeping, so a cache cannot grow without bound |
| `hive-dsl.gate` | Permit gate returning Results |
| `hive-dsl.lifecycle` | Start/stop scopes, managed executors and channels |
| `hive-dsl.managed-channel` | `core.async` channels owned by a scope — pub/sub and go-loops that close cleanly |
| `hive-dsl.resource` | Acquire/release scopes |
| `hive-dsl.batch` | Transaction batching with a transparent batch scope |
| `hive-dsl.context.identity` | Caller-id and project-scope encoding |
| `hive-dsl.typed.emit` | Typed Clojure annotations emitted from the ADT tables |

## Portability

The shipped namespaces are `.cljc` and are exercised on the JVM, on
ClojureScript, and on [cloture](https://github.com/ruricolist/cloture) (Clojure
on Common Lisp). The portable stratum is bounded by the poorest runtime that
tests it, which is why the source avoids conveniences the third arm lacks.

Malli value objects live under `schemas/`, which is deliberately **never** on
`:paths` — malli does not run on the native targets, so no shipped namespace may
see it.

## License

MIT.
