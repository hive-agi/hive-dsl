(ns hive-dsl.swarm-status
  "Closed-sum domain types for swarm slave lifecycle.

   Three ADTs:

   - SlaveStatus     — slave lifecycle states (mirrors datascript slave-statuses
                       enum but as a closed ADT, with :zombie added for
                       stale-sweep-marked rows).
   - LivenessSignal  — outcome of OS-level process liveness check.
   - LifecycleEvent  — transition events emitted by the registry on state
                       changes; ground truth for audit + the stale-sweep.

   SRP: Pure domain types, no I/O. Schema lives in
   hive-mcp.swarm.{datascript,datalevin}.schema; this ns provides typed
   construction + exhaustive dispatch."
  (:require [hive-dsl.adt :refer [defadt]]
            [hive-dsl.result.taxonomy :as tax]))

;; =============================================================================
;; ADT Definitions
;; =============================================================================

(defadt SlaveStatus
  "All valid slave statuses. Closed sum.

   :slave-status/zombie marks rows where the OS process is gone (kill -0
   failed) AND last-active-at is older than the stale threshold. agent_status
   default-filters these out unless :include-stale? is set."
  :slave-status/idle
  :slave-status/spawning
  :slave-status/starting
  :slave-status/initializing
  :slave-status/working
  :slave-status/blocked
  :slave-status/error
  :slave-status/terminated
  :slave-status/zombie)

(defadt LivenessSignal
  "OS-level liveness check outcome.

   :liveness/alive   — process responsive (kill -0 <pid> ok)
   :liveness/dead    — process gone (kill -0 returned ESRCH)
   :liveness/unknown — pid is nil (e.g. openrouter ling) or check failed
                       transiently (do NOT mark :zombie on unknown)."
  :liveness/alive
  :liveness/dead
  :liveness/unknown)

(defadt LifecycleEvent
  "Slave lifecycle transition events. One emitted per state change to
   the :swarm/lifecycle channel; receivers persist them as audit log
   and bump the corresponding :slave/{spawned-at,last-active-at,
   status-changed-at,alive?} attrs.

   :at is epoch milliseconds. Always set by the emitting site, never
   inferred — keeps audit lines reproducible across replays."
  [:lifecycle/spawned        {:slave-id string?
                              :at       int?
                              :pid      (some-fn nil? int?)}]
  [:lifecycle/status-changed {:slave-id string?
                              :from     keyword?
                              :to       keyword?
                              :at       int?}]
  [:lifecycle/heartbeat      {:slave-id string?
                              :at       int?}]
  [:lifecycle/zombified      {:slave-id       string?
                              :last-active-at int?
                              :at             int?}]
  [:lifecycle/terminated     {:slave-id string?
                              :reason   keyword?
                              :at       int?}])

;; =============================================================================
;; Error Taxonomy (registry coupling — slave-not-found etc.)
;; =============================================================================

(tax/register!
 #{:swarm/slave-not-registered
   :swarm/invalid-status-transition
   :swarm/stale-row
   :swarm/liveness-check-failed})
