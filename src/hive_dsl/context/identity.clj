(ns hive-dsl.context.identity
  "Context identity ADTs for channel isolation.

   Two value objects + two pure projection functions.
   All channel buffers (memory, catchup, async, hivemind) derive their
   keys from these types — eliminates ad-hoc string construction.

   SRP: Pure domain logic, no I/O. Follows kanban/domain.clj precedent."
  (:require [hive-dsl.adt :refer [defadt adt-case]]))

;; =============================================================================
;; ADT Definitions
;; =============================================================================

(defadt CallerId
  "Caller identity — who is making the MCP request.
   Extracted from _caller_id (CLAUDE_SWARM_SLAVE_ID)."
  :caller/coordinator
  [:caller/named {:slave-id string?}])

(defadt ProjectScope
  "Project scope — which project context the request targets."
  :project/global
  [:project/scoped {:project-id string?}])

;; =============================================================================
;; Pure Projections
;; =============================================================================

(defn caller-id-string
  "Extract the raw string from a CallerId ADT."
  [caller-id]
  (adt-case CallerId caller-id
            :caller/coordinator "coordinator"
            :caller/named       (:slave-id caller-id)))

(defn project-scope-string
  "Extract the raw string from a ProjectScope ADT (nil for global)."
  [project-scope]
  (adt-case ProjectScope project-scope
            :project/global nil
            :project/scoped (:project-id project-scope)))

(defn caller-id-key
  "Derive session-scoped buffer key from caller only.
   For piggyback channels where one session = one project."
  [cid]
  (caller-id-string cid))

(defn make-buffer-key
  "Derive buffer key vector from caller + scope.
   Canonical key for ALL channel buffers."
  [cid pscope]
  [(caller-id-string cid)
   (adt-case ProjectScope pscope
             :project/global  "global"
             :project/scoped  (:project-id pscope))])

(defn make-piggyback-agent-id
  "Derive piggyback agent ID string from caller + scope.
   Used for scoped piggyback channel identification."
  [cid pscope]
  (let [s (caller-id-string cid)]
    (adt-case ProjectScope pscope
              :project/global  s
              :project/scoped  (str s "-" (:project-id pscope)))))

;; =============================================================================
;; Boundary Coercion
;; =============================================================================

(defn parse-caller-id
  "Coerce raw _caller_id string to CallerId ADT.
   nil or \"coordinator\" → :caller/coordinator."
  [raw]
  (if (and raw (not= raw "coordinator"))
    (caller-id :caller/named {:slave-id raw})
    (caller-id :caller/coordinator)))

(defn parse-project-scope
  "Coerce raw project-id string to ProjectScope ADT.
   nil → :project/global."
  [raw]
  (if raw
    (project-scope :project/scoped {:project-id raw})
    (project-scope :project/global)))
