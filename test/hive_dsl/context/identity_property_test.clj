(ns hive-dsl.context.identity-property-test
  "Property tests for context identity ADTs."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.context.identity :as ctx-id]
            [hive-dsl.adt :as adt]
            [clojure.string :as str]))

;; =============================================================================
;; Generators (inline for hive-dsl test isolation)
;; =============================================================================

(def gen-slave-id
  (gen/fmap #(str "coordinator:" %) (gen/choose 100000 999999)))

(def gen-caller-id
  (gen/one-of
   [(gen/return (ctx-id/caller-id :caller/coordinator))
    (gen/fmap #(ctx-id/caller-id :caller/named {:slave-id %})
              gen-slave-id)]))

(def gen-project-id
  (gen/fmap #(str "project-" %)
            (gen/not-empty gen/string-alphanumeric)))

(def gen-project-scope
  (gen/one-of
   [(gen/return (ctx-id/project-scope :project/global))
    (gen/fmap #(ctx-id/project-scope :project/scoped {:project-id %})
              gen-project-id)]))

;; =============================================================================
;; Property: Buffer key always 2-element string vector
;; =============================================================================

(defspec buffer-key-always-2-element-string-vector 200
  (prop/for-all [c gen-caller-id
                 s gen-project-scope]
                (let [k (ctx-id/make-buffer-key c s)]
                  (and (vector? k)
                       (= 2 (count k))
                       (every? string? k)))))

;; =============================================================================
;; Property: Piggyback agent-id always non-blank string
;; =============================================================================

(defspec piggyback-agent-id-always-non-blank 200
  (prop/for-all [c gen-caller-id
                 s gen-project-scope]
                (let [aid (ctx-id/make-piggyback-agent-id c s)]
                  (and (string? aid)
                       (not (str/blank? aid))))))

;; =============================================================================
;; Property: Deterministic (referential transparency)
;; =============================================================================

(defspec projections-are-deterministic 200
  (prop/for-all [c gen-caller-id
                 s gen-project-scope]
                (and (= (ctx-id/make-buffer-key c s)
                        (ctx-id/make-buffer-key c s))
                     (= (ctx-id/make-piggyback-agent-id c s)
                        (ctx-id/make-piggyback-agent-id c s)))))

;; =============================================================================
;; Property: Totality — never throws for valid ADT inputs
;; =============================================================================

(defspec projections-never-throw 200
  (prop/for-all [c gen-caller-id
                 s gen-project-scope]
                (try
                  (ctx-id/make-buffer-key c s)
                  (ctx-id/make-piggyback-agent-id c s)
                  true
                  (catch Exception _ false))))

;; =============================================================================
;; Property: Serialization round-trip
;; =============================================================================

(defspec caller-id-serialization-round-trip 200
  (prop/for-all [c gen-caller-id]
                (= c (adt/deserialize (adt/serialize c)))))

(defspec project-scope-serialization-round-trip 200
  (prop/for-all [s gen-project-scope]
                (= s (adt/deserialize (adt/serialize s)))))

;; =============================================================================
;; Property: Coercion round-trip
;; =============================================================================

(defspec caller-id-coercion-round-trip 200
  (prop/for-all [c gen-caller-id]
                (let [raw (ctx-id/caller-id-string c)
                      back (ctx-id/parse-caller-id raw)]
      ;; coordinator → "coordinator" → coordinator (exact round-trip)
      ;; named → slave-id string → named (exact round-trip)
                  (= c back))))

;; =============================================================================
;; Property: Coordinator identity
;; =============================================================================

(deftest parse-caller-id-nil-always-coordinator
  (is (= :caller/coordinator
         (:adt/variant (ctx-id/parse-caller-id nil)))))

;; =============================================================================
;; Property: Buffer key coordinator fallback
;; =============================================================================

(deftest coordinator-global-buffer-key
  (is (= ["coordinator" "global"]
         (ctx-id/make-buffer-key
          (ctx-id/caller-id :caller/coordinator)
          (ctx-id/project-scope :project/global)))))

;; =============================================================================
;; Property: Piggyback agent-id contains project for scoped
;; =============================================================================

(defspec piggyback-agent-id-contains-project-when-scoped 200
  (prop/for-all [c gen-caller-id
                 pid gen-project-id]
                (let [scope (ctx-id/project-scope :project/scoped {:project-id pid})
                      aid (ctx-id/make-piggyback-agent-id c scope)]
                  (str/includes? aid pid))))

;; =============================================================================
;; Property: EDN round-trip
;; =============================================================================

(defspec caller-id-edn-round-trip 200
  (prop/for-all [c gen-caller-id]
                (= c (read-string (pr-str c)))))

(defspec project-scope-edn-round-trip 200
  (prop/for-all [s gen-project-scope]
                (= s (read-string (pr-str s)))))
