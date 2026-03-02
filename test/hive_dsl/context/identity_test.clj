(ns hive-dsl.context.identity-test
  "Unit tests for context identity ADTs."
  (:require [clojure.test :refer [deftest testing is are]]
            [hive-dsl.context.identity :as ctx-id]
            [hive-dsl.adt :as adt]))

;; =============================================================================
;; Constructor Tests
;; =============================================================================

(deftest caller-id-coordinator-test
  (let [c (ctx-id/caller-id :caller/coordinator)]
    (is (= :caller/coordinator (:adt/variant c)))
    (is (= :CallerId (:adt/type c)))
    (is (ctx-id/caller-id? c))))

(deftest caller-id-named-test
  (let [c (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})]
    (is (= :caller/named (:adt/variant c)))
    (is (= "coordinator:947426" (:slave-id c)))
    (is (ctx-id/caller-id? c))))

(deftest project-scope-global-test
  (let [s (ctx-id/project-scope :project/global)]
    (is (= :project/global (:adt/variant s)))
    (is (= :ProjectScope (:adt/type s)))
    (is (ctx-id/project-scope? s))))

(deftest project-scope-scoped-test
  (let [s (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})]
    (is (= :project/scoped (:adt/variant s)))
    (is (= "hive-mcp" (:project-id s)))
    (is (ctx-id/project-scope? s))))

;; =============================================================================
;; Projection Tests
;; =============================================================================

(deftest caller-id-string-test
  (is (= "coordinator"
         (ctx-id/caller-id-string (ctx-id/caller-id :caller/coordinator))))
  (is (= "coordinator:947426"
         (ctx-id/caller-id-string
          (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})))))

(deftest project-scope-string-test
  (is (nil? (ctx-id/project-scope-string (ctx-id/project-scope :project/global))))
  (is (= "hive-mcp"
         (ctx-id/project-scope-string
          (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})))))

(deftest make-buffer-key-test
  (testing "coordinator + global → [\"coordinator\" \"global\"]"
    (is (= ["coordinator" "global"]
           (ctx-id/make-buffer-key
            (ctx-id/caller-id :caller/coordinator)
            (ctx-id/project-scope :project/global)))))

  (testing "coordinator + scoped → [\"coordinator\" project-id]"
    (is (= ["coordinator" "hive-mcp"]
           (ctx-id/make-buffer-key
            (ctx-id/caller-id :caller/coordinator)
            (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})))))

  (testing "named + scoped → [slave-id project-id]"
    (is (= ["coordinator:947426" "hive-mcp"]
           (ctx-id/make-buffer-key
            (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})
            (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})))))

  (testing "named + global → [slave-id \"global\"]"
    (is (= ["coordinator:947426" "global"]
           (ctx-id/make-buffer-key
            (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})
            (ctx-id/project-scope :project/global))))))

(deftest make-piggyback-agent-id-test
  (testing "coordinator + global → \"coordinator\""
    (is (= "coordinator"
           (ctx-id/make-piggyback-agent-id
            (ctx-id/caller-id :caller/coordinator)
            (ctx-id/project-scope :project/global)))))

  (testing "coordinator + scoped → \"coordinator-project\""
    (is (= "coordinator-hive-mcp"
           (ctx-id/make-piggyback-agent-id
            (ctx-id/caller-id :caller/coordinator)
            (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})))))

  (testing "named + scoped → \"slave-id-project\""
    (is (= "coordinator:947426-hive-mcp"
           (ctx-id/make-piggyback-agent-id
            (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})
            (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})))))

  (testing "named + global → slave-id only"
    (is (= "coordinator:947426"
           (ctx-id/make-piggyback-agent-id
            (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})
            (ctx-id/project-scope :project/global))))))

;; =============================================================================
;; Boundary Coercion Tests
;; =============================================================================

(deftest parse-caller-id-test
  (testing "nil → coordinator"
    (is (= :caller/coordinator
           (:adt/variant (ctx-id/parse-caller-id nil)))))

  (testing "\"coordinator\" → coordinator"
    (is (= :caller/coordinator
           (:adt/variant (ctx-id/parse-caller-id "coordinator")))))

  (testing "named string → named"
    (let [c (ctx-id/parse-caller-id "coordinator:947426")]
      (is (= :caller/named (:adt/variant c)))
      (is (= "coordinator:947426" (:slave-id c))))))

(deftest parse-project-scope-test
  (testing "nil → global"
    (is (= :project/global
           (:adt/variant (ctx-id/parse-project-scope nil)))))

  (testing "string → scoped"
    (let [s (ctx-id/parse-project-scope "hive-mcp")]
      (is (= :project/scoped (:adt/variant s)))
      (is (= "hive-mcp" (:project-id s))))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest empty-string-caller-id-test
  (testing "empty string is treated as named (not coordinator)"
    (let [c (ctx-id/parse-caller-id "")]
      (is (= :caller/named (:adt/variant c)))
      (is (= "" (:slave-id c))))))

(deftest edn-round-trip-test
  (testing "CallerId survives pr-str / read-string"
    (let [c (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})
          s (pr-str c)
          c' (read-string s)]
      (is (= c c'))))

  (testing "ProjectScope survives pr-str / read-string"
    (let [s (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})
          ss (pr-str s)
          s' (read-string ss)]
      (is (= s s')))))

(deftest adt-serialization-round-trip-test
  (testing "CallerId serialize/deserialize round-trip"
    (let [c (ctx-id/caller-id :caller/named {:slave-id "coordinator:947426"})
          serialized (adt/serialize c)
          deserialized (adt/deserialize serialized)]
      (is (= c deserialized))))

  (testing "ProjectScope serialize/deserialize round-trip"
    (let [s (ctx-id/project-scope :project/scoped {:project-id "hive-mcp"})
          serialized (adt/serialize s)
          deserialized (adt/deserialize serialized)]
      (is (= s deserialized)))))
