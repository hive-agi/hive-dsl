(ns hive-dsl.context.identity-lookup-keys-trifecta-test
  "Trifecta and property coverage for `caller-id-lookup-keys` and
   `session-id-shape?`: the raw caller id first, then the base id with a
   session-id suffix removed when, and only when, the suffix has a known
   session-id shape."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.context.identity :as ci]
            [hive-test.trifecta :refer [deftrifecta]]))

;; =============================================================================
;; Generators
;; =============================================================================

(def ^:private gen-base
  (gen/fmap (fn [[head tail]] (str head tail))
            (gen/tuple (gen/elements ["coordinator" "swarm-ling" "slave" "a:b"])
                       gen/string-alphanumeric)))

(def ^:private gen-digits-suffix
  (gen/fmap str (gen/large-integer* {:min 0 :max 99999999})))

(def ^:private gen-hex8-suffix
  (gen/fmap #(subs (str %) 0 8) gen/uuid))

(def ^:private gen-session-suffix
  (gen/one-of [gen-digits-suffix gen-hex8-suffix]))

(def ^:private gen-foreign-suffix
  (gen/such-that #(not (ci/session-id-shape? %))
                 (gen/one-of [gen/string-alphanumeric
                              (gen/elements ["mcp" "3F9A0C1E" "3f9a0c" "3f9a0c1e0" "12a" "-1" "" " 42"])])
                 100))

(def ^:private gen-raw-caller-id
  (gen/one-of [(gen/return nil)
               (gen/return "coordinator")
               gen/string-alphanumeric
               (gen/fmap (fn [[b s]] (str b ":" s)) (gen/tuple gen-base gen-session-suffix))
               (gen/fmap (fn [[b s]] (str b ":" s)) (gen/tuple gen-base gen-foreign-suffix))]))

;; =============================================================================
;; Invariant and mutants
;; =============================================================================

(defn- lookup-keys-shape?
  "One or two string keys; a second key is a strict colon-prefix of the first."
  [ks]
  (and (vector? ks)
       (every? string? ks)
       (case (count ks)
         1 true
         2 (let [[raw base] ks]
             (and (seq base)
                  (str/starts-with? raw (str base ":"))
                  (ci/session-id-shape? (subs raw (inc (count base))))))
         false)))

(defn- mut-never-strips [raw]
  [(ci/caller-id-string (ci/parse-caller-id raw))])

(defn- mut-strips-any-suffix [raw]
  (let [s (ci/caller-id-string (ci/parse-caller-id raw))
        i (str/last-index-of s ":")]
    (if (and i (pos? i)) [s (subs s 0 i)] [s])))

(defn- mut-base-first [raw]
  (vec (reverse (ci/caller-id-lookup-keys raw))))

(defn- mut-splits-at-first-colon [raw]
  (let [s (ci/caller-id-string (ci/parse-caller-id raw))
        i (str/index-of s ":")]
    (if (and i (pos? i) (ci/session-id-shape? (subs s (inc i))))
      [s (subs s 0 i)]
      [s])))

(deftrifecta caller-id-lookup-keys-trifecta
  hive-dsl.context.identity/caller-id-lookup-keys
  {:golden-path "test/golden/hive-dsl/trifecta-caller-id-lookup-keys.edn"
   :cases       {:digits-suffix      "coordinator:947426"
                 :hex-suffix         "coordinator:3f9a0c1e"
                 :named-digits       "swarm-ling-7:12345"
                 :no-colon           "swarm-ling-7"
                 :coordinator-bare   "coordinator"
                 :nil-caller         nil
                 :empty-string       ""
                 :non-session-suffix "hive:mcp"
                 :short-hex-suffix   "coordinator:3f9a0c"
                 :upper-hex-suffix   "coordinator:3F9A0C1E"
                 :empty-suffix       "coordinator:"
                 :empty-base         ":12345"
                 :last-colon         "a:b:12345"}
   :gen         gen-raw-caller-id
   :pred        lookup-keys-shape?
   :num-tests   300
   :mutations   [["never-strips"          mut-never-strips]
                 ["strips-any-suffix"     mut-strips-any-suffix]
                 ["base-first"            mut-base-first]
                 ["splits-at-first-colon" mut-splits-at-first-colon]]})

;; =============================================================================
;; Named cases
;; =============================================================================

(deftest caller-id-lookup-keys-named-cases
  (testing "digits suffix (BB_MCP_SESSION_ID = $PPID) is removed for the base key"
    (is (= ["coordinator:947426" "coordinator"]
           (ci/caller-id-lookup-keys "coordinator:947426"))))
  (testing "8-char lowercase hex suffix (UUID-prefix fallback) is removed"
    (is (= ["swarm-ling-7:3f9a0c1e" "swarm-ling-7"]
           (ci/caller-id-lookup-keys "swarm-ling-7:3f9a0c1e"))))
  (testing "no colon: the raw id is the only key"
    (is (= ["swarm-ling-7"] (ci/caller-id-lookup-keys "swarm-ling-7"))))
  (testing "a suffix that is not a session id is kept intact"
    (is (= ["hive:mcp"] (ci/caller-id-lookup-keys "hive:mcp")))
    (is (= ["coordinator:3F9A0C1E"] (ci/caller-id-lookup-keys "coordinator:3F9A0C1E"))))
  (testing "nil and \"coordinator\" canonicalise to the coordinator id"
    (is (= ["coordinator"] (ci/caller-id-lookup-keys nil)))
    (is (= ["coordinator"] (ci/caller-id-lookup-keys "coordinator")))))

(deftest session-id-shape-cases
  (is (ci/session-id-shape? "947426"))
  (is (ci/session-id-shape? "0"))
  (is (ci/session-id-shape? "3f9a0c1e"))
  (is (not (ci/session-id-shape? "3F9A0C1E")))
  (is (not (ci/session-id-shape? "3f9a0c")))
  (is (not (ci/session-id-shape? "3f9a0c1e0")))
  (is (not (ci/session-id-shape? "")))
  (is (not (ci/session-id-shape? nil)))
  (is (not (ci/session-id-shape? "mcp"))))

;; =============================================================================
;; Properties over the producer's shape
;; =============================================================================

(defspec base-with-session-suffix-yields-raw-then-base 300
  (prop/for-all [base gen-base
                 sid  gen-session-suffix]
    (let [raw (str base ":" sid)]
      (= [raw base] (ci/caller-id-lookup-keys raw)))))

(defspec base-with-foreign-suffix-yields-raw-only 300
  (prop/for-all [base gen-base
                 sfx  gen-foreign-suffix]
    (let [raw (str base ":" sfx)]
      (= [raw] (ci/caller-id-lookup-keys raw)))))

(defspec first-key-is-the-canonical-caller-id 300
  (prop/for-all [raw gen-raw-caller-id]
    (= (ci/caller-id-string (ci/parse-caller-id raw))
       (first (ci/caller-id-lookup-keys raw)))))
