(ns hive-dsl.adt-registry-test
  "The ADT type registry is keyed by a type's BARE NAME, and `adt-case` resolves
   through it rather than through the var. Two namespaces declaring one name
   with different variant sets therefore make every match site depend on load
   order — the site is checked against whichever namespace loaded last.

   Measured in hive-store 2026-08-29: `hive-store.adt/SettlementOutcome` (5
   variants) and `monero-store.adt/SettlementOutcome` (6) collided that way, and
   a five-clause `adt-case` compiled after the library loaded failed with
   `missing #{:settle/suspect}`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.adt :as adt]))

(def ^:private registry #'adt/registry)

(defn- isolate-registry
  "Snapshot the process-global registry and put it back."
  [f]
  (let [before @@registry]
    (try (f) (finally (reset! @registry before)))))

(use-fixtures :each isolate-registry)

(def ^:private probe :__AdtRegistryProbe)

(defn- register! [owner variants]
  (adt/register-type! probe {:variants (set variants) :schemas {} :owner owner}))

(deftest one-owner-may-redefine-its-own-sum
  (testing "a reload that adds a variant is the author editing their own sum"
    (register! "lib.a" [:p/x :p/y])
    (register! "lib.a" [:p/x :p/y :p/z])
    (is (= #{:p/x :p/y :p/z} (adt/type-variants probe)))))

(deftest two-owners-that-agree-are-left-alone
  (testing "duplicate declarations with the same variants cannot disagree"
    (register! "lib.a" [:p/x :p/y])
    (register! "lib.b" [:p/y :p/x])
    (is (= #{:p/x :p/y} (adt/type-variants probe)))))

(deftest a-second-owner-with-a-different-sum-is-refused
  (register! "lib.a" [:p/x :p/y])
  (let [thrown (try (register! "lib.b" [:p/x :p/y :p/z]) nil
                    (catch clojure.lang.ExceptionInfo e e))]
    (testing "the declaration throws rather than silently winning"
      (is (some? thrown)))
    (testing "and the registry still holds what the first owner declared"
      (is (= #{:p/x :p/y} (adt/type-variants probe))))
    (testing "and the error names both namespaces and the exact disagreement"
      (is (= {:type probe
              :declared-by "lib.a"
              :redeclared-by "lib.b"
              :only-in-declared #{}
              :only-in-redeclared #{:p/z}}
             (ex-data thrown))))))

(deftest the-refusal-is-symmetric
  (testing "a second owner DROPPING a variant is the same fault"
    (register! "lib.a" [:p/x :p/y :p/z])
    (let [thrown (try (register! "lib.b" [:p/x :p/y]) nil
                      (catch clojure.lang.ExceptionInfo e e))]
      (is (some? thrown))
      (is (= #{:p/z} (:only-in-declared (ex-data thrown)))))))

(deftest a-registration-with-no-owner-still-works
  (testing "a direct caller predating :owner is not broken by the check"
    (adt/register-type! probe {:variants #{:p/x} :schemas {}})
    (adt/register-type! probe {:variants #{:p/x :p/y} :schemas {}})
    (is (= #{:p/x :p/y} (adt/type-variants probe)))))

(deftest defadt-stamps-the-declaring-namespace
  (testing "the owner comes from the macro, not from the caller"
    (let [temp (create-ns 'hive-dsl.adt-registry-test.tmp-owner)]
      (try
        (binding [*ns* temp]
          (refer-clojure)
          (eval '(clojure.core/refer 'hive-dsl.adt :only '[defadt]))
          (eval '(defadt __AdtRegistryProbe "probe" :p/x)))
        (is (= "hive-dsl.adt-registry-test.tmp-owner"
               (:owner (get @@registry probe))))
        (finally (remove-ns (ns-name temp)))))))
