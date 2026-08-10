(ns hive-dsl.result-schema-test
  "The malli value-objects must agree with the constructors, and must never
   reach the shipped classpath."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [malli.core :as m]
            [malli.generator :as mg]
            [hive-dsl.result :as r]
            [hive-dsl.result.taxonomy :as tax]
            [hive-dsl.result-schema :as rs]))

(deftest constructors-satisfy-their-schemas
  (testing "ok produces an Ok, err produces an Err"
    (is (m/validate rs/Ok (r/ok 1)))
    (is (m/validate rs/Err (r/err :boom {})))
    (is (m/validate rs/Result (r/ok :anything)))
    (is (m/validate rs/Result (r/err :boom {:extra 1}))))
  (testing "the arms are disjoint"
    (is (not (m/validate rs/Ok (r/err :boom {}))))
    (is (not (m/validate rs/Err (r/ok 1))))))

(deftest registered-category-enum-tracks-the-registry
  (let [schema (rs/registered-category)]
    (is (some? schema) "the taxonomy registers categories at load")
    (is (m/validate schema (first (tax/registered-categories))))))

(defspec every-generated-ok-is-a-result 100
  (prop/for-all [v (mg/generator rs/Ok)]
    (and (m/validate rs/Result v)
         (r/ok? v))))

(defspec every-generated-err-is-a-result 100
  (prop/for-all [v (mg/generator rs/Err)]
    (and (m/validate rs/Result v)
         (r/err? v))))

(defspec ok-round-trips-any-value 100
  (prop/for-all [v gen/any-printable]
    (m/validate rs/Ok (r/ok v))))

(deftest schemas-are-not-on-the-runtime-path
  (testing "the shipped :paths never see malli"
    (is (nil? (some #{"schemas"}
                    (:paths (read-string (slurp "deps.edn")))))
        "schemas must stay out of :paths — malli does not run on the native targets")))
