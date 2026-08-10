(ns hive-dsl.result-schema
  "Malli value-objects for the Result boundary.

   Lives on the non-runtime :schemas path: malli does not run on ClojureWasm or
   clojurust, so no namespace under src/ may require it. These schemas are the
   JVM-side contract — they drive generators, validation in tests, and the
   Typed Clojure projection — while the shipped code stays malli-free."
  (:require [hive-dsl.result.taxonomy :as tax]))

(def ErrorCategory
  "A keyword error category. Registered categories are the documented set;
   the schema stays open because callers may raise their own."
  :keyword)

(def Ok
  "The success arm: exactly {:ok value}."
  [:map {:closed true} [:ok :any]])

(def Err
  "The failure arm: {:error category, ...extra-data}. Extra keys are the
   error's payload and are deliberately unconstrained."
  [:map [:error ErrorCategory]])

(def Result
  "Ok or Err — a Result is never both."
  [:or Ok Err])

(defn registered-category
  "An enum schema over the categories registered at call time, or nil when the
   registry is empty. Built lazily so it reflects the live registry rather than
   load order."
  []
  (let [cats (vec (tax/registered-categories))]
    (when (seq cats)
      (into [:enum] cats))))
