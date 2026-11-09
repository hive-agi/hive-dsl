(ns hive-dsl.contract-coverage-test
  "Every public function under src carries a malli function contract, or a
   REASONED exemption. A ratchet: a new defn without a contract fails here.

   The contracts are `:malli/schema` metadata on each defn, written as QUOTED
   DATA. That is what keeps them inside the portability contract: the shipped
   namespaces still require nothing beyond clojure (malli does not run on
   cljw, cljrs or cloture), and metadata is inert on every runtime. Malli reads
   it only where malli is present, through `malli.instrument/collect!`, which
   this test calls before measuring.

   hive-dsl.typed.adt is outside the measured universe: it requires
   typed.clojure, which is only on the :typed alias, and it defines no
   function of its own (only t/ann and t/defalias forms and a macro)."
  (:require [clojure.test]
            [malli.instrument :as mi]
            [hive-schemas.coverage :as cov :refer [deftest-contract-coverage]]
            [hive-dsl.result]
            [hive-dsl.result.taxonomy]
            [hive-dsl.result.agentop]
            [hive-dsl.result.spec]
            [hive-dsl.adt]
            [hive-dsl.adt.schema]
            [hive-dsl.coerce]
            [hive-dsl.context.identity]
            [hive-dsl.typed.emit]
            [hive-dsl.batch]
            [hive-dsl.resource]
            [hive-dsl.bounded-atom]
            [hive-dsl.lifecycle]
            [hive-dsl.gate]
            [hive-dsl.managed-channel]))

(def contracted-namespaces
  "Every namespace under src whose defns carry :malli/schema metadata."
  '[hive-dsl.result hive-dsl.result.taxonomy hive-dsl.result.agentop
    hive-dsl.result.spec hive-dsl.adt hive-dsl.adt.schema hive-dsl.coerce
    hive-dsl.context.identity hive-dsl.typed.emit hive-dsl.batch
    hive-dsl.resource hive-dsl.bounded-atom hive-dsl.lifecycle hive-dsl.gate
    hive-dsl.managed-channel])

;; Registers every :malli/schema in the registry the gate reads.
(mi/collect! {:ns contracted-namespaces})

(def ^:private source-files
  "src minus hive-dsl.typed.adt (typed.clojure-only, no defns)."
  (into []
        (comp (map #(.getPath ^java.io.File %))
              (remove #(.endsWith ^String % "typed/adt.clj")))
        (cov/source-files ["src"])))

(deftest-contract-coverage every-function-is-contracted-or-exempt
  {:paths  source-files
   :exempt {}})
