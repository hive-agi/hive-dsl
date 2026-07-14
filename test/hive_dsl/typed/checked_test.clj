(ns hive-dsl.typed.checked-test
  "Rung-3 proofs, gated on typed.clj.checker (clojure -M:test:typed): the
   ann namespace checks clean and the defadt-alias fixture type-checks to
   zero errors. Without the checker every test passes vacuously."
  (:require [clojure.test :refer [deftest is]]))

(defn- check-ns-info* []
  (try (requiring-resolve 'typed.clj.checker/check-ns-info)
       (catch Throwable _ nil)))

(deftest ann-ns-checks-clean
  (when-let [check (check-ns-info*)]
    (let [{:keys [type-errors]} (check 'hive-dsl.typed.adt)]
      (is (empty? type-errors) (pr-str (mapv :message type-errors))))))

(deftest defadt-alias-fixture-type-checks
  (when-let [check (check-ns-info*)]
    (check 'hive-dsl.typed.adt)
    (let [{:keys [type-errors]} (check 'hive-dsl.typed-fixture.alias-green)]
      (is (empty? type-errors) (pr-str (mapv :message type-errors))))))
