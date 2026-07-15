(ns hive-dsl.adt.schema-test
  "adt->malli derives ONE malli :multi schema from an ADT; validate-malli guards
   an ADT value through it (backed by malli when present)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt :as adt]
            [hive-dsl.adt.schema :as as]))

(adt/defadt DemoAdt "test ADT"
  [:event/started  {:task string?}]
  [:event/progress {:pct int?}]
  :event/done)

(deftest adt->malli-derives-a-multi-schema
  (let [schema (as/adt->malli DemoAdt)]
    (testing "the whole ADT projects to ONE malli :multi value-object"
      (is (= :multi (first schema)))
      (is (= {:dispatch :adt/variant} (second schema)))
      (is (= #{:event/started :event/progress :event/done}
             (set (map first (drop 2 schema))))))))

(deftest validate-malli-guards-via-the-derived-schema
  (testing "a well-typed data variant is returned unchanged"
    (is (= {:adt/type :DemoAdt :adt/variant :event/started :task "X"}
           (as/validate-malli DemoAdt (demo-adt :event/started {:task "X"})))))
  (testing "a mistyped field is refused (schema/invalid) with an explanation"
    (let [e (try (as/validate-malli DemoAdt (demo-adt :event/started {:task 42}))
                 (catch clojure.lang.ExceptionInfo ex (ex-data ex)))]
      (is (= :schema/invalid (:error e)))
      (is (some? (:explanation e)))))
  (testing "an enum variant validates on its tag keys alone"
    (is (map? (as/validate-malli DemoAdt (demo-adt :event/done))))))
