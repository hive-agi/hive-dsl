(ns hive-dsl.typed.emit-test
  "Cold coverage for hive-dsl.typed.emit - no checker required: emission is
   plain data."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt :refer [defadt]]
            [hive-dsl.typed.emit :as emit])
  (:import [clojure.lang ExceptionInfo]))

(defadt EmitProbe
  "Emission probe: one data variant, one enum variant."
  [:probe/data {:radius number? :label string?}]
  :probe/unit)

(defadt EmitSolo
  "Single enum variant - union collapses to a bare HMap."
  :solo/only)

(deftest pred-type-maps-known-predicates
  (testing "fn objects - the post-load registry shape"
    (is (= 'typed.clojure/Num (emit/pred-type number?)))
    (is (= 'typed.clojure/Str (emit/pred-type string?))))
  (testing "symbols - the macro-time registry shape"
    (is (= 'typed.clojure/Num (emit/pred-type 'number?)))
    (is (= 'typed.clojure/Kw  (emit/pred-type 'keyword?)))))

(deftest pred-type-widens-unknown-predicates-to-any
  (is (= 'typed.clojure/Any (emit/pred-type odd?)))
  (is (= 'typed.clojure/Any (emit/pred-type (fn [x] (some? x)))))
  (is (= 'typed.clojure/Any (emit/pred-type 'no-such-pred?))))

(deftest adt-union-emits-sorted-per-variant-hmap-union
  (is (= '(typed.clojure/U
           (typed.clojure/HMap :mandatory {:adt/type    (typed.clojure/Val :EmitProbe)
                                           :adt/variant (typed.clojure/Val :probe/data)
                                           :radius      typed.clojure/Num
                                           :label       typed.clojure/Str})
           (typed.clojure/HMap :mandatory {:adt/type    (typed.clojure/Val :EmitProbe)
                                           :adt/variant (typed.clojure/Val :probe/unit)}))
         (emit/adt-union :EmitProbe))))

(deftest single-variant-adt-emits-bare-hmap
  (is (= '(typed.clojure/HMap :mandatory {:adt/type    (typed.clojure/Val :EmitSolo)
                                          :adt/variant (typed.clojure/Val :solo/only)})
         (emit/adt-union :EmitSolo))))

(deftest adt-union-throws-on-unregistered-type
  (is (thrown? ExceptionInfo (emit/adt-union :NoSuchAdt))))
