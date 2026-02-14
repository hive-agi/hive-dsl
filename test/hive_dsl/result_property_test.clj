(ns hive-dsl.result-property-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.result :as r]
            [hive-dsl.result.taxonomy :as tax]
            [hive-test.generators.result :as gen-r]
            [hive-test.properties :as props]))

;; --- P1: Totality — ok/err never throw for any input ---

(props/defprop-total p1-ok-totality r/ok gen/any-printable {:pred r/ok?})

(props/defprop-total p1-err-totality r/err gen-r/gen-err-category {:pred r/err?})

;; --- P2: Left identity — (bind (ok x) f) = (f x) ---

(defspec p2-left-identity 200
  (prop/for-all [v gen/any-printable
                 f (gen-r/gen-result-fn r/ok r/err)]
                (= (r/bind (r/ok v) f)
                   (f v))))

;; --- P3: Right identity — (bind m ok) = m ---

(defspec p3-right-identity 200
  (prop/for-all [m (gen-r/gen-result r/ok r/err)]
                (= (r/bind m r/ok)
                   m)))

;; --- P4: Associativity — (bind (bind m f) g) = (bind m #(bind (f %) g)) ---

(defspec p4-associativity 200
  (prop/for-all [m (gen-r/gen-result r/ok r/err)
                 f (gen-r/gen-result-fn r/ok r/err)
                 g (gen-r/gen-result-fn r/ok r/err)]
                (= (r/bind (r/bind m f) g)
                   (r/bind m (fn [x] (r/bind (f x) g))))))

;; --- P5: Functor law — (map-ok (err e) f) = (err e) ---

(defspec p5-functor-err-passthrough 200
  (prop/for-all [e (gen-r/gen-err r/err)]
                (= (r/map-ok e identity)
                   e)))

;; --- P6: Complementarity — ok? and err? are exact complements on Results ---

(props/defprop-complement p6-complementarity r/ok? r/err? (gen-r/gen-result r/ok r/err))

;; --- P7: try-effect totality — always returns Result, never throws ---

(defspec p7-try-effect-totality 200
  (prop/for-all [v gen/any-printable]
                (let [result (r/try-effect v)]
                  (r/ok? result))))

(deftest p7-try-effect-exception-totality
  (testing "try-effect catches all exceptions and returns err"
    (dotimes [_ 200]
      (let [result (r/try-effect (throw (ex-info "boom" {:i (rand-int 1000)})))]
        (is (r/err? result))
        (is (= :effect/exception (:error result)))))))

;; --- Additional: map-ok functor identity law ---

(defspec map-ok-identity-law 200
  (prop/for-all [m (gen-r/gen-result r/ok r/err)]
                (= (r/map-ok m identity)
                   (if (r/ok? m) m m))))

;; --- Additional: bind with err always returns same err ---

(defspec bind-err-absorption 200
  (prop/for-all [e (gen-r/gen-err r/err)
                 f (gen-r/gen-result-fn r/ok r/err)]
                (= (r/bind e f) e)))

;; --- Additional: known-error? is total ---

(defspec taxonomy-known-error-total 200
  (prop/for-all [kw gen/keyword]
                (boolean? (tax/known-error? kw))))
