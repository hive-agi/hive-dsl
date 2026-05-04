(ns hive-dsl.result-test
  (:require [clojure.test :refer [deftest testing is are]]
            [hive-dsl.result :as r]
            [hive-dsl.result.taxonomy :as tax]))

;; --- Constructors ---

(deftest ok-test
  (testing "ok wraps a value"
    (is (= {:ok 42} (r/ok 42)))
    (is (= {:ok nil} (r/ok nil)))
    (is (= {:ok [1 2 3]} (r/ok [1 2 3])))))

(deftest err-test
  (testing "err with category only"
    (is (= {:error :io/timeout} (r/err :io/timeout))))
  (testing "err with category and data"
    (is (= {:error :io/read-failure :path "/tmp/x"}
           (r/err :io/read-failure {:path "/tmp/x"})))))

;; --- Predicates ---

(deftest ok?-test
  (testing "ok? recognizes success results"
    (is (true? (r/ok? (r/ok 1))))
    (is (true? (r/ok? (r/ok nil)))))
  (testing "ok? rejects errors and non-results"
    (is (false? (r/ok? (r/err :x))))
    (is (false? (r/ok? {})))
    (is (false? (r/ok? nil)))
    (is (false? (r/ok? "string")))))

(deftest err?-test
  (testing "err? recognizes error results"
    (is (true? (r/err? (r/err :x))))
    (is (true? (r/err? (r/err :x {:msg "boom"})))))
  (testing "err? rejects ok and non-results"
    (is (false? (r/err? (r/ok 1))))
    (is (false? (r/err? {})))
    (is (false? (r/err? nil)))))

;; --- bind ---

(deftest bind-test
  (testing "bind applies f to ok value"
    (is (= (r/ok 10)
           (r/bind (r/ok 5) #(r/ok (* 2 %))))))
  (testing "bind short-circuits on err"
    (let [e (r/err :fail)]
      (is (= e (r/bind e #(r/ok (inc %)))))))
  (testing "bind chains"
    (is (= (r/ok 6)
           (-> (r/ok 1)
               (r/bind #(r/ok (+ % 2)))
               (r/bind #(r/ok (* % 2))))))))

;; --- map-ok ---

(deftest map-ok-test
  (testing "map-ok transforms ok value"
    (is (= (r/ok 10) (r/map-ok (r/ok 5) #(* 2 %)))))
  (testing "map-ok passes through err"
    (let [e (r/err :fail)]
      (is (= e (r/map-ok e inc))))))

;; --- map-err ---

(deftest map-err-test
  (testing "map-err transforms error"
    (is (= {:error :fail :decorated true}
           (r/map-err (r/err :fail) #(assoc % :decorated true)))))
  (testing "map-err passes through ok"
    (is (= (r/ok 42) (r/map-err (r/ok 42) #(assoc % :decorated true))))))

;; --- let-ok ---

(deftest let-ok-test
  (testing "let-ok binds and returns body"
    (is (= (r/ok 3)
           (r/let-ok [a (r/ok 1)
                      b (r/ok 2)]
                     (r/ok (+ a b))))))
  (testing "let-ok short-circuits on error"
    (let [e (r/err :nope)]
      (is (= e
             (r/let-ok [a (r/ok 1)
                        b e
                        c (r/ok 99)]
                       (r/ok (+ a b c))))))))

;; Strict-binding semantics (non-Result throws) are covered by
;; `hive-dsl.let-ok-strict-trifecta-test` — golden + property + mutation
;; including a `swallow-throw` mutation that mimics the pre-2026-05-02
;; silent behavior, so a future revert of the strict throw fails loudly.

(deftest let-ok-let-sugar-test
  (testing ":let binds plain values without Result coercion"
    (is (= (r/ok 6)
           (r/let-ok [a    (r/ok 1)
                      :let [b 2 c (+ a b)]
                      d    (r/ok 3)]
                     (r/ok (+ a b c d))))))
  (testing ":let still short-circuits on err in subsequent Result bindings"
    (let [e (r/err :downstream)]
      (is (= e
             (r/let-ok [a    (r/ok 1)
                        :let [b (inc a)]
                        c    e]
                       (r/ok (+ a b c)))))))
  (testing ":let bindings see prior Result-bound values"
    (is (= (r/ok 100)
           (r/let-ok [x    (r/ok 10)
                      :let [y (* x x)]]
                     (r/ok y)))))
  (testing ":let with non-vector bindings throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #":let requires a vector"
                          (macroexpand-1 '(hive-dsl.result/let-ok [:let foo] (r/ok :nope)))))))

;; --- ok-> ---

(deftest ok->-test
  (testing "ok-> threads through plain fns (auto-wrapped)"
    (is (= (r/ok 4)
           (r/ok-> (r/ok 1) inc inc inc))))
  (testing "ok-> with args (thread-first position)"
    (is (= (r/ok 3)
           (r/ok-> (r/ok 1) (+ 2)))))
  (testing "ok-> with switch fns returning Results"
    (let [double-ok (fn [x] (r/ok (* 2 x)))]
      (is (= (r/ok 10)
             (r/ok-> (r/ok 5) double-ok)))))
  (testing "ok-> short-circuits on error"
    (let [called (atom false)
          fail   (fn [_] (r/err :fail))
          spy    (fn [x] (reset! called true) x)]
      (is (= (r/err :fail)
             (r/ok-> (r/ok 1) fail spy)))
      (is (false? @called))))
  (testing "ok-> wraps plain initial expression"
    (is (= (r/ok 42)
           (r/ok-> 42))))
  (testing "ok-> with mix of plain and Result-returning steps"
    (let [validate (fn [x] (if (pos? x) (r/ok x) (r/err :not-positive)))]
      (is (= (r/ok 4)
             (r/ok-> (r/ok 3) validate inc))))))

;; --- ok->> ---

(deftest ok->>-test
  (testing "ok->> threads in last position"
    (is (= (r/ok 14)
           (r/ok->> (r/ok [1 2 3 4])
                    (map inc)
                    (reduce +)))))
  (testing "ok->> short-circuits on error"
    (let [called (atom false)
          e      (r/err :fail)]
      (is (= e
             (r/ok->> e
                      (map (fn [x] (reset! called true) x)))))
      (is (false? @called))))
  (testing "ok->> wraps plain initial expression"
    (is (= (r/ok [1 2 3])
           (r/ok->> [1 2 3]))))
  (testing "ok->> with switch fn"
    (let [safe-sum (fn [xs] (if (seq xs)
                              (r/ok (reduce + xs))
                              (r/err :empty-list)))]
      (is (= (r/ok 6)
             (r/ok->> (r/ok [1 2 3]) safe-sum)))
      (is (= (r/err :empty-list)
             (r/ok->> (r/ok []) safe-sum))))))

;; --- try-effect ---

(deftest try-effect-test
  (testing "try-effect wraps success"
    (is (= (r/ok 42) (r/try-effect 42))))
  (testing "try-effect catches exception"
    (let [result (r/try-effect (throw (ex-info "boom" {})))]
      (is (r/err? result))
      (is (= :effect/exception (:error result)))
      (is (string? (:message result))))))

;; --- try-effect* ---

(deftest try-effect*-test
  (testing "try-effect* wraps success with custom category"
    (is (= (r/ok "ok") (r/try-effect* :io/read-failure "ok"))))
  (testing "try-effect* catches with custom category"
    (let [result (r/try-effect* :io/read-failure
                                (throw (ex-info "file not found" {})))]
      (is (r/err? result))
      (is (= :io/read-failure (:error result)))
      (is (string? (:message result))))))

;; --- taxonomy ---

(deftest known-error?-test
  (testing "recognizes known errors"
    (is (true? (tax/known-error? :io/timeout)))
    (is (true? (tax/known-error? :effect/exception))))
  (testing "rejects unknown errors"
    (is (false? (tax/known-error? :foo/bar)))
    (is (false? (tax/known-error? :unknown)))))
