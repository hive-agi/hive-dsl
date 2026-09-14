(ns hive-dsl.rescue-ex-test
  (:require [hive-dsl.result :as r]
            [clojure.test :refer [deftest is]]))

(deftest rescue-ex-catches-an-exception-but-not-an-error
  (is (= :fb (r/rescue-ex :fb (throw (RuntimeException. "boom")))))
  (is (thrown? AssertionError (r/rescue-ex :fb (throw (AssertionError. "boom")))))
  (is (= :fb (r/rescue :fb (throw (AssertionError. "boom"))))))

(deftest rescue-ex-returns-body-on-success
  (is (= 42 (r/rescue-ex [] 42)))
  (is (= :ok (r/rescue-ex [] :ok))))

(deftest rescue-ex-attaches-error-metadata
  (let [result (r/rescue-ex [] (throw (RuntimeException. "x")))
        meta-map (::r/error (meta result))]
    (is (map? meta-map))
    (is (:message meta-map))
    (is (:form meta-map))))

(deftest rescue-ex-log-catches-an-exception-but-not-an-error
  (is (= :fb (r/rescue-ex-log "test" :fb (throw (RuntimeException. "boom")))))
  (is (thrown? AssertionError (r/rescue-ex-log "test" :fb (throw (AssertionError. "boom"))))))

(deftest rescue-ex-log-returns-body-on-success
  (is (= 42 (r/rescue-ex-log "test" [] 42)))
  (is (= :ok (r/rescue-ex-log "test" [] :ok))))

(deftest rescue-ex-log-attaches-error-metadata
  (let [result (r/rescue-ex-log "test" [] (throw (RuntimeException. "x")))
        meta-map (::r/error (meta result))]
    (is (map? meta-map))
    (is (:message meta-map))
    (is (:label meta-map))
    (is (:form meta-map))))
