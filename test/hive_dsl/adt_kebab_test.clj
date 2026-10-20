(ns hive-dsl.adt-kebab-test
  "camel->kebab is a character walk (portable to Basilisp). It must name
   constructors exactly as the regex version it replaced did."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt]))

;; SPDX-License-Identifier: MIT

(def ^:private camel->kebab @#'hive-dsl.adt/camel->kebab)

(defn- regex-version
  "The implementation camel->kebab replaced."
  [s]
  (-> s
      (str/replace #"([a-z0-9])([A-Z])" "$1-$2")
      (str/replace #"([A-Z]+)([A-Z][a-z])" "$1-$2")
      str/lower-case))

(deftest known-names
  (is (= "event-type" (camel->kebab "EventType")))
  (is (= "spawn-mode" (camel->kebab "SpawnMode")))
  (is (= "http-request" (camel->kebab "HTTPRequest")))
  (is (= "x2-y" (camel->kebab "X2Y")))
  (is (= "a" (camel->kebab "A")))
  (is (= "abc" (camel->kebab "ABC"))))

(deftest names-with-separators-get-no-extra-hyphen
  (testing "an upper-case char after a separator is not split"
    (is (= "my-type" (camel->kebab "my-Type")))
    (is (= "my_type" (camel->kebab "my_Type")))
    (is (= "a?b" (camel->kebab "a?B")))))

(deftest agrees-with-the-regex-version-exhaustively
  (let [strs (fn strs [alpha n]
               (if (zero? n) [""] (for [s (strs alpha (dec n)) c alpha] (str s c))))]
    (is (empty? (for [alpha ["aAbB1" "aAB-_1" "aAB?*."]
                      n (range 1 7)
                      s (strs alpha n)
                      :when (not= (regex-version s) (camel->kebab s))]
                  s)))))
