(ns hive-dsl.kondo-lint-test
  "Lint gate: clj-kondo, through the repo config (which loads the exported
   hooks), reports no errors and no warnings. The hooks ship to consumers in
   resources/clj-kondo.exports, so a hook regression is a consumer-facing
   false positive."
  (:require [clj-kondo.core :as kondo]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]))

(defn- findings
  "Error and warning findings for `paths`, linted with .clj-kondo/config.edn."
  [paths]
  (->> (kondo/run! {:lint paths :config-dir ".clj-kondo"})
       :findings
       (filter (comp #{:error :warning} :level))
       (mapv #(select-keys % [:filename :row :col :level :type :message]))))

(def ^:private hook-probe
  "Uses of the exported macros whose hooks used to type values wrongly:
   a defadt predicate typed nil (constant-false `is`), a with-scope binding
   typed nil (swap! on nil)."
  "(ns probe (:require [clojure.test :refer [is]]
                       [hive-dsl.result :as r]
                       [hive-dsl.adt :refer [defadt]]
                       [hive-dsl.resource :as res]))
   (defadt Color :red :green)
   (defn f []
     (is (color? (color :red)))
     (r/let-ok [x (r/err :e {}) y (r/ok (inc x))] (r/ok (inc y)))
     (r/ok-> (r/ok 1) (inc) (str))
     (res/with-scope [s] (swap! s conj 1)))")

(deftest exported-hooks-raise-no-false-positives
  (testing "defadt, let-ok, ok-> and with-scope uses lint clean"
    (let [dir  (.toFile (java.nio.file.Files/createTempDirectory
                         "kondo-probe" (make-array java.nio.file.attribute.FileAttribute 0)))
          file (io/file dir "probe.clj")]
      (try
        (spit file hook-probe)
        (is (= [] (findings [(str file)])))
        (finally
          (io/delete-file file true)
          (io/delete-file dir true))))))

(deftest src-and-test-lint-clean
  (testing "clj-kondo reports no errors or warnings on src and test"
    (is (= [] (findings ["src" "test"])))))
