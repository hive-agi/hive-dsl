(ns bb-defadt-result
  "Spike: verify hive-dsl.adt/defadt + hive-dsl.result work under babashka.

   Run:  bb -m bb-defadt-result
   Or:   bb spike/bb_defadt_result.clj"
  (:require [hive-dsl.adt :as adt :refer [defadt adt-case]]
            [hive-dsl.result :as r]))

(defadt EventType
  "Sample event ADT for the spike."
  [:event/started   {:task string?}]
  [:event/progress  {:message string?}]
  :event/completed)

(defadt SpawnMode
  "Enum-only ADT (no data variants)."
  :spawn/vterm
  :spawn/headless)

(defn render
  "Exhaustive pattern match — compile-time check that all variants are covered."
  [evt]
  (adt-case EventType evt
    :event/started   (str "task: " (:task evt))
    :event/progress  (str "msg: "  (:message evt))
    :event/completed "done"))

(defn validate-task
  "Switch step — returns Result."
  [s]
  (if (and (string? s) (seq s))
    (r/ok s)
    (r/err :task/empty {:got s})))

(defn enrich
  "Pure step — auto-wrapped by ok->."
  [s]
  (str "[enriched] " s))

(defn -main [& _args]
  (let [started   (event-type :event/started {:task "ship bb mode"})
        progress  (event-type :event/progress {:message "macroexpand ok"})
        done      (event-type :event/completed)
        vterm     (spawn-mode :spawn/vterm)]

    (println "== ADT construction ==")
    (println " started  " started)
    (println " progress " progress)
    (println " done     " done)
    (println " vterm    " vterm)

    (println "\n== Predicates + coercion ==")
    (println " event-type? started   =>" (event-type? started))
    (println " event-type? {}        =>" (event-type? {}))
    (println " ->event-type started  =>" (->event-type :event/started))
    (println " spawn-mode? vterm     =>" (spawn-mode? vterm))

    (println "\n== adt-case (exhaustive) ==")
    (doseq [e [started progress done]]
      (println " " (render e)))

    (println "\n== Result railway: ok-> ==")
    (let [pipeline (fn [task]
                     (r/ok-> (validate-task task)
                             enrich))]
      (println " good =>" (pipeline "spike-ok"))
      (println " bad  =>" (pipeline "")))

    (println "\n== Result railway: let-ok ==")
    (let [combo (r/let-ok [t       (validate-task "compose")
                           :let    [tag (str "ling/" t)]
                           checked (validate-task tag)]
                  (r/ok {:task t :tag tag :checked checked}))]
      (println " combo =>" combo))

    (println "\n== Strict let-ok: error short-circuit ==")
    (let [bad (r/let-ok [t (validate-task "")
                         _ (r/ok :unreached)]
                (r/ok :body-unreached))]
      (println " bad   =>" bad))

    (println "\n== Registry round-trip ==")
    (let [round (-> started adt/serialize adt/deserialize)]
      (println " serialize/deserialize ok? =>" (= started round)))

    (println "\nSPIKE: PASS")))

(when (= *file* (System/getProperty "babashka.file"))
  (-main))
