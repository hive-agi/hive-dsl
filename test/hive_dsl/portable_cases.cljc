(ns hive-dsl.portable-cases
  "The case table for the tri-runtime differential oracle: one host-free thunk
   per id, exercising the portable core.

   Host-free by construction — it must load and run unchanged on the JVM,
   ClojureWasm and clojurust. Nothing here may touch a host class, a JVM-only
   core fn, or IO. Adding a case here and regenerating the golden values
   (hive-dsl.portable-golden) is the whole workflow."
  (:require [hive-dsl.result :as r]
            [hive-dsl.result.taxonomy :as tax]
            [hive-dsl.result.agentop :as a]
            [hive-dsl.coerce :as c]
            [hive-dsl.adt :as adt]
            [hive-dsl.adt.schema :as as]
            [hive-dsl.conversation :as conv]
            [hive-dsl.swarm-status :as ss]
            [hive-dsl.resource :as res]
            [hive-dsl.batch :as b]
            [hive-dsl.context.identity :as ci]
            [hive-dsl.typed.emit :as te]))

(def cases
  "Ordered [id thunk] pairs. Ids are stable; a thunk must be deterministic."
  [[:result/ok            #(r/ok 1)]
   [:result/err           #(r/err :boom {:a 1})]
   [:result/ok?           #(r/ok? (r/ok 1))]
   [:result/err?          #(r/err? (r/err :boom {}))]
   [:result/let-ok        #(r/let-ok [x (r/ok 2) y (r/ok 3)] (r/ok (* x y)))]
   [:result/let-ok-short  #(r/let-ok [x (r/err :stop {}) y (r/ok 3)] (r/ok y))]
   [:result/map-ok        #(r/map-ok (r/ok 2) inc)]
   [:result/map-ok-err    #(r/map-ok (r/err :e {}) inc)]
   [:result/bind          #(r/bind (r/ok 2) (fn [v] (r/ok (inc v))))]
   [:result/rescue-coll   #(r/rescue [] (throw (ex-info "x" {})))]
   [:result/rescue-nil    #(r/rescue nil (throw (ex-info "x" {})))]
   [:result/rescue-ok     #(r/rescue :fb 42)]
   [:result/rescue-meta   #(:message (::r/error (meta (r/rescue [] (throw (ex-info "boom" {}))))))]
   [:result/rescue-log    #(r/rescue-log "lbl" :fb (throw (ex-info "x" {})))]
   [:result/rescue-fn     #((r/rescue-fn (fn [] (throw (ex-info "x" {}))) :fb))]
   [:result/try-effect    #(:error (r/try-effect (throw (ex-info "x" {}))))]
   [:taxonomy/known-error #(tax/known-error? :nope)]
   [:coerce/int           #(c/->int "42")]
   [:coerce/int-pad       #(c/->int " 42 ")]
   [:coerce/int-bad       #(c/->int "x")]
   [:coerce/int-num       #(c/->int 7)]
   [:coerce/double        #(c/->double "1.5")]
   [:coerce/double-pad    #(c/->double " 1.5 ")]
   [:coerce/double-bad    #(c/->double "x")]
   [:coerce/boolean       #(c/->boolean "true")]
   [:coerce/keyword       #(c/->keyword "abc")]
   [:coerce/vec-csv       #(c/->vec "a,b,c")]
   [:coerce/enum-ok       #(c/->enum "both" #{:outgoing :both})]
   [:coerce/enum-bad      #(:error (c/->enum "nope" #{:outgoing :both}))]
   [:adt/type             #(adt/adt-type {:adt/type :Foo :adt/variant :bar})]
   [:adt/valid            #(adt/adt-valid? {:adt/type :Foo :adt/variant :bar})]
   [:adt/schema-pred      #(as/pred->schema :int)]
   [:conv/status?         #(conv/delivery-status? {:adt/type :DeliveryStatus})]
   [:conv/->status        #(conv/->delivery-status :delivered)]
   [:swarm/status         #(ss/slave-status :slave-status/zombie)]
   [:swarm/status?        #(ss/slave-status? (ss/slave-status :slave-status/idle))]
   [:swarm/->status       #(ss/->slave-status :slave-status/working)]
   [:swarm/->status-bad   #(ss/->slave-status :slave-status/nope)]
   [:agentop/tap          #(a/tap (r/ok 1) identity)]
   [:agentop/fan-in-ok    #(a/fan-in [(r/ok 1) (r/ok 2)])]
   [:agentop/fan-in-err   #(:error (a/fan-in [(r/ok 1) (r/err :x {})]))]
   [:agentop/recover      #(a/recover (r/err :x {}) (fn [_] (r/ok :fixed)))]
   [:agentop/budget-ok    #(a/with-budget (atom 10) 3 (fn [] (r/ok :ran)))]
   [:agentop/budget-out   #(a/with-budget (atom 1) 3 (fn [] (r/ok :ran)))]
   [:agentop/retry-ok     #(a/retry-on (fn [] (r/ok :first)) {:max 3})]
   [:batch/normalize-map  #(b/normalize-tx-datum {:a 1})]
   [:batch/normalize-vec  #(b/normalize-tx-datum [:db/add 1 :a 2])]
   [:batch/count          #(b/batch-count (b/tx-batch :conn (fn [_ _] nil)))]
   [:resource/scope-acq   #(let [s (atom [])] (res/scope-acquire! s (fn [x] (r/ok x)) [:db]))]
   ;; context.identity — the ADT constructors, both projections, both coercions
   [:identity/coordinator     #(ci/parse-caller-id nil)]
   [:identity/named           #(ci/parse-caller-id "slave-7")]
   [:identity/coordinator-str #(ci/parse-caller-id "coordinator")]
   [:identity/caller-id?      #(ci/caller-id? (ci/parse-caller-id "slave-7"))]
   [:identity/caller-string   #(ci/caller-id-string (ci/parse-caller-id "slave-7"))]
   [:identity/caller-key      #(ci/caller-id-key (ci/parse-caller-id nil))]
   [:identity/scope-global    #(ci/parse-project-scope nil)]
   [:identity/scope-scoped    #(ci/parse-project-scope "hive")]
   [:identity/scope?          #(ci/project-scope? (ci/parse-project-scope "hive"))]
   [:identity/scope-string    #(ci/project-scope-string (ci/parse-project-scope "hive"))]
   [:identity/scope-string-nil #(ci/project-scope-string (ci/parse-project-scope nil))]
   [:identity/buffer-key      #(ci/make-buffer-key (ci/parse-caller-id "slave-7")
                                                   (ci/parse-project-scope "hive"))]
   [:identity/buffer-key-global #(ci/make-buffer-key (ci/parse-caller-id nil)
                                                     (ci/parse-project-scope nil))]
   [:identity/piggyback       #(ci/make-piggyback-agent-id (ci/parse-caller-id "slave-7")
                                                           (ci/parse-project-scope "hive"))]
   [:identity/piggyback-global #(ci/make-piggyback-agent-id (ci/parse-caller-id "slave-7")
                                                            (ci/parse-project-scope nil))]
   ;; typed.emit — both `pred-type` lookup tables, and the union over a
   ;; registered ADT. The fn-object rung goes through `resolve` at load time,
   ;; so it is the one entry here whose answer the runtimes could disagree on.
   [:emit/pred-sym            #(te/pred-type 'string?)]
   [:emit/pred-sym-map        #(te/pred-type 'map?)]
   [:emit/pred-fn             #(te/pred-type string?)]
   [:emit/pred-unknown        #(te/pred-type 'no-such-pred?)]
   [:emit/variant-hmap        #(te/variant-hmap :CallerId :caller/named)]
   [:emit/adt-union           #(te/adt-union :CallerId)]
   [:emit/adt-union-missing   #(te/adt-union :NoSuchAdtType)]])

(defn observe
  "Run every case, returning {id value}. A throwing thunk records
   {:portable-cases/threw <ex-message>} so a divergence in failure mode is
   still a comparable value."
  []
  (reduce (fn [acc [id thunk]]
            (assoc acc id
                   (try (thunk)
                        (catch #?(:clj Throwable :cljs :default :default :default) e
                          {:portable-cases/threw (ex-message e)}))))
          {}
          cases))
