(ns hive-dsl.try-ordered-trifecta-test
  "Coverage for `r/try-ordered` — Java's ordered per-class catch as one form,
   returning a PLAIN value at the interceptor edge.

   The acceptance case is the funeraria inventory multipart parse: ONE try with
   four ordered catches (FileUploadException -> 413, IOException -> 413,
   ExceptionInfo -> 413, Exception catch-all -> 400 JSON), each branch seeing
   the exception AND the original context and returning an UPDATED interceptor
   context. Rebuilt here with stand-in classes; funeraria is not a dependency.

   The FileUploadException stand-in is a REAL subclass of IOException
   (FileNotFoundException), so the ordering refusal — a subclass listed after
   its superclass is unreachable — is exercised for real, not merely asserted
   on paper.

   Trifecta: the `outcome` classifier is golden-snapshotted, property-checked
   and mutation-tested. The three required mutants — reorder the branches, drop
   the catch-all, catch Throwable instead of Exception — each must be caught."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-dsl.result :as r]
            [hive-test.trifecta :refer [deftrifecta]]))

;; ============================================================
;; Stand-in exception classes
;; ============================================================

;; FileUploadException has no JDK class. The stand-in is a REAL subclass of
;; IOException, so listing it AFTER IOException is genuinely unreachable and the
;; macro's refusal is exercised on a real subclass relationship.
(def FileUploadException java.io.FileNotFoundException)

(defn- throwable-for
  "The stand-in throwable a `kind` names, or nil for the success path."
  [kind]
  (case kind
    :file-upload (java.io.FileNotFoundException. "multipart too large")
    :io          (java.io.IOException. "multipart read failed")
    :ex-info     (ex-info "multipart rejected" {:part :file})
    :runtime     (RuntimeException. "unexpected failure")
    :npe         (NullPointerException. "nil part")
    :assertion   (AssertionError. "invariant violated")
    :ok          nil))

;; ============================================================
;; The four-branch ordered catch, as an interceptor edge writes it
;; ============================================================

(defn- run-ordered
  "ONE try, four ordered catches, each returning an UPDATED context (a plain
   value). The FileUploadException branch must win over IOException for a
   FileNotFoundException; ExceptionInfo must not be swallowed by IOException."
  [kind]
  (r/try-ordered {:status 0 :kind kind}
    [[FileUploadException
      (fn [e ctx] (assoc ctx :status 413 :branch :file-upload :msg (ex-message e)))]
     [java.io.IOException
      (fn [e ctx] (assoc ctx :status 413 :branch :io :msg (ex-message e)))]
     [clojure.lang.ExceptionInfo
      (fn [e ctx] (assoc ctx :status 413 :branch :ex-info :msg (ex-message e)))]
     [:else
      (fn [e ctx] (assoc ctx :status 400 :branch :catch-all
                             :body {:error (ex-message e)}))]]
    (if-let [t (throwable-for kind)]
      (throw t)
      {:status 0 :kind kind :branch :none})))

(defn outcome
  "Total classifier over `kind`: run the four-branch catch and reify an escape
   (an Error, or a throw from inside a handler) as data. The trifecta subject."
  [kind]
  (try
    (run-ordered kind)
    (catch Throwable t
      {:escaped (str (class t))})))

;; ============================================================
;; Mutants — broken implementations; each must be caught
;; ============================================================

(defn- mut-reorder
  "Bug: the branches are reordered so IOException (a superclass of the
   FileUploadException stand-in) is caught first — the very shadowing the macro
   refuses. Clojure's compiler accepts this ordering, so the macro is the only
   thing standing between it and a silently unreachable branch."
  [kind]
  (try
    (try
      (if-let [t (throwable-for kind)]
        (throw t)
        {:status 0 :kind kind :branch :none})
      (catch java.io.IOException e
        (assoc {:kind kind} :status 413 :branch :io :msg (ex-message e)))
      (catch java.io.FileNotFoundException e
        (assoc {:kind kind} :status 413 :branch :file-upload :msg (ex-message e)))
      (catch clojure.lang.ExceptionInfo e
        (assoc {:kind kind} :status 413 :branch :ex-info :msg (ex-message e)))
      (catch Exception e
        (assoc {:kind kind} :status 400 :branch :catch-all
                          :body {:error (ex-message e)})))
    (catch Throwable t
      {:escaped (str (class t))})))

(defn- mut-drop-catch-all
  "Bug: the catch-all is dropped, so an unclassified Exception escapes instead
   of becoming a 400."
  [kind]
  (try
    (r/try-ordered {:status 0 :kind kind}
      [[FileUploadException
        (fn [e ctx] (assoc ctx :status 413 :branch :file-upload :msg (ex-message e)))]
       [java.io.IOException
        (fn [e ctx] (assoc ctx :status 413 :branch :io :msg (ex-message e)))]
       [clojure.lang.ExceptionInfo
        (fn [e ctx] (assoc ctx :status 413 :branch :ex-info :msg (ex-message e)))]]
      (if-let [t (throwable-for kind)]
        (throw t)
        {:status 0 :kind kind :branch :none}))
    (catch Throwable t
      {:escaped (str (class t))})))

(defn- mut-catch-throwable
  "Bug: the catch-all catches Throwable instead of Exception, so a fatal Error
   is silently converted into a 400 instead of propagating."
  [kind]
  (try
    (r/try-ordered {:status 0 :kind kind}
      [[FileUploadException
        (fn [e ctx] (assoc ctx :status 413 :branch :file-upload :msg (ex-message e)))]
       [java.io.IOException
        (fn [e ctx] (assoc ctx :status 413 :branch :io :msg (ex-message e)))]
       [clojure.lang.ExceptionInfo
        (fn [e ctx] (assoc ctx :status 413 :branch :ex-info :msg (ex-message e)))]
       [Throwable
        (fn [e ctx] (assoc ctx :status 400 :branch :catch-all
                               :body {:error (ex-message e)}))]]
      (if-let [t (throwable-for kind)]
        (throw t)
        {:status 0 :kind kind :branch :none}))
    (catch Throwable t
      {:escaped (str (class t))})))

;; ============================================================
;; Generators and invariants
;; ============================================================

(def ^:private kinds
  [:file-upload :io :ex-info :runtime :npe :assertion :ok])

(def ^:private gen-kind
  (gen/elements kinds))

(defn- valid-outcome?
  "Post-condition: an outcome is either an escape or an updated context."
  [o]
  (or (string? (:escaped o))
      (and (contains? o :status) (contains? o :kind))))

;; ============================================================
;; Trifecta — golden + property + mutation
;; ============================================================

(deftrifecta try-ordered-trifecta
  hive-dsl.try-ordered-trifecta-test/outcome
  {:golden-path "test/golden/hive-dsl/trifecta-try-ordered.edn"
   :cases       {:file-upload :file-upload
                 :io          :io
                 :ex-info     :ex-info
                 :runtime     :runtime
                 :npe         :npe
                 :assertion   :assertion
                 :ok          :ok}
   :gen         gen-kind
   :pred        valid-outcome?
   :num-tests   200
   :mutations   [["reorder-branches"        mut-reorder]
                 ["drop-catch-all"          mut-drop-catch-all]
                 ["catch-throwable"         mut-catch-throwable]]})

;; ============================================================
;; Input-aware oracle — branch + status must match the throwable
;; ============================================================

(defn- oracle-pass?
  "The branch and status the four ordered catches must produce for `kind`,
   and that the result is a plain value (no Result keys)."
  [kind]
  (let [o (outcome kind)
        [branch status] (case kind
                          :file-upload [:file-upload 413]
                          :io          [:io 413]
                          :ex-info     [:ex-info 413]
                          :runtime     [:catch-all 400]
                          :npe         [:catch-all 400]
                          :assertion   nil
                          :ok          [:none 0])]
    (if (= kind :assertion)
      (and (contains? o :escaped)
           (not (contains? o :status)))
      (and (= branch (:branch o))
           (= status (:status o))
           (not (contains? o :ok))
           (not (contains? o :error))))))

(tc/defspec try-ordered-oracle 200
  (prop/for-all [kind gen-kind]
    (oracle-pass? kind)))

;; ============================================================
;; Ordering refusal — a subclass after its superclass is refused
;; ============================================================

(defn- refusal-category
  "Macroexpand `form`, returning :expanded, or the refusal's :category
   (unwrapping the CompilerException the expansion host wraps around it)."
  [form]
  ;; Bind *ns* to this test namespace so `macroexpand-1` resolves the `r` alias,
  ;; the FileUploadException stand-in and the JDK class symbols exactly as the
  ;; compiler does at file-load time. Without the binding the expansion depends
  ;; on whatever *ns* the test runner happens to be in.
  (binding [*ns* (find-ns 'hive-dsl.try-ordered-trifecta-test)]
    (try
      (let [_ (macroexpand-1 form)]
        :expanded)
      (catch Throwable t
        (or (some-> t ex-cause ex-data :category)
            (some-> t ex-data :category)
            :no-refusal)))))

(defn- refused?
  "True when expanding `form` is refused, by the macro's own guard OR by the
   fdef's argument validation (which the compiler runs first)."
  [form]
  (not= :expanded (refusal-category form)))

(deftest ordering-refusal
  (testing "a subclass listed after its superclass is refused at expansion"
    (is (= :result/try-ordered-unreachable-clause
           (refusal-category
            '(r/try-ordered ctx
               [[java.io.IOException (fn [e c] c)]
                [FileUploadException (fn [e c] c)]]
               1)))))
  (testing "a clause after :else is refused"
    (is (= :result/try-ordered-unreachable-clause
           (refusal-category
            '(r/try-ordered ctx
               [[:else (fn [e c] c)]
                [java.io.IOException (fn [e c] c)]]
               1)))))
  (testing "the well-ordered four-branch vector is accepted"
    (is (= :expanded
           (refusal-category
            '(r/try-ordered ctx
               [[FileUploadException (fn [e c] c)]
                [java.io.IOException (fn [e c] c)]
                [clojure.lang.ExceptionInfo (fn [e c] c)]
                [:else (fn [e c] c)]]
               1)))))
  (testing "a malformed clause vector is refused (shape-checked at expansion)"
    ;; The fdef on the macro makes the compiler validate the clause vector
    ;; before the macro body runs, so a malformed clause never expands. Where
    ;; no fdef is loaded, the macro's own guard raises
    ;; :result/try-ordered-malformed-clauses instead; either way it is refused.
    (is (refused? '(r/try-ordered ctx [[java.io.IOException]] 1)))
    (is (refused? '(r/try-ordered ctx (java.io.IOException (fn [e c] c)) 1)))))

;; ============================================================
;; Semantics the answer must keep
;; ============================================================

(deftest exceptioninfo-not-swallowed
  (testing "ExceptionInfo falls past an IOException branch to the catch-all"
    (let [o (r/try-ordered {:status 0}
              [[java.io.IOException
                (fn [_ ctx] (assoc ctx :branch :io))]
               [:else
                (fn [e ctx] (assoc ctx :branch :catch-all :msg (ex-message e)))]]
              (throw (ex-info "rejected" {:part :file})))]
      (is (= :catch-all (:branch o)))
      (is (= "rejected" (:msg o))))))

(deftest handler-throw-propagates
  (testing "an exception thrown inside a handler propagates unchanged"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"handler blew up"
         (r/try-ordered {:status 0}
           [[java.io.IOException (fn [_ _] (throw (ex-info "handler blew up" {})))]
            [:else (fn [_ ctx] (assoc ctx :branch :catch-all))]]
           (throw (java.io.IOException. "body")))))))

(deftest error-escapes-catch-all
  (testing ":else catches Exception, not Error — an Error propagates"
    (is (thrown? AssertionError
                 (r/try-ordered {:status 0}
                   [[java.io.IOException (fn [_ ctx] (assoc ctx :branch :io))]
                    [:else (fn [_ ctx] (assoc ctx :branch :catch-all))]]
                   (throw (AssertionError. "invariant")))))))

(deftest success-is-plain-body-value
  (testing "the body value is returned unchanged, and never wrapped in a Result"
    (let [o (r/try-ordered {:status 0}
              [[java.io.IOException (fn [_ ctx] (assoc ctx :branch :io))]
               [:else (fn [_ ctx] (assoc ctx :branch :catch-all))]]
              {:status 200 :body {:ok true}})]
      (is (= {:status 200 :body {:ok true}} o))
      (is (not (r/ok? o)))
      (is (not (r/err? o))))))

(deftest context-is-evaluated-once
  (testing "the ctx expression is evaluated exactly once"
    (let [n (atom 0)]
      (r/try-ordered (do (swap! n inc) {:status 0})
        [[java.io.IOException (fn [_ ctx] (assoc ctx :branch :io))]
         [:else (fn [_ ctx] (assoc ctx :branch :catch-all))]]
        (throw (java.io.IOException. "x")))
      (is (= 1 @n)))))

;; ============================================================
;; ACCEPTANCE — the funeraria inventory multipart case
;; ============================================================

(defn- inventory-handler
  "Rebuild of the inventory interceptor: ONE try around the multipart parse,
   four ordered catches, each returning an UPDATED interceptor context (a plain
   value). `throwable` is what the parse would raise."
  [request throwable]
  (r/try-ordered (assoc request :response nil :log [])
    [[FileUploadException
      (fn [e ctx] (-> ctx
                      (update :log conj [:warn (ex-message e)])
                      (assoc :response {:status 413 :body {:error "payload too large"}})))]
     [java.io.IOException
      (fn [e ctx] (-> ctx
                      (update :log conj [:warn (ex-message e)])
                      (assoc :response {:status 413 :body {:error "payload too large"}})))]
     [clojure.lang.ExceptionInfo
      (fn [e ctx] (-> ctx
                      (update :log conj [:warn (ex-message e)])
                      (assoc :response {:status 413 :body {:error "payload too large"}})))]
     [:else
      (fn [e ctx] (-> ctx
                      (update :log conj [:error (ex-message e)])
                      (assoc :response {:status 400 :body {:error (ex-message e)}})))]]
    (if throwable
      (throw throwable)
      (assoc request :response {:status 200 :body {:ok true}}))))

(deftest acceptance-inventory-multipart
  (let [req {:uri "/inventory/upload" :method :post}]
    (testing "FileUploadException -> 413 (and beats its IOException superclass)"
      (let [ctx (inventory-handler req (java.io.FileNotFoundException. "part too large"))]
        (is (= 413 (get-in ctx [:response :status])))
        (is (= [:warn "part too large"] (first (:log ctx))))
        (is (map? ctx))
        (is (not (r/ok? ctx)))
        (is (not (r/err? ctx)))))
    (testing "IOException -> 413"
      (let [ctx (inventory-handler req (java.io.IOException. "read failed"))]
        (is (= 413 (get-in ctx [:response :status])))
        (is (= [:warn "read failed"] (first (:log ctx))))))
    (testing "ExceptionInfo -> 413"
      (let [ctx (inventory-handler req (ex-info "rejected" {:part :file}))]
        (is (= 413 (get-in ctx [:response :status])))
        (is (= [:warn "rejected"] (first (:log ctx))))))
    (testing "any other Exception -> 400 JSON"
      (let [ctx (inventory-handler req (RuntimeException. "boom"))]
        (is (= 400 (get-in ctx [:response :status])))
        (is (= {:error "boom"} (get-in ctx [:response :body])))
        (is (= [:error "boom"] (first (:log ctx))))))
    (testing "success returns the request with a 200"
      (let [ctx (inventory-handler req nil)]
        (is (= 200 (get-in ctx [:response :status])))
        (is (= {:ok true} (get-in ctx [:response :body])))))))
