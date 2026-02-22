(ns hive-dsl.coerce
  "Pure MCP boundary coercion — string→typed value conversion.

   MCP JSON transports deliver ALL parameter values as strings.
   These functions systematically coerce strings to the declared types,
   returning Result maps ({:ok v} / {:error category ...}).

   Compose with hive-dsl.result/let-ok for railway-oriented pipelines.

   Zero external dependencies — pure Clojure."
  (:require [hive-dsl.result :as r]
            [clojure.data.json :as json]
            [clojure.string :as str]))

;; =============================================================================
;; Scalar Coercions
;; =============================================================================

(defn ->int
  "Coerce value to integer.

   \"3\"  → {:ok 3}
   3    → {:ok 3}
   nil  → {:ok nil}  (pass-through; use with defaults at call site)
   \"x\" → {:error :coerce/invalid-int ...}"
  [v]
  (cond
    (nil? v)     (r/ok nil)
    (integer? v) (r/ok (long v))
    (string? v)  (try
                   (r/ok (Long/parseLong v))
                   (catch NumberFormatException _
                     (r/err :coerce/invalid-int
                            {:message (str "Expected integer, got \"" v "\"")
                             :value v})))
    (number? v)  (r/ok (long v))
    :else        (r/err :coerce/invalid-int
                        {:message (str "Expected integer, got " (type v) ": " (pr-str v))
                         :value v})))

(defn ->double
  "Coerce value to double.

   \"0.9\" → {:ok 0.9}
   0.9   → {:ok 0.9}
   nil   → {:ok nil}"
  [v]
  (cond
    (nil? v)     (r/ok nil)
    (double? v)  (r/ok v)
    (number? v)  (r/ok (double v))
    (string? v)  (try
                   (r/ok (Double/parseDouble v))
                   (catch NumberFormatException _
                     (r/err :coerce/invalid-double
                            {:message (str "Expected number, got \"" v "\"")
                             :value v})))
    :else        (r/err :coerce/invalid-double
                        {:message (str "Expected number, got " (type v) ": " (pr-str v))
                         :value v})))

(defn ->keyword
  "Coerce value to keyword.

   \"both\"  → {:ok :both}
   :both   → {:ok :both}
   nil     → {:ok nil}"
  [v]
  (cond
    (nil? v)     (r/ok nil)
    (keyword? v) (r/ok v)
    (string? v)  (if (str/blank? v)
                   (r/ok nil)
                   (r/ok (keyword v)))
    :else        (r/err :coerce/invalid-keyword
                        {:message (str "Expected string or keyword, got " (type v))
                         :value v})))

(defn ->boolean
  "Coerce value to boolean.

   \"true\"  → {:ok true}
   \"false\" → {:ok false}
   true    → {:ok true}
   nil     → {:ok nil}"
  [v]
  (cond
    (nil? v)      (r/ok nil)
    (boolean? v)  (r/ok v)
    (string? v)   (case (str/lower-case (str/trim v))
                    ("true" "1" "yes")  (r/ok true)
                    ("false" "0" "no")  (r/ok false)
                    (r/err :coerce/invalid-boolean
                           {:message (str "Expected boolean, got \"" v "\"")
                            :value v}))
    :else         (r/err :coerce/invalid-boolean
                         {:message (str "Expected boolean, got " (type v))
                          :value v})))

(defn ->vec
  "Coerce value to vector.

   [\"a\"]       → {:ok [\"a\"]}
   \"[\\\"a\\\"]\" → {:ok [\"a\"]}  (JSON parse)
   nil          → {:ok nil}"
  [v]
  (cond
    (nil? v)        (r/ok nil)
    (vector? v)     (r/ok v)
    (sequential? v) (r/ok (vec v))
    (string? v)     (if (str/starts-with? (str/trim v) "[")
                      (try
                        (let [parsed (json/read-str v)]
                          (if (sequential? parsed)
                            (r/ok (vec parsed))
                            (r/err :coerce/invalid-vec
                                   {:message "JSON parsed to non-array"
                                    :value v})))
                        (catch Exception e
                          (r/err :coerce/invalid-vec
                                 {:message (str "Invalid JSON array: " (.getMessage e))
                                  :value v})))
                      (r/err :coerce/invalid-vec
                             {:message (str "Expected array, got string: \"" (subs v 0 (min 50 (count v))) "\"")
                              :value v}))
    :else           (r/err :coerce/invalid-vec
                           {:message (str "Expected array, got " (type v))
                            :value v})))

(defn ->enum
  "Coerce value to keyword and validate against allowed set.

   (->enum \"both\" #{:outgoing :incoming :both}) → {:ok :both}
   (->enum \"nope\" #{:outgoing :incoming :both}) → {:error ...}"
  [v allowed-set]
  (r/bind (->keyword v)
          (fn [kw]
            (if (nil? kw)
              (r/ok nil)
              (if (contains? allowed-set kw)
                (r/ok kw)
                (r/err :coerce/invalid-enum
                       {:message (str "\"" (name kw) "\" not in " (pr-str (mapv name allowed-set)))
                        :value v
                        :allowed allowed-set}))))))

;; =============================================================================
;; Declarative Map Coercion
;; =============================================================================

(defn- coerce-field
  "Coerce a single field value according to its type spec.

   Type specs:
     [:int]                       → ->int
     [:double]                    → ->double
     [:keyword]                   → ->keyword
     [:boolean]                   → ->boolean
     [:vec]                       → ->vec
     [:enum #{:a :b}]             → ->enum with allowed set"
  [v [type-kw & args]]
  (case type-kw
    :int     (->int v)
    :double  (->double v)
    :keyword (->keyword v)
    :boolean (->boolean v)
    :vec     (->vec v)
    :enum    (->enum v (first args))
    (r/err :coerce/unknown-type {:message (str "Unknown coerce type: " type-kw)})))

(defn coerce-map
  "Declarative map coercion — coerce param map fields per schema.

   schema: {field-key [type-spec ...]}
   params: raw param map (string values from MCP)

   Only coerces fields present in both schema and params.
   Unknown fields pass through unchanged.

   (coerce-map {:max_depth [:int]
                :direction [:enum #{:outgoing :incoming :both}]}
               {:max_depth \"3\" :direction \"both\" :scope \"hive-mcp\"})
   => {:ok {:max_depth 3 :direction :both :scope \"hive-mcp\"}}"
  [schema params]
  (loop [remaining (seq schema)
         acc params]
    (if-not remaining
      (r/ok acc)
      (let [[field type-spec] (first remaining)
            v (get acc field)]
        (if (nil? v)
          ;; Field not in params — skip
          (recur (next remaining) acc)
          ;; Coerce the field
          (let [result (coerce-field v type-spec)]
            (if (r/ok? result)
              (let [coerced (:ok result)]
                (recur (next remaining)
                       (if (nil? coerced)
                         (dissoc acc field)
                         (assoc acc field coerced))))
              ;; Error — enrich with field name
              (r/err (:error result)
                     (assoc (dissoc result :error)
                            :field field)))))))))
