(ns hive-dsl.adt
  "Closed algebraic data types (ADTs) with keyword dispatch.

   defadt defines sum types that are:
   - Closed at definition site (variant set is fixed at macro-expansion time)
   - Dispatched via keyword (works with Clojure `case`)
   - Coercible from keywords (:event/started → variant value)
   - Serializable as plain EDN maps (round-trip safe)
   - Registered in a global type registry

   Runtime representation: plain maps with :adt/type and :adt/variant keys.
   This keeps ADT values printable, serializable, and compatible with all
   Clojure map operations.

   Usage:
     ;; Data-carrying variants
     (defadt EventType
       \"Event types for hivemind communication.\"
       [:event/started   {:task string?}]
       [:event/progress  {:message string?}])

     ;; Enum variants (no associated data)
     (defadt SpawnMode
       \"Agent spawn modes.\"
       :spawn/vterm
       :spawn/headless)

     ;; Mixed
     (defadt Shape
       [:shape/circle {:radius number?}]
       [:shape/rect   {:w number? :h number?}]
       :shape/point)

   Generated API:
     (event-type :event/started {:task \"X\"})  ; constructor
     (event-type? x)                           ; type predicate
     (->event-type :event/started)             ; keyword coercion
     (adt-case EventType evt                   ; exhaustive match
       :event/started  (str (:task evt))
       :event/progress (str (:message evt)))"
  (:require [clojure.set :as set]
            [clojure.string :as str]))

;; =============================================================================
;; ADT Registry (parallel to result.taxonomy registry)
;; =============================================================================

(defonce ^:private registry (atom {}))

(defn register-type!
  "Register an ADT type with its variant set and schemas.
   Called by defadt macro — not for direct use."
  [type-kw type-meta]
  (swap! registry assoc type-kw type-meta))

(defn registered-types
  "Return the map of all registered ADT types."
  []
  @registry)

(defn type-registered?
  "True if the given type keyword is registered."
  [type-kw]
  (contains? @registry type-kw))

(defn type-variants
  "Return the set of variant keywords for a registered type.
   Returns nil if type is not registered."
  [type-kw]
  (get-in @registry [type-kw :variants]))

;; =============================================================================
;; ADT Value Operations
;; =============================================================================

(defn adt-type
  "Extract the type keyword from an ADT value."
  [x]
  (when (map? x) (:adt/type x)))

(defn adt-variant
  "Extract the variant keyword from an ADT value."
  [x]
  (when (map? x) (:adt/variant x)))

(defn adt?
  "True if x is any ADT value (has :adt/type and :adt/variant keys)."
  [x]
  (and (map? x)
       (contains? x :adt/type)
       (contains? x :adt/variant)))

(defn adt-valid?
  "True if x is a valid ADT value with a registered type and known variant."
  [x]
  (and (adt? x)
       (let [type-kw (:adt/type x)
             variant (:adt/variant x)]
         (and (type-registered? type-kw)
              (contains? (type-variants type-kw) variant)))))

(defn adt-data
  "Extract the data fields from an ADT value (everything except :adt/type and :adt/variant)."
  [x]
  (when (map? x)
    (dissoc x :adt/type :adt/variant)))

;; =============================================================================
;; Construction & Coercion
;; =============================================================================

(defn make-variant
  "Construct an ADT variant value. Low-level constructor — prefer the
   generated constructor from defadt for validation.

   (make-variant :EventType :event/started {:task \"X\"})
   => {:adt/type :EventType, :adt/variant :event/started, :task \"X\"}"
  ([type-kw variant-kw]
   {:adt/type type-kw :adt/variant variant-kw})
  ([type-kw variant-kw data]
   (if (and data (seq data))
     (merge {:adt/type type-kw :adt/variant variant-kw} data)
     {:adt/type type-kw :adt/variant variant-kw})))

(defn coerce-keyword
  "Coerce a keyword to an ADT variant value (no data fields).
   Returns nil if the keyword is not a valid variant of the type.

   (coerce-keyword :EventType :event/started)
   => {:adt/type :EventType, :adt/variant :event/started}"
  [type-kw variant-kw]
  (when (and (type-registered? type-kw)
             (contains? (type-variants type-kw) variant-kw))
    (make-variant type-kw variant-kw)))

;; =============================================================================
;; Serialization (EDN round-trip)
;; =============================================================================

(defn serialize
  "Serialize an ADT value to a plain map (EDN-safe).
   This is effectively identity since ADT values ARE plain maps,
   but ensures the return is a plain hash-map (not a record or subtype)."
  [x]
  (when (adt? x) (into {} x)))

(defn deserialize
  "Deserialize a plain map to a validated ADT value.
   Returns the map if valid, nil otherwise.

   (deserialize {:adt/type :EventType, :adt/variant :event/started, :task \"X\"})
   => {:adt/type :EventType, :adt/variant :event/started, :task \"X\"}"
  [m]
  (when (and (map? m)
             (contains? m :adt/type)
             (contains? m :adt/variant)
             (let [type-kw (:adt/type m)
                   variant (:adt/variant m)]
               (and (type-registered? type-kw)
                    (contains? (type-variants type-kw) variant))))
    m))

;; =============================================================================
;; Schema Validation (opt-in, like spec instrumentation)
;; =============================================================================

(defn validate
  "Validate an ADT value against its type's schema.
   Returns the value if valid, throws ex-info if invalid.
   Use for boundary validation (API entry points, deserialization).

   type-meta is the var value from defadt (e.g. EventType).

   (validate EventType (event-type :event/started {:task \"X\"}))
   => {:adt/type :EventType, :adt/variant :event/started, :task \"X\"}"
  [type-meta x]
  (let [type-kw (:type type-meta)
        variants (:variants type-meta)
        schemas (:schemas type-meta)]
    (when-not (map? x)
      (throw (ex-info "ADT value must be a map"
                      {:type type-kw :value x})))
    (when-not (= (:adt/type x) type-kw)
      (throw (ex-info (str "Type mismatch: expected " type-kw
                           ", got " (:adt/type x))
                      {:expected type-kw :actual (:adt/type x)})))
    (let [variant (:adt/variant x)]
      (when-not (contains? variants variant)
        (throw (ex-info (str "Unknown variant: " variant)
                        {:type type-kw :variant variant
                         :valid-variants variants})))
      (when-let [schema (get schemas variant)]
        (let [errors (into []
                           (keep (fn [[field pred]]
                                   (let [v (get x field ::missing)]
                                     (cond
                                       (= v ::missing)
                                       {:field field :error :missing}

                                       (not (pred v))
                                       {:field field :error :invalid :value v}))))
                           schema)]
          (when (seq errors)
            (throw (ex-info "Schema validation failed"
                            {:type type-kw :variant variant
                             :errors errors}))))))
    x))

;; =============================================================================
;; Variant Declaration Parsing (used by defadt macro)
;; =============================================================================

(defn- parse-variant-decl
  "Parse a variant declaration form.
   Keyword → {:variant kw :schema nil}
   [keyword schema-map] → {:variant kw :schema schema-map}"
  [v]
  (cond
    (keyword? v)
    {:variant v :schema nil}

    (and (vector? v) (= 2 (count v)) (keyword? (first v)) (map? (second v)))
    {:variant (first v) :schema (second v)}

    (and (vector? v) (= 1 (count v)) (keyword? (first v)))
    {:variant (first v) :schema nil}

    :else
    (throw (ex-info (str "Invalid variant declaration: " (pr-str v)
                         ". Expected keyword or [keyword schema-map].")
                    {:variant v}))))

(defn- camel->kebab
  "Convert CamelCase to kebab-case.
   EventType → event-type
   SpawnMode → spawn-mode"
  [s]
  (-> s
      (str/replace #"([a-z0-9])([A-Z])" "$1-$2")
      (str/replace #"([A-Z]+)([A-Z][a-z])" "$1-$2")
      str/lower-case))

;; =============================================================================
;; defadt Macro
;; =============================================================================

(defmacro defadt
  "Define a closed algebraic data type (sum type) with keyword dispatch.

   Each variant is either:
   - A bare keyword (enum variant, no associated data)
   - A vector [keyword schema-map] (data variant with typed fields)

   Generates:
   - `TypeName` var with type metadata {:type :TypeName :variants #{...} :schemas {...}}
   - `type-name` constructor fn (kebab-case of TypeName)
   - `type-name?` predicate fn
   - `->type-name` keyword coercion fn (returns nil for invalid keywords)

   Example:
     (defadt EventType
       \"Event types for hivemind communication.\"
       [:event/started   {:task string?}]
       [:event/progress  {:message string?}]
       :event/completed)

     (event-type :event/started {:task \"X\"})
     ;; => {:adt/type :EventType, :adt/variant :event/started, :task \"X\"}

     (event-type :event/completed)
     ;; => {:adt/type :EventType, :adt/variant :event/completed}

     (event-type? x) ;; => true/false

     (->event-type :event/started)
     ;; => {:adt/type :EventType, :adt/variant :event/started}"
  {:arglists '([type-name docstring? & variants])}
  [type-name & body]
  (let [[docstring variants] (if (string? (first body))
                               [(first body) (rest body)]
                               [nil body])
        type-kw (keyword (name type-name))
        parsed (mapv parse-variant-decl variants)
        _ (when (empty? parsed)
            (throw (ex-info (str "defadt " type-name " requires at least one variant")
                            {:type type-name})))
        variant-kws (mapv :variant parsed)
        variant-set (set variant-kws)
        ;; Check for duplicate variants
        _ (when (not= (count variant-kws) (count variant-set))
            (let [dupes (into [] (comp (filter #(> (val %) 1)) (map key))
                              (frequencies variant-kws))]
              (throw (ex-info (str "Duplicate variants in defadt " type-name ": " dupes)
                              {:type type-name :duplicates dupes}))))
        ;; Build schemas map — only for data-carrying variants
        ;; Schemas contain symbols (like string?) that will be resolved at load time
        schemas-form (into {} (keep (fn [{:keys [variant schema]}]
                                      (when schema [variant schema]))
                                    parsed))
        ;; Generate fn names: CamelCase → kebab-case
        kname (camel->kebab (name type-name))
        constructor-sym (symbol kname)
        pred-sym (symbol (str kname "?"))
        coerce-sym (symbol (str "->" kname))]
    `(do
       ;; Type metadata var
       (def ~(vary-meta type-name assoc
                        :doc (or docstring (str "ADT type " type-kw))
                        :adt-type type-kw)
         {:type ~type-kw
          :variants ~variant-set
          :schemas ~schemas-form})

       ;; Register in global ADT registry
       (register-type! ~type-kw {:variants ~variant-set
                                 :schemas ~schemas-form})

       ;; Constructor fn
       (defn ~constructor-sym
         ~(str "Construct a " (name type-name) " variant.\n"
               "  (" kname " variant-keyword) for enum variants\n"
               "  (" kname " variant-keyword data-map) for data variants\n\n"
               "  Throws ex-info for unknown variants.")
         ([variant-kw#]
          (when-not (contains? ~variant-set variant-kw#)
            (throw (ex-info (str "Unknown " ~(name type-name) " variant: " variant-kw#)
                            {:type ~type-kw
                             :variant variant-kw#
                             :valid-variants ~variant-set})))
          {:adt/type ~type-kw :adt/variant variant-kw#})
         ([variant-kw# data#]
          (when-not (contains? ~variant-set variant-kw#)
            (throw (ex-info (str "Unknown " ~(name type-name) " variant: " variant-kw#)
                            {:type ~type-kw
                             :variant variant-kw#
                             :valid-variants ~variant-set})))
          (merge {:adt/type ~type-kw :adt/variant variant-kw#} data#)))

       ;; Predicate fn
       (defn ~pred-sym
         ~(str "True if x is a " (name type-name) " ADT value.")
         [x#]
         (and (map? x#) (= (:adt/type x#) ~type-kw)))

       ;; Keyword coercion fn
       (defn ~coerce-sym
         ~(str "Coerce a keyword to a " (name type-name) " variant (no data fields).\n"
               "  Returns nil if keyword is not a valid variant.")
         [kw#]
         (when (contains? ~variant-set kw#)
           {:adt/type ~type-kw :adt/variant kw#}))

       ;; Return the type keyword
       ~type-kw)))

;; =============================================================================
;; adt-case: Exhaustive Pattern Matching
;; =============================================================================

(defmacro adt-case
  "Exhaustive pattern match on an ADT value's variant keyword.

   Checks at macro-expansion time that all variants are covered — compile-time
   exhaustiveness checking. Uses `case` under the hood for O(1) dispatch.

   type-ref must be a symbol that resolves to a defadt var (e.g. EventType).
   expr is evaluated once and dispatched on (:adt/variant expr).

   (adt-case EventType evt
     :event/started   (str \"task: \" (:task evt))
     :event/progress  (str \"msg: \" (:message evt))
     :event/completed \"done\")

   Compile-time error if:
   - Any variant is missing (non-exhaustive)
   - Any unknown variant is listed (typo detection)"
  [type-ref expr & clauses]
  (when-not (even? (count clauses))
    (throw (ex-info "adt-case requires even number of clauses (variant expr pairs)"
                    {:type type-ref :clause-count (count clauses)})))
  (let [type-meta (cond
                    (symbol? type-ref)
                    (if-let [v (resolve type-ref)]
                      @v
                      (throw (ex-info (str "Cannot resolve ADT type: " type-ref
                                           ". Is the namespace required?")
                                      {:type type-ref})))

                    (map? type-ref)
                    type-ref

                    :else
                    (throw (ex-info "adt-case type must be a symbol or type metadata map"
                                    {:type type-ref})))
        variants (:variants type-meta)
        _ (when-not variants
            (throw (ex-info (str "Not an ADT type: " type-ref " (no :variants key)")
                            {:type type-ref :meta type-meta})))
        clause-pairs (partition 2 clauses)
        covered (set (map first clause-pairs))
        missing (set/difference variants covered)
        extra (set/difference covered variants)]
    (when (seq missing)
      (throw (ex-info (str "Non-exhaustive adt-case for "
                           (:type type-meta) ": missing " missing)
                      {:type (:type type-meta)
                       :missing missing
                       :covered covered})))
    (when (seq extra)
      (throw (ex-info (str "Unknown variants in adt-case for "
                           (:type type-meta) ": " extra)
                      {:type (:type type-meta)
                       :extra extra
                       :valid variants})))
    `(case (:adt/variant ~expr)
       ~@(mapcat identity clause-pairs))))
