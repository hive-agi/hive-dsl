(ns hive-dsl.typed.emit
  "Typed Clojure type syntax AS DATA for registered ADTs: derives the
   per-variant HMap union of a defadt type from its variant schemas.
   Pure emission - no typed.clojure require, safe on any classpath.
   hive-dsl.typed.adt/defadt-alias consumes this at macroexpansion."
  (:require [hive-dsl.adt :as adt]))

(def ^:private sym->type
  "Variant-schema predicate symbols -> Typed Clojure syntax."
  {'any?               'typed.clojure/Any
   'boolean?           'typed.clojure/Bool
   'double?            'Double
   'int?               'typed.clojure/AnyInteger
   'integer?           'typed.clojure/AnyInteger
   'keyword?           'typed.clojure/Kw
   'map?               '(typed.clojure/Map typed.clojure/Any typed.clojure/Any)
   'nat-int?           'typed.clojure/AnyInteger
   'neg-int?           'typed.clojure/AnyInteger
   'number?            'typed.clojure/Num
   'pos-int?           'typed.clojure/AnyInteger
   'qualified-keyword? 'typed.clojure/Kw
   'set?               '(typed.clojure/Set typed.clojure/Any)
   'simple-keyword?    'typed.clojure/Kw
   'string?            'typed.clojure/Str
   'symbol?            'typed.clojure/Sym
   'uuid?              'typed.clojure/UUID
   'vector?            '(typed.clojure/Vec typed.clojure/Any)})

#?(:cljs
   (def ^:private cljs-core-preds
     "Core predicate symbol -> the fn object, for a host with no runtime
      `resolve`. A symbol absent here widens to typed.clojure/Any, exactly as
      an unresolvable var does elsewhere."
     {'any?               any?
      'boolean?           boolean?
      'double?            double?
      'int?               int?
      'integer?           integer?
      'keyword?           keyword?
      'map?               map?
      'nat-int?           nat-int?
      'neg-int?           neg-int?
      'number?            number?
      'pos-int?           pos-int?
      'qualified-keyword? qualified-keyword?
      'set?               set?
      'simple-keyword?    simple-keyword?
      'string?            string?
      'symbol?            symbol?
      'uuid?              uuid?
      'vector?            vector?}))

(def ^:private fn->type
  "Same table keyed by the clojure.core predicate fn objects - the shape the
   ADT registry holds after its defadt namespace is loaded.
   A predicate this host does not provide is SKIPPED, never named: naming one
   directly fails the whole namespace to load on a host that lacks it."
  (into {}
        (keep (fn [[s ty]]
                (when-let [f #?(:cljs (get cljs-core-preds s)
                                :default (when-let [v (resolve (symbol "clojure.core" (name s)))]
                                           (deref v)))]
                  [f ty])))
        sym->type))

(defn pred-type
  "Typed Clojure syntax for one variant-schema predicate: accepts the fn
   object or its symbol. Unknown predicates widen to typed.clojure/Any."
  [pred]
  (or (when (symbol? pred) (get sym->type pred))
      (get fn->type pred)
      'typed.clojure/Any))

(defn variant-hmap
  "HMap syntax for `variant-kw` of the registered ADT `type-kw`:
   :adt/type + :adt/variant Val keys plus one entry per schema field."
  [type-kw variant-kw]
  (let [schema (get-in (adt/registered-types) [type-kw :schemas variant-kw])]
    (list 'typed.clojure/HMap :mandatory
          (into {:adt/type    (list 'typed.clojure/Val type-kw)
                 :adt/variant (list 'typed.clojure/Val variant-kw)}
                (map (fn [[field pred]] [field (pred-type pred)]))
                schema))))

(defn adt-union
  "Per-variant HMap union syntax for the registered ADT `type-kw`, variants
   in sorted order. Throws ex-info when the type is not registered."
  [type-kw]
  (let [variants (adt/type-variants type-kw)]
    (when-not (seq variants)
      (throw (ex-info (str "adt-union: ADT type not registered: " type-kw)
                      {:type type-kw})))
    (let [hmaps (mapv #(variant-hmap type-kw %) (sort variants))]
      (if (= 1 (count hmaps))
        (first hmaps)
        (cons 'typed.clojure/U hmaps)))))
