(ns hive-dsl.adt.schema
  "Malli projection for hive-dsl ADTs — the hive-dsl leg of the malli macro layer.

   `adt->malli` derives ONE malli :multi schema (dispatching on :adt/variant) from
   an ADT's type-meta, turning its per-variant {field pred-fn} declarations into a
   single malli value-object. `validate-malli` validates an ADT value against it —
   the same throw-on-invalid contract as hive-dsl.adt/validate, but backed by
   malli, so an ADT gains malli explain / generators / hive-schemas test-synthesis
   for free from its ONE declaration.

   hive-dsl sits BELOW hive-spi and stays malli-free at base: malli.core is
   resolved LAZILY via requiring-resolve. When malli is absent, validate-malli
   falls back to hive-dsl.adt/validate (the hand-rolled path), so nothing breaks
   on a bare classpath."
  (:require [hive-dsl.adt :as adt]))

(defn- resolve-malli
  "{:validate .. :explain ..} from malli.core, or nil when malli is absent."
  []
  #?(:clj  (try {:validate (requiring-resolve 'malli.core/validate)
                 :explain  (requiring-resolve 'malli.core/explain)}
                (catch Throwable _ nil))
     :cljs nil))

(defn variant->map-schema
  "A malli :map schema for one variant: the :adt/type and :adt/variant tag keys
   pinned to their values, plus each declared field required and typed [:fn pred]."
  [type-kw variant field->pred]
  (into [:map
         [:adt/type [:= type-kw]]
         [:adt/variant [:= variant]]]
        (map (fn [[field pred]] [field [:fn pred]]))
        field->pred))

(defn adt->malli
  "ONE malli :multi schema (dispatch :adt/variant) for the whole ADT `type-meta`
   ({:type :variants :schemas}). Enum variants (no schema) validate the tag keys
   only; data variants add their field->pred map as [:fn ...] entries. This is the
   ADT as a single malli value-object — feed it to malli generators / explain / the
   hive-schemas test-synthesis levers."
  [{:keys [type variants schemas]}]
  (into [:multi {:dispatch :adt/variant}]
        (map (fn [variant]
               [variant (variant->map-schema type variant (get schemas variant {}))]))
        variants))

(defn validate-malli
  "Validate an ADT value `x` against its `type-meta`'s derived malli schema.
   Returns x when valid; throws ex-info {:error :schema/invalid ...} otherwise.
   When malli is absent, falls back to hive-dsl.adt/validate (the hand-rolled
   path), so the contract holds on any classpath."
  [type-meta x]
  (if-let [{:keys [validate explain]} (resolve-malli)]
    (let [schema (adt->malli type-meta)]
      (if (validate schema x)
        x
        (throw (ex-info "ADT malli validation failed"
                        {:error       :schema/invalid
                         :type        (:type type-meta)
                         :explanation (explain schema x)}))))
    (adt/validate type-meta x)))
