(ns ^:typed.clojure hive-dsl.typed.adt
  "Typed Clojure surface for hive-dsl.adt: trusted t/ann contracts for the
   public vars, shared aliases, and defadt-alias - t/defalias of a defadt
   type's per-variant HMap union derived from its variant schemas.
   Requires typed.clojure: load only in checked contexts (a :typed alias
   JVM, or a verifier's :ann-nses seam). Core hive-dsl namespaces never
   require this one."
  (:require [typed.clojure :as t]
            [hive-dsl.adt]
            [hive-dsl.typed.emit :as emit]))

(t/defalias AdtValue
  (t/HMap :mandatory {:adt/type t/Kw :adt/variant t/Kw}))

(t/defalias TypeMeta
  (t/HMap :mandatory {:variants (t/Set t/Kw)}
          :optional {:type t/Kw :schemas (t/Map t/Kw (t/Map t/Kw t/Any))}))

;; Contracts are trusted (hive-dsl.adt is not itself checked): they state
;; documented behaviour for well-formed ADT values.
(t/ann hive-dsl.adt/register-type! [t/Kw TypeMeta :-> (t/Map t/Kw TypeMeta)])
(t/ann hive-dsl.adt/registered-types [:-> (t/Map t/Kw TypeMeta)])
(t/ann hive-dsl.adt/type-registered? [t/Kw :-> t/Bool])
(t/ann hive-dsl.adt/type-variants [t/Kw :-> (t/Nilable (t/Set t/Kw))])
(t/ann hive-dsl.adt/adt-type [t/Any :-> (t/Nilable t/Kw)])
(t/ann hive-dsl.adt/adt-variant [t/Any :-> (t/Nilable t/Kw)])
(t/ann hive-dsl.adt/adt? [t/Any :-> t/Bool])
(t/ann hive-dsl.adt/adt-valid? [t/Any :-> t/Bool])
(t/ann hive-dsl.adt/adt-data [t/Any :-> (t/Nilable (t/Map t/Any t/Any))])
(t/ann hive-dsl.adt/make-variant
       (t/IFn [t/Kw t/Kw :-> AdtValue]
              [t/Kw t/Kw (t/Nilable (t/Map t/Any t/Any)) :-> AdtValue]))
(t/ann hive-dsl.adt/coerce-keyword [t/Kw t/Kw :-> (t/Nilable AdtValue)])
(t/ann hive-dsl.adt/serialize [t/Any :-> (t/Nilable (t/Map t/Any t/Any))])
(t/ann hive-dsl.adt/deserialize [t/Any :-> (t/Nilable AdtValue)])
(t/ann hive-dsl.adt/validate [TypeMeta t/Any :-> AdtValue])

(defmacro defadt-alias
  "t/defalias `alias-name` as the per-variant HMap union of the ADT
   registered under `type-kw`. The type's defadt namespace must be loaded.

   (defadt-alias EventTypeV :EventType)"
  [alias-name type-kw]
  `(t/defalias ~alias-name ~(emit/adt-union type-kw)))
