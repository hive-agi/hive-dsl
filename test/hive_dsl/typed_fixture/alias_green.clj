(ns ^:typed.clojure hive-dsl.typed-fixture.alias-green
  "GREEN fixture: defadt-alias derives the union type, adt-case refines it
   per branch. Requires typed.clojure - only loaded under :typed contexts,
   never by the cold test runner (no -test suffix)."
  (:require [typed.clojure :as t]
            [hive-dsl.adt :refer [defadt adt-case]]
            [hive-dsl.typed.adt :refer [defadt-alias]]))

(defadt AliasShape
  "Mixed data-carrying + enum variants."
  [:alias/circle {:radius number?}]
  [:alias/rect   {:w number? :h number?}]
  :alias/unit)

(defadt-alias AliasShapeV :AliasShape)

(t/ann area [AliasShapeV :-> t/Num])
(defn area [shape]
  (adt-case AliasShape shape
    :alias/circle (* 3.14 (:radius shape) (:radius shape))
    :alias/rect   (* (:w shape) (:h shape))
    :alias/unit   0))
