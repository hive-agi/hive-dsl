(ns hive-dsl.conversation-test
  "Unit tests for hive-dsl.conversation — ConversationMessage and
   DeliveryStatus ADTs: construction, predicates, and adt-case matching."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt :as adt]
            [hive-dsl.conversation :as conv]
            [hive-dsl.result.taxonomy :as tax]))

;; =============================================================================
;; ConversationMessage — Construction
;; =============================================================================

(deftest conversation-message-tell
  (testing "Tell variant carries all fields"
    (let [m (conv/conversation-message
             :conv/tell {:from "agent-a" :to "agent-b"
                         :content "hello" :correlation-id "c1"})]
      (is (= :ConversationMessage (:adt/type m)))
      (is (= :conv/tell (:adt/variant m)))
      (is (= "agent-a" (:from m)))
      (is (= "agent-b" (:to m)))
      (is (= "hello" (:content m)))
      (is (= "c1" (:correlation-id m))))))

(deftest conversation-message-ask-ling
  (testing "Ask-ling variant carries question and options"
    (let [m (conv/conversation-message
             :conv/ask-ling {:from "a" :to "b" :question "which?"
                             :options ["x" "y"] :correlation-id "c2"})]
      (is (= :conv/ask-ling (:adt/variant m)))
      (is (= "which?" (:question m)))
      (is (= ["x" "y"] (:options m))))))

(deftest conversation-message-respond
  (testing "Respond variant carries response and ask-id"
    (let [m (conv/conversation-message
             :conv/respond {:from "b" :to "a" :response "yes" :ask-id "c2"})]
      (is (= :conv/respond (:adt/variant m)))
      (is (= "yes" (:response m)))
      (is (= "c2" (:ask-id m))))))

(deftest conversation-message-broadcast
  (testing "Broadcast variant carries scope"
    (let [m (conv/conversation-message
             :conv/broadcast {:from "a" :content "hey all" :scope :team})]
      (is (= :conv/broadcast (:adt/variant m)))
      (is (= :team (:scope m))))))

(deftest conversation-message-rejects-unknown-variant
  (testing "Unknown variant throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown ConversationMessage variant"
                          (conv/conversation-message :conv/whisper {})))))

;; =============================================================================
;; ConversationMessage — Predicate
;; =============================================================================

(deftest conversation-message-predicate
  (testing "Predicate recognizes ConversationMessage values"
    (is (conv/conversation-message?
         (conv/conversation-message
          :conv/tell {:from "a" :to "b" :content "x" :correlation-id "c"}))))

  (testing "Predicate rejects non-ConversationMessage values"
    (is (not (conv/conversation-message? {})))
    (is (not (conv/conversation-message? nil)))))

;; =============================================================================
;; ConversationMessage — adt-case Exhaustive Matching
;; =============================================================================

(deftest conversation-message-adt-case
  (testing "Dispatches to correct branch for all variants"
    (let [tell (conv/conversation-message
                :conv/tell {:from "a" :to "b" :content "hi" :correlation-id "c1"})
          ask  (conv/conversation-message
                :conv/ask-ling {:from "a" :to "b" :question "q"
                                :options ["o"] :correlation-id "c2"})
          resp (conv/conversation-message
                :conv/respond {:from "b" :to "a" :response "r" :ask-id "c2"})
          bcast (conv/conversation-message
                 :conv/broadcast {:from "a" :content "all" :scope :global})]

      (is (= :tell
             (adt/adt-case conv/ConversationMessage tell
                           :conv/tell      :tell
                           :conv/ask-ling  :ask
                           :conv/respond   :respond
                           :conv/broadcast :broadcast)))

      (is (= :ask
             (adt/adt-case conv/ConversationMessage ask
                           :conv/tell      :tell
                           :conv/ask-ling  :ask
                           :conv/respond   :respond
                           :conv/broadcast :broadcast)))

      (is (= :respond
             (adt/adt-case conv/ConversationMessage resp
                           :conv/tell      :tell
                           :conv/ask-ling  :ask
                           :conv/respond   :respond
                           :conv/broadcast :broadcast)))

      (is (= :broadcast
             (adt/adt-case conv/ConversationMessage bcast
                           :conv/tell      :tell
                           :conv/ask-ling  :ask
                           :conv/respond   :respond
                           :conv/broadcast :broadcast))))))

(deftest conversation-message-adt-case-with-data-access
  (testing "Branch body can access data fields"
    (let [m (conv/conversation-message
             :conv/tell {:from "agent-x" :to "agent-y"
                         :content "payload" :correlation-id "cid"})]
      (is (= "agent-x → agent-y: payload"
             (adt/adt-case conv/ConversationMessage m
                           :conv/tell      (str (:from m) " → " (:to m) ": " (:content m))
                           :conv/ask-ling  (:question m)
                           :conv/respond   (:response m)
                           :conv/broadcast (:content m)))))))

;; =============================================================================
;; DeliveryStatus — Construction
;; =============================================================================

(deftest delivery-status-construction
  (testing "All enum variants construct correctly"
    (doseq [v [:delivery/queued :delivery/piggybacked :delivery/nats-sent
               :delivery/mailbox :delivery/failed]]
      (let [ds (conv/delivery-status v)]
        (is (= :DeliveryStatus (:adt/type ds)))
        (is (= v (:adt/variant ds)))))))

(deftest delivery-status-rejects-unknown-variant
  (testing "Unknown variant throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown DeliveryStatus variant"
                          (conv/delivery-status :delivery/unknown)))))

;; =============================================================================
;; DeliveryStatus — Predicate
;; =============================================================================

(deftest delivery-status-predicate
  (testing "Predicate recognizes DeliveryStatus values"
    (is (conv/delivery-status? (conv/delivery-status :delivery/queued))))

  (testing "Predicate rejects non-DeliveryStatus values"
    (is (not (conv/delivery-status? {})))
    (is (not (conv/delivery-status? nil)))))

;; =============================================================================
;; DeliveryStatus — adt-case Exhaustive Matching
;; =============================================================================

(deftest delivery-status-adt-case
  (testing "Dispatches to correct branch for all enum variants"
    (doseq [[variant expected] [[:delivery/queued      "queued"]
                                [:delivery/piggybacked "piggybacked"]
                                [:delivery/nats-sent   "nats-sent"]
                                [:delivery/mailbox     "mailbox"]
                                [:delivery/failed      "failed"]]]
      (let [ds (conv/delivery-status variant)]
        (is (= expected
               (adt/adt-case conv/DeliveryStatus ds
                             :delivery/queued      "queued"
                             :delivery/piggybacked "piggybacked"
                             :delivery/nats-sent   "nats-sent"
                             :delivery/mailbox     "mailbox"
                             :delivery/failed      "failed")))))))

;; =============================================================================
;; Registry
;; =============================================================================

(deftest types-registered-in-global-registry
  (testing "Both types are globally registered"
    (is (adt/type-registered? :ConversationMessage))
    (is (adt/type-registered? :DeliveryStatus)))

  (testing "Variants accessible via registry"
    (is (= #{:conv/tell :conv/ask-ling :conv/respond :conv/broadcast}
           (adt/type-variants :ConversationMessage)))
    (is (= #{:delivery/queued :delivery/piggybacked :delivery/nats-sent
             :delivery/mailbox :delivery/failed}
           (adt/type-variants :DeliveryStatus)))))

;; =============================================================================
;; Error Taxonomy
;; =============================================================================

(deftest conversation-error-categories-registered
  (testing "All conversation error categories are registered"
    (doseq [cat [:conversation/undeliverable
                 :conversation/timeout
                 :conversation/invalid-target
                 :conversation/inbox-full
                 :conversation/invalid-message]]
      (is (tax/known-error? cat)))))

;; =============================================================================
;; Serialization Round-trip
;; =============================================================================

(deftest conversation-message-serialization-roundtrip
  (testing "Tell variant survives serialize/deserialize"
    (let [m (conv/conversation-message
             :conv/tell {:from "a" :to "b" :content "x" :correlation-id "c"})]
      (is (= m (adt/deserialize (adt/serialize m))))))

  (testing "Broadcast variant survives serialize/deserialize"
    (let [m (conv/conversation-message
             :conv/broadcast {:from "a" :content "y" :scope :all})]
      (is (= m (adt/deserialize (adt/serialize m)))))))

(deftest delivery-status-serialization-roundtrip
  (testing "Enum variants survive serialize/deserialize"
    (doseq [v [:delivery/queued :delivery/piggybacked :delivery/nats-sent
               :delivery/mailbox :delivery/failed]]
      (let [ds (conv/delivery-status v)]
        (is (= ds (adt/deserialize (adt/serialize ds))))))))
