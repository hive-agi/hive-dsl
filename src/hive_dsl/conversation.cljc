(ns hive-dsl.conversation
  "Conversation ADTs for inter-agent messaging.

   ConversationMessage — closed sum type for all message shapes exchanged
   between agents (tell, ask, respond, broadcast).

   DeliveryStatus — delivery outcome for a conversation message.

   SRP: Pure domain types, no I/O."
  (:require [hive-dsl.adt :refer [defadt]]
            [hive-dsl.result.taxonomy :as tax]))

;; =============================================================================
;; ADT Definitions
;; =============================================================================

(defadt ConversationMessage
  "Closed sum type for inter-agent conversation messages."
  [:conv/tell        {:from string? :to string? :content string? :correlation-id string?}]
  [:conv/ask-ling    {:from string? :to string? :question string? :options vector? :correlation-id string?}]
  [:conv/respond     {:from string? :to string? :response string? :ask-id string?}]
  [:conv/broadcast   {:from string? :content string? :scope keyword?}])

(defadt DeliveryStatus
  "Delivery outcome for a conversation message."
  :delivery/queued
  :delivery/piggybacked
  :delivery/nats-sent
  :delivery/mailbox
  :delivery/failed)

;; =============================================================================
;; Error Taxonomy
;; =============================================================================

(tax/register!
 #{:conversation/undeliverable
   :conversation/timeout
   :conversation/invalid-target
   :conversation/inbox-full
   :conversation/invalid-message})
