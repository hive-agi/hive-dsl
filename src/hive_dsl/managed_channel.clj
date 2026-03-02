(ns hive-dsl.managed-channel
  "Lifecycle-managed channel patterns: go-loops, pub/sub, and fresh-channel helpers.

   Builds on `hive-dsl.lifecycle` to prevent the primary async leak pattern:
   go-loops consuming channels that never terminate.

   Provides:

   1. `managed-go-loop` -- go-loop tied to a channel with lifecycle cleanup.
      stop! closes the channel, go-loop reads nil and exits.

   2. `managed-pub-sub` -- lifecycle-managed pub/sub with topic routing.
      stop! closes all channels (pub + every sub).

   3. `with-go-loop` -- bracket macro for temporary go-loops with auto-cleanup.

   4. `fresh-channels!` -- create a map of named channels from specs,
      for the channel-per-lifecycle start pattern."
  (:require [clojure.core.async :as async]
            [hive-dsl.lifecycle :as lc]))

;; =============================================================================
;; ManagedGoLoop -- lifecycle-managed go-loop tied to a channel
;; =============================================================================

(defn- default-error-handler
  "Default error handler: prints to stderr."
  [e msg]
  (binding [*out* *err*]
    (println (str "ERROR in managed-go-loop handler: " (.getMessage ^Throwable e)
                  " | msg: " (pr-str msg)))))

(defrecord ManagedGoLoop [handler-fn opts state channel]
  ;; state:   atom of :stopped | :started
  ;; channel: atom of nil | core.async channel

  lc/Lifecycle
  (start! [this]
    (let [current @state]
      (when (= current :started)
        (throw (ex-info "ManagedGoLoop already started"
                        {:name (:name opts) :state current})))
      (let [buf-size   (or (:buf-size opts) 100)
            on-error   (or (:on-error opts) default-error-handler)
            ch         (async/chan buf-size)]
        (reset! channel ch)
        (reset! state :started)
        ;; Spawn the go-loop. It terminates when ch is closed (reads nil).
        (async/go-loop []
          (when-let [msg (async/<! ch)]
            (try
              (handler-fn msg)
              (catch Throwable t
                (try
                  (on-error t msg)
                  (catch Throwable _ignore))))
            (recur)))
        this)))

  (stop! [this]
    (when-not (= :stopped @state)
      (when-let [ch @channel]
        (async/close! ch))
      (reset! channel nil)
      (reset! state :stopped))
    this)

  lc/Channeled
  (put! [_this val]
    (when-not (= :started @state)
      (throw (ex-info "Cannot put to stopped go-loop"
                      {:name (:name opts) :state @state})))
    (async/>!! @channel val))

  (take! [_this]
    (when-not (= :started @state)
      (throw (ex-info "Cannot take from stopped go-loop"
                      {:name (:name opts) :state @state})))
    (async/<!! @channel)))

(defn managed-go-loop
  "Create a lifecycle-managed go-loop that consumes from a channel.
   Implements Lifecycle protocol. stop! closes the channel, which
   terminates the go-loop (it reads nil and exits).

   handler-fn: (fn [msg] ...) called for each message
   opts: {:buf-size 100 :name \"my-loop\" :on-error (fn [e msg] ...)}"
  [handler-fn opts]
  (->ManagedGoLoop handler-fn (or opts {}) (atom :stopped) (atom nil)))

;; =============================================================================
;; ManagedPubSub -- lifecycle-managed pub/sub pattern
;; =============================================================================

(defrecord ManagedPubSub [topic-fn opts state pub-channel pub subs]
  ;; state:       atom of :stopped | :started
  ;; pub-channel: atom of nil | core.async channel (the input channel)
  ;; pub:         atom of nil | core.async pub
  ;; subs:        atom of {} -- topic -> sub-channel

  lc/Lifecycle
  (start! [this]
    (let [current @state]
      (when (= current :started)
        (throw (ex-info "ManagedPubSub already started"
                        {:name (:name opts) :state current})))
      (let [buf-size (or (:buf-size opts) 100)
            ch       (async/chan buf-size)
            p        (async/pub ch topic-fn)]
        (reset! pub-channel ch)
        (reset! pub p)
        (reset! subs {})
        (reset! state :started)
        this)))

  (stop! [this]
    (when-not (= :stopped @state)
      ;; Unsub + close all subscriber channels
      (let [p @pub]
        (doseq [[topic sub-ch] @subs]
          (when p (async/unsub p topic sub-ch))
          (async/close! sub-ch))
        (reset! subs {}))
      ;; Close the pub input channel
      (when-let [ch @pub-channel]
        (async/close! ch))
      (reset! pub-channel nil)
      (reset! pub nil)
      (reset! state :stopped))
    this))

(defn managed-pub-sub
  "Lifecycle-managed pub/sub. publish! sends to topic, subscribe! returns
   a channel for a topic. stop! closes all channels.
   opts: {:buf-size 100 :name \"events\"}"
  [topic-fn opts]
  (->ManagedPubSub topic-fn (or opts {}) (atom :stopped) (atom nil) (atom nil) (atom {})))

(defn publish!
  "Publish a message to the pub/sub. The message is put on the input
   channel; the pub routes it to matching subscribers via topic-fn."
  [managed-ps msg]
  (when-not (= :started @(:state managed-ps))
    (throw (ex-info "Cannot publish to stopped pub/sub"
                    {:name (get-in managed-ps [:opts :name])
                     :state @(:state managed-ps)})))
  (async/>!! @(:pub-channel managed-ps) msg))

(defn subscribe!
  "Subscribe to a topic. Returns a core.async channel that receives
   messages matching the topic. The channel is tracked for lifecycle cleanup."
  ([managed-ps topic] (subscribe! managed-ps topic 100))
  ([managed-ps topic buf-size]
   (when-not (= :started @(:state managed-ps))
     (throw (ex-info "Cannot subscribe to stopped pub/sub"
                     {:name (get-in managed-ps [:opts :name])
                      :state @(:state managed-ps)
                      :topic topic})))
   (let [sub-ch (async/chan buf-size)]
     (async/sub @(:pub managed-ps) topic sub-ch)
     (swap! (:subs managed-ps) assoc topic sub-ch)
     sub-ch)))

;; =============================================================================
;; with-go-loop -- bracket macro for temporary go-loops
;; =============================================================================

(defmacro with-go-loop
  "Bracket pattern for a temporary go-loop.

   Creates a managed-go-loop, starts it, runs body, then stops it in finally.
   The loop binding is the started ManagedGoLoop -- use (lc/put! loop-sym msg)
   to send messages.

   Usage:
     (with-go-loop [my-loop handler-fn {:buf-size 10}]
       (lc/put! my-loop {:type :event :data 42})
       ...)
   ;; auto-stopped on scope exit"
  [[sym handler-fn opts] & body]
  `(let [raw# (managed-go-loop ~handler-fn ~opts)
         ~sym (lc/start! raw#)]
     (try
       (do ~@body)
       (finally
         (try
           (lc/stop! ~sym)
           (catch Throwable t#
             (binding [*out* *err*]
               (println "WARN: stop! failed during with-go-loop cleanup:"
                        (.getMessage t#)))))))))

;; =============================================================================
;; fresh-channels! -- channel-per-lifecycle start pattern
;; =============================================================================

(defn fresh-channels!
  "Create fresh channels for a lifecycle start. Returns map of name->chan.
   On stop, call (close-channels! result) to close all channels.

   channel-specs: {:tx-chan {:buf 100} :ctrl-chan {:buf 1}}

   Each spec supports:
     :buf  -- buffer size (required, positive integer)"
  [channel-specs]
  (reduce-kv
    (fn [m ch-name spec]
      (let [buf (or (:buf spec) 100)]
        (assoc m ch-name (async/chan buf))))
    {}
    channel-specs))

(defn close-channels!
  "Close all channels in a channel map (as returned by fresh-channels!).
   Idempotent -- safe to call on already-closed channels."
  [channel-map]
  (doseq [[_name ch] channel-map]
    (when ch
      (async/close! ch))))
