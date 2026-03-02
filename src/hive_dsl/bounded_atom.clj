(ns hive-dsl.bounded-atom
  "Bounded atom primitive — size-limited, TTL-aware map atoms.

   Wraps a standard Clojure atom holding a map of entries, enforcing:
   - Maximum entry count (eviction on overflow)
   - TTL-based expiry (entries older than ttl-ms evicted on access)
   - Eviction policies: :lru (least recently accessed), :fifo (oldest created), :ttl (expired only)

   Each entry is stored as: key -> {:data X :created-at T :last-accessed T}

   Designed to replace unbounded defonce atoms that grow without limit,
   causing GC pressure and eventual OOM/death-spiral.

   Usage:
     (def my-store (bounded-atom {:max-entries 1000
                                  :ttl-ms 300000
                                  :eviction-policy :lru}))

     (bput! my-store :key-1 {:task \"X\"})
     (bget my-store :key-1)

   Global sweep:
     (register-sweepable! my-store :my-store)
     (sweep-all!)  ;; evicts expired entries from all registered atoms

   Auto-sweep scheduler:
     (start-sweep-scheduler! {:interval-ms 60000})  ;; sweep every 60s
     (sweep-scheduler-status)                        ;; check scheduler state
     (stop-sweep-scheduler!)                         ;; stop background sweep

   Observability hooks:
     :on-evict — (fn [{:keys [name evicted-count reason entries]}]) called after evictions
     :on-sweep — (fn [{:keys [name evicted-count remaining duration-ms]}]) called after sweep"
  (:import [java.util.concurrent ScheduledThreadPoolExecutor ThreadFactory TimeUnit]))

;; =============================================================================
;; Sweepable Registry — global registry for lifecycle sweep (gc-fix-4)
;; =============================================================================

(defonce ^:private sweepable-registry (atom {}))

(defn register-sweepable!
  "Register a bounded atom in the global sweep registry.
   name-kw is a keyword identifier for diagnostics."
  [batom name-kw]
  (swap! sweepable-registry assoc name-kw batom)
  nil)

(defn unregister-sweepable!
  "Remove a bounded atom from the sweep registry."
  [name-kw]
  (swap! sweepable-registry dissoc name-kw)
  nil)

(defn registered-sweepables
  "Return the map of all registered sweepable bounded atoms."
  []
  @sweepable-registry)

;; =============================================================================
;; Entry helpers
;; =============================================================================

(defn- now-ms []
  (System/currentTimeMillis))

(defn- wrap-entry
  "Wrap a data value into a bounded-atom entry with timestamps."
  [data]
  (let [t (now-ms)]
    {:data data
     :created-at t
     :last-accessed t}))

(defn- touch-entry
  "Update the :last-accessed timestamp of an entry."
  [entry]
  (assoc entry :last-accessed (now-ms)))

(defn- expired?
  "True if entry has exceeded the TTL."
  [entry ttl-ms]
  (when ttl-ms
    (> (- (now-ms) (:created-at entry)) ttl-ms)))

;; =============================================================================
;; Eviction logic
;; =============================================================================

(defn- evict-expired
  "Remove all entries that have exceeded TTL. Returns [new-map evicted-count]."
  [m ttl-ms]
  (if-not ttl-ms
    [m 0]
    (let [now (now-ms)
          expired-keys (into []
                             (comp (filter (fn [[_ entry]]
                                            (> (- now (:created-at entry)) ttl-ms)))
                                   (map key))
                             m)
          n (count expired-keys)]
      (if (zero? n)
        [m 0]
        [(apply dissoc m expired-keys) n]))))

(defn- select-eviction-victims
  "Select n keys to evict according to policy. Returns vector of keys."
  [m n policy]
  (case policy
    :lru (->> m
              (sort-by (fn [[_ entry]] (:last-accessed entry)))
              (take n)
              (mapv key))
    :fifo (->> m
               (sort-by (fn [[_ entry]] (:created-at entry)))
               (take n)
               (mapv key))
    ;; :ttl — no capacity-based eviction, only TTL (handled separately)
    []))

(defn- enforce-capacity
  "Evict entries to bring map within max-entries. Returns [new-map evicted-count reason].
   TTL eviction runs first, then capacity-based if still over limit."
  [m {:keys [max-entries ttl-ms eviction-policy]}]
  (let [[m1 ttl-evicted] (evict-expired m ttl-ms)
        over-count (- (count m1) max-entries)]
    (if (<= over-count 0)
      [m1 ttl-evicted (when (pos? ttl-evicted) :ttl)]
      (if (= eviction-policy :ttl)
        ;; :ttl policy does not capacity-evict — only TTL. Refuse if still over.
        [m1 ttl-evicted (when (pos? ttl-evicted) :ttl)]
        (let [victims (select-eviction-victims m1 over-count eviction-policy)
              m2 (apply dissoc m1 victims)
              cap-evicted (count victims)
              total (+ ttl-evicted cap-evicted)]
          [m2 total (if (pos? ttl-evicted)
                      :ttl+capacity
                      :capacity)])))))

;; =============================================================================
;; Callback helper
;; =============================================================================

(defn- fire-on-evict!
  "Fire the :on-evict callback if present and evictions occurred.
   Safe — catches and ignores callback exceptions to never break core ops."
  [batom evicted-count reason entries]
  (when (and (pos? evicted-count)
             (get-in batom [:opts :on-evict]))
    (try
      ((get-in batom [:opts :on-evict])
       {:name (:name batom)
        :evicted-count evicted-count
        :reason reason
        :entries entries})
      (catch Exception _
        ;; Never let observability callbacks break core operations
        nil))))

;; =============================================================================
;; Bounded Atom constructor
;; =============================================================================

(defn bounded-atom
  "Create a bounded atom — a map-valued atom with size and TTL limits.

   Options:
     :max-entries     — maximum number of entries (required)
     :ttl-ms          — time-to-live in milliseconds (nil = no TTL)
     :eviction-policy — :lru, :fifo, or :ttl (default :lru)
     :on-evict        — optional callback (fn [{:keys [name evicted-count reason entries]}])
                         called after evictions during bput!/bounded-swap!/bounded-reset!
     :on-sweep        — optional callback (fn [{:keys [name evicted-count remaining duration-ms]}])
                         called after sweep! completes
     :name            — optional string name for this atom (used in callbacks and metrics)

   Returns a map:
     {:atom     (atom {})         ;; the underlying atom
      :opts     {:max-entries N ...}  ;; config for enforcement
      :name     \"my-store\"}     ;; optional name for observability"
  [{:keys [max-entries ttl-ms eviction-policy on-evict on-sweep name]
    :or {eviction-policy :lru}
    :as opts}]
  (assert (pos-int? max-entries) "max-entries must be a positive integer")
  (assert (#{:lru :fifo :ttl} eviction-policy)
          (str "eviction-policy must be :lru, :fifo, or :ttl — got " eviction-policy))
  (assert (or (nil? ttl-ms) (pos-int? ttl-ms))
          "ttl-ms must be a positive integer or nil")
  {:atom (atom {})
   :name name
   :opts {:max-entries max-entries
          :ttl-ms ttl-ms
          :eviction-policy eviction-policy
          :on-evict on-evict
          :on-sweep on-sweep}})

;; =============================================================================
;; Core operations
;; =============================================================================

(defn bget
  "Get entry data by key. Touches the entry (updates :last-accessed).
   Returns the data value or nil if not found / expired."
  [batom k]
  (let [{:keys [atom opts]} batom
        {:keys [ttl-ms]} opts
        entry (get @atom k)]
    (when entry
      (if (expired? entry ttl-ms)
        (do (swap! atom dissoc k) nil)
        (do (swap! atom update k touch-entry)
            (:data entry))))))

(defn bput!
  "Put a single entry. Enforces capacity, evicting per policy when full.
   Returns {:result new-val :evicted-count N :eviction-reason reason}.
   Fires :on-evict callback if evictions occurred."
  [batom k data]
  (let [{:keys [atom opts]} batom
        entry (wrap-entry data)
        result (volatile! nil)
        new-state (swap! atom
                         (fn [m]
                           (let [m' (assoc m k entry)
                                 [m'' evicted reason] (enforce-capacity m' opts)]
                             (vreset! result {:evicted-count evicted
                                              :eviction-reason reason})
                             m'')))
        res @result]
    (fire-on-evict! batom (:evicted-count res) (:eviction-reason res) (count new-state))
    (assoc res :result new-state)))

(defn bounded-swap!
  "Like swap! but enforces max-entries, evicting per policy when full.
   f receives the current entry-map and must return a new entry-map.
   Raw entries are maps of key -> {:data X :created-at T :last-accessed T}.

   Returns {:result new-val :evicted-count N :eviction-reason reason}.
   Fires :on-evict callback if evictions occurred."
  [batom f & args]
  (let [{:keys [atom opts]} batom
        result (volatile! nil)
        new-state (swap! atom
                         (fn [m]
                           (let [m' (apply f m args)
                                 [m'' evicted reason] (enforce-capacity m' opts)]
                             (vreset! result {:evicted-count evicted
                                              :eviction-reason reason})
                             m'')))
        res @result]
    (fire-on-evict! batom (:evicted-count res) (:eviction-reason res) (count new-state))
    (assoc res :result new-state)))

(defn bounded-reset!
  "Bulk replacement with size check. new-map must be a map of
   key -> {:data X :created-at T :last-accessed T}.
   Entries exceeding max-entries are evicted per policy.

   Returns {:result new-val :evicted-count N :eviction-reason reason}.
   Fires :on-evict callback if evictions occurred."
  [batom new-map]
  (let [{:keys [atom opts]} batom
        [enforced evicted reason] (enforce-capacity new-map opts)]
    (reset! atom enforced)
    (fire-on-evict! batom evicted reason (count enforced))
    {:result enforced
     :evicted-count evicted
     :eviction-reason reason}))

(defn bcount
  "Return the number of entries in the bounded atom."
  [batom]
  (count @(:atom batom)))

(defn bkeys
  "Return the keys of the bounded atom."
  [batom]
  (keys @(:atom batom)))

(defn bclear!
  "Clear all entries from the bounded atom."
  [batom]
  (reset! (:atom batom) {})
  nil)

;; =============================================================================
;; Sweep — bulk TTL eviction across all registered atoms
;; =============================================================================

(defn sweep!
  "Evict expired entries from a single bounded atom.
   Returns {:name name-kw :evicted-count N :remaining N :duration-ms N}.
   Fires :on-sweep callback if configured."
  [batom name-kw]
  (let [{:keys [atom opts]} batom
        {:keys [ttl-ms on-sweep]} opts
        start (System/nanoTime)]
    (if-not ttl-ms
      (let [result {:name name-kw :evicted-count 0 :remaining (count @atom)
                    :duration-ms 0.0}]
        (when on-sweep
          (try (on-sweep result) (catch Exception _ nil)))
        result)
      (let [old-count (count @atom)
            _ (swap! atom (fn [m] (first (evict-expired m ttl-ms))))
            new-count (count @atom)
            duration-ms (/ (- (System/nanoTime) start) 1e6)
            result {:name name-kw
                    :evicted-count (- old-count new-count)
                    :remaining new-count
                    :duration-ms duration-ms}]
        (when on-sweep
          (try (on-sweep result) (catch Exception _ nil)))
        result))))

(defn sweep-all!
  "Iterate all registered bounded atoms, evict expired entries.
   Returns vector of per-atom stats."
  []
  (let [registry @sweepable-registry]
    (mapv (fn [[name-kw batom]]
            (sweep! batom name-kw))
          registry)))

;; =============================================================================
;; Auto-sweep scheduler — periodic background sweep (D3)
;; =============================================================================

(defonce ^:private *sweep-scheduler (atom nil))

(defn sweep-scheduler-status
  "Return {:running? bool :interval-ms N :last-sweep-at inst :sweep-count N}"
  []
  (if-let [state @*sweep-scheduler]
    {:running?      true
     :interval-ms   (:interval-ms state)
     :last-sweep-at (:last-sweep-at state)
     :sweep-count   (:sweep-count state)}
    {:running?      false
     :interval-ms   nil
     :last-sweep-at nil
     :sweep-count   0}))

(defn stop-sweep-scheduler!
  "Stop periodic sweep. Idempotent."
  []
  (when-let [state @*sweep-scheduler]
    (try
      (.shutdownNow ^ScheduledThreadPoolExecutor (:executor state))
      (catch Exception _ nil))
    (reset! *sweep-scheduler nil))
  nil)

(defn start-sweep-scheduler!
  "Start periodic sweep of all registered bounded atoms.
   Returns a stop function. Idempotent — second call is no-op.

   Options:
     :interval-ms       — sweep interval in milliseconds (default: 300000 = 5 min)
     :on-sweep-complete — optional callback (fn [results]) called after each sweep
                           with the vector of per-atom sweep stats"
  ([] (start-sweep-scheduler! {}))
  ([{:keys [interval-ms on-sweep-complete]
     :or {interval-ms 300000}}]
   (if @*sweep-scheduler
     ;; Already running — return existing stop-fn (idempotent)
     stop-sweep-scheduler!
     (let [sweep-state (atom {:sweep-count   0
                              :last-sweep-at nil})
           tf (reify ThreadFactory
                (newThread [_ r]
                  (doto (Thread. r "hive-bounded-atom-sweep")
                    (.setDaemon true))))
           executor (ScheduledThreadPoolExecutor. 1 tf)
           task (fn []
                  (try
                    (let [results (sweep-all!)
                          now     (java.time.Instant/now)]
                      (swap! sweep-state
                             (fn [s]
                               (-> s
                                   (update :sweep-count inc)
                                   (assoc :last-sweep-at now))))
                      (when on-sweep-complete
                        (try
                          (on-sweep-complete results)
                          (catch Exception _ nil))))
                    (catch Exception _ nil)))]
       (.scheduleAtFixedRate executor
                             ^Runnable task
                             (long interval-ms)
                             (long interval-ms)
                             TimeUnit/MILLISECONDS)
       (reset! *sweep-scheduler
               {:executor      executor
                :interval-ms   interval-ms
                :sweep-count   0
                :last-sweep-at nil
                :sweep-state   sweep-state})
       ;; Sync sweep-state changes back to the scheduler atom for status reads
       (add-watch sweep-state ::sync-to-scheduler
                  (fn [_ _ _ new-state]
                    (swap! *sweep-scheduler
                           (fn [s]
                             (when s
                               (merge s (select-keys new-state [:sweep-count :last-sweep-at])))))))
       stop-sweep-scheduler!))))
