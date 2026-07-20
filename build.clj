(ns build
  "Shared hive library build: source jar + Clojars deploy, with a
   datahike-style version = 0.{minor}.{git-commit-count} so every commit
   yields a new immutable version.

   Reads project coordinates from ./version.edn in the consuming repo:
     {:lib     io.github.hive-agi/hive-knowledge
      :minor   1
      :license {:name \"EPL-2.0\" :url \"https://www.eclipse.org/legal/epl-2.0/\"}
      :scm-url \"https://github.com/hive-agi/hive-knowledge\"
      :src-dirs [\"src\" \"resources\"]}

   Tasks (invoke with `clojure -T:build <task>`):
     jar      build the source jar under target/
     install  jar + install to the local ~/.m2 (no network)
     deploy   jar + push to Clojars (needs CLOJARS_USERNAME / CLOJARS_PASSWORD)"
  (:require [clojure.tools.build.api :as b]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [deps-deploy.deps-deploy :as dd]))

(def ^:private cfg (edn/read-string (slurp "version.edn")))
(def lib (:lib cfg))
;; Version source of truth = the repo's existing VERSION file (same value the
;; GitHub release.yml tags as v{VERSION}), so the Clojars coord matches the
;; git-tag coord 1:1. Falls back to datahike-style 0.{minor}.{git-count} only
;; when no VERSION file exists. CI checkout needs fetch-depth:0 for the fallback.
(def version
  (let [f (io/file "VERSION")]
    (if (.exists f)
      (str/trim (slurp f))
      (format "0.%s.%s" (:minor cfg 0) (b/git-count-revs nil)))))
(def ^:private class-dir "target/classes")
(def ^:private src-dirs (:src-dirs cfg ["src"]))
(def ^:private jar-file (format "target/%s-%s.jar" (name lib) version))

(defn- basis [] (b/create-basis {:project "deps.edn"}))

(defn- write-pom []
  (b/write-pom
   {:class-dir class-dir
    :lib       lib
    :version   version
    :basis     (basis)
    ;; pom :src references source roots only (not resource dirs)
    :src-dirs  (vec (remove #{"resources"} src-dirs))
    :scm       {:url (:scm-url cfg)
                :tag (b/git-process {:git-args "rev-parse HEAD"})}
    :pom-data  [[:licenses
                 [:license
                  [:name (get-in cfg [:license :name] "EPL-2.0")]
                  [:url  (get-in cfg [:license :url]
                                 "https://www.eclipse.org/legal/epl-2.0/")]]]]}))

(defn clean [_] (b/delete {:path "target"}))

(defn jar
  "Build the source jar (pom + copied sources) under target/."
  [_]
  (clean nil)
  (write-pom)
  (b/copy-dir {:src-dirs src-dirs :target-dir class-dir})
  (b/jar {:class-dir class-dir :jar-file jar-file})
  (println "Built" (str lib) version "->" jar-file))

(defn install
  "Build + install to the local ~/.m2 repository (offline; for verification)."
  [_]
  (jar nil)
  (dd/deploy {:installer :local
              :artifact  jar-file
              :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
  (println "Installed" (str lib) version "to ~/.m2"))

(defn- already-published?
  "True if this exact lib+version jar is already on Clojars. Clojars is immutable,
   so re-deploying an existing version errors — this lets `deploy` no-op safely on
   every push to main (only VERSION bumps produce a new release)."
  []
  (let [[grp art] (str/split (str lib) #"/")
        url (format "https://repo.clojars.org/%s/%s/%s/%s-%s.jar"
                    (str/replace grp "." "/") art version art version)]
    (try
      (let [conn (doto ^java.net.HttpURLConnection (.openConnection (java.net.URL. url))
                   (.setRequestMethod "HEAD")
                   (.setConnectTimeout 10000)
                   (.setReadTimeout 10000))]
        (= 200 (.getResponseCode conn)))
      (catch Throwable _ false))))

(defn deploy
  "Build + deploy to Clojars. Requires CLOJARS_USERNAME and CLOJARS_PASSWORD
   (a deploy token, not your account password) in the environment. No-ops if the
   version is already published (idempotent — safe to run on every main push)."
  [_]
  (if (already-published?)
    (println "Skip:" (str lib) version "already on Clojars — bump VERSION to release.")
    (do
      (jar nil)
      (dd/deploy {:installer :remote
                  :artifact  jar-file
                  :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
      (println "Deployed" (str lib) version "to Clojars"))))

(defn- already-published-private?
  "True if this exact lib+version pom is already in the private Gitea registry.
   Authed HEAD — private-org reads require credentials."
  [url username token]
  (let [[grp art] (str/split (str lib) #"/")
        pom-url (format "%s/%s/%s/%s/%s-%s.pom"
                        url (str/replace grp "." "/") art version art version)
        auth (str "Basic " (.encodeToString (java.util.Base64/getEncoder)
                                            (.getBytes (str username ":" token))))]
    (try
      (let [conn (doto ^java.net.HttpURLConnection (.openConnection (java.net.URL. pom-url))
                   (.setRequestMethod "HEAD")
                   (.setRequestProperty "Authorization" auth)
                   (.setConnectTimeout 10000)
                   (.setReadTimeout 10000))]
        (= 200 (.getResponseCode conn)))
      (catch Throwable _ false))))

(defn deploy-private
  "Build + deploy to the private Gitea Maven registry (hive-agi org).
   Env: GITEA_MAVEN_TOKEN (required, non-blank), GITEA_MAVEN_USERNAME (default buddhilw),
   GITEA_MAVEN_URL (default https://gitea.hive-mcp.com/api/packages/hive-agi/maven).
   No-ops when this version already exists in the registry (idempotent)."
  [_]
  (let [env (fn [k fallback]
              (let [v (System/getenv k)]
                (if (str/blank? v) fallback v)))
        url (env "GITEA_MAVEN_URL" "https://gitea.hive-mcp.com/api/packages/hive-agi/maven")
        username (env "GITEA_MAVEN_USERNAME" "buddhilw")
        token (System/getenv "GITEA_MAVEN_TOKEN")]
    (when (str/blank? token)
      (throw (ex-info "GITEA_MAVEN_TOKEN is required (non-blank)" {:env "GITEA_MAVEN_TOKEN"})))
    (if (already-published-private? url username token)
      (println "Skip:" (str lib) version "already in private registry — bump VERSION to release.")
      (do
        (jar nil)
        (dd/deploy {:installer  :remote
                    :artifact   jar-file
                    :pom-file   (b/pom-path {:lib lib :class-dir class-dir})
                    :repository {"gitea" {:url url :username username :password token}}})
        (println "Deployed" (str lib) version "to" url)))))