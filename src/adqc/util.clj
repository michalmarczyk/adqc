(ns adqc.util
  (:use [clojure.contrib.string :as str :only []]))

(defn extract-info [extractors item]
  (reduce (fn [m [k e]]
            (assoc m k (e item)))
          {}
          extractors))

(defn keyword->getter [k]
  (symbol (apply str "get" (map str/capitalize (.split #"-" (name k))))))

(defn make-extractor [arg spec]
  (cond (keyword? spec)
        `(fn [~arg]
           (. ~arg ~(keyword->getter spec)))
        (symbol? spec)
        `(fn [~arg]
           (~spec ~arg))
        (and (seq? spec) (#{'-> '->>} (first spec)))
        `(fn [~arg]
           (~(first spec)
            ~(if (keyword? (second spec))
               `(. ~arg ~(keyword->getter (second spec)))
               `(~(first spec) ~arg ~(second spec)))
            ~@(nnext spec)))))

(defmacro defextractors [name hint & exts]
  (let [arg (with-meta (gensym "item") {:tag hint})]
    `(def ~name
          ~(into {} (map (fn [[k e-spec]]
                           [k (make-extractor arg e-spec)])
                         (partition 2 exts))))))

(defn select-keys-with
  "Returns a map containing only those entries in map whose key satisfies pred."
  [map pred]
  (select-keys map (filter pred (keys map))))

(defn to-java-name [clj-name & {:keys [prefix suffix]}]
  (let [prefix (when prefix (str prefix \-))
        suffix (when suffix (str \- suffix))]
    (->> (str prefix clj-name suffix)
         (.split #"-")
         (mapcat (juxt #(Character/toUpperCase
                         (.charAt ^String % 0))
                       #(subs ^String % 1)))
         (apply str)
         symbol)))

(defn to-factory-name [clj-name & {:keys [prefix suffix]}]
  (let [prefix (when prefix (str prefix \-))
        suffix (when suffix (str \- suffix))]
    (symbol (str "make-" prefix clj-name suffix))))
