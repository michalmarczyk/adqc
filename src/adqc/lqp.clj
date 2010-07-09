(ns adqc.lqp
  (:use [adqc.sql :only [parse-sql]]
        [clojure
         [zip :as zip :only []]
         [walk :as walk :only []]
         [set :as set :only []]
         [string :as str :only []]]))

;;; TODO: use IllegalStateException for ambiguous column names etc.

;;; catalog should be queried for schema information, cardinalities
;;; (if available...?) and the like, connection-info is for whatever
;;; is needed to reach the given data source
(defprotocol PPhysicalSource
  (get-relation-list [self])
  (get-relation-head [self rel]))

(defrecord PhysicalSource [node-catalog connection-info])

;;; every operator returning a set of tuples will need to implement this
(defprotocol PRelation
  (get-head [self] "Returns a seqable of column names.")
  (has-column [self col] "Returns self when col is recognised, nil otherwise."))

(defrecord PhysicalRelation
  [name physical-source]
  PRelation
  (get-head [self] (get-relation-head physical-source name))
  (has-column [self col]
    (let [h (get-head self)]
      (if (some #{col} h) self))))

;;; TODO: should I throw 'ambiguity exception' here or at the call site?
(defn find-column-source [col srcs]
  (let [candidates (filter #(has-column % col) srcs)]
    (case (count candidates)
      0 (throw (IllegalStateException. (str "Could not locate column: " col)))
      1 (first candidates)
      (throw (IllegalStateException. (str "Ambiguous column name: " col))))))

#_
(defn )

#_
(defn preprocess-query
  "Takes a catalogue and a freshly parsed query AST and fills in
  the blanks (attribute sources &c.)."
  [catalog query]
  (let [sl (:select-list query)
        fl (:from-list query)]
    ;; how about grouping a bunch of assertions here
    (if (some is-star? sl) ; what about COUNT(*) etc.
      (if (< 1 (count sl))
        (throw (IllegalStateException. "Malformed select list"))
        (let [h (mapcat get-head fl)]
          (if (< (count (set h)) (count h))
            (throw (IllegalStateException. "Ambiguous column names in the head"))
            (map make-column )))) ; <- TODO: workin' here!
      ;; TODO: take into account AS annotations on cols and rels,
      ;; look into expressions
      ())))

#_
(defn initial-lqp [catalog query]
  (let [query (preprocess-query catalog query)]
    ...))
