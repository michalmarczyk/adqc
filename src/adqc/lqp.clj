(ns adqc.lqp
  (:use [adqc
         [sql :only [parse-sql]]
         [util :only [defextractors extract-info]]]
        [clojure
         [zip :as zip :only []]
         [walk :as walk :only []]
         [set :as set :only []]
         [string :as str :only []]]
        [clojure.contrib
         [java-utils :as ju :only []]])
  (:import uk.org.ogsadai.dqp.execute.QueryPlanBuilder
           uk.org.ogsadai.converters.databaseschema.ColumnMetaData
           uk.org.ogsadai.dqp.common.DataNodeTable
           uk.org.ogsadai.tuple.TupleTypes))

(declare initial-lqp validate-lqp)

;;; actually a :gen-class might be more convenient eventually,
;;; but this will be better for the REPLing experience
(deftype ADQCQueryPlanBuilder []
  uk.org.ogsadai.dqp.execute.QueryPlanBuilder
  (buildQueryPlan
   [_ sql dqp-resource-accessor request-details]
   (let [query (parse-sql sql)
         data-dictionary (.. dqp-resource-accessor
                             getFederation
                             getDataDictionary)]
     (initial-lqp query data-dictionary))))

(defmacro defoperator [name]
  `(defrecord ~name [~'head ~'body ~'children]))

(defoperator TableScanOperator)
(defoperator ProjectOperator)
(defoperator SelectOperator)
(defoperator ProductOperator)
(defoperator InnerThetaJoinOperator)
(defoperator ExchangeOperator)

(def tuple-types
     (into {}
           (map-indexed (fn [i n]
                          [(keyword (str/lower-case n)) i])
                        (ju/wall-hack-field TupleTypes "names" nil))))

(def tuple-types-reverse
     (zipmap (vals tuple-types) (keys tuple-types)))

(def data-types
     (into {}
           (map (fn [f] [(-> (.getName ^java.lang.reflect.Field f)
                             str/lower-case
                             keyword)
                         (.get ^java.lang.reflect.Field f nil)])
                (filter #(java.lang.reflect.Modifier/isStatic
                          (.getModifiers ^java.lang.reflect.Field %))
                        (.getFields java.sql.Types)))))

(def data-types-reverse
     (zipmap (vals data-types) (keys data-types)))

;;; TODO: improve defextractors so this can be made prettier
(defextractors column-info-extractors ColumnMetaData
  :name            :name
  :tuple-type      (-> :tuple-type tuple-types-reverse)
  :data-type       (-> :data-type data-types-reverse)
  :column-size     :column-size
  :position        :position
  :primary-key?    (-> .isPrimaryKey)
  :nullable?       (-> .isNullable)
  :default-value   :default-value
  :table-meta-data :table)

;;; TODO: return all relevant data
(defn get-table-schema [data-dictionary table-name]
  (let [schema (-> data-dictionary
                   (.getTableSchema table-name))
        table-meta (.getSchema schema)
        cols (map #(extract-info column-info-extractors
                                 (.getColumn table-meta %))
                  (->> table-meta .getColumnCount inc (range 1)))
        ]
    {:columns (vec cols)
     ;; could get resource IDs here instead:
     :data-nodes (into [] (map #(.getDataNode ^DataNodeTable %)
                               (.getDataNodeTables schema)))}))

;;; TODO: put this where it logically belongs
(defprotocol PSplitConjunction
  (split-conjunction [this]))

(extend-type adqc.sql.records.AndExpression
  PSplitConjunction
  (split-conjunction [this]
    (let [{:keys [lhs rhs]} this]
      (reduce into #{} (map split-conjunction [lhs rhs])))))

(extend-protocol PSplitConjunction
  Object
  (split-conjunction [this] #{this}))

;;; TODO: replace with sth better?
;;; actually this should be equijoin-predicate? and check whether
;;; it's a simple attr1 = attr2 test with attr1 & 2 coming from
;;; different sources
(defn equijoin-predicate? [pred]
  (= "=" (:pred-name pred)))

;;; TODO: is it worthwhile to break partitions requiring full cross products?
(defn partition-scans
  "Partitions the input collection of table-schemas into disjoint colls
  of schemas of tables residing on the same data nodes."
  [table-schemas]
  (->> table-schemas
       ;; TODO: this isn't very refined:
       (group-by (comp first :data-nodes))
       vals))

;;; TODO: in progress
;;; * extract join predicates from from-list
(defn initial-lqp [query data-dictionary]
  (let [{:keys [select-list from-list where]} query
        table-schemas (map (comp get-table-schema :id)
                           from-list)
        scan-ops (map (fn make-scan-op [table-schema]
                        ;; "select * from foo" ?
                        (TableScanOperator. table-schema nil nil))
                      table-schemas)
        scan-groups (partition-scans scan-ops)
        renames (into {} (map (juxt (comp :id :attr) :as)
                              (filter :as from-list)))
        preds (split-conjunction where)
        equipreds (filter equijoin-predicate? preds)]
    ()))

#_
(defn validate-lqp
  "Checks that the HEADs of child operators match the expectations
  of parent operators."
  [lqp])
