(ns adqc.lqp
  (:use [adqc
         [sql :only [parse-sql]]
         [util :only [defextractors extract-info]]]
        [adqc.sql
         [protocols :only [attributes]]
         [records :as asr :only []]]
        [clojure
         [zip :as zip :only []]
         [walk :as walk :only []]
         [set :as set :only []]
         [string :as str :only []]]
        [clojure.contrib
         [java-utils :as ju :only []]])
  (:import uk.org.ogsadai.dqp.execute.QueryPlanBuilder
           [uk.org.ogsadai.converters.databaseschema
            ColumnMetaData TableMetaData]
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
  `(do (defrecord ~name [~'head ~'body ~'children])
       (defmethod print-method ~name [op# ~' ^java.io.Writer w]
         (.write ~'w "#:")
         (.write ~'w ~(clojure.core/name name))
         (print-method (select-keys op# [:head :children]) ~'w))))

(defoperator NilOperator)
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
        attrs (map #(asr/attribute (:name %) table-name nil)
                   cols)]
    (with-meta
      {:attributes attrs
       :columns (vec cols)
       ;; could get resource IDs here instead:
       :data-nodes (into [] (map #(.getDataNode ^DataNodeTable %)
                                 (.getDataNodeTables schema)))}
      {:type ::table-schema})))

#_
(defmethod print-method ::table-schema [table-schema ^java.io.Writer w]
  (.write w "#:table-schema")
  (print-method (vec (map :name (:columns table-schema))) w))

;;; print-method overrides are needed to avoid huge XML printouts
;;; for a sane REPLing experience

(defmethod print-method ColumnMetaData [cmd ^java.io.Writer w]
  (doto w
    (.write "#<ColumnMetaData ")
    (.write (.getName cmd))
    (.write ">")))

(defmethod print-method TableMetaData [tmd ^java.io.Writer w]
  (doto w
    (.write "#<TableMetaData ")
    (.write (.getName tmd))
    (.write ">")))

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
  (split-conjunction [this] #{this})
  nil
  (split-conjunction [_] nil))

(defn attribute? [expr]
  (identical? adqc.sql.records.Attribute
              (class expr)))

(defn clear-attribute-source
  "Returns a clone of attr with :src set to nil."
  [attr]
  (assoc attr :src nil))

;;; TODO: replace with sth better?
;;; see PredicateTypeExprVisitor
(defn attribute-equality-predicate? [pred-expr]
  (and (= "=" (-> pred-expr :pred :pred-name))
       (every? attribute? ((juxt :lhs :rhs) pred-expr))))

;;; TODO: is it worthwhile to break partitions requiring full cross products?
(defn partition-scans
  "Partitions the input collection of table-schemas into disjoint colls
  of schemas of tables residing on the same data nodes."
  [scan-ops]
  (->> scan-ops
       ;; TODO: this isn't very refined:
       (group-by (comp first :data-nodes :body))
       vals))

;;; NB: attribute comparison includes checking sources
;;; TODO: check if explicitly renamed sources need to worry about
;;; TODO: clashes with non-qualified attrs in predicates
(defn predicate-sources
  "Returns a subset of ops containing just the operators whose headings
  include attributes mentioned in the pred."
  [pred ops]
  (let [pred-attrs (attributes pred)]
    (->> ops
         (filter #(->> %
                       ((juxt :head
                              (comp (partial map clear-attribute-source)
                                    :head)))
                       (apply concat)
                       (some pred-attrs)))
         (into #{}))))

;;; TODO: in progress (OP? = optimisation phase?)
;;; * DONE: prepare a TableScanOperator on each table involved
;;; * prefer FilteredTableScanOperator where possible (OP?)
;;; * DONE: partition scan ops into groups targetting the same data node
;;; * DONE (?): fish out join predicates from WHERE
;;; * handle explicit JOINs in FROM (by building sub-LQPs)
;;; * for each partition, buid a graph of TSOs connected with equijoin preds
;;; * split each such graph into connected components
;;; * construct a join tree for each component...
;;; * ...then a product tree for roots of the join trees
;;; * randomise placement of ExchangeOperators
(defn initial-lqp [statement data-dictionary]
  (let [{:keys [select-list from-list where]} (:query statement)
        table-schemas (map (comp (partial get-table-schema data-dictionary) :id)
                           (:sources from-list))
        scan-ops (map (fn make-scan-op [table-schema]
                        ;; TODO: decide what to put in :body
                        ;; for TSOs **and modify partition-scans** if needed
                        (TableScanOperator. (:attributes table-schema)
                                            table-schema ; put the query in here?
                                            nil))
                      table-schemas)
        scan-groups (partition-scans scan-ops)
        renames (into {} (map (juxt (comp :id :attr) :as)
                              (filter :as from-list)))
        preds (split-conjunction where)
        {equipreds true
         nonequipreds false} (group-by attribute-equality-predicate? preds)]
    scan-groups))

#_
(defn validate-lqp
  "Checks that the HEADs of child operators match the expectations
  of parent operators."
  [lqp])
