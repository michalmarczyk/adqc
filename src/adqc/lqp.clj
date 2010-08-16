(ns adqc.lqp
  (:use [adqc
         [sql :only [parse-sql]]
         [util :only [defextractors extract-info]]]
        [adqc.lqp operators]
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
           uk.org.ogsadai.tuple.TupleTypes
           [adqc.sql.records Attribute InfixPredicateExpression ColumnStar]))

;;; TODO: Move all functions which turned out to be necessary for LQP
;;; TODO: building into the namespaces where they properly belong.

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
        attrs (map #(asr/make-attribute (:name %) table-name nil)
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
  (instance? Attribute expr))

(defn clear-attribute-source
  "Returns a clone of attr with :src set to nil."
  [attr]
  (assoc attr :src nil))

(defn binary-predicate-expression? [pred-expr]
  (some #(instance? % pred-expr)
        [adqc.sql.records.InfixPredicateExpression
         adqc.sql.records.AndExpression
         adqc.sql.records.OrExpression]))

(defn infix-predicate-expression? [pred-expr]
  (instance? InfixPredicateExpression
             pred-expr))

;;; TODO: replace with sth better?
;;; see PredicateTypeExprVisitor
(defn attribute-equality-predicate? [pred-expr]
  (and (infix-predicate-expression? pred-expr)
       (= "=" (-> pred-expr :pred :pred-name))
       (every? attribute? ((juxt :lhs :rhs) pred-expr))))

(defn attribute-eqv?
  "Test attribute equivalence disregarding attribute sources."
  [attr1 attr2]
  (= (clear-attribute-source attr1)
     (clear-attribute-source attr2)))

;;; TODO: is it worthwhile to break partitions requiring full cross products?
(defn partition-scans
  "Partitions the input collection of table-schemas into disjoint colls
  of schemas of tables residing on the same data nodes."
  [scan-ops]
  (->> scan-ops
       ;; TODO: this isn't very refined:
       (group-by (comp first :data-nodes :body))
       vals)) ; TODO: put this into a set for easier (in? pred part) ?

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

(defn column-star? [col-expr]
  (instance? ColumnStar (:attr col-expr)))

(defn column-star-select-list? [select-list]
  (let [cols (:cols select-list)]
    (and (== (count cols) 1)
         (column-star? (first cols)))))

(defn join-group [equipreds ops]
  (reduce (fn [left right]
            (if-let [join-pred
                     (some (fn [equipred]
                             (let [lhs (:lhs equipred)
                                   rhs (:rhs equipred)]
                               (or (and (some #{lhs} (:head left))
                                        (some #{rhs} (:head right)))
                                   (and (some #{rhs} (:head left))
                                        (some #{lhs} (:head right))))))
                           equipreds)]
              (make-inner-theta-join-operator
               (concat (:head left)
                       (:head right))
               join-pred
               [left right])
              (make-product-operator
               (concat (:head left)
                       (:head right))
               nil
               [left right])))
          ops))

(defn add-exchange [op]
  (make-exchange-operator (:head op) nil [op]))

(defn add-exchanges [ops]
  (if (< 2 (count ops))
    ops
    (vec (concat [(first ops)]
                 (map add-exchange (rest ops))))))

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
  (let [{:keys [select-list from-list where-clause]} (:query statement)
        table-schemas (map (comp (partial get-table-schema data-dictionary) :id)
                           (:sources from-list))
        scan-ops (map (fn make-scan-op [table-schema]
                        ;; TODO: decide what to put in :body
                        ;; for TSOs **and modify partition-scans** if needed
                        (make-table-scan-operator
                         (:attributes table-schema)
                         table-schema   ; put the query in here?
                         nil))
                      table-schemas)
        scan-groups (partition-scans scan-ops)
        renames (into {} (map (juxt (comp :id :attr) :as)
                              (filter :as from-list)))
        preds (split-conjunction (:pred where-clause))
        {equipreds true
         nonequipreds false} (group-by attribute-equality-predicate? preds)
        #_equipreds-sources #_(into {} (map (fn [ep]
                                              (predicate-sources ep scan-ops))
                                            equipreds))
        partition-joins (add-exchanges
                         (map (partial join-group equipreds) scan-groups))
        joins (join-group equipreds partition-joins)]
    (make-root-operator (:head joins) nil joins)))

#_
(defn validate-lqp
  "Checks that the HEADs of child operators match the expectations
  of parent operators."
  [lqp])
