(ns adqc.sql
  (:use [adqc.util :only [defextractors extract-info]]
        [clojure
         [zip :as zip :only []]
         [walk :as walk :only []]
         [set :as set :only []]]
        [clojure.contrib
         [string :as str :only []]
         [def :only [defalias]]])
  (:import uk.org.ogsadai.parser.sql92query.SQLQueryParser
           org.antlr.runtime.tree.CommonTree
           org.antlr.runtime.CommonToken))

(defmacro defantlrparsers [& m&fs]
  `(let ~'[sql-query-parser (SQLQueryParser/getInstance)]
     ~@(for [[method-suffix fn-suffix]
             (into {} (map vec (partition 2 m&fs)))]
         `(defn ~(symbol (str "antlr-parse-" fn-suffix))
            ~(str "Parses the given SQL string with the "
                  "parseSQL" method-suffix
                  " method of the ANTLR-based praser.")
            [~'sql]
            (. ~'sql-query-parser
               ~(symbol (str "parseSQL" method-suffix))
               ~'sql)))))

(defantlrparsers
  ""                       sql
  ForCondition             condition
  ForDerivedColumn         derived-column
  ForLiteral               literal
  ForValueExpressionOrStar value-expression-or-star)

(defalias antlr-parse-expression antlr-parse-value-expression-or-star)

(defn antlr-ast-zip
  [root]
  (zip/zipper (constantly true)
              (fn children [node]
                (let [cnt (.getChildCount ^CommonTree node)]
                  (map #(.getChild ^CommonTree node %) (range cnt))))
              ;; this is wrong, make-node should return
              ;; a *new* branch node
              #_
              (fn make-node [node ^java.util.Collection children]
                (.addChildren ^CommonTree node
                              (java.util.ArrayList. children)))
              (fn make-node [node children]
                (throw
                 (Exception.
                  "adding nodes to ANTLR AST zipper not supported")))
              root))

(defprotocol ANTLR->Clojure
  (antlr->clojure [self]))

(defextractors token-info-extractors CommonToken
  :ttype :type
  :text  :text)

(defn node-children [^CommonTree node]
  (let [cnt (.getChildCount node)]
    (loop [i 0 children []]
      (if (< i cnt)
        (recur (inc i)
               (conj children
                     (-> (.getChild node i)
                         antlr->clojure)))
        children))))

(defextractors node-info-extractors CommonTree
  :ntype    :type
  :nkey     (->> :text (str/replace-re #"_" "-") (str/lower-case) keyword)
  :token    (-> :token antlr->clojure)
  :text     :text
  :children node-children)

(extend-protocol ANTLR->Clojure
  CommonTree
  (antlr->clojure [self] (extract-info node-info-extractors self))
  CommonToken
  (antlr->clojure [self] (extract-info token-info-extractors self)))

(defprotocol ToSQL
  (to-sql [self]))

(extend-protocol ToSQL
  String
  ;; no escaping done by me -- I'm dealing with SQL queries which should
  ;; have well-prepared strings!
  (to-sql [self] self)
  Number
  (to-sql [self] (str self)))

(defprotocol SQLExpression
  (attributes [self])
  (rename-attributes [self m]))

(extend-protocol SQLExpression
  String
  (attributes [_] #{})
  (rename-attributes [self _] self)
  Number
  (attributes [_] #{})
  (rename-attributes [self _] self))

(defrecord InfixOperator [n]
  ToSQL
  (to-sql [self] n))

(defrecord InfixOperatorExpression [op lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs op rhs]))))
  SQLExpression
  (attributes
   [self]
   (set/union (attributes lhs)
              (attributes rhs)))
  (rename-attributes
   [self m]
   (InfixOperatorExpression.
    op
    (rename-attributes lhs m)
    (rename-attributes rhs m))))

(defrecord FunctionApplicationExpression [fn args]
  ToSQL
  (to-sql
   [self]
   (str fn "(" (apply str (interpose ", " (map to-sql args))) ")"))
  SQLExpression
  (attributes [self] (apply set/union (map attributes args)))
  (rename-attributes
   [self m]
   (vec (map (partial rename-attributes m) args))))

(defrecord Attribute [id src t]
  ToSQL
  (to-sql
   [self]
   (if src (str src "." id) id))
  SQLExpression
  (attributes [self] #{self})
  (rename-attributes
   [self m]
   (if-let [new-id (m id)]
     (Attribute. new-id src t)
     self)))

(defrecord ColumnStar []
  ToSQL
  (to-sql [self] "*")
  SQLExpression
  (attributes [self] #{})
  (rename-attributes [self _] self))

(defrecord InfixPredicate [n]
  ToSQL
  (to-sql [self] n))

(defrecord InfixPredicateExpression [pred lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map to-sql [lhs pred rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (InfixPredicateExpression.
    pred
    (rename-attributes lhs m)
    (rename-attributes rhs m))))

(defmulti parenthesise type)

(defmethod parenthesise ::compound-predicate
  [expr] (str "(" (to-sql expr) ")"))
(defmethod parenthesise :default
  [expr] (to-sql expr))

(defrecord PredicateConjunctionExpression [lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map parenthesise [lhs "AND" rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (PredicateConjunctionExpression. (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

(derive PredicateConjunctionExpression ::compound-predicate)

(defrecord PredicateDisjunctionExpression [lhs rhs]
  ToSQL
  (to-sql [self] (apply str (interpose " " (map parenthesise [lhs "OR" rhs]))))
  SQLExpression
  (attributes [self] (set/union (attributes lhs) (attributes rhs)))
  (rename-attributes
   [self m]
   (PredicateDisjunctionExpression. (rename-attributes lhs m)
                                    (rename-attributes rhs m))))

(derive PredicateDisjunctionExpression ::compound-predicate)

(defrecord PredicateNegationExpression [arg]
  ToSQL
  (to-sql [self] (str "NOT " (parenthesise arg)))
  SQLExpression
  (attributes [self] (attributes arg))
  (rename-attributes
   [self m]
   (PredicateNegationExpression. (rename-attributes arg m))))

(derive PredicateNegationExpression ::compound-predicate)

(defrecord IsNullExpression [col]
  ToSQL
  (to-sql [self] (str (to-sql col) " IS NULL"))
  SQLExpression
  (attributes [self] (attributes col))
  (rename-attributes
   [self m]
   (IsNullExpression. (rename-attributes col m))))

;;; this strikes me as less then satisfactory
(def node-type-numbers
     {15 ::antlr-function
      16 ::antlr-not
      18 ::antlr-tablecolumn
      23 ::antlr-is-null
      30 ::antlr-id
      31 ::antlr-int
      34 ::antlr-string
      49 ::antlr-star
      65 ::antlr-infix-plus
      66 ::antlr-infix-minus
      67 ::antlr-infix-div
      79 ::antlr-or
      80 ::antlr-and
      84 ::antlr-between
      86 ::antlr-=
      87 ::antlr-<>
      88 ::antlr-!=
      89 ::antlr-<
      90 ::antlr->
      91 ::antlr->=
      92 ::antlr-<=
      95 ::antlr-like})

;;; do I want to haul some context around
;;; (to be queried for the source of an attribute etc.)?
;;; NB. a context argument could be used to disambiguate stars
(defmulti transform-node (comp node-type-numbers :ntype))

(defmacro deftn [dispatch-val children-pat & body]
  `(defmethod transform-node ~dispatch-val
     [{~children-pat :children}]
     ~@body))

(defmethod transform-node nil [token] token)

(deftn ::antlr-function [f & args]
  (FunctionApplicationExpression. f args))

(deftn ::antlr-tablecolumn children
  (if (next children)
    (Attribute. (second children) (first children) nil)
    (Attribute. (first children) nil nil)))

(defmacro deftion [dispatch-val op]
  `(deftn ~dispatch-val [lhs# rhs#]
     (InfixOperatorExpression. (InfixOperator. ~(str op)) lhs# rhs#)))

(deftion ::antlr-infix-plus +)
(deftion ::antlr-infix-minus -)
(deftion ::antlr-infix-div /)

(defmacro deftipn [dispatch-val pred]
  `(deftn ~dispatch-val [lhs# rhs#]
     (InfixPredicateExpression. (InfixPredicate. ~(str pred)) lhs# rhs#)))

(deftipn ::antlr-= =)
(deftipn ::antlr-< <)
(deftipn ::antlr-> >)
(deftipn ::antlr-<= <=)
(deftipn ::antlr->= >=)
(deftipn ::antlr-!= !=)
(deftipn ::antlr-<> <>)
(deftipn ::antlr-like LIKE)

(deftn ::antlr-is-null [col] (IsNullExpression. col))

(deftn ::antlr-and [lhs rhs] (PredicateConjunctionExpression. lhs rhs))
(deftn ::antlr-or  [lhs rhs] (PredicateDisjunctionExpression. lhs rhs))
(deftn ::antlr-not [arg]     (PredicateNegationExpression.    arg))

;;; do I want to have a separate Between record instead?
(deftn ::antlr-between [attr low high]
  (let [leq (InfixPredicate. "<=")]
    (PredicateConjunctionExpression.
     (InfixPredicateExpression. leq low attr)
     (InfixPredicateExpression. leq attr high))))

(defmethod transform-node ::antlr-id [{text :text}] text) ; is this ok?
(defmethod transform-node ::antlr-int [{text :text}] (BigInteger. text))
(defmethod transform-node ::antlr-string [{text :text}] text)

(deftn ::antlr-star children
  (if-let [[x y] children]
    (InfixOperatorExpression. (InfixOperator. "*") x y)
    (ColumnStar.)))

;;; Clojure maps and vectors will only occur here where antlr->clojure
;;; constructs them, so I'm free to use map? and vector? to determine
;;; whether I'm changing something in postwalk.
(defn transform-ast [ast]
  (walk/postwalk
   (fn [item]
     (cond (map? item) (transform-node item)
           ;; vec necessary (because clojure.lang.MapEntry satisfies vector?)
           ;; -- I wonder if there is potential for breakage that I can't see
           (vector? item) (vec (map transform-node item))
           :else item))
   ast))

(defmacro defparsefns [& s&ts]
  `(do ~@(for [[suffix text-type] (into {} (map vec (partition 2 s&ts)))
               :let [fn-name (str "parse-" suffix)]]
           `(defn ~(symbol fn-name)
              ~(str "Parses the given " text-type " with OGSA-DAI's ANTLR-based\n"
                    "parser and converts the result to a Clojure tree.")
              [~'sql]
              (-> ~'sql
                  ~(symbol (str "antlr-" fn-name))
                  antlr->clojure
                  transform-ast)))))

(defparsefns
  sql            "SQL"
  expression     "SQL expression"
  condition      "SQL predicate"
  literal        "SQL literal"
  derived-column "SQL derived column")
